//! `Client::connect` + `open_table` (delta-rs style entry points).

use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{
    cluster_id_is_unset, connect_control, validate_staging, ClientConfig, Staging, StagingConfig,
    StagingService, StreamMode,
};
use crate::error::{Error, Result};
use crate::lock::{new_instance_uuid, LockFile};
use crate::naming::{decode_serial_seq, validate_cluster_id, validate_writer_id, WriterIdentity};
use crate::notify::{Notifier, NotifyRequest};
use crate::recovery::{next_epoch, plan_recovery, RecoveryPlan};
use crate::schema::{fetch_table_schema, split_qualified};
use crate::staging::{StagingHandle, StagingStore};
use crate::state::{StateFile, STATE_VERSION};
use crate::table::TableWriter;

pub struct Client {
    cfg: ClientConfig,
    /// The staging location actually in use: either the customer's own config
    /// or what the server handed back. Everything downstream reads this, not
    /// `cfg.staging`, which under the default Relyt-managed mode names no
    /// bucket at all.
    staging: Arc<StagingHandle>,
    control: tokio_postgres::Client,
    notifier: Arc<Notifier>,
    /// The `<cluster>` staging-path segment, resolved once at connect (see
    /// [`crate::config::cluster_id_is_unset`] for the precedence rules).
    /// Resolved Relyt instance id (see `WriterIdentity::cluster_id`).
    cluster_id: String,
}

impl Client {
    /// Establish staging access and the Relyt control connection, resolve the
    /// cluster identity, and start the background notify task.
    ///
    /// Under the default [`crate::Staging::Relyt`] the staging location and its
    /// credentials are fetched here, so the control connection must be up
    /// before staging access exists -- the reverse of the customer-owned
    /// order, where both are known from the config.
    pub async fn connect(cfg: ClientConfig) -> Result<Client> {
        cfg.validate()?;
        let (control, connection) = connect_control(&cfg.control_dsn).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "client control connection closed");
            }
        });

        let staging_cfg = resolve_staging(&cfg, &control).await?;
        // Under Relyt-managed staging the credentials can be re-read from the
        // master later (rotation); a customer-owned bucket has nothing to
        // re-read.
        let managed_dsn = match &cfg.staging {
            Staging::Relyt => Some(cfg.control_dsn.clone()),
            Staging::Customer(_) => None,
        };
        let staging = Arc::new(StagingHandle::new(staging_cfg, managed_dsn)?);
        let cluster_id = resolve_cluster_id(&cfg, &control).await?;
        {
            let live = staging.current();
            tracing::info!(
                staging = staging_kind(&cfg.staging),
                endpoint = %live.cfg.endpoint,
                bucket = %live.cfg.bucket,
                prefix = %live.cfg.prefix,
                service = ?live.cfg.service,
                cluster_id = %cluster_id,
                "staging resolved"
            );
        }

        let notifier = Arc::new(Notifier::spawn(cfg.control_dsn.clone(), staging.clone()));
        if staging.managed_dsn().is_some() {
            spawn_staging_refresh(&staging, cfg.staging_refresh_interval);
        }
        Ok(Client {
            cfg,
            staging,
            control,
            notifier,
            cluster_id,
        })
    }

    /// Recovery handshake + writer construction. Returns the
    /// writer and the recovery plan (the caller seeks Kafka to
    /// `plan.kafka_resume_offset` before producing new appends).
    pub async fn open_table(
        &self,
        table: &str,
        writer_id: &str,
    ) -> Result<(TableWriter, RecoveryPlan)> {
        validate_writer_id(writer_id)?;

        // 1. Schema + OIDs. The type whitelist is checked inside
        //    fetch_table_schema; the PK requirement is per-mode, because only
        //    the upsert path needs an ON CONFLICT target.
        let schema = fetch_table_schema(&self.control, table).await?;
        if self.cfg.stream_mode == StreamMode::Upsert && schema.pk.is_empty() {
            return Err(Error::Schema(
                "upsert mode requires the target table to have a primary key \
                 (use StreamMode::InsertOnly for tables without one)"
                    .into(),
            ));
        }

        let ident = WriterIdentity {
            cluster_id: self.cluster_id.clone(),
            db_oid: schema.db_oid,
            rel_oid: schema.rel_oid,
            writer_id: writer_id.to_string(),
        };

        // 2. Writer lease: at most one live process per (table, writer_id).
        //    Taken before state.json is read so no concurrent holder can be
        //    mid-write on it; released on any later failure in this function
        //    (best-effort — an unreleased lease self-expires after
        //    lock_lease_timeout).
        let instance_uuid = new_instance_uuid();
        acquire_writer_lease(&self.store(), &ident, &instance_uuid, &self.cfg).await?;
        let res = self
            .open_table_locked(table, schema, &ident, &instance_uuid)
            .await;
        if res.is_err() {
            if let Ok(Some(l)) = self.store().read_lock(&ident).await {
                if l.instance_uuid == instance_uuid {
                    let _ = self.store().delete_lock(&ident).await;
                }
            }
        }
        res
    }

    async fn open_table_locked(
        &self,
        table: &str,
        schema: crate::schema::TableSchema,
        ident: &WriterIdentity,
        instance_uuid: &str,
    ) -> Result<(TableWriter, RecoveryPlan)> {
        let ident = ident.clone();
        let group = ident.serial_group();

        // Lease held (see `open_table`): a stale contract-violation record
        // from a previous writer of this stream must not poison this one.
        if self.notifier.clear_fatal(&group) {
            tracing::info!(
                serial_group = %group,
                "cleared a previous writer's serial-contract fatal on reopen"
            );
        }

        // 3. Persistent state (fail-loud read) + cross-checks. db_oid/rel_oid
        //    are small per-cluster integers, so two clusters mistakenly
        //    sharing one prefix WILL collide; the recorded names are the one
        //    executable guard for that deployment rule. A rename (same OIDs,
        //    different name) is legal and only logged.
        let state = self.store().read_state(&ident).await?;
        let (schema_name, table_name) = split_qualified(table)?;
        let database: String = self
            .control
            .query_one("SELECT current_database()", &[])
            .await?
            .get(0);
        if let Some(st) = &state {
            if st.cluster_id != ident.cluster_id
                || st.writer_id != ident.writer_id
                || st.database != database
            {
                return Err(Error::Config(format!(
                    "writer state at `{}` was written by cluster={} db={} writer={} but this \
                     session resolves to cluster={} db={} writer={} — two clusters/databases \
                     are sharing one staging prefix; fix the prefix (or delete the state \
                     object if this is intentional)",
                    ident.state_key(),
                    st.cluster_id,
                    st.database,
                    st.writer_id,
                    ident.cluster_id,
                    database,
                    ident.writer_id,
                )));
            }
            if st.schema != schema_name || st.table != table_name {
                tracing::info!(
                    old = %format!("{}.{}", st.schema, st.table),
                    new = %format!("{schema_name}.{table_name}"),
                    "table was renamed since the last session (same OIDs); staging identity \
                     is OID-based so nothing is orphaned"
                );
            }
        }

        // 3. The group's consumption watermark. Insert-only streams carry
        //    serial fields too (M=1), so the server answers for both modes.
        //    A missing UDF (older server) degrades to full re-notify:
        //    identifier idempotency + the submission gate make replays
        //    harmless.
        let watermark: Option<i64> = match self
            .control
            .query_one(
                "SELECT pg_catalog.relyt_get_serial_group_watermark($1)",
                &[&group],
            )
            .await
        {
            Ok(row) => row.get(0),
            Err(e) if is_undefined_function(&e) => {
                tracing::warn!(
                    "relyt_get_serial_group_watermark missing on server; \
                     degrading to full re-notify backfill"
                );
                None
            }
            Err(e) => return Err(e.into()),
        };

        // 4. LIST the writer's staging directory.
        let (staged, unknown) = self.store().list_staged(&ident).await?;
        for name in &unknown {
            tracing::warn!(file = %name, "unrecognized file under staging prefix (ignored)");
        }

        let mut plan = plan_recovery(staged, watermark)?;
        // The Kafka resume position is the max of the LIST-derived value and
        // the recorded server-confirmed offset: staging can lag the state
        // after objects were GC-ed, the state can lag staging after a crash.
        if let Some(st) = &state {
            if let Some(rec) = st.resume_offset {
                plan.kafka_resume_offset = Some(match plan.kafka_resume_offset {
                    Some(v) => v.max(rec + 1),
                    None => rec + 1,
                });
            }
        }

        // 5. Pick the session epoch: wall clock, bumped past every anchor —
        //    staged objects, the server watermark's epoch, and the recorded
        //    max_epoch_used (which survives both of the former being gone).
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_millis() as u64;
        let mut anchor = plan.max_staged;
        if let Some(w) = watermark {
            let (wm_epoch, wm_seq) = decode_serial_seq(w);
            if anchor.map_or(true, |(e, s)| (wm_epoch, wm_seq) > (e, s)) {
                anchor = Some((wm_epoch, wm_seq));
            }
        }
        if let Some(st) = &state {
            if anchor.map_or(true, |(e, _)| st.max_epoch_used > e) {
                anchor = Some((st.max_epoch_used, 0));
            }
        }
        let epoch = next_epoch(now_ms, anchor);
        // Pre-check the encoding NOW: a clock far in the future must fail
        // here, not as an orphan object after a successful put.
        crate::naming::encode_serial_seq(epoch, 0)?;

        // 6. Persist the state BEFORE the first object can be written: the
        //    epoch anchor must be durable before anything uses the epoch.
        let new_state = StateFile {
            v: STATE_VERSION,
            resume_offset: state.as_ref().and_then(|s| s.resume_offset),
            max_epoch_used: epoch,
            cluster_id: ident.cluster_id.clone(),
            database,
            schema: schema_name,
            table: table_name,
            writer_id: ident.writer_id.clone(),
            updated_at_ms: now_ms,
        };
        self.store().write_state(&ident, &new_state).await?;

        // 7. Re-notify the backfill through the writer's queue (fast path);
        //    the gate and identifier idempotency swallow anything already
        //    known. Serial fields are unconditional: insert-only runs with
        //    M=1 serialization too, which is what gives it a watermark.
        let url_base = self.staging.current().url_base.clone();
        for f in &plan.backfill {
            self.notifier.enqueue(NotifyRequest {
                serial_group: group.clone(),
                end_offset: f.end_offset,
                identifier: f.identifier(&ident)?,
                source_url: format!("{}/{}", url_base, f.object_key(&ident)),
                target: ident.rel_oid,
                delimiter: self.cfg.csv.delimiter,
                upsert: self.cfg.stream_mode == StreamMode::Upsert,
                serial_seq: f.serial_seq()?,
                retry_max: self.cfg.retry_max.or(Some(-1)),
            })?;
        }

        let staged_offset = plan.kafka_resume_offset.map(|o| o - 1);
        let names = (
            new_state.database.clone(),
            new_state.schema.clone(),
            new_state.table.clone(),
        );
        let writer = TableWriter::new(
            schema,
            ident,
            names,
            self.cfg.clone(),
            self.staging.clone(),
            self.notifier.clone(),
            epoch,
            staged_offset,
            // Seed the in-process "last persisted resume_offset" with what
            // state.json already records, so an idle writer's periodic state
            // heartbeat re-writes that value instead of None.
            new_state.resume_offset,
            // What recovery found staged above the watermark: the lag sampler
            // starts from this list and appends every later rotation.
            plan.backfill
                .iter()
                .filter_map(|f| f.serial_seq().ok().map(|s| (s, f.epoch_ms)))
                .collect(),
            instance_uuid.to_string(),
        );
        // The one-line restart audit trail: everything recovery decided.
        tracing::info!(
            table = %table,
            writer_id = %writer.identity().writer_id,
            serial_group = %group,
            resume_offset = ?plan.kafka_resume_offset,
            backfill_files = plan.backfill.len(),
            server_watermark = ?watermark,
            epoch,
            "open_table recovery complete"
        );
        Ok((writer, plan))
    }
}

/// Split `s3://<endpoint>/<bucket>[/<prefix>]` and fill in what the URL does
/// not carry.
///
/// The scheme is `s3` for both object stores -- it says "S3-style URL", not
/// which store -- so the store comes from the host: an Alibaba OSS endpoint
/// is `*.aliyuncs.com`, and everything else is S3 or S3-compatible. The
/// region follows the same host (`derive_aws_region`), which is why the
/// server needs to provision only this one string.
fn parse_staging_url(
    url: &str,
    access_key_id: String,
    secret_access_key: String,
) -> Result<StagingConfig> {
    let rest = url.strip_prefix("s3://").ok_or_else(|| {
        Error::Config(format!(
            "staging url from the server must start with `s3://`, got `{url}`"
        ))
    })?;
    let (endpoint, after_host) = rest.split_once('/').ok_or_else(|| {
        Error::Config(format!(
            "staging url from the server has no bucket: expected \
             s3://<endpoint>/<bucket>[/<prefix>], got `{url}`"
        ))
    })?;
    let after_host = after_host.trim_start_matches('/');
    let (bucket, prefix) = match after_host.split_once('/') {
        Some((b, p)) => (b, p),
        None => (after_host, ""),
    };
    if endpoint.is_empty() || bucket.is_empty() {
        return Err(Error::Config(format!(
            "staging url from the server has an empty endpoint or bucket: `{url}`"
        )));
    }
    let service = if endpoint
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(endpoint)
        .ends_with(".aliyuncs.com")
    {
        StagingService::Oss
    } else {
        StagingService::S3
    };
    let staging = StagingConfig {
        endpoint: endpoint.to_string(),
        bucket: bucket.to_string(),
        prefix: prefix.trim_matches('/').to_string(),
        access_key_id,
        secret_access_key,
        service,
        // The server hands out a location, never a region: a Relyt-managed
        // bucket is expected on OSS (needs none) or on AWS (the endpoint
        // carries it), and derive_aws_region recovers the latter.
        region: None,
    };
    // Anything else cannot be signed, and under Relyt-managed staging the
    // customer has no field to fix that with -- so say who can, rather than
    // pointing at StagingConfig.region as the customer-owned check does.
    if staging.service == StagingService::S3 && staging.resolve_region().is_none() {
        return Err(Error::Config(format!(
            "the staging url the Relyt master handed out (`{url}`) is on an S3-compatible \
             store whose region cannot be derived from its host; Relyt-managed staging \
             supports `*.aliyuncs.com` and `*.amazonaws.com(.cn)` endpoints. Ask the Relyt \
             administrator to provision one of those, or use your own bucket through \
             Staging::Customer(..) with an explicit region"
        )));
    }
    Ok(staging)
}

impl Client {
    /// The store of the current staging snapshot (cheap: the operator is
    /// reference-counted).
    fn store(&self) -> StagingStore {
        self.staging.current().store.clone()
    }

    /// The live staging handle, shared with every writer this client opened.
    /// The e2e suite installs a revoked key through it to drive the rotation
    /// paths, which no test can otherwise reach: a second valid key pair for
    /// the same bucket is not something a test can mint.
    #[doc(hidden)]
    pub fn staging_handle(&self) -> Arc<StagingHandle> {
        self.staging.clone()
    }

    /// Run one credential refresh now, instead of waiting for the periodic
    /// task. Used by the e2e suite to assert what a refresh does when the
    /// master's location no longer matches this process's.
    #[doc(hidden)]
    pub async fn refresh_staging(&self) -> Result<bool> {
        refresh_managed_staging(&self.staging, "explicit").await
    }
}

/// Re-read the staging credentials from the Relyt master and install them if
/// they changed. A no-op (`Ok(false)`) for a customer-owned bucket. Called by
/// the periodic refresh task and by a writer whose upload was denied.
pub(crate) async fn refresh_managed_staging(handle: &StagingHandle, reason: &str) -> Result<bool> {
    let Some(dsn) = handle.managed_dsn() else {
        return Ok(false);
    };
    let (control, conn) = connect_control(dsn).await?;
    tokio::spawn(conn);
    let fresh = fetch_managed_staging(&control).await?;
    let changed = handle.apply(fresh)?;
    if changed {
        tracing::info!(
            reason,
            "staging credentials refreshed from the Relyt master"
        );
    }
    Ok(changed)
}

/// Process-level task: re-read managed staging credentials every
/// `staging_refresh_interval`, so a rotation on the Relyt side reaches every
/// writer without a restart. Holds a Weak so it ends with the last handle.
fn spawn_staging_refresh(handle: &Arc<StagingHandle>, every: Duration) {
    let weak = Arc::downgrade(handle);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(every);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await; // immediate first tick: skip
        loop {
            interval.tick().await;
            let Some(handle) = weak.upgrade() else {
                return;
            };
            if let Err(e) = refresh_managed_staging(&handle, "periodic").await {
                tracing::warn!(error = %e,
                    "staging credential refresh failed; keeping the current credentials");
            }
        }
    });
}

/// Log-friendly name of the staging variant (the config itself is logged
/// field by field right after, credentials masked).
fn staging_kind(s: &Staging) -> &'static str {
    match s {
        Staging::Relyt => "Relyt",
        Staging::Customer(_) => "Customer",
    }
}

/// Resolve the staging location: the customer's own config when they supplied
/// one, otherwise the server's `relyt_get_ingest_staging_config()`.
///
/// The variant alone decides; there is no owner flag to cross-check. Whatever
/// comes back goes through the same structural checks as a customer-supplied
/// config -- an operator can mis-provision a GUC as easily as a customer can
/// mis-write a config file.
async fn resolve_staging(
    cfg: &ClientConfig,
    control: &tokio_postgres::Client,
) -> Result<StagingConfig> {
    if let Staging::Customer(s) = &cfg.staging {
        return Ok(s.clone());
    }
    fetch_managed_staging(control).await
}

/// Ask the Relyt master for the staging location and credentials
/// (`relyt_get_ingest_staging_config()`), parsed and structurally checked.
async fn fetch_managed_staging(control: &tokio_postgres::Client) -> Result<StagingConfig> {
    let row = control
        .query_one(
            "SELECT url, access_key_id, secret_access_key \
             FROM pg_catalog.relyt_get_ingest_staging_config()",
            &[],
        )
        .await
        .map_err(|e| {
            if is_undefined_function(&e) {
                Error::Config(
                    "this server has no relyt_get_ingest_staging_config(): it predates \
                     Relyt-managed ingest staging. Upgrade the instance, or set \
                     Staging::Customer(..) with your own bucket."
                        .into(),
                )
            } else if is_insufficient_privilege(&e) {
                Error::Config(
                    "the ingest account may not execute relyt_get_ingest_staging_config(); \
                     ask the Relyt administrator to grant it, or use your own bucket."
                        .into(),
                )
            } else {
                // Includes the server's own "not provisioned on this instance"
                // error, which already names the missing setting and says what
                // to do -- pass it through rather than paraphrasing it.
                e.into()
            }
        })?;
    let staging = parse_staging_url(row.get(0), row.get(1), row.get(2))?;
    validate_staging(&staging)?;
    Ok(staging)
}

/// Resolve the `<cluster>` path segment: explicit config wins, then the
/// server's `relyt_get_instance_id()`; a cluster with no configured identity
/// (or a pre-UDF server) requires the explicit setting.
async fn resolve_cluster_id(
    cfg: &ClientConfig,
    control: &tokio_postgres::Client,
) -> Result<String> {
    if let Some(c) = &cfg.cluster_id {
        if cluster_id_is_unset(c) {
            return Err(Error::Config(
                "cfg.cluster_id is set to an 'unset' sentinel; provide a real identifier".into(),
            ));
        }
        validate_cluster_id(c)?;
        return Ok(c.clone());
    }
    let server: Option<String> = match control
        .query_one("SELECT pg_catalog.relyt_get_instance_id()", &[])
        .await
    {
        Ok(row) => row.get(0),
        Err(e) if is_undefined_function(&e) => None,
        Err(e) => return Err(e.into()),
    };
    match server {
        Some(v) if !cluster_id_is_unset(&v) => {
            // The server value becomes a path segment too; a misconfigured
            // relyt.instanceid must fail here, not scatter objects.
            validate_cluster_id(&v).map_err(|e| {
                Error::Config(format!(
                    "server-provided instance id (relyt_get_instance_id()) is unusable as a \
                     staging path segment: {e}; set ClientConfig::cluster_id explicitly"
                ))
            })?;
            Ok(v)
        }
        _ => Err(Error::Config(
            "cluster identity unavailable: the server has no relyt.instanceid configured \
             (or predates relyt_get_instance_id()); set ClientConfig::cluster_id explicitly — \
             db/rel OIDs are only unique within one cluster, so a shared bucket without \
             cluster scoping WILL collide"
                .into(),
        )),
    }
}

fn is_undefined_function(e: &tokio_postgres::Error) -> bool {
    e.as_db_error()
        .map(|db| db.code() == &tokio_postgres::error::SqlState::UNDEFINED_FUNCTION)
        .unwrap_or(false)
}

fn is_insufficient_privilege(e: &tokio_postgres::Error) -> bool {
    e.as_db_error()
        .map(|db| db.code() == &tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE)
        .unwrap_or(false)
}

/// Acquire the writer lease, or explain who holds it. Optimistic
/// write-then-read-back (see `lock.rs` for the protocol and its limits).
async fn acquire_writer_lease(
    staging: &crate::staging::StagingStore,
    ident: &WriterIdentity,
    instance_uuid: &str,
    cfg: &ClientConfig,
) -> Result<()> {
    let lease_ms = cfg.lock_lease_timeout.as_millis() as u64;
    // A cleanly dropped predecessor releases its lease within ~one heartbeat
    // tick (2s); poll briefly so an immediate reopen of the same writer does
    // not bounce off a release that is merely in flight. A true live holder
    // keeps its heartbeat fresh and we fail after the wait.
    const ACQUIRE_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
    let wait_started = std::time::Instant::now();
    loop {
        match staging.read_lock(ident).await? {
            Some(cur)
                if cur.instance_uuid != instance_uuid
                    && !cur.is_stale(lease_ms)
                    && cur.provably_dead_on_this_host() =>
            {
                // kill -9 / container-restart fast path: the holder ran on
                // this very machine and its pid is gone — no need to wait
                // out the lease.
                tracing::info!(
                    key = %ident.lock_key(),
                    holder = %cur.describe(),
                    "taking over lease from a provably dead same-host process"
                );
                break;
            }
            Some(cur) if cur.instance_uuid != instance_uuid && !cur.is_stale(lease_ms) => {
                if wait_started.elapsed() >= ACQUIRE_WAIT {
                    return Err(Error::WriterLocked(format!(
                        "{} at `{}` — another process is writing this (table, writer_id); \
                         stop it, or wait out the lease ({}s), or delete the lock object \
                         after confirming the holder is dead",
                        cur.describe(),
                        ident.lock_key(),
                        cfg.lock_lease_timeout.as_secs()
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            Some(cur) if cur.is_stale(lease_ms) => {
                tracing::warn!(
                    key = %ident.lock_key(),
                    holder = %cur.describe(),
                    "taking over stale writer lease"
                );
                break;
            }
            _ => break,
        }
    }
    staging
        .write_lock(ident, &LockFile::new(instance_uuid))
        .await?;
    // Settle delay: without conditional PUT (OSS), a concurrent acquirer may
    // overwrite our record; whoever reads back their own uuid after the delay
    // wins, the other fails loudly. The residual race (both read back their
    // own write) is caught by the first heartbeat's read-back within
    // lock_heartbeat_interval.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    match staging.read_lock(ident).await? {
        Some(l) if l.instance_uuid == instance_uuid => Ok(()),
        Some(l) => Err(Error::WriterLocked(format!(
            "lost the acquire race: {} at `{}`",
            l.describe(),
            ident.lock_key()
        ))),
        None => Err(Error::WriterLocked(format!(
            "lock at `{}` vanished during acquire (concurrent force-release?); retry",
            ident.lock_key()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_url_splits_and_derives_the_store() {
        // Alibaba OSS, with a prefix.
        let c = parse_staging_url(
            "s3://oss-cn-hangzhou.aliyuncs.com/relyt-staging/inst-1024",
            "ak".into(),
            "sk".into(),
        )
        .unwrap();
        assert_eq!(c.endpoint, "oss-cn-hangzhou.aliyuncs.com");
        assert_eq!(c.bucket, "relyt-staging");
        assert_eq!(c.prefix, "inst-1024");
        assert_eq!(c.service, StagingService::Oss);
        // OSS needs no region and the endpoint yields none.
        assert_eq!(c.resolve_region(), None);

        // An internal OSS endpoint is still OSS.
        let c = parse_staging_url(
            "s3://oss-cn-hangzhou-internal.aliyuncs.com/b/p",
            "ak".into(),
            "sk".into(),
        )
        .unwrap();
        assert_eq!(c.service, StagingService::Oss);

        // AWS S3, no prefix: bucket root, and the region comes from the host.
        let c = parse_staging_url(
            "s3://s3.ap-east-1.amazonaws.com/relyt-staging",
            "ak".into(),
            "sk".into(),
        )
        .unwrap();
        assert_eq!(c.bucket, "relyt-staging");
        assert_eq!(c.prefix, "");
        assert_eq!(c.service, StagingService::S3);
        assert_eq!(c.resolve_region().as_deref(), Some("ap-east-1"));

        // A multi-segment prefix stays whole.
        let c =
            parse_staging_url("s3://s3.amazonaws.com/b/a/b/c", "ak".into(), "sk".into()).unwrap();
        assert_eq!(c.bucket, "b");
        assert_eq!(c.prefix, "a/b/c");
    }

    #[test]
    fn staging_url_with_a_port_or_an_opaque_host() {
        // A port hides neither the store kind nor the region.
        let c = parse_staging_url(
            "s3://oss-cn-hangzhou.aliyuncs.com:443/b/p",
            "ak".into(),
            "sk".into(),
        )
        .unwrap();
        assert_eq!(c.service, StagingService::Oss);
        let c = parse_staging_url(
            "s3://s3.ap-east-1.amazonaws.com:443/b",
            "ak".into(),
            "sk".into(),
        )
        .unwrap();
        assert_eq!(c.resolve_region().as_deref(), Some("ap-east-1"));

        // An S3-compatible store the client cannot sign for: the error names
        // the administrator, not a field that does not exist in managed mode.
        let err = parse_staging_url("s3://minio.internal:9000/b/p", "ak".into(), "sk".into())
            .unwrap_err()
            .to_string();
        assert!(err.contains("Relyt administrator"), "got: {err}");
        assert!(!err.contains("StagingConfig.region"), "got: {err}");
    }

    #[test]
    fn staging_url_rejects_malformed_input() {
        // Wrong scheme: the server validates this too, but a client-side
        // check keeps the failure at connect rather than at first upload.
        assert!(parse_staging_url("https://host/b", "a".into(), "s".into()).is_err());
        // No bucket segment.
        assert!(parse_staging_url("s3://host", "a".into(), "s".into()).is_err());
        // Empty host.
        assert!(parse_staging_url("s3:///b", "a".into(), "s".into()).is_err());
        // Empty bucket.
        assert!(parse_staging_url("s3://host/", "a".into(), "s".into()).is_err());
    }
}
