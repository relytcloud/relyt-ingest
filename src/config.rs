//! Client / writer configuration.
//!
//! Defaults: rotation
//! thresholds are SDK init parameters with defaults 64MB / 15s, stream mode
//! defaults to upsert with an insert-only switch, and retry_max defaults to a
//! finite value so a poison file eventually parks in FAIL where the head-stuck
//! alarm can see it (-1 means the job never reaches FAIL and the
//! group head oscillates READY<->RUNNING forever).

use std::fmt;
use std::time::Duration;

use crate::error::{Error, Result};

/// How the target table is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamMode {
    /// Update stream: the Relyt master upserts on the primary key; a serial group per
    /// writer keeps per-partition ordering (last write wins per PK).
    #[default]
    Upsert,
    /// Insert-only stream: plain insert on the Relyt master (no upsert, no
    /// intra-file PK dedup, so the target needs no primary key). Jobs still
    /// carry serial_group/serial_seq (M=1 in-group serialization): the
    /// server-side group watermark is what makes replays after a crash
    /// idempotent for this mode too.
    InsertOnly,
}

/// Who owns the staging bucket, and therefore where its settings come from.
///
/// The two modes differ in more than the bucket: under `Relyt` no credential
/// ever appears in your configuration or your repository, because the client
/// fetches one at connect time. Under `Customer` your credentials necessarily
/// reach the Relyt master as well -- its loader has to read the objects you
/// wrote -- so that mode is a different security posture, not just a
/// different location. See GUIDE.md.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StagingOwner {
    /// Relyt-managed bucket (the default): leave [`ClientConfig::staging`]
    /// unset and the client asks the server for the location and credentials.
    #[default]
    Relyt,
    /// Your own bucket: [`ClientConfig::staging`] must be filled in, and the
    /// client never asks the server for staging settings.
    Customer,
}

/// OSS/S3 staging bucket access. Only needed with [`StagingOwner::Customer`];
/// under the default the server supplies the equivalent at connect time.
/// Credentials are fixed AK/SK; rotation is an operational SOP with an
/// old/new overlap window, so the SDK just takes
/// whatever it is given.
#[derive(Clone)]
pub struct StagingConfig {
    /// e.g. "oss-cn-shanghai.aliyuncs.com" (no scheme).
    pub endpoint: String,
    pub bucket: String,
    /// Prefix under which all staged files live, e.g. "ingest-staging/".
    /// File layout below it is `<table>/<writer_id>/<epoch>-<seq>-o<start>-<end>.csv`.
    pub prefix: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// "oss" or "s3" — selects the OpenDAL service.
    pub service: StagingService,
    /// AWS region for SigV4 (S3 only; OSS ignores it). `None` derives it from
    /// the endpoint (`s3.<region>.amazonaws.com`); S3-compatible services with
    /// opaque endpoints (MinIO, R2) must set it explicitly (R2 wants "auto").
    pub region: Option<String>,
}

/// Where the `<cluster_id>` staging-path segment (the Relyt instance id — one per deployed instance/DWSU) comes from, in priority order:
/// 1. `ClientConfig::cluster_id` when set (unconditional override — e2e and
///    clusters without an instance id use this);
/// 2. the Relyt master's `relyt_get_instance_id()`;
/// 3. neither -> `Client::connect` fails: db_oid/rel_oid are only unique
///    within one cluster, so an unscoped shared bucket WILL collide.
///
/// The UDF returning "-1" (the unconfigured default), an empty string or
/// NULL, and UNDEFINED_FUNCTION on masters that predate it, all count as "the
/// cluster has no configured identity".
pub fn cluster_id_is_unset(v: &str) -> bool {
    v.is_empty() || v == "-1"
}

/// Hand-written so credentials never reach a log through `?cfg` or a panic
/// backtrace: the secret is fully redacted, the key id keeps a short prefix
/// so operators can still tell which credential pair is in use.
impl fmt::Debug for StagingConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StagingConfig")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field(
                "access_key_id",
                &crate::notify::mask_secret(&self.access_key_id),
            )
            .field(
                "secret_access_key",
                &crate::notify::mask_secret(&self.secret_access_key),
            )
            .field("service", &self.service)
            .field("region", &self.region)
            .finish()
    }
}

impl StagingConfig {
    /// The region SigV4 signs with: the explicit `region` when given,
    /// otherwise derived from an AWS-shaped endpoint.
    pub fn resolve_region(&self) -> Option<String> {
        self.region
            .clone()
            .or_else(|| derive_aws_region(&self.endpoint))
    }
}

/// The `application_name` every connection this crate opens announces itself
/// with, so a server-side log line, `pg_stat_activity` row, or DBA looking at
/// a stuck session can tell an ingest connection from anything else. Job rows
/// carry the same information through `add_by` (see
/// [`crate::notify::ADD_BY_INGEST_RS_SDK`]); this covers the session side,
/// which `add_by` cannot reach because loads execute in a background worker.
pub(crate) const APPLICATION_NAME: &str = concat!("relyt-ingest/", env!("CARGO_PKG_VERSION"));

/// Parse `control_dsn` into a connection config that announces
/// [`APPLICATION_NAME`] unless the DSN already sets one -- an explicit value
/// is the operator's choice and wins.
///
/// Parsed, not string-appended. tokio-postgres accepts both the `key=value`
/// form and `postgresql://` URLs; appending ` application_name=...` to a URL
/// lands inside its last component (the dbname, or a `?sslmode=` value) and
/// the server then rejects the connection with an error that names the wrong
/// thing. Parsing also makes "already set" a real check rather than a
/// substring match that a password containing the word would trip.
pub(crate) fn control_config(dsn: &str) -> Result<tokio_postgres::Config> {
    let mut cfg: tokio_postgres::Config = dsn.parse().map_err(|e| {
        Error::Config(format!(
            "control_dsn is not a valid tokio-postgres connection string: {}",
            crate::error::describe_db_error(&e)
        ))
    })?;
    if cfg.get_application_name().is_none() {
        cfg.application_name(APPLICATION_NAME);
    }
    Ok(cfg)
}

/// Open a control connection from `control_dsn` (see [`control_config`]).
/// The caller spawns the returned connection future, exactly as with
/// `tokio_postgres::connect`.
pub(crate) async fn connect_control(
    dsn: &str,
) -> Result<(
    tokio_postgres::Client,
    tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
)> {
    Ok(control_config(dsn)?.connect(tokio_postgres::NoTls).await?)
}

/// Structural checks on one staging location, wherever it came from.
///
/// Applied to a customer-supplied config in [`ClientConfig::validate`] and to
/// the server's answer in `Client::connect`: a value filled in by an operator
/// through a GUC can be as wrong as one written by a customer, and letting the
/// server's answer skip these would only defer the failure to the first
/// upload.
pub(crate) fn validate_staging(s: &StagingConfig) -> Result<()> {
    // The options blob is a flat `k=v,k=v` string with no escaping (server-side
    // format), so a comma or a double quote inside a
    // credential silently breaks the load and only surfaces when the server
    // tries to parse it.
    for (name, value) in [
        ("access_key_id", &s.access_key_id),
        ("secret_access_key", &s.secret_access_key),
    ] {
        if value.is_empty() {
            return Err(Error::Config(format!("staging {name} is empty")));
        }
        if let Some(bad) = value.chars().find(|c| *c == ',' || *c == '"') {
            return Err(Error::Config(format!(
                "staging {name} contains `{bad}`, which would corrupt the job options list"
            )));
        }
    }
    if s.bucket.is_empty() || s.endpoint.is_empty() {
        return Err(Error::Config("staging bucket/endpoint must be set".into()));
    }
    // Fail here, at connect time, rather than as a signature rejection on the
    // first upload: SigV4 needs the real region, and only AWS-shaped endpoints
    // let us derive it.
    if s.service == StagingService::S3 && s.resolve_region().is_none() {
        return Err(Error::Config(format!(
            "staging service is S3 but no region was given and none can be derived \
             from endpoint `{}`; set StagingConfig.region (S3-compatible stores like \
             MinIO/R2 need it explicitly)",
            s.endpoint
        )));
    }
    Ok(())
}

/// Extract the region from an AWS endpoint. The client MUST sign with the
/// real AWS region name (a bare `s3.amazonaws.com` means `us-east-1`); how
/// the Relyt master derives a region from the job's URL for its own reads is
/// its business and never what SigV4 wants on the wire.
///
/// Recognized shapes, with or without a `.cn` suffix:
///   s3.amazonaws.com                 -> us-east-1
///   s3.<region>.amazonaws.com        -> <region>
///   s3-<region>.amazonaws.com        -> <region>   (legacy dashed form)
/// Anything else (MinIO, R2, ...) -> None: the caller must configure it.
pub(crate) fn derive_aws_region(endpoint: &str) -> Option<String> {
    let host = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"))
        .unwrap_or(endpoint);
    let rest = host
        .strip_suffix(".amazonaws.com.cn")
        .or_else(|| host.strip_suffix(".amazonaws.com"))?;
    if rest == "s3" {
        return Some("us-east-1".to_string());
    }
    let region = rest
        .strip_prefix("s3.")
        .or_else(|| rest.strip_prefix("s3-"))?;
    if region.is_empty()
        || !region
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return None;
    }
    Some(region.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagingService {
    Oss,
    S3,
}

/// CSV serialization knobs. The on-the-wire format itself (quoting, NULL,
/// header) is fixed by the crate's CSV serializer to what the Relyt master's loader expects;
/// only the delimiter is configurable because it must round-trip into the
/// job's load options.
/// How staged CSV objects are stored. The Relyt master sniffs gzip magic bytes
/// and decompresses as it reads the file, so this is a pure
/// client-side choice: gzip trades a little producer CPU for ~5-10x less
/// upload bandwidth and staging storage (CSV compresses very well).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StagingCompression {
    /// gzip (default). Objects are named `*.csv.gz`.
    #[default]
    Gzip,
    /// Uncompressed `*.csv` — for debugging (objects readable as-is) or when
    /// producer CPU is the scarcer resource.
    Plain,
}

#[derive(Debug, Clone)]
pub struct CsvConfig {
    pub delimiter: char,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self { delimiter: ',' }
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Who owns the staging bucket. Leave at the default
    /// ([`StagingOwner::Relyt`]) and the client fetches the location and
    /// credentials from the server at connect time, so nothing secret lives
    /// in your configuration.
    pub staging_owner: StagingOwner,
    /// Your own staging bucket. Required with [`StagingOwner::Customer`] and
    /// rejected otherwise -- the two are cross-checked at connect, because
    /// the alternative failure is silent and bad: a bucket filled in while
    /// the owner says Relyt would send your data to Relyt's bucket instead.
    pub staging: Option<StagingConfig>,
    /// Control connection DSN (tokio-postgres format). Used for schema fetch,
    /// `relyt_get_serial_group_watermark`, and `zdb_add_async_load_job`.
    /// The account only needs: EXECUTE on the two UDFs + read access to the
    /// target table's metadata (no direct job-table reads).
    pub control_dsn: String,

    /// Rotate the current staging file when it reaches this many bytes.
    /// The 64MB default is sized for one or a few writers; with W writers in
    /// one process the buffered memory bound is W x this value, so size it
    /// accordingly (see GUIDE.md "容量估算" / README "Deployment sizing").
    pub rotate_size_bytes: u64,
    /// ... or when the oldest buffered row is this old, whichever first.
    pub rotate_interval_max: Duration,

    pub stream_mode: StreamMode,

    /// gzip staged objects (default) or store them plain. See
    /// [`StagingCompression`]. The rotation size threshold is judged on the
    /// UNCOMPRESSED CSV size either way — it describes the load batch, not the
    /// bytes on the wire.
    pub staging_compression: StagingCompression,

    /// Explicit cluster identity for staging paths; overrides the server's
    /// `relyt_get_instance_id()`; "-1", an empty string and NULL there count as unset.
    pub cluster_id: Option<String>,

    /// How often each writer's staging GC pass runs. The pass deletes only
    /// objects that are simultaneously (a) at or below the server group
    /// watermark, (b) older than `gc_retain_days`, and (c) in excess of
    /// `gc_retain_min_files`; see `table.rs::gc_pass` for the hard rules.
    pub gc_interval: Duration,
    /// Minimum age (days, judged by the epoch embedded in the object name)
    /// before a consumed object may be deleted.
    pub gc_retain_days: u32,
    /// Never shrink a writer's staging directory below this many objects.
    pub gc_retain_min_files: usize,

    /// Writer-lease heartbeat period (see `lock.rs`). The holder rewrites
    /// `_meta/.../lock` this often; a competing `open_table` refuses to start
    /// while the recorded heartbeat is younger than `lock_lease_timeout`.
    pub lock_heartbeat_interval: Duration,
    /// Age after which a lease is considered abandoned and may be taken over.
    /// Must dwarf both the heartbeat period and worst-case clock skew between
    /// writer hosts.
    pub lock_lease_timeout: Duration,

    /// Server-side per-job retry budget (`retry_max` job column).
    /// `Some(n)`: after n failed executions the job parks in FAIL (visible,
    /// audited, manually retried/skipped per SOP). `None` maps to -1 =
    /// infinite retries — poison files then never reach FAIL; only use this
    /// when an external watcher handles head-stuck alarms.
    /// Default 15 ≈ ~5h to converge with the server's backoff (10 ≈ 3.5h,
    /// 20 ≈ 8.5h).
    pub retry_max: Option<i32>,

    pub csv: CsvConfig,

    /// Consumption-lag sampling period for [`crate::TableWriter::lag`]
    /// (crate::table::TableWriter::lag); internal default 30s. Hidden: not a
    /// customer knob, only the e2e suite shrinks it.
    #[doc(hidden)]
    pub lag_sample_interval: Duration,

    /// How often the lag sampler re-LISTs the staging directory -- the
    /// expensive half of a sample (GC keeps that directory at tens of
    /// thousands of objects on purpose); the watermark query still runs
    /// every `lag_sample_interval`. Internal default 5min. Hidden.
    #[doc(hidden)]
    pub lag_list_interval: Duration,

    /// How often an idle writer rewrites `_meta/.../state.json` as a liveness
    /// beacon even when `resume_offset` did not move. Internal default 4h.
    /// Hidden: only the e2e suite shrinks it.
    #[doc(hidden)]
    pub state_heartbeat_interval: Duration,

    /// Test-only escape hatch: skips the RANGE checks in [`Self::validate`]
    /// (never the structural ones — empty/corrupting credentials, missing
    /// region, delimiter collisions stay fatal). The e2e suite shrinks GC
    /// and lease timings far below their production floors; production code
    /// must leave this false.
    #[doc(hidden)]
    pub bypass_range_checks: bool,
}

impl ClientConfig {
    pub const DEFAULT_ROTATE_SIZE: u64 = 64 * 1024 * 1024;
    /// Upper bound for `rotate_size_bytes`. A rotation renders the whole
    /// file into one in-memory String and uploads it with a single PUT
    /// (no multipart), so the cap must stay well inside both the process
    /// memory budget and the object stores' single-object limit (5GiB on
    /// S3). 512MiB is already 8x the default; a mistyped unit (MB written
    /// as bytes) is what this check is for.
    pub const MAX_ROTATE_SIZE: u64 = 512 * 1024 * 1024;
    pub const DEFAULT_ROTATE_INTERVAL: Duration = Duration::from_secs(15);
    pub const DEFAULT_RETRY_MAX: i32 = 15;
    pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(3600);
    pub const DEFAULT_GC_RETAIN_DAYS: u32 = 7;
    pub const DEFAULT_GC_RETAIN_MIN_FILES: usize = 50_000;
    pub const DEFAULT_LOCK_HEARTBEAT: Duration = Duration::from_secs(30);
    pub const DEFAULT_LOCK_LEASE_TIMEOUT: Duration = Duration::from_secs(180);

    /// Reject configurations that would corrupt the job options list or the
    /// CSV downstream. The options blob is a flat `k=v,k=v` string with no
    /// escaping (server-side format), so a comma or
    /// a double quote inside a credential silently breaks the load and only
    /// surfaces when the server tries to parse it.
    pub fn validate(&self) -> Result<()> {
        // Owner and staging must agree. Both mismatches are rejected, not
        // reconciled: filling in a bucket while the owner still says Relyt
        // would otherwise send the data to Relyt's bucket without a word.
        match (self.staging_owner, &self.staging) {
            (StagingOwner::Customer, None) => {
                return Err(Error::Config(
                    "staging_owner is Customer but ClientConfig::staging is not set; supply \
                     endpoint, bucket, prefix, credentials (and region for MinIO/R2), or \
                     leave staging_owner at its default to use the Relyt-managed bucket"
                        .into(),
                ))
            }
            (StagingOwner::Relyt, Some(_)) => {
                return Err(Error::Config(
                    "ClientConfig::staging is set but staging_owner is Relyt (the default), \
                     which fetches the bucket from the server and would ignore it; set \
                     staging_owner = StagingOwner::Customer to use your own bucket, or clear \
                     staging"
                        .into(),
                ))
            }
            // Relyt mode: the server's answer goes through the same checks in
            // Client::connect, once it is known.
            (StagingOwner::Relyt, None) => {}
            (StagingOwner::Customer, Some(s)) => validate_staging(s)?,
        }
        match self.csv.delimiter {
            '"' | '\n' | '\r' => {
                return Err(Error::Config(format!(
                    "csv delimiter `{}` collides with the quoting/record syntax",
                    self.csv.delimiter
                )))
            }
            _ => {}
        }
        if self.rotate_size_bytes == 0 {
            return Err(Error::Config("rotate_size_bytes must be > 0".into()));
        }
        if !self.bypass_range_checks {
            self.validate_ranges()?;
        }
        Ok(())
    }

    /// Sanity ranges for every tunable, so a typo'd unit (ms vs s, MB vs
    /// bytes) fails at connect time instead of as silent misbehavior. Signed
    /// values reject negatives with a pointer at the right spelling.
    fn validate_ranges(&self) -> Result<()> {
        fn range_dur(name: &str, v: Duration, min: Duration, max: Duration) -> Result<()> {
            if v < min || v > max {
                return Err(Error::Config(format!(
                    "{name} = {v:?} is outside the allowed range [{min:?}, {max:?}]"
                )));
            }
            Ok(())
        }
        if self.rotate_size_bytes > Self::MAX_ROTATE_SIZE {
            return Err(Error::Config(format!(
                "rotate_size_bytes = {} exceeds the {}MiB maximum: a rotation renders the whole \
                 file in memory and uploads it as one object (did you mean MB?)",
                self.rotate_size_bytes,
                Self::MAX_ROTATE_SIZE / (1024 * 1024)
            )));
        }
        range_dur(
            "rotate_interval_max",
            self.rotate_interval_max,
            Duration::from_secs(1),
            Duration::from_secs(6 * 3600),
        )?;
        range_dur(
            "gc_interval",
            self.gc_interval,
            Duration::from_secs(15 * 60),
            Duration::from_secs(7 * 24 * 3600),
        )?;
        if !(1..=365).contains(&self.gc_retain_days) {
            return Err(Error::Config(format!(
                "gc_retain_days = {} is outside the allowed range [1, 365]",
                self.gc_retain_days
            )));
        }
        if !(3..=1_000_000).contains(&self.gc_retain_min_files) {
            return Err(Error::Config(format!(
                "gc_retain_min_files = {} is outside the allowed range [3, 1000000]",
                self.gc_retain_min_files
            )));
        }
        range_dur(
            "lock_heartbeat_interval",
            self.lock_heartbeat_interval,
            Duration::from_secs(1),
            Duration::from_secs(600),
        )?;
        // A lease must comfortably outlive missed heartbeats or every stall
        // becomes a takeover; 24h caps how long a crashed writer can block.
        let lease_floor = self.lock_heartbeat_interval * 3;
        if self.lock_lease_timeout < lease_floor
            || self.lock_lease_timeout > Duration::from_secs(24 * 3600)
        {
            return Err(Error::Config(format!(
                "lock_lease_timeout = {:?} is outside the allowed range                  [3 x lock_heartbeat_interval = {lease_floor:?}, 24h]",
                self.lock_lease_timeout
            )));
        }
        if let Some(n) = self.retry_max {
            if n < 0 {
                return Err(Error::Config(format!(
                    "retry_max = {n} is negative; use retry_max = None for                      infinite server-side retries"
                )));
            }
        }
        Ok(())
    }

    /// A config for the Relyt-managed staging bucket: pass only the control
    /// DSN, and `Client::connect` fetches the bucket and its credentials from
    /// the server. This is the recommended shape -- nothing secret ends up in
    /// your configuration or your repository.
    pub fn new(control_dsn: impl Into<String>) -> Self {
        Self::with_parts(StagingOwner::Relyt, None, control_dsn)
    }

    /// A config for your own staging bucket. Everything the client needs must
    /// be spelled out, including a region when the endpoint is not an
    /// AWS/OSS one the client can derive it from. See [`StagingOwner`] for
    /// how the two modes differ beyond the location.
    pub fn with_customer_staging(staging: StagingConfig, control_dsn: impl Into<String>) -> Self {
        Self::with_parts(StagingOwner::Customer, Some(staging), control_dsn)
    }

    fn with_parts(
        staging_owner: StagingOwner,
        staging: Option<StagingConfig>,
        control_dsn: impl Into<String>,
    ) -> Self {
        Self {
            staging_owner,
            staging,
            control_dsn: control_dsn.into(),
            rotate_size_bytes: Self::DEFAULT_ROTATE_SIZE,
            rotate_interval_max: Self::DEFAULT_ROTATE_INTERVAL,
            stream_mode: StreamMode::default(),
            staging_compression: StagingCompression::default(),
            cluster_id: None,
            gc_interval: Self::DEFAULT_GC_INTERVAL,
            gc_retain_days: Self::DEFAULT_GC_RETAIN_DAYS,
            gc_retain_min_files: Self::DEFAULT_GC_RETAIN_MIN_FILES,
            lock_heartbeat_interval: Self::DEFAULT_LOCK_HEARTBEAT,
            lock_lease_timeout: Self::DEFAULT_LOCK_LEASE_TIMEOUT,
            retry_max: Some(Self::DEFAULT_RETRY_MAX),
            csv: CsvConfig::default(),
            lag_sample_interval: Duration::from_secs(30),
            lag_list_interval: Duration::from_secs(5 * 60),
            state_heartbeat_interval: Duration::from_secs(4 * 3600),
            bypass_range_checks: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A structurally valid config to mutate per range case.
    fn base() -> ClientConfig {
        ClientConfig::with_customer_staging(
            StagingConfig {
                endpoint: "oss-cn-hangzhou.aliyuncs.com".into(),
                bucket: "b".into(),
                prefix: "p".into(),
                access_key_id: "ak".into(),
                secret_access_key: "sk".into(),
                service: StagingService::Oss,
                region: None,
            },
            "host=h user=u dbname=d",
        )
    }

    fn expect_range_err(cfg: &ClientConfig, param: &str) {
        let msg = cfg.validate().expect_err("must be rejected").to_string();
        assert!(msg.contains(param), "error must name `{param}`, got: {msg}");
    }

    #[test]
    fn control_config_sets_application_name_on_both_dsn_forms() {
        let kv = control_config("host=h port=5432 user=u dbname=d").unwrap();
        assert_eq!(kv.get_application_name(), Some(APPLICATION_NAME));
        assert_eq!(kv.get_dbname(), Some("d"));

        // The URL form is where string-appending used to break: the suffix
        // landed inside dbname (or the sslmode value).
        let url = control_config("postgresql://u:pw@h:5432/prod?sslmode=disable").unwrap();
        assert_eq!(url.get_application_name(), Some(APPLICATION_NAME));
        assert_eq!(url.get_dbname(), Some("prod"));
        assert_eq!(url.get_ssl_mode(), tokio_postgres::config::SslMode::Disable);
    }

    #[test]
    fn control_config_keeps_an_explicit_application_name() {
        let cfg = control_config("host=h user=u application_name=mine").unwrap();
        assert_eq!(cfg.get_application_name(), Some("mine"));
    }

    #[test]
    fn control_config_rejects_garbage() {
        let err = control_config("this is not a dsn").unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
    }

    #[test]
    fn staging_owner_and_staging_must_agree() {
        // Customer mode without a staging config: the client would have
        // nowhere to upload to.
        let mut cfg = base();
        cfg.staging = None;
        let msg = cfg.validate().expect_err("must be rejected").to_string();
        assert!(msg.contains("staging_owner is Customer"), "got: {msg}");

        // The dangerous direction: a bucket filled in while the owner is
        // still the default. Silently ignoring it would send the customer's
        // data to Relyt's bucket, so it is an error, not a precedence rule.
        let mut cfg = base();
        cfg.staging_owner = StagingOwner::Relyt;
        let msg = cfg.validate().expect_err("must be rejected").to_string();
        assert!(msg.contains("staging_owner is Relyt"), "got: {msg}");

        // Relyt mode with nothing set is the recommended shape and passes;
        // the server's answer is validated later, in Client::connect.
        let cfg = ClientConfig::new("host=h user=u dbname=d");
        assert_eq!(cfg.staging_owner, StagingOwner::Relyt);
        assert!(cfg.staging.is_none());
        cfg.validate().expect("the default shape is valid");
    }

    #[test]
    fn defaults_pass_validation() {
        base().validate().expect("defaults are within every range");
    }

    #[test]
    fn rotate_size_range() {
        let mut cfg = base();
        cfg.rotate_size_bytes = ClientConfig::MAX_ROTATE_SIZE; // exactly 512MiB: ok
        cfg.validate().unwrap();
        cfg.rotate_size_bytes += 1;
        expect_range_err(&cfg, "rotate_size_bytes");
        cfg.rotate_size_bytes = 16 * 1024 * 1024 * 1024; // the old 16GiB cap is gone
        expect_range_err(&cfg, "rotate_size_bytes");
        cfg.rotate_size_bytes = 0;
        expect_range_err(&cfg, "rotate_size_bytes");
    }

    #[test]
    fn rotate_interval_range() {
        let mut cfg = base();
        cfg.rotate_interval_max = Duration::from_secs(6 * 3600); // 6h cap: ok
        cfg.validate().unwrap();
        cfg.rotate_interval_max = Duration::from_secs(6 * 3600 + 1);
        expect_range_err(&cfg, "rotate_interval_max");
        cfg.rotate_interval_max = Duration::from_millis(500); // sub-second
        expect_range_err(&cfg, "rotate_interval_max");
    }

    #[test]
    fn gc_ranges() {
        let mut cfg = base();
        cfg.gc_interval = Duration::from_secs(15 * 60 - 1);
        expect_range_err(&cfg, "gc_interval");
        cfg.gc_interval = Duration::from_secs(7 * 24 * 3600 + 1);
        expect_range_err(&cfg, "gc_interval");

        let mut cfg = base();
        cfg.gc_retain_days = 0;
        expect_range_err(&cfg, "gc_retain_days");
        cfg.gc_retain_days = 366;
        expect_range_err(&cfg, "gc_retain_days");
        cfg.gc_retain_days = 365;
        cfg.validate().unwrap();

        let mut cfg = base();
        cfg.gc_retain_min_files = 2;
        expect_range_err(&cfg, "gc_retain_min_files");
        cfg.gc_retain_min_files = 1_000_001;
        expect_range_err(&cfg, "gc_retain_min_files");
        cfg.gc_retain_min_files = 3;
        cfg.validate().unwrap();
    }

    #[test]
    fn lock_ranges() {
        let mut cfg = base();
        cfg.lock_heartbeat_interval = Duration::from_millis(900);
        expect_range_err(&cfg, "lock_heartbeat_interval");

        let mut cfg = base();
        // lease below 3x heartbeat: every stall would read as a takeover.
        cfg.lock_lease_timeout = cfg.lock_heartbeat_interval * 3 - Duration::from_secs(1);
        expect_range_err(&cfg, "lock_lease_timeout");
        cfg.lock_lease_timeout = Duration::from_secs(24 * 3600 + 1);
        expect_range_err(&cfg, "lock_lease_timeout");
    }

    #[test]
    fn negative_retry_max_rejected_with_hint() {
        let mut cfg = base();
        cfg.retry_max = Some(-1);
        let msg = cfg.validate().expect_err("negative rejected").to_string();
        assert!(msg.contains("retry_max") && msg.contains("None"), "{msg}");
        cfg.retry_max = Some(0); // 0 = fail on first error: legal
        cfg.validate().unwrap();
        cfg.retry_max = None; // infinite: legal spelling
        cfg.validate().unwrap();
    }

    #[test]
    fn bypass_skips_ranges_but_not_structure() {
        let mut cfg = base();
        cfg.bypass_range_checks = true;
        cfg.gc_interval = Duration::from_secs(2); // out of range: tolerated
        cfg.validate().unwrap();
        cfg.staging.as_mut().unwrap().access_key_id = "with,comma".into(); // structural: still fatal
        cfg.validate().unwrap_err();
    }

    #[test]
    fn aws_region_derivation() {
        assert_eq!(
            derive_aws_region("s3.ap-east-1.amazonaws.com").as_deref(),
            Some("ap-east-1")
        );
        assert_eq!(
            derive_aws_region("s3-us-west-2.amazonaws.com").as_deref(),
            Some("us-west-2")
        );
        // Bare endpoint signs as us-east-1 (whatever alias the Relyt master
        // uses internally is never what SigV4 wants on the wire).
        assert_eq!(
            derive_aws_region("s3.amazonaws.com").as_deref(),
            Some("us-east-1")
        );
        assert_eq!(
            derive_aws_region("s3.cn-north-1.amazonaws.com.cn").as_deref(),
            Some("cn-north-1")
        );
        // Scheme prefixes are tolerated.
        assert_eq!(
            derive_aws_region("https://s3.ap-east-1.amazonaws.com").as_deref(),
            Some("ap-east-1")
        );
        // Non-AWS endpoints cannot be derived.
        assert_eq!(derive_aws_region("oss-cn-hangzhou.aliyuncs.com"), None);
        assert_eq!(derive_aws_region("minio.internal:9000"), None);
        assert_eq!(derive_aws_region("mybucket.s3.fake.example.com"), None);
    }
}
