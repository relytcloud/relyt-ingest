//! Staging object store: put (whole object, deterministic name = idempotent
//! retry), list (recovery source of truth), and the writer's persistent
//! state under `_meta/` (see the `state` module).
//!
//! All keys derive from a [`WriterIdentity`]; the layout contract lives in
//! [`crate::naming`]'s module docs.

use std::sync::{Arc, RwLock};

use opendal::{services, Operator};

use crate::config::{StagingConfig, StagingService};
use crate::error::{Error, Result};
use crate::lock::LockFile;
use crate::naming::{StagedFile, WriterIdentity};
use crate::state::StateFile;

#[derive(Clone)]
pub struct StagingStore {
    op: Operator,
}

impl StagingStore {
    pub fn new(cfg: &StagingConfig) -> Result<Self> {
        let op = match cfg.service {
            StagingService::Oss => {
                let builder = services::Oss::default()
                    .endpoint(&format!("https://{}", cfg.endpoint))
                    .bucket(&cfg.bucket)
                    .root(&normalize_root(&cfg.prefix))
                    .access_key_id(&cfg.access_key_id)
                    .access_key_secret(&cfg.secret_access_key);
                Operator::new(builder)?.finish()
            }
            StagingService::S3 => {
                // SigV4 requires the real region; `ClientConfig::validate`
                // already guaranteed this resolves, but constructing a store
                // directly (tests) can still hit the error path.
                let region = cfg.resolve_region().ok_or_else(|| {
                    Error::Config(format!("no region for S3 endpoint `{}`", cfg.endpoint))
                })?;
                let builder = services::S3::default()
                    .endpoint(&format!("https://{}", cfg.endpoint))
                    .bucket(&cfg.bucket)
                    .root(&normalize_root(&cfg.prefix))
                    .access_key_id(&cfg.access_key_id)
                    .secret_access_key(&cfg.secret_access_key)
                    .region(&region)
                    .disable_ec2_metadata();
                Operator::new(builder)?.finish()
            }
        };
        Ok(Self { op })
    }

    /// Whole-object write. Same key + same bytes on retry — no .tmp+rename,
    /// no multipart bookkeeping needed at these sizes (<=64MB).
    pub async fn put(&self, key: &str, bytes: impl Into<opendal::Buffer>) -> Result<()> {
        self.op.write(key, bytes).await?;
        Ok(())
    }

    /// List every staged file under the writer's staging directory, parsed.
    /// Unparseable names are returned separately so the caller can warn —
    /// silence would hide corruption of the recovery source of truth.
    pub async fn list_staged(
        &self,
        ident: &WriterIdentity,
    ) -> Result<(Vec<StagedFile>, Vec<String>)> {
        let dir = ident.staging_dir();
        let mut staged = Vec::new();
        let mut unknown = Vec::new();
        let entries = self.op.list(&dir).await?;
        for entry in entries {
            if entry.metadata().is_dir() {
                continue;
            }
            let name = entry.name().to_string();
            match StagedFile::parse_basename(&name) {
                Some(f) => staged.push(f),
                None => unknown.push(name),
            }
        }
        Ok((staged, unknown))
    }

    /// Read the writer's persistent state. Fail-loud policy:
    /// NotFound = fresh writer -> `Ok(None)`; an object that exists but
    /// cannot be read or parsed -> `Err`, never silently `None` — that would
    /// quietly disable the resume shortcut AND the epoch anti-rollback
    /// anchor. The operator escape hatch is [`Self::delete_state`].
    pub async fn read_state(&self, ident: &WriterIdentity) -> Result<Option<StateFile>> {
        let key = ident.state_key();
        match self.op.read(&key).await {
            Ok(buf) => {
                let bytes = buf.to_vec();
                let state = StateFile::from_bytes(&bytes).map_err(|e| {
                    Error::Config(format!(
                        "corrupt writer state at `{key}`: {e}; refusing to guess — \
                         delete the object explicitly to start fresh"
                    ))
                })?;
                Ok(Some(state))
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Overwrite the writer's persistent state.
    pub async fn write_state(&self, ident: &WriterIdentity, state: &StateFile) -> Result<()> {
        self.op.write(&ident.state_key(), state.to_bytes()).await?;
        Ok(())
    }

    /// Delete one staged object (GC only; staged files are otherwise
    /// immutable). The RAM/IAM policy for the SDK account should scope
    /// delete to `<prefix>/staging/*`.
    pub async fn delete_staged(&self, ident: &WriterIdentity, f: &StagedFile) -> Result<()> {
        self.op.delete(&f.object_key(ident)).await?;
        Ok(())
    }

    /// Read the writer lease. NotFound -> Ok(None); corrupt -> Err with the
    /// same fail-loud rationale as `read_state` (a garbled lock must not be
    /// silently treated as absent — delete it explicitly to force-release).
    pub async fn read_lock(&self, ident: &WriterIdentity) -> Result<Option<LockFile>> {
        let key = ident.lock_key();
        match self.op.read(&key).await {
            Ok(buf) => {
                let bytes = buf.to_vec();
                let lock = serde_json::from_slice::<LockFile>(&bytes).map_err(|e| {
                    Error::Config(format!(
                        "corrupt writer lock at `{key}`: {e}; refusing to guess —                          delete the object explicitly to force-release"
                    ))
                })?;
                Ok(Some(lock))
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Overwrite the writer lease (acquire, heartbeat renew, or takeover).
    pub async fn write_lock(&self, ident: &WriterIdentity, lock: &LockFile) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(lock).expect("LockFile serializes");
        self.op.write(&ident.lock_key(), bytes).await?;
        Ok(())
    }

    /// Delete the writer lease (release, or operator force-release).
    pub async fn delete_lock(&self, ident: &WriterIdentity) -> Result<()> {
        self.op.delete(&ident.lock_key()).await?;
        Ok(())
    }

    /// Operator escape hatch: drop the state object so the next `open_table`
    /// treats the writer as fresh (full re-notify; the identifier unique key
    /// and the submission gate absorb the replays).
    pub async fn delete_state(&self, ident: &WriterIdentity) -> Result<()> {
        self.op.delete(&ident.state_key()).await?;
        Ok(())
    }
}

fn normalize_root(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_normalization() {
        assert_eq!(normalize_root(""), "/");
        assert_eq!(normalize_root("/staging/"), "/staging/");
        assert_eq!(normalize_root("a/b"), "/a/b/");
    }
}

/// `s3://<endpoint>/<bucket>[/<prefix>]` -- the url form the Relyt master's
/// loader consumes; every staged object url is `<base>/<key>`.
pub fn staging_url_base(cfg: &StagingConfig) -> String {
    let scheme = match cfg.service {
        StagingService::Oss | StagingService::S3 => "s3",
    };
    let prefix = cfg.prefix.trim_matches('/');
    if prefix.is_empty() {
        format!("{scheme}://{}/{}", cfg.endpoint, cfg.bucket)
    } else {
        format!("{scheme}://{}/{}/{prefix}", cfg.endpoint, cfg.bucket)
    }
}

/// One immutable snapshot of the staging location: the config, the store
/// built from it, and the url base every object url derives from. Handed out
/// by [`StagingHandle::current`], used for one operation, never mutated.
pub struct StagingLive {
    pub cfg: StagingConfig,
    pub store: StagingStore,
    pub url_base: String,
}

impl StagingLive {
    fn build(cfg: StagingConfig) -> Result<Self> {
        let store = StagingStore::new(&cfg)?;
        let url_base = staging_url_base(&cfg);
        Ok(Self {
            cfg,
            store,
            url_base,
        })
    }
}

/// The staging location in use, swappable at run time.
///
/// Under Relyt-managed staging the credentials come from the master and the
/// master's operator rotates them; a long-running writer must pick the new
/// pair up without a restart. `current()` returns the live snapshot and
/// `apply()` installs a fresh config when its credentials differ. Only the
/// credentials may change: bucket and prefix are baked into every path and
/// identifier this process has already published, so a different location is
/// refused (the old snapshot stays) and needs a restart.
///
/// `std::sync::RwLock` on purpose: the guard is released before any await --
/// callers clone the Arc out -- matching the crate's lock discipline.
pub struct StagingHandle {
    live: RwLock<Arc<StagingLive>>,
    /// The control DSN to re-fetch credentials with under Relyt-managed
    /// staging; None for a customer-owned bucket, whose credentials are static
    /// configuration and never refreshed.
    managed_dsn: Option<String>,
}

impl StagingHandle {
    pub fn new(cfg: StagingConfig, managed_dsn: Option<String>) -> Result<Self> {
        Ok(Self {
            live: RwLock::new(Arc::new(StagingLive::build(cfg)?)),
            managed_dsn,
        })
    }

    pub fn current(&self) -> Arc<StagingLive> {
        self.live.read().unwrap().clone()
    }

    /// The control DSN when credentials are Relyt-managed, None otherwise.
    pub fn managed_dsn(&self) -> Option<&str> {
        self.managed_dsn.as_deref()
    }

    /// Install `fresh` if its credentials differ from the live snapshot.
    /// `Ok(true)` = swapped, `Ok(false)` = nothing changed, `Err` = `fresh`
    /// names a different location, which is refused.
    pub fn apply(&self, fresh: StagingConfig) -> Result<bool> {
        let cur = self.current();
        if fresh.endpoint != cur.cfg.endpoint
            || fresh.bucket != cur.cfg.bucket
            || fresh.prefix != cur.cfg.prefix
            || fresh.service != cur.cfg.service
        {
            return Err(Error::Config(format!(
                "staging location changed on the server (was `{}`, now `{}`); bucket and \
                 prefix are fixed for the life of a process, restart the writer to move",
                cur.url_base,
                staging_url_base(&fresh)
            )));
        }
        if fresh.access_key_id == cur.cfg.access_key_id
            && fresh.secret_access_key == cur.cfg.secret_access_key
            && fresh.region == cur.cfg.region
        {
            return Ok(false);
        }
        let live = Arc::new(StagingLive::build(fresh)?);
        *self.live.write().unwrap() = live;
        Ok(true)
    }
}

#[cfg(test)]
mod handle_tests {
    use super::*;

    fn cfg(ak: &str, prefix: &str) -> StagingConfig {
        StagingConfig {
            endpoint: "oss-cn-hangzhou.aliyuncs.com".into(),
            bucket: "b".into(),
            prefix: prefix.into(),
            access_key_id: ak.into(),
            secret_access_key: "sk".into(),
            service: StagingService::Oss,
            region: None,
        }
    }

    #[test]
    fn apply_swaps_credentials_but_refuses_a_new_location() {
        let h = StagingHandle::new(cfg("ak1", "p"), Some("host=h".into())).unwrap();
        assert_eq!(
            h.current().url_base,
            "s3://oss-cn-hangzhou.aliyuncs.com/b/p"
        );
        assert!(
            !h.apply(cfg("ak1", "p")).unwrap(),
            "same credentials: no swap"
        );
        assert!(h.apply(cfg("ak2", "p")).unwrap(), "new key: swapped");
        assert_eq!(h.current().cfg.access_key_id, "ak2");
        let err = h.apply(cfg("ak3", "other")).unwrap_err().to_string();
        assert!(err.contains("location changed"), "got {err}");
        assert_eq!(h.current().cfg.access_key_id, "ak2", "old snapshot kept");
    }
}
