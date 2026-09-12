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

/// Where the staging bucket comes from, and therefore where its settings live.
///
/// The two variants differ in more than the bucket: under `Relyt` no
/// credential ever appears in your configuration or your repository, because
/// the client fetches one at connect time. Under `Customer` your credentials
/// necessarily reach the Relyt master as well -- its loader has to read the
/// objects you wrote -- so that variant is a different security posture, not
/// just a different location. See GUIDE.md.
///
/// One field, two shapes: the bucket lives inside the `Customer` variant, so
/// there is no separate owner flag that could disagree with it.
#[derive(Debug, Clone, Default)]
pub enum Staging {
    /// Relyt-managed bucket (the default): the client asks the server for the
    /// location and credentials at connect time.
    #[default]
    Relyt,
    /// Your own bucket, fully described; the client never asks the server for
    /// staging settings.
    Customer(StagingConfig),
}

impl Staging {
    /// The customer-supplied bucket, if this is the `Customer` variant.
    pub fn customer(&self) -> Option<&StagingConfig> {
        match self {
            Staging::Relyt => None,
            Staging::Customer(s) => Some(s),
        }
    }
}

/// OSS/S3 staging bucket access. Only needed with [`Staging::Customer`];
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
    /// Region for SigV4 (S3 and S3-compatible stores; OSS ignores it). `None`
    /// derives it from the endpoint for every store the client knows by host
    /// -- AWS, Tencent COS, Kingsoft KS3, UCloud US3, Volcengine TOS, see
    /// `classify_endpoint` -- while S3-compatible services with opaque endpoints
    /// (MinIO, R2) must set it explicitly (R2 wants "auto").
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
            .or_else(|| derive_region(&self.endpoint))
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

/// Object-store endpoints the client recognises by host: the store kind, and
/// where the region sits in the name. This is the same set the Relyt master
/// reads staging locations with, kept in step on purpose -- a host the master
/// can read but the client cannot sign for is exactly the gap this table
/// closes -- so a change on either side must be mirrored on the other.
///
/// Row = (host prefix, host suffixes, store). The region is the text between
/// prefix and suffix without its leading `.` or `-`:
/// `cos.ap-shanghai.myqcloud.com` -> `ap-shanghai`,
/// `s3-us-west-2.amazonaws.com` -> `us-west-2`; OSS needs none. Two departures
/// from the master's table: a bare `s3.amazonaws.com` is `us-east-1` (SigV4
/// signs with the real region; the master's internal alias for it is its own
/// business), and Google Cloud Storage is left out until signing against its
/// interoperability endpoint has been verified.
///
/// Coverage: AWS S3 and Alibaba OSS are exercised end to end. The other stores
/// are recognised by host and signed as S3-compatible, but no end-to-end test
/// runs against them yet.
const KNOWN_ENDPOINTS: &[(&str, &[&str], StagingService)] = &[
    (
        "s3",
        &[".amazonaws.com", ".amazonaws.com.cn"],
        StagingService::S3,
    ),
    (
        "internal.s3",
        &[".amazonaws.com", ".amazonaws.com.cn"],
        StagingService::S3,
    ),
    ("oss", &[".aliyuncs.com"], StagingService::Oss),
    ("cos", &[".myqcloud.com"], StagingService::S3),
    ("ks3", &[".ksyuncs.com"], StagingService::S3),
    ("s3", &[".ufileos.com"], StagingService::S3),
    (
        "tos-s3",
        &[".volces.com", ".ivolces.com"],
        StagingService::S3,
    ),
];

/// Classify an endpoint by host: the store kind and, for S3 and S3-compatible
/// stores, the region SigV4 signs with. `None` for a host outside
/// [`KNOWN_ENDPOINTS`] (MinIO, a custom domain): the caller decides whether
/// that is an error (Relyt-managed staging has nothing to fall back on) or
/// something the customer supplies through `StagingConfig::region`.
pub(crate) fn classify_endpoint(endpoint: &str) -> Option<(StagingService, Option<String>)> {
    let host = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"))
        .unwrap_or(endpoint);
    // A `:port` suffix is not part of the name.
    let host = host.rsplit_once(':').map_or(host, |(h, _)| h);
    for (prefix, suffixes, service) in KNOWN_ENDPOINTS {
        let Some(after_prefix) = host.strip_prefix(prefix) else {
            continue;
        };
        for suffix in suffixes.iter() {
            let Some(middle) = after_prefix.strip_suffix(suffix) else {
                continue;
            };
            if *service == StagingService::Oss {
                return Some((StagingService::Oss, None));
            }
            let region = match middle {
                "" => "us-east-1".to_string(),
                m if m.starts_with('.') || m.starts_with('-') => m[1..].to_string(),
                // `s3x.amazonaws.com`: shares the prefix, is not this shape.
                _ => return None,
            };
            if region.is_empty()
                || !region
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            {
                return None;
            }
            return Some((*service, Some(region)));
        }
    }
    None
}

/// The region [`classify_endpoint`] derives for `endpoint`, if any.
pub(crate) fn derive_region(endpoint: &str) -> Option<String> {
    classify_endpoint(endpoint).and_then(|(_, region)| region)
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

#[derive(Clone)]
pub struct ClientConfig {
    /// Where the staging bucket comes from. Leave at the default
    /// ([`Staging::Relyt`]) and the client fetches the location and
    /// credentials from the server at connect time, so nothing secret lives
    /// in your configuration; [`Staging::Customer`] carries your own bucket.
    pub staging: Staging,
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

    /// Under Relyt-managed staging, how often the client re-reads the staging
    /// credentials from the master, so a key rotation on the Relyt side is
    /// picked up without a restart; a denied upload also triggers an
    /// immediate refresh. Ignored for a customer-owned bucket. Default 5min.
    pub staging_refresh_interval: Duration,

    /// Consumption-lag sampling period for [`crate::TableWriter::lag`]
    /// (crate::table::TableWriter::lag); internal default 30s. Hidden: not a
    /// customer knob, only the e2e suite shrinks it.
    #[doc(hidden)]
    pub lag_sample_interval: Duration,

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

/// Hand-written for the same reason as [`StagingConfig`]'s: `control_dsn` is
/// the one secret a Relyt-managed deployment still holds, and a derived Debug
/// would print its password verbatim through any `?cfg` in a log line.
impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("staging", &self.staging)
            .field("control_dsn", &mask_dsn(&self.control_dsn))
            .field("rotate_size_bytes", &self.rotate_size_bytes)
            .field("rotate_interval_max", &self.rotate_interval_max)
            .field("stream_mode", &self.stream_mode)
            .field("staging_compression", &self.staging_compression)
            .field("cluster_id", &self.cluster_id)
            .field("gc_interval", &self.gc_interval)
            .field("gc_retain_days", &self.gc_retain_days)
            .field("gc_retain_min_files", &self.gc_retain_min_files)
            .field("lock_heartbeat_interval", &self.lock_heartbeat_interval)
            .field("lock_lease_timeout", &self.lock_lease_timeout)
            .field("retry_max", &self.retry_max)
            .field("csv", &self.csv)
            .field("staging_refresh_interval", &self.staging_refresh_interval)
            .field("lag_sample_interval", &self.lag_sample_interval)
            .field("state_heartbeat_interval", &self.state_heartbeat_interval)
            .field("bypass_range_checks", &self.bypass_range_checks)
            .finish()
    }
}

/// Redact the password in a tokio-postgres DSN for display, in both forms it
/// accepts: `password=...` (bare or single-quoted) in the key=value form, and
/// `://user:password@` in URLs. Everything else stays visible -- host,
/// database and user are what a troubleshooter needs to see.
pub(crate) fn mask_dsn(dsn: &str) -> String {
    if let Some(scheme_end) = dsn.find("://") {
        let rest = &dsn[scheme_end + 3..];
        if let Some(at) = rest.find('@') {
            if let Some(colon) = rest[..at].find(':') {
                return format!(
                    "{}{}:***{}",
                    &dsn[..scheme_end + 3],
                    &rest[..colon],
                    &rest[at..]
                );
            }
        }
        return dsn.to_string();
    }
    let mut out = String::with_capacity(dsn.len());
    let mut rest = dsn;
    // ASCII lowercasing keeps byte offsets, so `i` indexes `rest` directly.
    while let Some(i) = rest.to_ascii_lowercase().find("password=") {
        let key_end = i + "password=".len();
        out.push_str(&rest[..key_end]);
        out.push_str("***");
        let value = &rest[key_end..];
        let consumed = match value.strip_prefix('\'') {
            // Quoted value: through the closing quote, skipping `\'` escapes.
            Some(quoted) => {
                let mut escaped = false;
                let mut end = None;
                for (j, c) in quoted.char_indices() {
                    if c == '\'' && !escaped {
                        end = Some(j);
                        break;
                    }
                    escaped = c == '\\' && !escaped;
                }
                end.map_or(value.len(), |j| 1 + j + 1)
            }
            None => value.find(char::is_whitespace).unwrap_or(value.len()),
        };
        rest = &value[consumed..];
    }
    out.push_str(rest);
    out
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
    pub const DEFAULT_STAGING_REFRESH: Duration = Duration::from_secs(5 * 60);

    /// Reject configurations that would corrupt the job options list or the
    /// CSV downstream. The options blob is a flat `k=v,k=v` string with no
    /// escaping (server-side format), so a comma or
    /// a double quote inside a credential silently breaks the load and only
    /// surfaces when the server tries to parse it.
    pub fn validate(&self) -> Result<()> {
        // Under Relyt the server's answer goes through the same checks in
        // Client::connect, once it is known.
        if let Staging::Customer(s) = &self.staging {
            validate_staging(s)?;
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
        // Below 30s the refresh becomes a noticeable load on the master for
        // no operational gain; above a day a rotation's overlap window would
        // have to be absurdly long to be safe.
        range_dur(
            "staging_refresh_interval",
            self.staging_refresh_interval,
            Duration::from_secs(30),
            Duration::from_secs(24 * 3600),
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
        Self::with_staging(Staging::Relyt, control_dsn)
    }

    /// A config for your own staging bucket. Everything the client needs must
    /// be spelled out, including a region when the endpoint is not an
    /// AWS/OSS one the client can derive it from. See [`Staging`] for
    /// how the two modes differ beyond the location.
    pub fn with_customer_staging(staging: StagingConfig, control_dsn: impl Into<String>) -> Self {
        Self::with_staging(Staging::Customer(staging), control_dsn)
    }

    fn with_staging(staging: Staging, control_dsn: impl Into<String>) -> Self {
        Self {
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
            staging_refresh_interval: Self::DEFAULT_STAGING_REFRESH,
            lag_sample_interval: Duration::from_secs(30),
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
    fn staging_is_one_field_with_two_shapes() {
        // Relyt mode is the recommended shape and passes as-is; the server's
        // answer is validated later, in Client::connect.
        let cfg = ClientConfig::new("host=h user=u dbname=d");
        assert!(matches!(cfg.staging, Staging::Relyt));
        assert!(cfg.staging.customer().is_none());
        cfg.validate().expect("the default shape is valid");

        // Customer mode carries the bucket inside the variant, so "customer
        // without a bucket" and "bucket under Relyt" cannot be written down.
        let cfg = base();
        assert!(cfg.staging.customer().is_some());
        cfg.validate().expect("a complete customer config is valid");
    }

    #[test]
    fn debug_output_redacts_the_dsn_password_in_both_forms() {
        let mut cfg = ClientConfig::new("host=h user=u password=s3cret dbname=d");
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("s3cret"), "got: {shown}");
        assert!(shown.contains("password=***"), "got: {shown}");
        assert!(
            shown.contains("host=h"),
            "non-secret parts stay visible: {shown}"
        );

        cfg.control_dsn = "postgresql://u:s3cret@h:5432/d?sslmode=require".into();
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("s3cret"), "got: {shown}");
        assert!(
            shown.contains("postgresql://u:***@h:5432/d"),
            "got: {shown}"
        );
    }

    #[test]
    fn mask_dsn_handles_quoted_and_missing_passwords() {
        assert_eq!(
            mask_dsn("host=h password='a b' user=u"),
            "host=h password=*** user=u"
        );
        assert_eq!(mask_dsn("host=h PASSWORD=x"), "host=h PASSWORD=***");
        assert_eq!(mask_dsn("host=h user=u"), "host=h user=u");
        assert_eq!(mask_dsn("postgresql://u@h/d"), "postgresql://u@h/d");
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
        if let Staging::Customer(s) = &mut cfg.staging {
            s.access_key_id = "with,comma".into(); // structural: still fatal
        }
        cfg.validate().unwrap_err();
    }

    #[test]
    fn endpoint_classification_matches_what_the_master_reads() {
        use StagingService::{Oss, S3};
        let c = classify_endpoint;
        let s3 = |r: &str| Some((S3, Some(r.to_string())));
        // AWS, in every spelling; a port or a scheme hides nothing.
        assert_eq!(c("s3.ap-east-1.amazonaws.com"), s3("ap-east-1"));
        assert_eq!(c("s3.ap-east-1.amazonaws.com:443"), s3("ap-east-1"));
        assert_eq!(c("https://s3.ap-east-1.amazonaws.com"), s3("ap-east-1"));
        assert_eq!(c("s3-us-west-2.amazonaws.com"), s3("us-west-2"));
        assert_eq!(c("s3.cn-north-1.amazonaws.com.cn"), s3("cn-north-1"));
        // Bare endpoint signs as us-east-1 (whatever alias the Relyt master
        // uses internally is never what SigV4 wants on the wire).
        assert_eq!(c("s3.amazonaws.com"), s3("us-east-1"));
        // OSS: native signing, no region; internal endpoints included.
        assert_eq!(c("oss-cn-hangzhou.aliyuncs.com"), Some((Oss, None)));
        assert_eq!(
            c("oss-cn-hangzhou-internal.aliyuncs.com"),
            Some((Oss, None))
        );
        // The S3-compatible stores the master also reads.
        assert_eq!(c("cos.ap-shanghai.myqcloud.com"), s3("ap-shanghai"));
        assert_eq!(c("ks3-cn-beijing.ksyuncs.com"), s3("cn-beijing"));
        assert_eq!(c("s3-cn-bj.ufileos.com"), s3("cn-bj"));
        assert_eq!(c("tos-s3-cn-beijing.volces.com"), s3("cn-beijing"));
        // Outside the table: the caller has to be told the region.
        assert_eq!(c("minio.internal:9000"), None);
        assert_eq!(c("mybucket.s3.fake.example.com"), None);
        assert_eq!(c("s3x.amazonaws.com"), None);
        assert_eq!(
            derive_region("cos.ap-shanghai.myqcloud.com").as_deref(),
            Some("ap-shanghai")
        );
        assert_eq!(derive_region("oss-cn-hangzhou.aliyuncs.com"), None);
    }
}
