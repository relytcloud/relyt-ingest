//! Notify thread: decoupled from the write path by an in-process FIFO. A
//! master outage means latency, never data loss and never
//! backpressure on `append` — the queue is tiny (one small struct per staged
//! file; a day of backlog ≈ ~1MB).
//!
//! Fast path = this thread retrying forever; correctness backstop = recovery
//! backfill from OSS LIST at `open_table` (the only thing that can replay
//! requests lost with a crashed process).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_postgres::error::SqlState;

use crate::error::{Error, Result};
use crate::staging::StagingHandle;

/// Wire values the submission UDF expects.
///
/// `add_by` is the job's origin, and it is what lets a server-side failure be
/// attributed to a client: the Relyt master stores it on the job row and
/// renders it wherever job info is returned. Before the master knew this
/// SDK's value, jobs from here were attributed to a different origin.
pub const ADD_BY_INGEST_RS_SDK: i32 = 4;
pub const STATUS_READY: i32 = 2;
pub const SOURCE_TYPE_S3: i32 = 1;
pub const TARGET_TYPE_HEAP: i32 = 3;

/// One staged file to announce to the server.
#[derive(Clone, Debug)]
pub struct NotifyRequest {
    /// The writer's serial_group (`<db_oid>:<rel_oid>:<writer_id>`). Always
    /// present: insert-only streams submit under serial groups too (M=1),
    /// which is what gives them the server-side watermark that recovery and
    /// GC prune against — only the upsert load mode and intra-file PK dedup
    /// stay Upsert-specific. Doubles as the per-writer queue/reporting key.
    pub serial_group: String,
    /// Kafka end-offset the file covers; recorded on confirmation so the
    /// state ticker can advance `resume_offset` (never past what the server
    /// accepted).
    pub end_offset: i64,
    /// Identifier `<db_oid>:<rel_oid>:<writer_id>:<epoch>:<seq>`.
    pub identifier: String,
    /// Full s3/oss URL of the staged CSV (goes into `source`).
    pub source_url: String,
    /// OID of the target table. Submitted through the UDF's `target oid`
    /// overload: no name resolution happens at notify time, so a concurrent
    /// RENAME (or a DROP of an unrelated same-named table) can no longer
    /// wedge the queue on a permanently-failing regclass cast.
    pub target: u32,
    /// CSV delimiter the staged object was written with. The load options
    /// are assembled at submit time from this and the staging credentials
    /// current at that moment, so a request that waits in the queue across a
    /// credential rotation goes out with the key that is valid then.
    pub delimiter: char,
    /// Whether the load runs as an upsert (`mode=upsert`) or a plain insert.
    pub upsert: bool,
    /// serial_seq of the staged file (`StagedFile::serial_seq`): the value
    /// the server serializes and watermarks the group on, and the value the
    /// notify loop reports back as confirmed.
    pub serial_seq: i64,
    pub retry_max: Option<i32>,
}

/// Mask one secret for display: the middle is always hidden behind a fixed
/// `***` (so the length is not leaked), and only a short head/tail survives
/// to tell two keys apart -- 3+3 chars for values of 16 chars or more (an
/// object-storage secret key is 30-40), 2+2 for 9..=15 (typical access key
/// ids), nothing at all for 8 or fewer (`******`). This is what
/// `Debug for StagingConfig` / `Debug for NotifyRequest` rely on to keep
/// credentials out of logs.
pub fn mask_secret(v: &str) -> String {
    let n = v.chars().count();
    let keep = match n {
        0..=8 => return "******".to_string(),
        9..=15 => 2,
        _ => 3,
    };
    let head: String = v.chars().take(keep).collect();
    let tail: String = v.chars().skip(n - keep).collect();
    format!("{head}***{tail}")
}

/// Mask only the `access_key_id=` / `secret_access_key=` VALUES inside a job
/// options blob, leaving every other `k=v` visible. Parsing mirrors the Relyt
/// master's own masking: key match is case-insensitive, `=` separates, the
/// value runs until `,`, whitespace, `'` or `"`.
pub fn mask_options(options: &str) -> String {
    const KEYS: [&str; 2] = ["access_key_id", "secret_access_key"];
    let mut out = String::with_capacity(options.len());
    let mut rest = options;
    while !rest.is_empty() {
        // Find the earliest occurrence of any key (case-insensitive).
        let lower = rest.to_ascii_lowercase();
        let hit = KEYS
            .iter()
            .filter_map(|k| lower.find(k).map(|i| (i, k.len())))
            .min_by_key(|(i, _)| *i);
        let Some((i, klen)) = hit else {
            out.push_str(rest);
            break;
        };
        // Emit up to and including the key.
        out.push_str(&rest[..i + klen]);
        let after_key = &rest[i + klen..];
        // Require `=` (allow surrounding spaces) to treat it as a k=v pair.
        let eq_rel = after_key.find('=');
        match eq_rel {
            Some(e) if after_key[..e].trim().is_empty() => {
                out.push_str(&after_key[..=e]);
                // Like the Relyt master: skip (and keep) any quotes/whitespace that
                // open the value, then mask up to the closing terminator.
                let after_eq = &after_key[e + 1..];
                let lead = after_eq
                    .find(|c: char| !(matches!(c, '\'' | '"') || c.is_whitespace()))
                    .unwrap_or(after_eq.len());
                out.push_str(&after_eq[..lead]);
                let value_start = &after_eq[lead..];
                let vlen = value_start
                    .find(|c: char| matches!(c, ',' | '\'' | '"') || c.is_whitespace())
                    .unwrap_or(value_start.len());
                out.push_str(&mask_secret(&value_start[..vlen]));
                rest = &value_start[vlen..];
            }
            _ => {
                // Key text without `=`: not a credential assignment, keep going.
                rest = after_key;
            }
        }
    }
    out
}

/// Job options blob: comma-separated k=v list in the form the Relyt master's
/// submission UDF expects: `access_key_id=..,secret_access_key=..,format=csv,\
/// header=true,delimiter="<d>"[,mode=upsert]`. The delimiter value is
/// double-quoted -- the UDF requires that for a comma delimiter and accepts
/// it for any char. `mode=upsert` makes the server-side load an upsert on the
/// primary key (submit-time checked: the target must have a PK); without it
/// a load is plain insert and duplicate PKs across files error.
/// The staging AK/SK ride inside (fixed keys, rotation via config + overlap
/// window).
pub fn build_copy_options(
    access_key_id: &str,
    secret_access_key: &str,
    delimiter: char,
    upsert: bool,
) -> String {
    let mut s = format!(
        "access_key_id={access_key_id},secret_access_key={secret_access_key},\
         format=csv,header=true,delimiter=\"{delimiter}\""
    );
    if upsert {
        s.push_str(",mode=upsert");
    }
    s
}

/// SQL for the 15-arg submission UDF. Parameterized — never string-concat.
/// `$8::oid` (Rust u32) selects the UDF's `target oid` overload: no name
/// resolution happens at submit time, by design (see `NotifyRequest::target`).
/// The explicit cast is load-bearing — the UDF has text and oid overloads,
/// and an uncast parameter leaves the parse-stage type unknown, which makes
/// function resolution fail with "function is not unique".
const ADD_JOB_SQL: &str = "SELECT pg_catalog.zdb_add_async_load_job(\
     $1, $2, $3, $4, $5, $6, $7, $8::oid, $9, $10, $11, $12, $13, $14, $15)";

/// What the notify loop reports back to the writers.
#[derive(Default)]
struct NotifyState {
    /// Per writer: serial_group -> (largest serial_seq
    /// the server has accepted for that writer, the Kafka end_offset carried
    /// by that same file). The state ticker persists the end_offset half as
    /// the writer's resume_offset. Confirmations arrive in seq order (FIFO
    /// queue, seqs allocated under the rotation lock), so a max-by-seq
    /// update keeps the two halves describing the same file.
    confirmed: HashMap<String, (i64, i64)>,
    /// serial_group -> error text of the first non-retryable serial-contract
    /// violation (gate 23P01) that writer hit. Once an entry exists, the
    /// writer's `append`/`flush` fail with SerialContractViolation until an
    /// operator resolves the identity clash; entries are never removed.
    fatal: HashMap<String, String>,
}

pub struct Notifier {
    /// serial_group -> sender into that writer's own FIFO + loop task, i.e.
    /// ONE channel per writer, not one channel per request.
    ///
    /// Two forces decide this shape, and they pull in opposite directions:
    ///
    /// - Within a writer, submissions must stay strictly ordered. The
    ///   server's watermark gate swallows any seq at or below the group's
    ///   watermark as an already-consumed replay, so a later seq arriving
    ///   first would silently discard the earlier file. A single-consumer
    ///   FIFO per writer is what makes "submitted in seq order" true.
    /// - Across writers, a stuck submission must not spread. A request is
    ///   retried forever (a master outage is latency, never loss), so one
    ///   shared queue would let writer A's unretryable request block every
    ///   other writer behind it. That shared-queue design is what this
    ///   started as; review flagged it as cross-table head-of-line
    ///   blocking and this is the fix.
    ///
    /// Hence per-writer: strict order inside, no coupling outside. There is
    /// deliberately no ordering across writers -- same-key ordering under
    /// multiple partitions rests on the producer keeping each key on one
    /// partition (see the lib.rs contract list).
    ///
    /// In the recommended deployment (one process per Kafka partition) this
    /// map holds exactly one entry; it grows only when one process drives
    /// several tables or partitions.
    ///
    /// Sizing: one live connection per writer (plus the client's control
    /// connection) is an intentional isolation trade-off; multiplexing for
    /// hundreds of writers is a tracked follow-up, not done here.
    queues: Mutex<HashMap<String, mpsc::UnboundedSender<NotifyRequest>>>,
    /// Postgres connection string (Data Source Name, tokio-postgres
    /// `key=value` format) — `ClientConfig::control_dsn`. Kept so each loop
    /// task can build, and rebuild after a drop, its own connection.
    dsn: String,
    /// Shared report board between the loop tasks (writers of it) and
    /// `TableWriter` (reader, via `confirmed()`/`fatal()`).
    state: Arc<Mutex<NotifyState>>,
    /// The staging location, read at submit time for the credentials that go
    /// into the job options.
    staging: Arc<StagingHandle>,
}

impl Notifier {
    /// Create the notifier; per-writer loop tasks are spawned lazily on the
    /// first request for each writer. Each task owns its own connection and
    /// reconnects on failure, so writers also do not share a retry fate.
    pub fn spawn(dsn: String, staging: Arc<StagingHandle>) -> Self {
        Self {
            queues: Mutex::new(HashMap::new()),
            dsn,
            state: Arc::new(Mutex::new(NotifyState::default())),
            staging,
        }
    }

    /// Enqueue; returns immediately. Fails only if the writer's notify task
    /// is gone (it never exits on its own while its sender is alive).
    pub fn enqueue(&self, req: NotifyRequest) -> Result<()> {
        let mut queues = self.queues.lock().unwrap();
        let tx = queues.entry(req.serial_group.clone()).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(notify_loop(
                self.dsn.clone(),
                rx,
                self.state.clone(),
                self.staging.clone(),
            ));
            tx
        });
        tx.send(req).map_err(|_| Error::WriterClosed)
    }

    /// Highest accepted (serial_seq, end_offset) for this writer.
    pub fn confirmed(&self, key: &str) -> Option<(i64, i64)> {
        self.state.lock().unwrap().confirmed.get(key).copied()
    }

    /// A recorded, non-retryable contract violation for this writer.
    pub fn fatal(&self, key: &str) -> Option<String> {
        self.state.lock().unwrap().fatal.get(key).cloned()
    }

    /// Forget a recorded contract violation for this writer. Called by
    /// `open_table` once it HOLDS the writer lease: the lease means this
    /// process is now the stream's sole writer, and the new writer's epoch is
    /// chosen above every anchor (staging, server watermark, state), so the
    /// (epoch, seq) that tripped 23P01 cannot recur. The old entry is a
    /// record of history, not of a live condition — keeping it would poison
    /// every reopen of the same (table, writer_id) in this process for the
    /// Client's whole lifetime. If the identity clash is in fact still live,
    /// the next submission trips the gate again and re-arms `fatal`.
    pub fn clear_fatal(&self, key: &str) -> bool {
        self.state.lock().unwrap().fatal.remove(key).is_some()
    }

    /// Arm the contract violation that `notify_loop` records when the server
    /// answers with one. Tests only: it is the one stopped state a writer
    /// cannot reach without a live server rejecting a submission, which is
    /// why `fatal_error`'s coverage test could not reach it from `table.rs`.
    #[cfg(test)]
    pub(crate) fn arm_fatal_for_test(&self, key: &str, msg: &str) {
        self.state
            .lock()
            .unwrap()
            .fatal
            .insert(key.to_string(), msg.to_string());
    }
}

async fn notify_loop(
    dsn: String,
    mut rx: mpsc::UnboundedReceiver<NotifyRequest>,
    state: Arc<Mutex<NotifyState>>,
    staging: Arc<StagingHandle>,
) {
    let mut client: Option<tokio_postgres::Client> = None;
    while let Some(req) = rx.recv().await {
        // Retry this request forever; ordering within the queue is preserved
        // (single consumer). The server tolerates out-of-order seq arrival —
        // scheduling gates on seq, not submission order — so strictly this
        // could be parallel, but FIFO keeps the failure story simple.
        let mut backoff = Duration::from_millis(200);
        loop {
            if client.is_none() {
                match connect(&dsn).await {
                    Ok(c) => client = Some(c),
                    Err(e) => {
                        tracing::warn!(error = %e, "notify: connect failed, retrying");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        continue;
                    }
                }
            }
            let submit_started = std::time::Instant::now();
            match submit(client.as_ref().unwrap(), &req, &staging).await {
                Ok(outcome @ (SubmitOutcome::Accepted | SubmitOutcome::Idempotent)) => {
                    tracing::info!(
                        identifier = %req.identifier,
                        serial_seq = req.serial_seq,
                        end_offset = req.end_offset,
                        idempotent = matches!(outcome, SubmitOutcome::Idempotent),
                        elapsed_ms = submit_started.elapsed().as_millis() as u64,
                        "notify: server accepted staged file"
                    );
                    // Confirmed = the server owns this file now. The queue is
                    // FIFO per writer and rotation allocates seqs under a
                    // lock, so confirmations arrive in seq order and a plain
                    // max is a correct watermark.
                    let mut st = state.lock().unwrap();
                    let slot = st
                        .confirmed
                        .entry(req.serial_group.clone())
                        .or_insert((req.serial_seq, req.end_offset));
                    if req.serial_seq >= slot.0 {
                        *slot = (req.serial_seq, req.end_offset);
                    }
                    break;
                }
                Ok(SubmitOutcome::ContractViolation(msg)) => {
                    // Deployment bug (duplicate writer_id / epoch rollback).
                    // Do not spin on it: surface loudly and drop the request —
                    // recovery backfill will hit the same wall and the
                    // operator must resolve the identity clash first.
                    tracing::error!(identifier = %req.identifier, %msg,
                        "notify: serial contract violation — dropping request, fix writer identity");
                    state
                        .lock()
                        .unwrap()
                        .fatal
                        .entry(req.serial_group.clone())
                        .or_insert_with(|| msg.clone());
                    break;
                }
                Err(e) => {
                    tracing::warn!(identifier = %req.identifier, error = %e,
                        "notify: submit failed, retrying");
                    client = None; // reconnect on any error; cheap and safe
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}

enum SubmitOutcome {
    Accepted,
    /// Watermark gate case (a) — seq <= group terminal watermark — or the
    /// identifier already exists: the work is already done or in the queue.
    Idempotent,
    ContractViolation(String),
}

async fn submit(
    client: &tokio_postgres::Client,
    req: &NotifyRequest,
    staging: &StagingHandle,
) -> std::result::Result<SubmitOutcome, tokio_postgres::Error> {
    // Credentials are read here, not at enqueue: a request that sat in the
    // queue across a rotation must carry the key that is valid now.
    let options = {
        let live = staging.current();
        build_copy_options(
            &live.cfg.access_key_id,
            &live.cfg.secret_access_key,
            req.delimiter,
            req.upsert,
        )
    };
    let res = client
        .query_one(
            ADD_JOB_SQL,
            &[
                &req.identifier,
                &ADD_BY_INGEST_RS_SDK,
                &STATUS_READY,
                &1i32, // version
                &SOURCE_TYPE_S3,
                &req.source_url,
                &TARGET_TYPE_HEAP,
                &req.target,
                // The Relyt master schedules only jobs with priority > 0; <= 0
                // is never picked up. 1 is the conventional value.
                &1i32, // priority
                &options,
                &Option::<String>::None, // msg
                &Option::<String>::None, // source_detail
                &req.serial_group,
                &req.serial_seq,
                &req.retry_max,
            ],
        )
        .await;
    match res {
        Ok(_row) => Ok(SubmitOutcome::Accepted),
        Err(e) => {
            if let Some(db) = e.as_db_error() {
                // Identifier unique-conflict = idempotent success (server
                // contract). The watermark gate also reports
                // idempotent success as a normal (non-error) return.
                if db.code() == &SqlState::UNIQUE_VIOLATION {
                    return Ok(SubmitOutcome::Idempotent);
                }
                // Gate case (b) -- a different identifier claiming an
                // in-flight seq -- raises ERRCODE_EXCLUSION_VIOLATION (23P01)
                // on the Relyt master: the writer's
                // epoch/seq contract is broken, so surface it instead of
                // retrying (retry can never succeed while the other job is
                // non-final).
                if db.code() == &SqlState::EXCLUSION_VIOLATION {
                    return Ok(SubmitOutcome::ContractViolation(db.message().to_string()));
                }
            }
            Err(e)
        }
    }
}

async fn connect(dsn: &str) -> Result<tokio_postgres::Client> {
    let (client, connection) = crate::config::connect_control(dsn).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(error = %e, "notify: control connection closed");
        }
    });
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_options_shape() {
        assert_eq!(
            build_copy_options("AK", "SK", '|', true),
            "access_key_id=AK,secret_access_key=SK,format=csv,header=true,\
             delimiter=\"|\",mode=upsert"
        );
        assert_eq!(
            build_copy_options("AK", "SK", ',', false),
            "access_key_id=AK,secret_access_key=SK,format=csv,header=true,delimiter=\",\""
        );
    }

    #[test]
    fn mask_secret_rules() {
        // 30-char secret key: 3 + 3 visible, 24 hidden behind a fixed `***`.
        assert_eq!(mask_secret("D3g5mSAkhvADz9Py5kvW4x4Q0PJV1M"), "D3g***V1M");
        assert_eq!(mask_secret("abcdefghijklmnop"), "abc***nop"); // exactly 16: 3+3
        assert_eq!(mask_secret("LTAI5tAbCdEfGh"), "LT***Gh"); // 14: 2+2
        assert_eq!(mask_secret("abcdefghi"), "ab***hi"); // exactly 9: 2+2
        assert_eq!(mask_secret("abcdefgh"), "******"); // exactly 8: nothing visible
        assert_eq!(mask_secret("abc"), "******"); // short
        assert_eq!(mask_secret(""), "******"); // empty
                                               // The mask never echoes the length: two very different lengths, same shape.
        assert_eq!(
            mask_secret(&"x".repeat(40)).len(),
            mask_secret(&"y".repeat(16)).len()
        );
    }

    #[test]
    fn mask_options_touches_only_credential_values() {
        let blob = build_copy_options(
            "LTAI5tAbCdEfGh",
            "D3g5mSAkhvADz9Py5kvW4x4Q0PJV1M",
            ',',
            true,
        );
        assert_eq!(
            mask_options(&blob),
            "access_key_id=LT***Gh,secret_access_key=D3g***V1M,\
             format=csv,header=true,delimiter=\",\",mode=upsert"
        );
        // No `=` after the key: not an assignment, text kept verbatim.
        assert_eq!(
            mask_options("ACCESS_KEY_ID 'abcdefghij' x=1"),
            "ACCESS_KEY_ID 'abcdefghij' x=1"
        );
        // Case-insensitive key, quoted value: only the value inside is masked.
        assert_eq!(
            mask_options("Secret_Access_Key=\"abcdefghij\" x=1"),
            "Secret_Access_Key=\"ab***ij\" x=1"
        );
    }
}
