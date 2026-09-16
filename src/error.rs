//! SDK error taxonomy.
//!
//! The split that matters operationally:
//! a `zdb_add_async_load_job` submission can come back as
//! - idempotent success (watermark gate: seq <= group terminal watermark, or the
//!   same identifier already exists) -> NOT an error, the notify loop moves on;
//! - contract violation (same serial_group + same serial_seq, different
//!   identifier, not yet terminal) -> a hard, non-retryable bug signal
//!   (`Error::SerialContractViolation`);
//! - transient failure (master down, network) -> retried forever by the notify
//!   thread, never surfaced through `append`/`flush`.

use thiserror::Error;

/// Render a postgres error the way an operator needs to read it.
///
/// `tokio_postgres::Error`'s own `Display` is the constant string "db error":
/// the server's message, its hint and its SQLSTATE all live in the `DbError`
/// hanging off it. Server-side errors this crate surfaces are mostly things
/// only the message can explain -- an unprovisioned ingest staging area names
/// the missing setting and hints at what to do about it -- so dropping it
/// turns an actionable failure into a dead end.
pub(crate) fn describe_db_error(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => match db.hint() {
            Some(hint) => format!("{} (hint: {})", db.message(), hint),
            None => db.message().to_string(),
        },
        // Not a server-side error at all (connection refused, TLS, protocol):
        // the outer error chain is the informative part there.
        None => e.to_string(),
    }
}

#[derive(Debug, Error)]
pub enum Error {
    /// Table schema fetched from Relyt does not match what the caller supplies,
    /// or the table has no primary key (PK is mandatory for upsert mode).
    #[error("schema error: {0}")]
    Schema(String),

    /// A column type is outside the supported whitelist. Fail-loud by design:
    /// we never silently coerce; `TableWriter::schema` shows what was accepted.
    #[error("unsupported arrow type for column `{column}`: {data_type}")]
    UnsupportedType { column: String, data_type: String },

    /// Identifier / serial_group construction exceeded server-side limits
    /// (128B identifier, 64B serial_group) even after shortening rules.
    #[error("naming error: {0}")]
    Naming(String),

    /// Gate case (b): same (serial_group, serial_seq) already occupied by a
    /// different, non-terminal identifier. This means two writers share a
    /// writer_id or the epoch went backwards — a deployment bug, not retryable.
    #[error("serial contract violation: {0}")]
    SerialContractViolation(String),

    /// `open_table` found a live lease for this `(table, writer_id)`: another
    /// process is (or very recently was) writing this stream. Not retryable
    /// from inside — stop the other process, or wait out the lease, or delete
    /// the lock object to force-release after confirming the holder is dead.
    #[error("writer is locked: {0}")]
    WriterLocked(String),

    /// The heartbeat found the lease held by someone else: this writer was
    /// preempted (lease expired during a stall, or a force-takeover) and MUST
    /// stop writing — its submissions could otherwise race the new holder's.
    #[error("writer was fenced: {0}")]
    WriterFenced(String),

    /// Object storage failure (after the staging layer's own retries).
    #[error("staging storage error: {0}")]
    Storage(#[from] opendal::Error),

    /// The file at the head of this writer's rotation pipeline has failed to
    /// stage `attempts` times in a row. Returned once
    /// `staging_error_after_attempts` is reached by `flush` / `close`
    /// instead of waiting on, and by an `append` that would otherwise park
    /// on the full queue -- that batch was NOT taken and can be appended
    /// again later. Nothing is lost: the pipeline keeps retrying the same
    /// bytes with backoff, and a later `flush` waits again. The rows behind
    /// it are not durable, so do not commit their Kafka offsets.
    #[error(
        "staging the current file has failed {attempts} time(s) and is still being retried; \
         last error: {last}"
    )]
    StagingStalled { attempts: u32, last: String },

    /// A rotation stage failed in a way retrying cannot fix: the rows were
    /// already sealed into a file, and every attempt to render, compress or
    /// upload it hits the same error. Unlike
    /// [`StagingStalled`](Self::StagingStalled) this will not clear on its
    /// own -- waiting is pointless. The rows are not durable, so do not
    /// commit their Kafka offsets; fix what the message names, then restart
    /// and let the recovery handshake replay them.
    ///
    /// `source` is an `Arc` because the same failure is reported to every
    /// waiter on the writer, and `Error` itself cannot be cloned (its
    /// storage and database variants wrap types that are not).
    #[error("rotation cannot proceed: {stage} of the current file keeps failing with: {source}")]
    RotationFailed {
        stage: &'static str,
        #[source]
        source: std::sync::Arc<Error>,
    },

    /// The order tripwire fired: a file reached this writer's upload stage
    /// with a seq that is not the next one. That is a broken SDK invariant
    /// (Kafka offsets, being caller input, are checked by `append` instead),
    /// not an operational fault,
    /// and the rows in the gap are NOT in staging -- a restart resumes after
    /// the highest staged offset and skips them. Stop consuming this
    /// stream, do not commit its Kafka offsets, roll back or upgrade the
    /// SDK, then rewind the consumer to the offset range in the message.
    /// Every `append` / `flush` / `close` on the writer fails with this
    /// from now on.
    #[error("staging order violation: {0}")]
    StagingOrderViolation(String),

    /// Control-plane (postgres) failure surfaced through a synchronous call
    /// path such as `Client::connect` / `open_table`. The notify thread never
    /// returns this; it retries internally.
    ///
    /// Rendered through `describe_db_error` rather than the inner error's
    /// own `Display`, which is the bare string "db error" -- everything the
    /// operator needs sits in the DbError beside it.
    #[error("relyt control connection error: {}", describe_db_error(.0))]
    Database(#[from] tokio_postgres::Error),

    #[error("configuration error: {0}")]
    Config(String),

    /// The table handle was closed / the background writer task is gone.
    #[error("writer closed")]
    WriterClosed,
}

pub type Result<T> = std::result::Result<T, Error>;
