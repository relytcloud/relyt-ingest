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

    /// The data or the schema of a sealed file was rejected, so no number of
    /// retries can turn it into a CSV: the rows are in a file the pipeline
    /// cannot produce. The stage stops rather than spinning on it.
    ///
    /// This is narrower than "the stage failed". Storage trouble, and a
    /// panic or cancellation inside a blocking task, are retried instead and
    /// surface as [`StagingStalled`](Self::StagingStalled) — they may clear
    /// on their own, and stopping a stream that would have healed costs more
    /// than waiting.
    ///
    /// The rows are not durable, so do not commit their Kafka offsets; fix
    /// what the message names (usually the data or the table definition),
    /// then restart and let the recovery handshake replay them. No offset
    /// rewind is needed: nothing was skipped.
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

/// Whether this error is a rejection of the caller's own input: the
/// arguments they passed, their configuration, or the target table's
/// definition.
///
/// The distinguishing property is that it cannot fix itself. It does not
/// depend on the network, on storage, on another process or on the passage
/// of time, so the same call will fail the same way until someone changes
/// the code, the table, or the operational step that produced it.
///
/// `WriterLocked` is deliberately NOT one of these, and it is the one most
/// easily mistaken for it: a rolling restart produces it for as long as the
/// previous holder's lease takes to expire, and it clears on its own.
pub(crate) fn is_input_error(e: &Error) -> bool {
    matches!(
        e,
        Error::Config(_) | Error::Schema(_) | Error::UnsupportedType { .. } | Error::Naming(_)
    )
}

/// Log an input error on its way back to the caller.
///
/// These are the only errors the SDK logs on behalf of the caller, and the
/// reason is that they are integration faults: an application that swallows
/// the returned `Err` -- or unwraps it into a panic whose message scrolls
/// past -- would otherwise leave no trace of a stream that never started.
/// Everything else is either retried internally (and logged where it
/// happens) or is a stopped stream, which has its own alarm.
///
/// ERROR, not an alarm line: the stream has not stopped, it never began, and
/// the alarm keyword is reserved for a running stream that died.
pub(crate) fn log_input_error(op: &str, e: Error) -> Error {
    if is_input_error(&e) {
        tracing::error!(
            operation = op,
            error = %e,
            "rejected by the SDK: the call, the configuration or the table definition does not \
             meet the contract. This will not fix itself -- correct it and restart; see GUIDE.md"
        );
    }
    e
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_errors_are_the_ones_the_caller_has_to_fix() {
        // The caller's arguments, configuration or table definition.
        assert!(is_input_error(&Error::Config(
            "rotate_size_bytes = 0".into()
        )));
        assert!(is_input_error(&Error::Schema(
            "append batch schema != table schema".into()
        )));
        assert!(is_input_error(&Error::UnsupportedType {
            column: "c".into(),
            data_type: "Interval".into(),
        }));
        assert!(is_input_error(&Error::Naming("writer_id too long".into())));

        // Clears on its own once the previous lease expires: treating this
        // as an integration fault would have every rolling restart look like
        // a misconfiguration.
        assert!(!is_input_error(&Error::WriterLocked("held by w0".into())));

        // The environment, or a stream that stopped -- neither is something
        // the caller passed in.
        assert!(!is_input_error(&Error::WriterFenced("preempted".into())));
        assert!(!is_input_error(&Error::StagingOrderViolation("gap".into())));
        assert!(!is_input_error(&Error::StagingStalled {
            attempts: 3,
            last: "put: timeout".into(),
        }));
        assert!(!is_input_error(&Error::WriterClosed));
    }
}
