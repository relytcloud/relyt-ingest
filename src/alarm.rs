//! Alarm lines: one grep-able log line per event that stops a stream.
//!
//! The SDK runs inside the customer's process, so its logs go to the
//! customer's collector, not ours. The point of a fixed keyword is therefore
//! not that Relyt sees the alarm — it is that the customer can configure one
//! rule, on one string, and catch every way an ingest stream stops. The
//! format is the one the rest of the product already emits, so an operator
//! who knows it does not learn a second one:
//!
//! ```text
//! RELYT_OBSERVE_ALARM:[ALARM_LEVEL=Fatal,ALARM_LOG_TIME=2030-01-02 03:04:05,ALARM_LOG_MODULE=INGEST-SDK],ALARM_MSG=...
//! ```
//!
//! Two things follow from living in someone else's process:
//!
//! - **`tracing` needs a subscriber.** A library emits nothing on its own. An
//!   application without one gets no alarm lines — GUIDE.md says so where it
//!   tells operators to build a rule on the keyword. The line is emitted at
//!   ERROR so that the common `RUST_LOG=warn` still carries it; matching the
//!   keyword rather than the level is about precision, not about surviving a
//!   filter.
//! - **The message has to be actionable by the reader.** The customer cannot
//!   look at our catalog, so a line that only names an internal condition is
//!   useless to them. Every alarm here says what stopped, which stream, and
//!   what the operator does next — including the Kafka offsets to rewind to
//!   when that is the remedy.

use std::fmt::Write as _;

use crate::error::Error;

/// Severity, spelled as the collector's rules expect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AlarmLevel {
    /// The stream has stopped and will not restart by itself.
    Fatal,
}

impl AlarmLevel {
    fn as_str(self) -> &'static str {
        match self {
            AlarmLevel::Fatal => "Fatal",
        }
    }
}

/// `ALARM_LOG_MODULE` verbatim. The other components prefix theirs with
/// `HDPS-` because they run inside a Relyt deployment; this one runs in the
/// customer's process and is named for what it is.
const MODULE: &str = "INGEST-SDK";

/// `YYYY-MM-DD HH:MM:SS` in UTC, from the system clock.
///
/// Hand-rolled rather than pulled from a date crate: this is the only place
/// in the SDK that needs a formatted wall clock, and a dependency in a
/// library is a dependency in every application that links it.
fn log_time(now_ms: u64) -> String {
    let secs = (now_ms / 1000) as i64;
    let days = secs.div_euclid(86_400);
    let in_day = secs.rem_euclid(86_400);

    // Civil-from-days (Howard Hinnant), the same algorithm `csv::write_date32`
    // uses for DATE columns.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    let (h, min, s) = (in_day / 3600, (in_day % 3600) / 60, in_day % 60);
    let mut out = String::with_capacity(19);
    let _ = write!(out, "{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{s:02}");
    out
}

/// Emit one alarm line. `msg` is already the operator-facing text.
fn emit(level: AlarmLevel, msg: &str) {
    // ERROR, not the INFO the in-house implementations use. They run inside
    // a Relyt deployment where we configure the subscriber; this one runs in
    // the customer's process, where `RUST_LOG=warn` is an ordinary choice
    // and would drop an INFO line altogether -- the alarm channel would go
    // silent while the documentation promised it could not. Being the same
    // level as the ordinary failure log is the lesser cost: the keyword is
    // still what an alerting rule matches, and it selects exactly the
    // subset of ERROR that has stopped a stream.
    tracing::error!(
        "RELYT_OBSERVE_ALARM:[ALARM_LEVEL={},ALARM_LOG_TIME={},ALARM_LOG_MODULE={}],ALARM_MSG={}",
        level.as_str(),
        log_time(crate::lock::now_ms()),
        MODULE,
        msg
    );
}

/// A stream has stopped for good: one alarm line naming the stream, the
/// cause, and what the operator has to do about it.
///
/// Called once per writer at the moment the state is reached, from whichever
/// task discovers it — so an idle application still gets the alarm, without
/// waiting for its next `append`.
pub(crate) fn stream_stopped(table: &str, writer_id: &str, group: &str, cause: &Error) {
    let action = remedy(cause);
    emit(
        AlarmLevel::Fatal,
        &format!(
            "ingest stream stopped and will not resume on its own. \
             table={table} writer_id={writer_id} serial_group={group} \
             cause={cause} action={action}"
        ),
    );
}

/// What the person reading the alarm has to do. Kept next to the alarm rather
/// than in the error text: the error says what happened, this says what to do,
/// and only the latter differs between an operator and a caller catching the
/// same error in code.
fn remedy(cause: &Error) -> &'static str {
    match cause {
        // The gap is ahead of the resume point, so a restart alone skips it:
        // the offsets to rewind to are in the cause text.
        Error::StagingOrderViolation(_) => {
            "stop consuming this stream, do NOT commit its Kafka offsets, then rewind the \
             consumer to the offset range named above and restart; a restart on its own \
             resumes after the gap and those rows would never load"
        }
        // Another process holds the lease. Rewinding is wrong here: whatever
        // this writer staged is either loaded or picked up by the successor.
        Error::WriterFenced(_) => {
            "another process took over this writer_id; stop this one and check for a \
             duplicate deployment. Do not rewind offsets: the writer that holds the lease \
             continues the stream"
        }
        Error::SerialContractViolation(_) => {
            "two writers share a writer_id, or the epoch went backwards; stop this stream \
             and resolve the identity clash before restarting"
        }
        // Retrying is pointless, but nothing was skipped: the rows never
        // reached staging, so a restart replays them from the recovery plan.
        Error::RotationFailed { .. } => {
            "repair what the cause names (usually the data or the table definition), then \
             restart; do not commit the Kafka offsets of the rows that were in flight, and \
             no rewind is needed"
        }
        _ => "see the cause; do not commit the Kafka offsets of the rows that were in flight",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_time_formats_utc_to_the_second() {
        // 2030-01-02 03:04:05 UTC
        assert_eq!(log_time(1_893_553_445_000), "2030-01-02 03:04:05");
        // Epoch, and a leap day, to pin the civil-from-days arithmetic.
        assert_eq!(log_time(0), "1970-01-01 00:00:00");
        assert_eq!(log_time(1_709_164_799_000), "2024-02-28 23:59:59");
        assert_eq!(log_time(1_709_164_800_000), "2024-02-29 00:00:00");
        // Sub-second input is truncated, not rounded.
        assert_eq!(log_time(1_893_553_445_999), "2030-01-02 03:04:05");
    }

    #[test]
    fn every_stopped_state_has_its_own_remedy() {
        let order = Error::StagingOrderViolation("seqs 8..=8 never arrived".into());
        assert!(remedy(&order).contains("rewind"));

        let fenced = Error::WriterFenced("taken over".into());
        assert!(fenced_says_do_not_rewind(&fenced));

        let clash = Error::SerialContractViolation("duplicate writer_id".into());
        assert!(remedy(&clash).contains("identity clash"));

        let rotation = Error::RotationFailed {
            stage: "render",
            source: std::sync::Arc::new(Error::Schema("bad column".into())),
        };
        assert!(remedy(&rotation).contains("no rewind is needed"));

        // The four remedies must be distinct: an operator acts on this text,
        // and two states sharing one instruction would send them wrong.
        let all = [
            remedy(&order),
            remedy(&fenced),
            remedy(&clash),
            remedy(&rotation),
        ];
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    fn fenced_says_do_not_rewind(e: &Error) -> bool {
        let r = remedy(e);
        r.contains("Do not rewind") && r.contains("duplicate deployment")
    }

    /// The whole point of the order-violation alarm: the operator must be
    /// able to read the Kafka offsets to rewind to straight off the line.
    #[test]
    fn the_order_violation_alarm_carries_the_offsets_to_rewind_to() {
        // The text the tripwire produces (check_order in table.rs).
        let cause = Error::StagingOrderViolation(
            "file epoch 1789460671000 seq 9 (Kafka offsets [130, 140]) reached the upload \
             stage after epoch 1789460671000 seq 7 (offsets up to 110): seqs 8..=8 of epoch \
             1789460671000 never arrived; Kafka offsets 111..=129 are not in staging. A \
             restart resumes after the highest staged offset and will NOT re-stage that gap."
                .into(),
        );
        let msg = format!(
            "ingest stream stopped and will not resume on its own. \
             table={} writer_id={} serial_group={} cause={} action={}",
            "public.orders",
            "orders-p0",
            "13727:54321:orders-p0",
            cause,
            remedy(&cause)
        );
        // What the customer greps for, and what they need from the hit.
        assert!(msg.contains("111..=129"), "the gap to rewind to: {msg}");
        assert!(msg.contains("orders-p0"), "which stream: {msg}");
        assert!(msg.contains("rewind the consumer"), "what to do: {msg}");
        assert!(msg.contains("do NOT commit"), "what not to do: {msg}");
    }

    #[test]
    fn the_alarm_line_carries_the_keyword_module_and_stream() {
        // The format is a contract with the collector's rule, so it is
        // asserted literally rather than by shape.
        let line = format!(
            "RELYT_OBSERVE_ALARM:[ALARM_LEVEL={},ALARM_LOG_TIME={},ALARM_LOG_MODULE={}],ALARM_MSG={}",
            AlarmLevel::Fatal.as_str(),
            log_time(1_893_553_445_000),
            MODULE,
            "x"
        );
        assert_eq!(
            line,
            "RELYT_OBSERVE_ALARM:[ALARM_LEVEL=Fatal,ALARM_LOG_TIME=2030-01-02 03:04:05,\
             ALARM_LOG_MODULE=INGEST-SDK],ALARM_MSG=x"
        );
        // Grepping for RELYT alone must find it: that is what the customer
        // is told to do.
        assert!(line.contains("RELYT"));
    }
}
