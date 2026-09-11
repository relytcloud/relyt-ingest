//! `open_table` recovery protocol, one round trip:
//!
//! 1. fetch schema + PK check (local cache as fallback — not implemented in
//!    the skeleton);
//! 2. fetch the group watermark: `relyt_get_serial_group_watermark(group)`
//!    — the largest serial_seq among the group's terminal jobs (completed,
//!    or skipped by an operator), NULL when none exists;
//! 3. LIST own staging prefix.
//!
//! From (2)+(3), compute:
//! - Kafka resume position = max end-offset over ALL staged files + 1
//!   (including old-epoch tails — data staged is data owned);
//! - backfill list = staged files with serial_seq > watermark, re-notified in
//!   seq order (identifier idempotency + watermark gate make replays free).

use crate::error::Result;
use crate::naming::StagedFile;

#[derive(Debug, PartialEq, Eq)]
pub struct RecoveryPlan {
    /// The **next** Kafka offset to consume — seek the consumer here.
    /// `None` = the SDK has no record of this writer (nothing ever staged
    /// and no state file): fall back to your own committed offset.
    ///
    /// Pairs with [`TableWriter::staged_offset`](crate::TableWriter::staged_offset),
    /// which is the **last** durably staged offset (inclusive); the two obey
    /// `kafka_resume_offset == staged_offset + 1` when both exist.
    pub kafka_resume_offset: Option<i64>,
    /// Files that may not have reached the server, oldest first.
    /// Internal: `open_table` already re-notifies these; callers never need it.
    #[doc(hidden)]
    pub backfill: Vec<StagedFile>,
    /// Highest (epoch, seq) seen in staging: the writer must continue above
    /// this even when the wall clock went backwards (epoch anti-rollback).
    /// Internal bookkeeping for `open_table`.
    #[doc(hidden)]
    pub max_staged: Option<(u64, u32)>,
}

/// Pure function so the protocol is testable without OSS/postgres.
/// `watermark` is the UDF result (None = no terminal row, or UDF missing and
/// the caller degraded to full re-notify).
pub fn plan_recovery(mut staged: Vec<StagedFile>, watermark: Option<i64>) -> Result<RecoveryPlan> {
    staged.sort_unstable_by_key(|f| (f.epoch_ms, f.seq));

    // Duplicate (epoch, seq) should be impossible under the naming contract
    // (one writer, seqs allocated under a lock, failed puts burn their seq).
    // Seeing one means two objects share a seq -- e.g. two live writers on
    // one writer_id. Warn loudly; recovery itself stays deterministic (both
    // files re-notify, the identifier unique key arbitrates server-side).
    for w in staged.windows(2) {
        if w[0].epoch_ms == w[1].epoch_ms && w[0].seq == w[1].seq {
            tracing::warn!(
                epoch = w[0].epoch_ms,
                seq = w[0].seq,
                "duplicate (epoch, seq) in staging -- two writers may share a writer_id"
            );
        }
    }

    let kafka_resume_offset = staged.iter().map(|f| f.end_offset + 1).max();
    let max_staged = staged.last().map(|f| (f.epoch_ms, f.seq));

    let mut backfill = Vec::new();
    for f in staged {
        let seq = f.serial_seq()?;
        if watermark.map_or(true, |w| seq > w) {
            backfill.push(f);
        }
    }
    Ok(RecoveryPlan {
        kafka_resume_offset,
        backfill,
        max_staged,
    })
}

/// First epoch for a fresh writer session: wall-clock millis, bumped above
/// any epoch already present in staging (anti-rollback).
pub fn next_epoch(now_ms: u64, max_staged: Option<(u64, u32)>) -> u64 {
    match max_staged {
        Some((max_epoch, _)) if now_ms <= max_epoch => max_epoch + 1,
        _ => now_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::naming::encode_serial_seq;

    fn f(epoch: u64, seq: u32, start: i64, end: i64) -> StagedFile {
        StagedFile {
            epoch_ms: epoch,
            seq,
            start_offset: start,
            end_offset: end,
            compressed: false,
        }
    }

    #[test]
    fn empty_staging() {
        let plan = plan_recovery(vec![], None).unwrap();
        assert_eq!(plan.kafka_resume_offset, None);
        assert!(plan.backfill.is_empty());
        assert_eq!(plan.max_staged, None);
    }

    #[test]
    fn resume_spans_epochs() {
        // Old-epoch tail has the highest kafka offset — it still counts.
        let staged = vec![
            f(100, 5, 0, 999),
            f(200, 0, 1000, 1499),
            f(100, 6, 1500, 1999),
        ];
        let plan = plan_recovery(staged, None).unwrap();
        assert_eq!(plan.kafka_resume_offset, Some(2000));
        // No watermark: everything is backfill, in (epoch, seq) order.
        let seqs: Vec<(u64, u32)> = plan.backfill.iter().map(|x| (x.epoch_ms, x.seq)).collect();
        assert_eq!(seqs, vec![(100, 5), (100, 6), (200, 0)]);
        assert_eq!(plan.max_staged, Some((200, 0)));
    }

    #[test]
    fn watermark_prunes_backfill() {
        let staged = vec![f(100, 5, 0, 9), f(100, 6, 10, 19), f(100, 7, 20, 29)];
        let w = encode_serial_seq(100, 6).unwrap();
        let plan = plan_recovery(staged, Some(w)).unwrap();
        // Only seq 7 is above the watermark; 5 and 6 are already terminal.
        assert_eq!(plan.backfill.len(), 1);
        assert_eq!(plan.backfill[0].seq, 7);
        // Kafka position is NOT pruned by the watermark: staged = owned.
        assert_eq!(plan.kafka_resume_offset, Some(30));
    }

    #[test]
    fn watermark_at_head_means_no_backfill() {
        let staged = vec![f(100, 5, 0, 9)];
        let w = encode_serial_seq(100, 5).unwrap();
        let plan = plan_recovery(staged, Some(w)).unwrap();
        assert!(plan.backfill.is_empty());
        assert_eq!(plan.kafka_resume_offset, Some(10));
    }

    #[test]
    fn epoch_anti_rollback() {
        assert_eq!(next_epoch(1000, None), 1000);
        assert_eq!(next_epoch(1000, Some((999, 3))), 1000);
        // Clock went backwards relative to staged data: bump past it.
        assert_eq!(next_epoch(1000, Some((1000, 3))), 1001);
        assert_eq!(next_epoch(1000, Some((5000, 0))), 5001);
    }
}
