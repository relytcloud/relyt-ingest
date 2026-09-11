//! Persistent writer state: `_meta/<cluster_id>/<db_oid>/<rel_oid>/<writer_id>/state.json`.
//!
//! One small JSON object per writer, three jobs:
//!
//! - `resume_offset`: the Kafka end-offset of the newest file the SERVER has
//!   accepted. Recovery takes `max(resume_offset + 1, LIST-derived)` as the
//!   resume position, so a lagging value only costs re-reading offsets that
//!   staging already owns — it must never lead what the server confirmed.
//! - `max_epoch_used`: anti-rollback anchor. `next_epoch` takes the max of
//!   this, the LIST-derived epoch and the server watermark's epoch, closing
//!   the "clock went backwards + staged objects already GC-ed" hole.
//! - the name fields: cross-checks. db_oid/rel_oid are small per-cluster
//!   integers; two clusters mistakenly sharing one staging prefix WILL
//!   collide on them, and comparing the recorded names against the current
//!   resolution is the one executable guard for the "prefix per cluster"
//!   deployment rule. A rename (same OIDs, different name) is legal and only
//!   logged.
//!
//! Read policy is fail-loud: NotFound means a fresh
//! writer; an object that exists but cannot be read or parsed is an ERROR,
//! never silently treated as absent — that would quietly disable both the
//! resume shortcut and the anti-rollback anchor. The operator escape hatch
//! is deleting the object.

use serde::{Deserialize, Serialize};

pub const STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateFile {
    pub v: u32,
    /// Kafka end-offset of the newest server-confirmed file; `None` until
    /// the first confirmation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_offset: Option<i64>,
    /// Highest epoch this writer has ever selected (inclusive).
    pub max_epoch_used: u64,
    /// The Relyt instance id (DWSU) this state was written under — the
    /// cross-instance collision guard for a shared bucket.
    pub cluster_id: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub writer_id: String,
    /// Wall-clock millis of the last write; doubles as a liveness beacon for
    /// the consumption-lag alarm. The `_meta/` prefix carries no lifecycle
    /// rule, so this is bookkeeping, not an expiry defence.
    pub updated_at_ms: u64,
}

impl StateFile {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec_pretty(self).expect("StateFile serialization is infallible")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<StateFile, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let s = StateFile {
            v: STATE_VERSION,
            resume_offset: Some(1234567),
            max_epoch_used: 1_787_905_758_194,
            cluster_id: "1024".into(),
            database: "prod".into(),
            schema: "public".into(),
            table: "events".into(),
            writer_id: "kafka-p0".into(),
            updated_at_ms: 1_787_905_758_194,
        };
        assert_eq!(StateFile::from_bytes(&s.to_bytes()).unwrap(), s);
    }

    #[test]
    fn resume_offset_absent_roundtrips_as_none() {
        let s = StateFile {
            v: STATE_VERSION,
            resume_offset: None,
            max_epoch_used: 5,
            cluster_id: "c".into(),
            database: "d".into(),
            schema: "s".into(),
            table: "t".into(),
            writer_id: "w".into(),
            updated_at_ms: 1,
        };
        let bytes = s.to_bytes();
        assert!(!String::from_utf8_lossy(&bytes).contains("resume_offset"));
        assert_eq!(StateFile::from_bytes(&bytes).unwrap(), s);
    }

    #[test]
    fn garbage_is_an_error_not_none() {
        assert!(StateFile::from_bytes(b"{not json").is_err());
        assert!(StateFile::from_bytes(b"{}").is_err()); // missing fields
    }
}
