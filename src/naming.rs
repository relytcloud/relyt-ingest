//! Deterministic naming & serial_seq encoding.
//!
//! Everything here is a *contract*: object keys are the recovery source of
//! truth (LIST), serial_seq ordering is what the server serializes on, and
//! identifiers are the idempotency key. No UUIDs, no .tmp+rename — a retry
//! overwrites the same object with the same bytes.
//!
//! ## Layout
//!
//! ```text
//! <prefix>/staging/<cluster_id>/<db_oid>/<rel_oid>/<writer_id>/<epoch:013>-<seq:07>-o<start>-<end>.csv[.gz]
//! <prefix>/_meta/  <cluster_id>/<db_oid>/<rel_oid>/<writer_id>/state.json
//! ```
//!
//! - identity is OID-based, so a table rename never orphans staging data or
//!   its serial group; DROP+CREATE of a same-named table gets a NEW rel_oid
//!   and therefore a fresh directory (old objects are reclaimed by the
//!   bucket lifecycle rule mounted on `staging/` only — never mount one on
//!   `_meta/`);
//! - `<cluster_id>` isolates clusters sharing one bucket: db_oid/rel_oid are
//!   only unique within a cluster;
//! - no schema level on purpose: rel_oid is database-unique, and a
//!   namespace level would let `ALTER TABLE ... SET SCHEMA` orphan staging;
//! - epoch is zero-padded to 13 digits and seq to 7, so the LEXICOGRAPHIC
//!   key order equals the numeric `(epoch, seq)` order — incremental GC
//!   LISTs depend on this. 13 digits covers the 43-bit epoch ceiling
//!   (8,796,093,022,207) and every realistic wall-clock millis value.
//!
//! ## Key-prefix invariant
//!
//! No staged file's full key may be a prefix of another key: the Relyt master
//! consumes `source` by prefix. The `.csv` suffix plus the uniqueness of
//! `(epoch, seq)` within a writer directory guarantees this; keep it in mind
//! when adding new object kinds (bookkeeping lives under `_meta/`, never
//! beside the staged files).

use crate::error::{Error, Result};

/// serial_seq = epoch_ms(43bit) << 20 | seq(20bit). SDK-private encoding —
/// the server only relies on it being monotonically increasing per group.
/// 43 + 20 = 63 bits: encode(MAX_EPOCH_MS, MAX_SEQ) == i64::MAX exactly.
pub const SEQ_BITS: u32 = 20;
pub const MAX_SEQ: u32 = (1 << SEQ_BITS) - 1; // ~1.04M files per epoch
pub const MAX_EPOCH_MS: u64 = (1 << 43) - 1;

/// Fixed widths that make key order == numeric order.
pub const EPOCH_PAD: usize = 13;
pub const SEQ_PAD: usize = 7;

/// Server-side identifier limit. With OID-based
/// identities and the writer_id cap below, identifiers are structurally
/// bounded far under this; the constant remains for the defensive check.
pub const IDENTIFIER_MAX_LEN: usize = 128;

/// Longest accepted writer_id. Keeps identifiers bounded and directory names
/// sane; enforced by [`validate_writer_id`] at `open_table`.
pub const WRITER_ID_MAX_LEN: usize = 64;

#[inline]
pub fn encode_serial_seq(epoch_ms: u64, seq: u32) -> Result<i64> {
    if epoch_ms > MAX_EPOCH_MS {
        return Err(Error::Naming(format!("epoch {epoch_ms} exceeds 43 bits")));
    }
    if seq > MAX_SEQ {
        // Callers roll to a fresh epoch before this can fire: the seq space
        // never wraps within one epoch.
        return Err(Error::Naming(format!("seq {seq} exceeds 20 bits")));
    }
    Ok(((epoch_ms << SEQ_BITS) | seq as u64) as i64)
}

/// Inverse of [`encode_serial_seq`]. Not used by the write path — it exists
/// for operators reading a raw `serial_seq` back into `(epoch_ms, seq)` when
/// investigating a stuck group.
#[inline]
pub fn decode_serial_seq(serial_seq: i64) -> (u64, u32) {
    let v = serial_seq as u64;
    (v >> SEQ_BITS, (v & MAX_SEQ as u64) as u32)
}

/// `writer_id` rules, enforced once at `open_table`: 1..=64 chars from
/// `[A-Za-z0-9._-]`, not starting with `_` (the `_meta/` convention reserves
/// the underscore prefix for bookkeeping), no path or identifier separators.
/// The rules every staging path segment obeys: non-empty, at most
/// `WRITER_ID_MAX_LEN` bytes, `[A-Za-z0-9._-]` only, and not a bare run of
/// dots -- `.` and `..` resolve to the enclosing directory once the key is
/// read as a path, which would move `_meta/` bookkeeping up a level. `kind`
/// names the segment in the error text.
fn validate_path_segment(kind: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > WRITER_ID_MAX_LEN {
        return Err(Error::Config(format!(
            "{kind} must be 1..={WRITER_ID_MAX_LEN} chars, got {}",
            value.len()
        )));
    }
    if value.chars().all(|c| c == '.') {
        return Err(Error::Config(format!(
            "{kind} `{value}` is not a valid path segment"
        )));
    }
    if let Some(bad) = value
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return Err(Error::Config(format!(
            "{kind} contains `{bad}`; allowed characters are [A-Za-z0-9._-]"
        )));
    }
    Ok(())
}

pub fn validate_writer_id(writer_id: &str) -> Result<()> {
    validate_path_segment("writer_id", writer_id)?;
    if writer_id.starts_with('_') {
        return Err(Error::Config(
            "writer_id must not start with '_' (reserved for bookkeeping)".into(),
        ));
    }
    Ok(())
}

/// `cluster_id` rules -- it is the first path segment under both `staging/`
/// and `_meta/`, so it gets the same treatment as `writer_id`: 1..=64 chars
/// from `[A-Za-z0-9._-]`, and not a bare `.`/`..` (path traversal would put
/// `state.json`/`lock` somewhere a lifecycle rule can delete them). Applied
/// to the configured value and to what the server returns alike.
pub fn validate_cluster_id(cluster_id: &str) -> Result<()> {
    validate_path_segment("cluster_id", cluster_id)
}

/// The OID-based identity every path and server-side name derives from.
/// Resolved once in `open_table` and immutable for the writer's life.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterIdentity {
    /// The Relyt instance id — one deployed instance (a DWSU) has exactly
    /// one, exposed server-side as `relyt_get_instance_id()` (GUC
    /// `relyt.instanceid`); `ClientConfig::cluster_id` can override it.
    /// Used as the first staging path segment: db/rel OIDs are only unique
    /// within one instance, so instances sharing a bucket need this
    /// namespace.
    pub cluster_id: String,
    pub db_oid: u32,
    pub rel_oid: u32,
    pub writer_id: String,
}

impl WriterIdentity {
    /// `staging/<cluster_id>/<db_oid>/<rel_oid>/<writer_id>/` — trailing slash,
    /// ready for LIST.
    pub fn staging_dir(&self) -> String {
        format!(
            "staging/{}/{}/{}/{}/",
            self.cluster_id, self.db_oid, self.rel_oid, self.writer_id
        )
    }

    /// `_meta/<cluster_id>/<db_oid>/<rel_oid>/<writer_id>/state.json`.
    pub fn state_key(&self) -> String {
        format!(
            "_meta/{}/{}/{}/{}/state.json",
            self.cluster_id, self.db_oid, self.rel_oid, self.writer_id
        )
    }

    /// `_meta/<cluster_id>/<db_oid>/<rel_oid>/<writer_id>/lock` — the writer
    /// lease (see `lock.rs`). Lives beside state.json under `_meta/`, which
    /// never carries a bucket lifecycle rule.
    pub fn lock_key(&self) -> String {
        format!(
            "_meta/{}/{}/{}/{}/lock",
            self.cluster_id, self.db_oid, self.rel_oid, self.writer_id
        )
    }

    /// `<db_oid>:<rel_oid>:<writer_id>` — rename-immune, always well under
    /// the server's 64B cap (10+10+64+2 worst case would exceed it, but
    /// writer_id is capped at 64 TOTAL length checked here defensively).
    pub fn serial_group(&self) -> String {
        let g = format!("{}:{}:{}", self.db_oid, self.rel_oid, self.writer_id);
        debug_assert!(g.len() <= 88, "serial_group unexpectedly long: {g}");
        g
    }
}

/// One staged CSV object, identified entirely by its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedFile {
    pub epoch_ms: u64,
    pub seq: u32,
    /// Kafka offset range [start, end] covered by this file (inclusive).
    pub start_offset: i64,
    pub end_offset: i64,
    /// gzip-compressed (`.csv.gz`) vs plain (`.csv`). Carried in the name so
    /// recovery/GC rebuild the exact key; the server sniffs magic bytes and
    /// needs no hint. `x.csv` IS a key-prefix of `x.csv.gz`, which is fine
    /// only because a seq never has two objects (a failed put burns its seq).
    pub compressed: bool,
}

impl StagedFile {
    pub fn serial_seq(&self) -> Result<i64> {
        encode_serial_seq(self.epoch_ms, self.seq)
    }

    /// Object key relative to the staging prefix:
    /// `staging/<cluster_id>/<db_oid>/<rel_oid>/<writer_id>/<epoch:013>-<seq:07>-o<start>-<end>.csv[.gz]`
    pub fn object_key(&self, ident: &WriterIdentity) -> String {
        format!(
            "{dir}{epoch:0epad$}-{seq:0spad$}-o{start}-{end}{ext}",
            dir = ident.staging_dir(),
            epoch = self.epoch_ms,
            seq = self.seq,
            start = self.start_offset,
            end = self.end_offset,
            epad = EPOCH_PAD,
            spad = SEQ_PAD,
            ext = if self.compressed { ".csv.gz" } else { ".csv" },
        )
    }

    /// Parse the `<epoch>-<seq>-o<start>-<end>.csv` basename; recovery walks
    /// the LIST result through this. Tolerates non-padded segments (numeric
    /// parse ignores leading zeros), so pre-padding objects still parse.
    /// Unknown files are the caller's problem (reported, never silently
    /// skipped).
    pub fn parse_basename(name: &str) -> Option<StagedFile> {
        let (stem, compressed) = match name.strip_suffix(".csv.gz") {
            Some(stem) => (stem, true),
            None => (name.strip_suffix(".csv")?, false),
        };
        // <epoch>-<seq>-o<start>-<end>; offsets may not be negative (enforced
        // at append) so a plain split on '-' is unambiguous.
        let mut parts = stem.split('-');
        let epoch_ms: u64 = parts.next()?.parse().ok()?;
        let seq: u32 = parts.next()?.parse().ok()?;
        let start_offset: i64 = parts.next()?.strip_prefix('o')?.parse().ok()?;
        let end_offset: i64 = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(StagedFile {
            epoch_ms,
            seq,
            start_offset,
            end_offset,
            compressed,
        })
    }

    /// Job identifier: `<db_oid>:<rel_oid>:<writer_id>:<epoch>:<seq>`.
    /// No cluster segment: the job table itself is per-cluster. Structurally
    /// bounded (~108 bytes worst case) — the length check is defensive only.
    pub fn identifier(&self, ident: &WriterIdentity) -> Result<String> {
        let id = format!(
            "{}:{}:{}:{}:{}",
            ident.db_oid, ident.rel_oid, ident.writer_id, self.epoch_ms, self.seq
        );
        if id.len() > IDENTIFIER_MAX_LEN {
            return Err(Error::Naming(format!(
                "identifier `{id}` exceeds {IDENTIFIER_MAX_LEN} bytes"
            )));
        }
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compressed_suffix_round_trips() {
        let ident = WriterIdentity {
            cluster_id: "1024".into(),
            db_oid: 13727,
            rel_oid: 54321,
            writer_id: "w0".into(),
        };
        let gz = StagedFile {
            epoch_ms: 1_700_000_000_000,
            seq: 7,
            start_offset: 10,
            end_offset: 20,
            compressed: true,
        };
        let key = gz.object_key(&ident);
        assert!(key.ends_with("-o10-20.csv.gz"), "{key}");
        let base = key.rsplit('/').next().unwrap();
        assert_eq!(StagedFile::parse_basename(base), Some(gz.clone()));

        let plain = StagedFile {
            compressed: false,
            ..gz
        };
        let key = plain.object_key(&ident);
        assert!(key.ends_with("-o10-20.csv"), "{key}");
        let base = key.rsplit('/').next().unwrap();
        assert_eq!(StagedFile::parse_basename(base), Some(plain));
    }

    fn ident() -> WriterIdentity {
        WriterIdentity {
            cluster_id: "1024".into(),
            db_oid: 13727,
            rel_oid: 54321,
            writer_id: "w-0".into(),
        }
    }

    #[test]
    fn serial_seq_roundtrip_and_ordering() {
        let a = encode_serial_seq(1_724_500_000_000, 0).unwrap();
        let b = encode_serial_seq(1_724_500_000_000, 1).unwrap();
        let c = encode_serial_seq(1_724_500_000_001, 0).unwrap();
        assert!(
            a < b && b < c,
            "newer epoch must outrank any seq of an older epoch"
        );
        assert_eq!(decode_serial_seq(b), (1_724_500_000_000, 1));
        assert!(encode_serial_seq(1, MAX_SEQ + 1).is_err());
        assert!(encode_serial_seq(MAX_EPOCH_MS + 1, 0).is_err());
    }

    #[test]
    fn encoding_saturates_exactly_at_i64_max() {
        // 43 + 20 bits fill an i64 without touching the sign bit.
        assert_eq!(encode_serial_seq(MAX_EPOCH_MS, MAX_SEQ).unwrap(), i64::MAX);
    }

    #[test]
    fn object_key_layout_and_roundtrip() {
        let f = StagedFile {
            epoch_ms: 1_724_500_000_000,
            seq: 42,
            start_offset: 1000,
            end_offset: 1999,
            compressed: false,
        };
        let key = f.object_key(&ident());
        assert_eq!(
            key,
            "staging/1024/13727/54321/w-0/1724500000000-0000042-o1000-1999.csv"
        );
        let basename = key.rsplit('/').next().unwrap();
        assert_eq!(StagedFile::parse_basename(basename).unwrap(), f);
    }

    #[test]
    fn key_order_equals_numeric_order() {
        // The padding exists for exactly this property: lexicographic order
        // of basenames == numeric (epoch, seq) order, including small test
        // epochs that would otherwise have fewer digits.
        let mk = |e, s| {
            StagedFile {
                epoch_ms: e,
                seq: s,
                start_offset: 0,
                end_offset: 0,
                compressed: false,
            }
            .object_key(&ident())
        };
        let keys = [mk(7, 5), mk(7, 40), mk(100, 0), mk(1_724_500_000_000, 0)];
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys.as_slice(), sorted.as_slice());
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(StagedFile::parse_basename("state.json").is_none());
        assert!(StagedFile::parse_basename("1-2-o3-4.parquet").is_none());
        assert!(StagedFile::parse_basename("1-2-3-4.csv").is_none());
        assert!(StagedFile::parse_basename("1-2-o3-4-5.csv").is_none());
    }

    #[test]
    fn identifier_is_oid_based() {
        let f = StagedFile {
            epoch_ms: 1,
            seq: 2,
            start_offset: 0,
            end_offset: 9,
            compressed: false,
        };
        assert_eq!(f.identifier(&ident()).unwrap(), "13727:54321:w-0:1:2");
    }

    #[test]
    fn serial_group_is_oid_based() {
        assert_eq!(ident().serial_group(), "13727:54321:w-0");
    }

    #[test]
    fn writer_id_rules() {
        assert!(validate_writer_id("kafka-p0").is_ok());
        assert!(validate_writer_id("W.9_x").is_ok());
        assert!(validate_writer_id("").is_err());
        assert!(validate_writer_id("_meta").is_err()); // reserved prefix
        assert!(validate_writer_id("a/b").is_err()); // path separator
        assert!(validate_writer_id("a:b").is_err()); // identifier separator
        assert!(validate_writer_id("..").is_err()); // traversal, same rule as cluster_id
        assert!(validate_writer_id(".").is_err());
        assert!(validate_writer_id(&"x".repeat(65)).is_err());
    }

    #[test]
    fn cluster_id_rules() {
        assert!(validate_cluster_id("1024").is_ok()); // what relyt_get_instance_id() returns
        assert!(validate_cluster_id("dwsu-prod.01_a").is_ok());
        assert!(validate_cluster_id("").is_err());
        assert!(validate_cluster_id("a/b").is_err()); // would add a path level
        assert!(validate_cluster_id("..").is_err()); // traversal
        assert!(validate_cluster_id(".").is_err());
        assert!(validate_cluster_id("a:b").is_err());
        assert!(validate_cluster_id("a b").is_err());
        assert!(validate_cluster_id(&"x".repeat(65)).is_err());
    }
}
