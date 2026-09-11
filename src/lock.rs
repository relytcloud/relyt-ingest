//! Writer lease lock: at most one live process per `(table, writer_id)`.
//!
//! Why it exists: two processes sharing a writer identity interleave their
//! seq allocation in one serial group. Once the group watermark passes the
//! faster process's seq, the slower one's submissions are swallowed by the
//! server's replay gate as already-consumed — it believes they succeeded and
//! advances its resume offset: silent data loss, plus both processes
//! trampling one state.json. The lock turns that misconfiguration into a
//! loud error at `open_table`, or a fenced writer within one heartbeat.
//!
//! Protocol (optimistic write-then-read-back — OSS has no conditional PUT in
//! opendal 0.50, so the same non-atomic protocol runs on every backend):
//!
//! 1. acquire: read the lock object; a holder with a fresh heartbeat wins
//!    (`Err(WriterLocked)`), a stale or absent one is claimable. Write our
//!    own record, wait a settle delay, read back: still us -> held, someone
//!    else -> they won.
//! 2. heartbeat (every `lock_heartbeat_interval`): read back FIRST — a
//!    different uuid means we were preempted (clock skew, a long GC pause, a
//!    force-takeover): mark the writer fenced and stop writing. Otherwise
//!    rewrite with a fresh `heartbeat_at_ms`.
//! 3. release (writer drop): read back, delete only if still ours.
//!
//! This is a lease, not a mutex: a paused-and-resumed process can still slip
//! a write into the takeover window. The server-side same-seq gate (23P01)
//! and the watermark stay the correctness backstop; the lock's job is fast,
//! loud detection. `lock_lease_timeout` must dwarf worst-case clock skew
//! between writer hosts (default 180s vs a 30s heartbeat).

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Contents of `_meta/.../lock`. Everything except `instance_uuid` is for
/// the human reading an "already locked" error or the object itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockFile {
    pub v: u32,
    /// Random per-process identity; ownership comparisons use only this.
    pub instance_uuid: String,
    pub pid: u32,
    pub host: String,
    pub acquired_at_ms: u64,
    /// Refreshed by the heartbeat; a reader treats the lock as stale once
    /// `now - heartbeat_at_ms > lock_lease_timeout`.
    pub heartbeat_at_ms: u64,
}

pub const LOCK_VERSION: u32 = 1;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Best available machine identity: the kernel's hostname (authoritative on
/// Linux, and what a K8s pod restart preserves), falling back to $HOSTNAME,
/// then to a sentinel that DISABLES same-host reasoning (two hosts both
/// named "unknown-host" must never look like one machine).
pub const UNKNOWN_HOST: &str = "unknown-host";

pub fn hostname() -> String {
    if let Ok(h) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
        let h = h.trim();
        if !h.is_empty() {
            return h.to_string();
        }
    }
    std::env::var("HOSTNAME").unwrap_or_else(|_| UNKNOWN_HOST.into())
}

/// Process identity for lock ownership. Random entropy (not host+pid alone):
/// pids recycle, and two containers can share both hostname and pid 1.
pub fn new_instance_uuid() -> String {
    let mut h = RandomState::new().build_hasher();
    h.write_u64(now_ms());
    let r1 = h.finish();
    let mut h2 = RandomState::new().build_hasher();
    h2.write_u64(r1);
    format!(
        "{}:{}:{r1:016x}{:016x}",
        hostname(),
        std::process::id(),
        h2.finish()
    )
}

impl LockFile {
    pub fn new(instance_uuid: &str) -> Self {
        let now = now_ms();
        Self {
            v: LOCK_VERSION,
            instance_uuid: instance_uuid.to_string(),
            pid: std::process::id(),
            host: hostname(),
            acquired_at_ms: now,
            heartbeat_at_ms: now,
        }
    }

    pub fn is_stale(&self, lease_timeout_ms: u64) -> bool {
        // saturating: a holder clock ahead of ours must not look stale.
        now_ms().saturating_sub(self.heartbeat_at_ms) > lease_timeout_ms
    }

    /// Same-host death check: the holder ran on THIS machine and its pid is
    /// gone, so the lease can be taken over immediately instead of waiting
    /// out the timeout (the kill -9 / container-restart fast path). Strictly
    /// conservative: any doubt — different or unknown host, pid still
    /// present (even if recycled), non-Linux where /proc is absent — means
    /// "not provably dead" and the caller falls back to lease expiry.
    pub fn provably_dead_on_this_host(&self) -> bool {
        if !cfg!(target_os = "linux") {
            return false;
        }
        if self.host == UNKNOWN_HOST || self.host != hostname() {
            return false;
        }
        !std::path::Path::new(&format!("/proc/{}", self.pid)).exists()
    }

    /// One line for error messages: who holds it and how fresh they are.
    pub fn describe(&self) -> String {
        format!(
            "held by {} (pid {}) on {}, last heartbeat {}s ago",
            self.instance_uuid,
            self.pid,
            self.host,
            now_ms().saturating_sub(self.heartbeat_at_ms) / 1000
        )
    }
}
