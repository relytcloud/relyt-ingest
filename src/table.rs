//! `TableWriter`: buffered append + rotation + flush.
//!
//! `append` is non-blocking in the common case: batches go into an in-memory
//! buffer and the call returns. Rotation (dedup -> CSV -> OSS put -> notify
//! enqueue) happens when either threshold trips (size / age), on an explicit
//! `flush`, or on the background age ticker. The customer contract: commit the
//! Kafka offset only after `flush().await` returned (or `staged_offset()` >=
//! the batch's last offset).
//!
//! Rotation is strictly serialised under the writer state lock, and that is
//! load-bearing rather than incidental:
//!
//! - the buffer is only ever drained by the task that will also put and
//!   enqueue it, so a failed put can put the batches back (no silent hole
//!   between "buffer emptied" and "object on OSS");
//! - seqs are allocated, staged and announced in one critical section, so the
//!   server sees a group's files in increasing seq order. Out-of-order
//!   arrival would let a later file reach FINISH first, raise the group
//!   watermark past an earlier seq, and make the server swallow that earlier
//!   file as an already-consumed replay -- a silently skipped load.
//!
//! The cost is that an `append` which trips the size threshold pays the OSS
//! latency, and concurrent appends queue behind it. TODO: move rotation
//! onto a dedicated writer task fed by a channel, which keeps the ordering
//! guarantee without blocking producers.

use std::cmp;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;

use crate::config::{ClientConfig, StagingCompression, StreamMode};
use crate::csv::CsvFormatter;
use crate::dedup::dedup_last_wins;
use crate::error::{Error, Result};
use crate::naming::{StagedFile, WriterIdentity};
use crate::notify::{Notifier, NotifyRequest};
use crate::schema::TableSchema;
use crate::staging::{StagingHandle, StagingStore};
use crate::state::{StateFile, STATE_VERSION};

pub struct TableWriter {
    inner: Arc<WriterInner>,
}

/// Point-in-time consumption-lag sample, refreshed every ~30s by a
/// background task. Feed it to your metrics system via [`TableWriter::lag`];
/// `lag_seconds` is the alerting unit (an offset gap has no stable meaning
/// across traffic levels).
#[derive(Debug, Clone)]
pub struct LagSnapshot {
    /// Age of the OLDEST staged file the server has not consumed yet
    /// (derived from the file's creation epoch vs the server's group
    /// watermark). 0 = fully caught up.
    pub lag_seconds: u64,
    /// Number of staged files above the server watermark.
    pub lag_files: usize,
    /// Age of the oldest in-memory (not yet staged) batch — the batching
    /// leg of the pipeline. 0 = empty buffer.
    pub buffered_age_seconds: u64,
    /// When this sample was taken.
    pub sampled_at: std::time::SystemTime,
}

struct WriterInner {
    state: Mutex<WriterState>,
    schema: TableSchema,
    /// OID-based identity every path and server-side name derives from.
    /// Its serial_group doubles as the notify-report key.
    ident: WriterIdentity,
    /// (database, schema, table) names at open time — recorded into
    /// state.json for the cross-cluster collision check and for operators.
    names: (String, String, String),
    serial_group: String,
    cfg: ClientConfig,
    /// The staging location actually in use -- the customer's own bucket or
    /// what the master handed back -- as a swappable handle: under
    /// Relyt-managed staging the credentials inside it are refreshed while
    /// this writer runs. Every operation takes the snapshot current at that
    /// moment (`store()` / `url_base()`).
    staging: Arc<StagingHandle>,
    notifier: Arc<Notifier>,
    /// This process's lease identity (`lock.rs`); the heartbeat task renews
    /// `_meta/.../lock` under it and fences the writer if the lock is lost.
    instance_uuid: String,
    /// Some(reason) once the lease heartbeat found the lock held by someone
    /// else (or unverifiable for a whole lease period): every append/flush
    /// then fails with WriterFenced. Never reset — a fenced writer must be
    /// reopened, not resumed.
    ///
    /// `std::sync::Mutex`, not the tokio one, because every access is a read
    /// or a write of one small value with no IO in between. The rule that
    /// makes that safe: a guard from this mutex (or from `lag` below) must
    /// never be alive across an `.await`, or the task parks holding it and
    /// whoever needs it next waits on a future that may never be polled.
    /// The spawned tasks are proved by the compiler (`tokio::spawn` demands
    /// `Send`, and `MutexGuard` is not), but `append`/`flush`/`close` are
    /// public async fns whose `Send`-ness the caller decides, so there the
    /// compiler will not catch it -- CI runs
    /// `clippy::await_holding_lock` as a deny to cover both.
    fenced: std::sync::Mutex<Option<String>>,
    /// Latest consumption-lag sample (see [`LagSnapshot`]); None until the
    /// first background sample lands.
    lag: std::sync::Mutex<Option<LagSnapshot>>,
}

struct WriterState {
    /// Buffered batches with their Kafka offset ranges [start, end].
    buffered: Vec<(RecordBatch, i64, i64)>,
    /// Sum of `get_array_memory_size()` over `buffered`. Stand-in for the
    /// size threshold only until the first rotation measures `avg_row_bytes`
    /// (arrow memory over-bills sliced batches by their whole backing
    /// buffer, so it is never used once a real CSV measurement exists).
    buffered_bytes_estimate: u64,
    /// When the oldest still-buffered batch arrived — what the age half of
    /// the rotation threshold (`rotate_interval_max`) compares against.
    /// None whenever the buffer is empty; (re)set by the next `append`.
    oldest_buffered_at: Option<Instant>,
    /// Current epoch: the high 43 bits of every serial_seq staged from now
    /// on. Chosen at `open_table` as max(LIST, server watermark,
    /// state.max_epoch_used) so it never rolls back; rolled forward (state
    /// persisted first) when `next_seq` hits MAX_SEQ.
    epoch_ms: u64,
    /// Next unused seq within `epoch_ms` (the low 20 bits). Allocated under
    /// the rotation lock; a failed rotation does NOT return its seq — the
    /// hole is harmless, reuse could bind two files to one seq.
    next_seq: u32,
    /// Every file this writer knows to be staged and not yet consumed, as
    /// (serial_seq, epoch_ms): seeded from the recovery listing at open, one
    /// entry appended per rotation, pruned by the lag sampler as the server
    /// watermark passes them. The lag numbers are computed from this list.
    known_files: Vec<(i64, u64)>,
    /// Rows currently buffered (for the CSV-byte size estimate).
    buffered_rows: usize,
    /// Average CSV bytes per surviving row, measured on the last rotation.
    /// None until the first rotation; the arrow memory estimate covers the
    /// first file.
    avg_row_bytes: Option<f64>,
    /// Highest Kafka end-offset that is durably staged (file on OSS).
    staged_offset: Option<i64>,
    /// resume_offset recorded in the last state.json write, so the ticker
    /// only pays for a write when the confirmation actually moved. Seeded at
    /// open with the value recovered from state.json: it must never regress
    /// to None, or the idle heartbeat rewrite would erase the recorded offset.
    resume_persisted: Option<i64>,
    /// Wall-clock of the last state.json write; the ticker refreshes it
    /// periodically even without progress (liveness beacon).
    state_written_at: Instant,
}

impl TableWriter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: TableSchema,
        ident: WriterIdentity,
        names: (String, String, String),
        cfg: ClientConfig,
        staging: Arc<StagingHandle>,
        notifier: Arc<Notifier>,
        initial_epoch_ms: u64,
        staged_offset: Option<i64>,
        resume_persisted: Option<i64>,
        known_files: Vec<(i64, u64)>,
        instance_uuid: String,
    ) -> Self {
        let serial_group = ident.serial_group();
        let inner = Arc::new(WriterInner {
            state: Mutex::new(WriterState {
                buffered: Vec::new(),
                buffered_bytes_estimate: 0,
                oldest_buffered_at: None,
                buffered_rows: 0,
                avg_row_bytes: None,
                epoch_ms: initial_epoch_ms,
                next_seq: 0,
                staged_offset,
                resume_persisted,
                known_files,
                state_written_at: Instant::now(),
            }),
            schema,
            ident,
            names,
            serial_group,
            cfg,
            staging,
            notifier,
            instance_uuid,
            fenced: std::sync::Mutex::new(None),
            lag: std::sync::Mutex::new(None),
        });
        spawn_ticker(&inner);
        spawn_gc(&inner);
        spawn_lock_heartbeat(&inner);
        spawn_lag_monitor(&inner);
        Self { inner }
    }

    /// The writer's OID-based identity (paths, identifiers, serial group all
    /// derive from it). Diagnostics/e2e affordance, not supported API.
    #[doc(hidden)]
    pub fn identity(&self) -> &WriterIdentity {
        &self.inner.ident
    }

    /// The serial group this writer submits under.
    pub fn serial_group(&self) -> &str {
        &self.inner.serial_group
    }

    /// Arrow schema of the target table (whitelisted types only).
    pub fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema.arrow.clone()
    }

    /// Buffered append: enqueue `batch` covering Kafka offsets
    /// `[start_offset, end_offset]` and return. Durability comes from `flush`.
    pub async fn append(
        &self,
        batch: RecordBatch,
        start_offset: i64,
        end_offset: i64,
    ) -> Result<()> {
        self.inner.check_health()?;
        // Offsets are encoded into the staged file name, and the recovery
        // parser splits on '-' assuming they are non-negative. An unchecked
        // negative offset would produce a name that parses back wrong (or not
        // at all), silently shrinking the recovery set.
        if start_offset < 0 || end_offset < start_offset {
            return Err(Error::Config(format!(
                "invalid Kafka offset range [{start_offset}, {end_offset}]: \
                 offsets must be >= 0 and end >= start"
            )));
        }
        if !schema_compatible(&batch.schema(), &self.inner.schema.arrow) {
            return Err(Error::Schema(
                "append batch schema != table schema (column names / types)".into(),
            ));
        }
        let should_rotate = {
            let mut st = self.inner.state.lock().await;
            st.buffered_bytes_estimate += batch.get_array_memory_size() as u64;
            st.buffered_rows += batch.num_rows();
            st.buffered.push((batch, start_offset, end_offset));
            if st.oldest_buffered_at.is_none() {
                st.oldest_buffered_at = Some(Instant::now());
            }
            // Size threshold in the unit that matters: rendered CSV bytes.
            // The average row width from the last rotation calibrates it;
            // until one exists, the arrow memory estimate stands in. This
            // also neutralises the sliced-batch problem, where
            // get_array_memory_size bills a tiny slice for its whole backing
            // buffer and would otherwise force a rotation of a few rows.
            let est_bytes = match st.avg_row_bytes {
                Some(avg) => (st.buffered_rows as f64 * avg) as u64,
                None => st.buffered_bytes_estimate,
            };
            est_bytes >= self.inner.cfg.rotate_size_bytes
                || st
                    .oldest_buffered_at
                    .map(|t| t.elapsed() >= self.inner.cfg.rotate_interval_max)
                    .unwrap_or(false)
        };
        if should_rotate {
            self.inner.rotate().await?;
        }
        Ok(())
    }

    /// Force-stage everything buffered; returns the staged offset (highest
    /// Kafka end-offset durable on OSS) after the write.
    pub async fn flush(&self) -> Result<Option<i64>> {
        self.inner.check_health()?;
        self.inner.rotate().await?;
        Ok(self.inner.state.lock().await.staged_offset)
    }

    /// The **last** Kafka offset (inclusive) that is durably staged.
    ///
    /// This is the consumer-commit gate: commit offsets up to `end` only
    /// once `staged_offset() >= end` (a `flush` that returns `Ok` already
    /// guarantees it for everything appended before the call). Pairs with
    /// [`RecoveryPlan::kafka_resume_offset`](crate::RecoveryPlan), which is
    /// the **next** offset to consume.
    pub async fn staged_offset(&self) -> Option<i64> {
        self.inner.state.lock().await.staged_offset
    }

    /// `Some(reason)` once this writer hit a non-retryable serial-contract
    /// violation: its notifications are being dropped and an operator has to
    /// resolve the writer-identity clash before it can make progress again.
    pub fn fatal_error(&self) -> Option<String> {
        self.inner.notifier.fatal(&self.inner.serial_group)
    }

    /// Latest consumption-lag sample (refreshed ~every 30s in the
    /// background; `None` until the first sample). Export `lag_seconds` to
    /// your monitoring system — alerting guidance lives in GUIDE.md.
    pub fn lag(&self) -> Option<LagSnapshot> {
        self.inner.lag.lock().unwrap().clone()
    }

    /// Graceful shutdown: drain the buffer (one final rotation), persist the
    /// resume state, then release the writer lease so a successor can start
    /// immediately instead of waiting out `lock_lease_timeout`. Call this
    /// from your SIGTERM handler; a writer that is merely dropped still
    /// releases its lease, but only within ~2s and without the final drain.
    /// Returns the last durably staged offset.
    ///
    /// A fenced writer has nothing to drain (writes were already refused)
    /// and the lease belongs to its successor — `close` then returns the
    /// fencing error and touches nothing.
    pub async fn close(self) -> Result<Option<i64>> {
        if let Some(reason) = self.inner.fenced.lock().unwrap().clone() {
            return Err(Error::WriterFenced(reason));
        }
        self.inner.rotate().await?;
        self.inner.persist_state_if_due().await;
        let staged = self.inner.state.lock().await.staged_offset;
        if let Ok(Some(l)) = self.inner.store().read_lock(&self.inner.ident).await {
            if l.instance_uuid == self.inner.instance_uuid {
                let _ = self.inner.store().delete_lock(&self.inner.ident).await;
            }
        }
        tracing::info!(
            group = %self.inner.serial_group,
            staged_offset = ?staged,
            "writer closed: buffer drained, lease released"
        );
        Ok(staged)
    }
}

impl WriterInner {
    /// The store of the staging snapshot current right now.
    fn store(&self) -> StagingStore {
        self.staging.current().store.clone()
    }

    /// The url base of the staging snapshot current right now.
    fn url_base(&self) -> String {
        self.staging.current().url_base.clone()
    }

    fn is_fenced(&self) -> bool {
        self.fenced.lock().unwrap().is_some()
    }

    fn check_health(&self) -> Result<()> {
        if let Some(reason) = self.fenced.lock().unwrap().clone() {
            return Err(Error::WriterFenced(reason));
        }
        match self.notifier.fatal(&self.serial_group) {
            Some(msg) => Err(Error::SerialContractViolation(msg)),
            None => Ok(()),
        }
    }

    /// Rotate the current buffer into exactly one staged CSV object + one
    /// notify request. One append burst = at most one object per rotation
    /// under the double threshold.
    ///
    /// Runs entirely under the state lock — see the module docs for why the
    /// drain/put/enqueue triple must not be split.
    async fn rotate(&self) -> Result<()> {
        let rotate_started = Instant::now();
        let mut st = self.state.lock().await;
        if st.buffered.is_empty() {
            return Ok(());
        }

        // Everything needed to undo the drain if staging fails. The consumed
        // (epoch, seq) is deliberately NOT part of the rollback: a retry uses
        // a fresh seq and the failed one stays a hole. Holes are harmless --
        // the group watermark is a high-water mark and the scheduler only
        // gates on rows that exist -- whereas REUSING a seq after a put of
        // unknown fate risks two different objects claiming one seq if the
        // first write actually landed.
        let prev_bytes = st.buffered_bytes_estimate;
        let prev_oldest = st.oldest_buffered_at;

        let prev_rows = st.buffered_rows;
        let batches: Vec<(RecordBatch, i64, i64)> = std::mem::take(&mut st.buffered);
        st.buffered_bytes_estimate = 0;
        st.buffered_rows = 0;
        st.oldest_buffered_at = None;

        let start = batches.iter().map(|(_, s, _)| *s).min().unwrap();
        let end = batches.iter().map(|(_, _, e)| *e).max().unwrap();

        if st.next_seq > crate::naming::MAX_SEQ {
            // Seq exhausted: roll to a fresh epoch.
            st.epoch_ms += 1;
            st.next_seq = 0;
        }
        let file = StagedFile {
            epoch_ms: st.epoch_ms,
            seq: st.next_seq,
            start_offset: start,
            end_offset: end,
            compressed: self.cfg.staging_compression == StagingCompression::Gzip,
        };
        st.next_seq += 1;

        // Encode the serial_seq and identifier BEFORE any IO: an
        // out-of-range epoch must fail here, not after a successful put
        // where it would strand an orphan object the server was never told
        // about and the rollback cannot classify.
        let serial_seq = file.serial_seq()?;
        let identifier = file.identifier(&self.ident)?;
        let object_key = file.object_key(&self.ident);

        let restore = |st: &mut WriterState, batches: Vec<(RecordBatch, i64, i64)>| {
            // Nothing else can have touched the buffer: appends need the same
            // lock, which this task holds for the whole rotation.
            st.buffered = batches;
            st.buffered_bytes_estimate = prev_bytes;
            st.buffered_rows = prev_rows;
            st.oldest_buffered_at = prev_oldest.or_else(|| Some(Instant::now()));
        };

        let avg_row_bytes = st.avg_row_bytes;
        let staged_stats = match self.stage(&batches, &object_key, avg_row_bytes).await {
            Ok(stats) => stats,
            Err(e) => {
                restore(&mut st, batches);
                return Err(e);
            }
        };
        if staged_stats.rows > 0 {
            st.avg_row_bytes = Some(staged_stats.bytes as f64 / staged_stats.rows as f64);
        }

        // Durable now: record the offset, then announce it. An enqueue failure
        // means the notify task is gone (process shutting down); the object is
        // on OSS, so recovery backfill still owns it -- do not roll back.
        st.staged_offset = Some(match st.staged_offset {
            Some(prev) => prev.max(file.end_offset),
            None => file.end_offset,
        });
        st.known_files.push((serial_seq, file.epoch_ms));
        tracing::info!(
            db = %self.names.0,
            table = %format!("{}.{}", self.names.1, self.names.2),
            writer_id = %self.ident.writer_id,
            object = %format!("{}/{}", self.url_base(), object_key),
            start_offset = file.start_offset,
            end_offset = file.end_offset,
            rows = staged_stats.rows,
            deduped = prev_rows.saturating_sub(staged_stats.rows),
            bytes = staged_stats.bytes,
            stored_bytes = staged_stats.stored_bytes,
            serial_seq,
            elapsed_ms = rotate_started.elapsed().as_millis() as u64,
            "staged file durable"
        );
        // Serial fields are unconditional: insert-only streams run with M=1
        // serialization too — that is what gives them a server-side group
        // watermark for recovery pruning. Only the upsert load mode and the
        // intra-file PK dedup stay Upsert-specific.
        self.notifier.enqueue(NotifyRequest {
            serial_group: self.serial_group.clone(),
            end_offset: file.end_offset,
            identifier,
            source_url: format!("{}/{}", self.url_base(), object_key),
            target: self.ident.rel_oid,
            delimiter: self.cfg.csv.delimiter,
            upsert: self.cfg.stream_mode == StreamMode::Upsert,
            serial_seq,
            retry_max: self.cfg.retry_max.or(Some(-1)),
        })?;
        Ok(())
    }

    /// Dedup + serialize + put. Split out so `rotate` has a single fallible
    /// step to roll back around. Returns the surviving row count and the
    /// rendered CSV size, which calibrate the size threshold.
    ///
    /// `avg_row_bytes` is the previous rotation's measurement (None on the
    /// first one) and only sizes the output buffer up front.
    async fn stage(
        &self,
        batches: &[(RecordBatch, i64, i64)],
        object_key: &str,
        avg_row_bytes: Option<f64>,
    ) -> Result<StageStats> {
        // 1. Intra-file PK dedup, last write wins (hard requirement).
        let only_batches: Vec<RecordBatch> = batches.iter().map(|(b, _, _)| b.clone()).collect();
        let keep = if self.cfg.stream_mode == StreamMode::Upsert {
            dedup_last_wins(&only_batches, &self.schema.pk_indices()?)?
        } else {
            only_batches
                .iter()
                .map(|b| (0..b.num_rows()).collect())
                .collect()
        };

        // 2. Serialize to CSV (header always on).
        let fmt = CsvFormatter::new(self.cfg.csv.delimiter);
        let names: Vec<&str> = self
            .schema
            .arrow
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let mut body = fmt.header(&names);
        // Reserve the whole file up front from the measured average: without
        // it a 64MB body grows through ~20 reallocation +
        // copy rounds, all under the rotation lock.
        {
            let rows_total: usize = keep.iter().map(|r| r.len()).sum();
            let per_row = avg_row_bytes.unwrap_or(128.0).max(1.0);
            let est = (rows_total as f64 * per_row) as usize;
            body.reserve(est.min(MAX_CSV_RESERVE));
        }
        let mut kept_rows = 0usize;
        for (b, rows) in only_batches.iter().zip(keep.iter()) {
            kept_rows += rows.len();
            fmt.format_rows(b, rows, &mut body)?;
        }
        let bytes = body.len();

        // 3. Optional gzip, then a deterministic-name put (a retry overwrites
        //    the same object). gzip level 6 is the bandwidth/CPU sweet spot
        //    for CSV; the server sniffs the magic bytes, no option needed.
        let payload = if self.cfg.staging_compression == StagingCompression::Gzip {
            use std::io::Write;
            let mut enc = flate2::write::GzEncoder::new(
                Vec::with_capacity(bytes / 4),
                flate2::Compression::default(),
            );
            enc.write_all(body.as_bytes())
                .and_then(|_| enc.finish())
                .map_err(|e| Error::Config(format!("gzip of staged CSV failed: {e}")))?
        } else {
            body.into_bytes()
        };
        let stored_bytes = payload.len();
        // Buffer, not Vec: a retry after a credential refresh needs the bytes
        // again, and a Buffer clone is a reference count, not a copy.
        let payload = opendal::Buffer::from(payload);
        if let Err(e) = self.store().put(object_key, payload.clone()).await {
            // A denied upload under Relyt-managed staging is most likely a key
            // the master has since rotated: refresh once, and retry once only
            // if that produced a different key. Anything else propagates.
            let denied =
                matches!(&e, Error::Storage(s) if s.kind() == opendal::ErrorKind::PermissionDenied);
            if !(denied
                && crate::client::refresh_managed_staging(&self.staging, "upload denied").await?)
            {
                return Err(e);
            }
            self.store().put(object_key, payload).await?;
        }
        Ok(StageStats {
            rows: kept_rows,
            bytes,
            stored_bytes,
        })
    }

    /// Rotate if the oldest buffered row has aged past `rotate_interval_max`.
    /// The threshold is evaluated here rather than only inside `append` so it
    /// still fires when the partition goes idle mid-buffer.
    async fn rotate_if_aged(&self) -> Result<()> {
        // Same gate as append/flush: a fenced writer must not stage anything
        // more, whatever path asks for it.
        self.check_health()?;
        let aged = {
            let st = self.state.lock().await;
            !st.buffered.is_empty()
                && st
                    .oldest_buffered_at
                    .map(|t| t.elapsed() >= self.cfg.rotate_interval_max)
                    .unwrap_or(false)
        };
        if aged {
            self.rotate().await?;
        }
        Ok(())
    }

    /// Refresh `_meta/.../state.json`: advance `resume_offset` when the
    /// notify loop confirmed new files (never past what the server accepted;
    /// lagging only costs a redundant idempotent re-notify on recovery), and
    /// rewrite periodically even without progress so `updated_at_ms` doubles
    /// as a liveness beacon for the consumption-lag alarm.
    async fn persist_state_if_due(&self) {
        // A fenced writer no longer owns state.json: the new holder rewrote it
        // with its own (larger) epoch, and our stale copy must not overwrite
        // that anchor.
        if self.is_fenced() {
            return;
        }
        let heartbeat_every = self.cfg.state_heartbeat_interval;

        let confirmed_end = self.notifier.confirmed(&self.serial_group).map(|(_, e)| e);
        let (resume, epoch, due) = {
            let st = self.state.lock().await;
            let advanced = match (confirmed_end, st.resume_persisted) {
                (Some(c), Some(p)) => c > p,
                (Some(_), None) => true,
                (None, _) => false,
            };
            let heartbeat = st.state_written_at.elapsed() >= heartbeat_every;
            if !advanced && !heartbeat {
                return;
            }
            // resume_offset only ever moves forward.
            let resume = match (confirmed_end, st.resume_persisted) {
                (Some(c), Some(p)) => Some(c.max(p)),
                (Some(c), None) => Some(c),
                (None, p) => p,
            };
            (resume, st.epoch_ms, true)
        };
        debug_assert!(due);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let state = StateFile {
            v: STATE_VERSION,
            resume_offset: resume,
            max_epoch_used: epoch,
            cluster_id: self.ident.cluster_id.clone(),
            database: self.names.0.clone(),
            schema: self.names.1.clone(),
            table: self.names.2.clone(),
            writer_id: self.ident.writer_id.clone(),
            updated_at_ms: now_ms,
        };
        // Re-check right before the write: the heartbeat may have fenced us
        // while the state lock was released above.
        if self.is_fenced() {
            return;
        }
        match self.store().write_state(&self.ident, &state).await {
            Ok(()) => {
                let mut st = self.state.lock().await;
                st.resume_persisted = resume;
                st.state_written_at = Instant::now();
            }
            Err(e) => {
                // Non-fatal: state is a shortcut + beacon; recovery still has
                // LIST and the server watermark.
                tracing::warn!(error = %e, group = %self.serial_group,
                    "failed to persist writer state, will retry on the next tick");
            }
        }
    }
}

impl WriterInner {
    /// One staging-GC pass. An object is deleted only when ALL
    /// of these hold:
    ///   1. its serial_seq <= the SERVER group watermark (consumed);
    ///   2. it is older than `gc_retain_days` (age = now - its epoch, the
    ///      write-time wall clock embedded in the name);
    ///   3. deleting it keeps the directory at >= `gc_retain_min_files`.
    ///
    /// Hard rules: the newest object is never deleted (it feeds the resume
    /// computation even when fully consumed); unknown objects are warned
    /// about, never deleted (evidence); a failed watermark query skips the
    /// whole pass — stale or guessed watermarks must never drive deletion.
    ///
    /// Operational coupling worth knowing: a poison file parks the group
    /// head in FAIL, the watermark stops, and GC stops WITH it — staging
    /// grows in step with the job queue until the operator runs the
    /// retry/skip SOP. That is intended; the consumption-lag alarm watches
    /// object counts for exactly this.
    async fn gc_pass(&self) {
        // Fresh short-lived connection: the pass is hourly and must not
        // share retry fate with the notify loops.
        let (client, conn) = match crate::config::connect_control(&self.cfg.control_dsn).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "gc: connect failed, skipping this pass");
                return;
            }
        };
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let watermark: i64 = match client
            .query_one(
                "SELECT pg_catalog.relyt_get_serial_group_watermark($1)",
                &[&self.serial_group],
            )
            .await
        {
            Ok(row) => match row.get::<_, Option<i64>>(0) {
                Some(w) => w,
                None => return, // nothing consumed yet, nothing to delete
            },
            Err(e) => {
                tracing::warn!(error = %e, "gc: watermark query failed, skipping this pass");
                return;
            }
        };

        let (mut staged, unknown) = match self.store().list_staged(&self.ident).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "gc: LIST failed, skipping this pass");
                return;
            }
        };
        if !unknown.is_empty() {
            tracing::warn!(
                count = unknown.len(),
                "gc: unrecognized objects in staging dir left untouched (evidence)"
            );
        }
        staged.sort_unstable_by_key(|f| (f.epoch_ms, f.seq));
        let total = staged.len();
        if total <= self.cfg.gc_retain_min_files {
            return;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let min_age_ms = self.cfg.gc_retain_days as u64 * 24 * 3600 * 1000;

        let mut deletable = Vec::new();
        // `total - 1`: the newest object is never a candidate.
        for f in &staged[..total - 1] {
            let seq = match f.serial_seq() {
                Ok(s) => s,
                Err(_) => continue,
            };
            if seq <= watermark && now_ms.saturating_sub(f.epoch_ms) > min_age_ms {
                deletable.push(f.clone());
            }
        }
        // Floor: keep at least gc_retain_min_files objects overall.
        let budget = total - self.cfg.gc_retain_min_files;
        deletable.truncate(budget);

        let mut deleted = 0usize;
        for f in &deletable {
            match self.store().delete_staged(&self.ident, f).await {
                Ok(()) => deleted += 1,
                Err(e) => {
                    tracing::warn!(error = %e, "gc: delete failed, stopping this pass");
                    break;
                }
            }
        }
        if deleted > 0 {
            tracing::info!(deleted, remaining = total - deleted, group = %self.serial_group,
                "gc: reclaimed consumed staging objects");
        }
    }
}

/// Hourly staging GC per writer; `Weak` so a dropped writer ends the task.
fn spawn_gc(inner: &Arc<WriterInner>) {
    let weak = Arc::downgrade(inner);
    let period = inner.cfg.gc_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // interval fires immediately on the first tick; skip it so a burst of
        // writer restarts does not stampede LIST+watermark queries.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let inner: Arc<WriterInner> = match Weak::upgrade(&weak) {
                Some(i) => i,
                None => break,
            };
            // A fenced writer must not touch the directory the new holder
            // now owns (its GC would race the successor's).
            if inner.is_fenced() {
                break;
            }
            inner.gc_pass().await;
        }
    });
}

/// Cap on the CSV buffer reservation, so a bogus `avg_row_bytes` (or a huge
/// `rotate_size_bytes`) cannot ask the allocator for an absurd block up
/// front; past this the String grows the ordinary way.
const MAX_CSV_RESERVE: usize = 256 * 1024 * 1024;

/// What one staging pass produced, for threshold calibration.
struct StageStats {
    rows: usize,
    /// Rendered CSV size — what the rotation threshold is calibrated on.
    bytes: usize,
    /// Bytes actually put to staging (== bytes when uncompressed).
    stored_bytes: usize,
}

/// Compare only what the load path depends on: column order, names and types.
/// `Field` equality would also compare nullability and metadata, which differ
/// harmlessly between a delta-rs-produced batch and the schema we read from
/// the catalog.
fn schema_compatible(a: &arrow_schema::Schema, b: &arrow_schema::Schema) -> bool {
    a.fields().len() == b.fields().len()
        && a.fields()
            .iter()
            .zip(b.fields().iter())
            .all(|(x, y)| x.name() == y.name() && x.data_type() == y.data_type())
}

/// Background ticker: age-based rotation + watermark persistence.
///
/// Holds a `Weak`, so dropping the `TableWriter` ends the task instead of
/// leaking it for the lifetime of the process.
fn spawn_ticker(inner: &Arc<WriterInner>) {
    let weak = Arc::downgrade(inner);
    let period = cmp::max(
        inner.cfg.rotate_interval_max / 2,
        Duration::from_millis(200),
    );
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let inner: Arc<WriterInner> = match Weak::upgrade(&weak) {
                Some(i) => i,
                None => break, // writer dropped
            };
            // Fenced: stop producing objects and state writes for good. The
            // data path is already refusing append/flush; this closes the
            // background path the same way.
            if inner.is_fenced() {
                tracing::info!(group = %inner.serial_group,
                    "ticker stopped: writer is fenced");
                break;
            }
            // Fatal serial-contract violation: the notifier has stopped
            // submitting for this group and check_health refuses every
            // rotation, so ticking on would log the same failure every period
            // until the process exits. Stop, as for fenced; the buffered rows
            // stay unstaged until the writer is reopened.
            if let Some(reason) = inner.notifier.fatal(&inner.serial_group) {
                tracing::warn!(group = %inner.serial_group, %reason,
                    "ticker stopped: serial-contract violation; buffered rows are not staged");
                break;
            }
            if let Err(e) = inner.rotate_if_aged().await {
                tracing::warn!(error = %e, group = %inner.serial_group,
                    "age-triggered rotation failed; data stays buffered for the next attempt");
            }
            inner.persist_state_if_due().await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{StagingConfig, StagingService};
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;

    fn schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    /// A writer wired to a staging store that is never reached: every test
    /// below fails before any IO.
    fn test_writer() -> TableWriter {
        let arrow = StdArc::new(schema(vec![Field::new("id", DataType::Int64, false)]));
        let table_schema = TableSchema {
            arrow: arrow.clone(),
            pk: vec!["id".to_string()],
            db_oid: 13727,
            rel_oid: 54321,
        };
        let staging_cfg = StagingConfig {
            endpoint: "oss-example.aliyuncs.com".into(),
            bucket: "b".into(),
            prefix: "p".into(),
            access_key_id: "ak".into(),
            secret_access_key: "sk".into(),
            service: StagingService::Oss,
            region: None,
        };
        let cfg = ClientConfig::with_customer_staging(
            staging_cfg.clone(),
            "host=127.0.0.1 port=1 user=nobody dbname=nobody",
        );
        let staging =
            Arc::new(StagingHandle::new(staging_cfg, None).expect("build staging handle"));
        let notifier = Arc::new(Notifier::spawn(cfg.control_dsn.clone(), staging.clone()));
        TableWriter::new(
            table_schema,
            WriterIdentity {
                cluster_id: "e2e".into(),
                db_oid: 13727,
                rel_oid: 54321,
                writer_id: "w0".into(),
            },
            ("d".into(), "public".into(), "t".into()),
            cfg,
            staging,
            notifier,
            1,
            None,
            None,
            Vec::new(),
            "test-instance".into(),
        )
    }

    fn one_row(arrow: arrow_schema::SchemaRef) -> RecordBatch {
        RecordBatch::try_new(arrow, vec![StdArc::new(Int64Array::from(vec![1i64]))]).unwrap()
    }

    #[tokio::test]
    async fn append_rejects_invalid_offsets() {
        let w = test_writer();
        let b = one_row(w.schema());
        // Negative offsets would produce a staged file name the recovery
        // parser cannot read back, silently shrinking the recovery set.
        assert!(w.append(b.clone(), -1, 5).await.is_err());
        // end < start is nonsense and would corrupt the resume offset.
        assert!(w.append(b.clone(), 9, 3).await.is_err());
        // Sane range is accepted (buffered, no IO yet).
        assert!(w.append(b, 0, 0).await.is_ok());
    }

    #[tokio::test]
    async fn append_rejects_foreign_schema() {
        let w = test_writer();
        let other = StdArc::new(schema(vec![Field::new("id", DataType::Utf8, false)]));
        let b = RecordBatch::try_new(
            other,
            vec![StdArc::new(arrow_array::StringArray::from(vec!["x"]))],
        )
        .unwrap();
        assert!(w.append(b, 0, 0).await.is_err());
    }

    #[test]
    fn schema_compat_ignores_nullability_and_metadata() {
        let a = schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let b = schema(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true)
                .with_metadata([("k".to_string(), "v".to_string())].into()),
        ]);
        assert!(schema_compatible(&a, &b));
    }

    #[test]
    fn schema_compat_rejects_name_and_type_drift() {
        let a = schema(vec![Field::new("id", DataType::Int64, false)]);
        assert!(!schema_compatible(
            &a,
            &schema(vec![Field::new("other", DataType::Int64, false)])
        ));
        assert!(!schema_compatible(
            &a,
            &schema(vec![Field::new("id", DataType::Int32, false)])
        ));
        assert!(!schema_compatible(
            &a,
            &schema(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false)
            ])
        ));
    }
}

/// Writer-lease heartbeat (`lock.rs` step 2). Ticks every 2s so a dropped
/// writer releases its lease within ~2s, but only touches object storage
/// every `lock_heartbeat_interval`. Fences the writer (never unfences) when
/// the lock is observed held by someone else, deleted, or unverifiable for a
/// whole `lock_lease_timeout` — a writer that cannot prove it still holds
/// the lease must stop writing rather than risk racing the new holder.
fn spawn_lock_heartbeat(inner: &Arc<WriterInner>) {
    let weak: Weak<WriterInner> = Arc::downgrade(inner);
    let staging = inner.staging.clone();
    let ident = inner.ident.clone();
    let me = inner.instance_uuid.clone();
    let period = inner.cfg.lock_heartbeat_interval;
    let lease = inner.cfg.lock_lease_timeout;
    tokio::spawn(async move {
        let tick = Duration::from_secs(2).min(period);
        let mut interval = tokio::time::interval(tick);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval.tick().await; // immediate first tick: skip
        let mut since_beat = period; // renew on the first real tick
        let mut last_verified = Instant::now();
        loop {
            interval.tick().await;
            let inner: Arc<WriterInner> = match Weak::upgrade(&weak) {
                Some(i) => i,
                None => {
                    // Writer dropped: release the lease iff still ours.
                    if let Ok(Some(l)) = staging.current().store.read_lock(&ident).await {
                        if l.instance_uuid == me {
                            let _ = staging.current().store.delete_lock(&ident).await;
                        }
                    }
                    return;
                }
            };
            since_beat += tick;
            if since_beat < period {
                continue;
            }
            match staging.current().store.read_lock(&ident).await {
                Ok(Some(l)) if l.instance_uuid == me => {
                    let mut renewed = l;
                    renewed.heartbeat_at_ms = crate::lock::now_ms();
                    match staging.current().store.write_lock(&ident, &renewed).await {
                        Ok(()) => {
                            last_verified = Instant::now();
                            since_beat = Duration::ZERO;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, key = %ident.lock_key(),
                                "writer lease renew failed; will retry");
                            // Symmetric with the read-failure branch below: a
                            // lease we could not RENEW for a whole lease period
                            // looks stale to everyone else and may already be
                            // taken over -- stop before they start (review
                            // a327a5cd).
                            if last_verified.elapsed() >= lease {
                                let reason = format!(
                                    "could not renew the lease at `{}` for {}s (storage write \
                                     errors); it is stale to competing writers -- stopping",
                                    ident.lock_key(),
                                    lease.as_secs()
                                );
                                tracing::error!("{reason}");
                                *inner.fenced.lock().unwrap() = Some(reason);
                                return;
                            }
                        }
                    }
                }
                Ok(Some(l)) => {
                    let reason = format!(
                        "lease at `{}` is now {} — this writer was preempted and must stop \
                         (reopen the table to resume as the sole writer)",
                        ident.lock_key(),
                        l.describe()
                    );
                    tracing::error!("{reason}");
                    *inner.fenced.lock().unwrap() = Some(reason);
                    return;
                }
                Ok(None) => {
                    let reason = format!(
                        "lease at `{}` was deleted (operator force-release) — stopping",
                        ident.lock_key()
                    );
                    tracing::error!("{reason}");
                    *inner.fenced.lock().unwrap() = Some(reason);
                    return;
                }
                Err(e) => {
                    tracing::warn!(error = %e, key = %ident.lock_key(),
                        "writer lease check failed; will retry");
                    if last_verified.elapsed() >= lease {
                        let reason = format!(
                            "could not verify the lease at `{}` for {}s (storage errors); \
                             a competing writer may have taken over — stopping",
                            ident.lock_key(),
                            lease.as_secs()
                        );
                        tracing::error!("{reason}");
                        *inner.fenced.lock().unwrap() = Some(reason);
                        return;
                    }
                }
            }
        }
    });
}

/// Consumption-lag sampler (GUIDE.md "消费延迟监控"): every `lag_sample_interval`
/// (~30s), read the server group watermark over one long-lived control
/// connection and publish a [`LagSnapshot`] through [`TableWriter::lag`].
///
/// The numbers come from the writer's own record of what it staged
/// (`WriterState::known_files`, seeded from the recovery listing at open and
/// extended by every rotation) minus what the watermark says is consumed. No
/// directory listing is involved, so a file stuck at the head of the group is
/// visible at the very next sample rather than after the next listing. Every
/// 10th sample (~5min) also logs a one-line status heartbeat. A failed sample
/// only skips the round (and drops the connection for a reconnect); this
/// task can never affect the data path.
fn spawn_lag_monitor(inner: &Arc<WriterInner>) {
    const LOG_EVERY_N: u32 = 10;
    let weak: Weak<WriterInner> = Arc::downgrade(inner);
    let sample_every = inner.cfg.lag_sample_interval;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(sample_every);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval.tick().await; // immediate first tick: skip
        let mut rounds = 0u32;
        // One connection for the life of the sampler, re-opened after any
        // failure. A fresh connection per sample cost the master a backend
        // fork every 30s per writer.
        let mut control: Option<tokio_postgres::Client> = None;
        loop {
            interval.tick().await;
            let inner: Arc<WriterInner> = match Weak::upgrade(&weak) {
                Some(i) => i,
                None => return,
            };
            // A fenced writer's lag is not a thing anymore.
            if inner.is_fenced() {
                return;
            }
            rounds += 1;

            if control.is_none() {
                match crate::config::connect_control(&inner.cfg.control_dsn).await {
                    Ok((c, conn)) => {
                        tokio::spawn(conn);
                        control = Some(c);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, group = %inner.serial_group,
                            "lag sample: control connection failed, skipping round");
                        continue;
                    }
                }
            }
            let watermark: Option<i64> = match control
                .as_ref()
                .expect("connected above")
                .query_one(
                    "SELECT pg_catalog.relyt_get_serial_group_watermark($1)",
                    &[&inner.serial_group],
                )
                .await
            {
                Ok(row) => row.get(0),
                Err(e) => {
                    tracing::warn!(error = %e, group = %inner.serial_group,
                        "lag sample: watermark query failed, reconnecting next round");
                    control = None;
                    continue;
                }
            };

            let now_ms = crate::lock::now_ms();
            let (lag_files, lag_seconds, buffered_age_seconds, staged_offset, buffered_rows) = {
                let mut st = inner.state.lock().await;
                let (files, secs) = lag_of(&mut st.known_files, watermark, now_ms);
                (
                    files,
                    secs,
                    st.oldest_buffered_at
                        .map(|t| t.elapsed().as_secs())
                        .unwrap_or(0),
                    st.staged_offset,
                    st.buffered_rows,
                )
            };
            *inner.lag.lock().unwrap() = Some(LagSnapshot {
                lag_seconds,
                lag_files,
                buffered_age_seconds,
                sampled_at: std::time::SystemTime::now(),
            });
            if rounds % LOG_EVERY_N == 0 {
                tracing::info!(
                    group = %inner.serial_group,
                    lag_seconds,
                    lag_files,
                    buffered_age_seconds,
                    buffered_rows,
                    staged_offset = ?staged_offset,
                    server_watermark = ?watermark,
                    "writer status heartbeat"
                );
            }
        }
    });
}

/// Drop from `known` every file at or below `watermark` (consumed) and
/// measure what is left: (how many files, age in seconds of the oldest by
/// its write-time epoch). `None` means the server has consumed nothing yet,
/// so everything counts.
fn lag_of(known: &mut Vec<(i64, u64)>, watermark: Option<i64>, now_ms: u64) -> (usize, u64) {
    known.retain(|(seq, _)| watermark.is_none_or(|w| *seq > w));
    let oldest = known.iter().map(|(_, epoch)| *epoch).min();
    (
        known.len(),
        oldest.map(|e| now_ms.saturating_sub(e) / 1000).unwrap_or(0),
    )
}

#[cfg(test)]
mod lag_tests {
    use super::lag_of;

    #[test]
    fn lag_counts_only_files_above_the_watermark() {
        let now = 1_000_000_000u64;
        let mut known = vec![(10, now - 90_000), (11, now - 60_000), (12, now - 5_000)];
        // Nothing consumed: all three count, the oldest is 90s old.
        assert_eq!(lag_of(&mut known.clone(), None, now), (3, 90));
        // Watermark at 11: only seq 12 remains, 5s old, and the others are
        // pruned from the list for good.
        assert_eq!(lag_of(&mut known, Some(11), now), (1, 5));
        assert_eq!(known, vec![(12, now - 5_000)]);
        // Caught up.
        assert_eq!(lag_of(&mut known, Some(12), now), (0, 0));
        assert!(known.is_empty());
    }
}
