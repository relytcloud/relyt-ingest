//! `TableWriter`: buffered append + rotation pipeline + flush.
//!
//! `append` does no IO and no heavy CPU: batches go into an in-memory buffer
//! and the call returns. When either rotation threshold trips (size / age),
//! on an explicit `flush`, or on the background age ticker, the buffer is
//! *sealed*: taken out whole, given the next serial seq, and handed to this
//! writer's rotation pipeline -- three background stages, render (dedup +
//! CSV) -> gzip -> put + notify, joined by bounded channels, each stage one
//! task working through files in seq order, the CPU stages on the blocking
//! pool so they never hold a tokio worker. The customer contract is
//! unchanged: commit the Kafka offset only after `flush().await` returned
//! (or `staged_offset()` >= the batch's last offset).
//!
//! Three invariants the serialised design held with one big lock are held
//! here by construction:
//!
//! - **seq order.** Seqs are allocated under the state lock and the file is
//!   handed over under `seal_lock`, so channel order is seq order; every
//!   stage is a single task over a FIFO, so files reach the put stage, and
//!   therefore the notify queue, in increasing seq. Out-of-order arrival at
//!   the server would let a later file reach FINISH first, raise the group
//!   watermark past an earlier seq, and make the server swallow that earlier
//!   file as an already-consumed replay -- a silently skipped load.
//! - **durable before announced.** `staged_offset` advances and the notify
//!   request is enqueued only after the put returned, inside the put stage.
//! - **no lost rows.** A sealed file is immutable and owned by the pipeline
//!   until it is durable. A failed put retries the same bytes to the same
//!   key (an idempotent overwrite) rather than returning rows to a buffer
//!   they could be re-cut from under another seq; a stage that keeps failing
//!   holds the pipeline, and `flush`/`close` report it as
//!   [`Error::StagingStalled`] after `staging_error_after_attempts`. Nothing
//!   is dropped short of the process exiting, at which point Kafka
//!   re-delivers from the last committed offset -- what the contract above
//!   exists for.
//!
//! Backpressure: the seal -> render channel holds `rotation_queue_depth`
//! files; while it is full, the `append` that trips a threshold waits. Memory
//! per writer is bounded by (1 buffer + queue depth + files in flight across
//! the three stages) x `rotate_size_bytes`; the sizing table in GUIDE.md
//! spells it out.

use std::cmp;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::MissedTickBehavior;

use crate::config::{ClientConfig, StagingCompression, StreamMode};
use crate::csv::CsvFormatter;
use crate::dedup::dedup_last_wins;
use crate::error::{Error, Result};
use crate::naming::{StagedFile, WriterIdentity, MAX_SEQ};
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
    /// Age of the OLDEST file this writer has sealed and the server has not
    /// consumed yet, measured from the moment it was sealed — so a file
    /// still being rendered, compressed, uploaded or retried counts here,
    /// not only one already on staging. 0 = fully caught up. Files that
    /// recovery found on staging at open carry the epoch in their name
    /// instead, which is older than their real seal time: they over-report,
    /// never under-report.
    pub lag_seconds: u64,
    /// Number of files sealed by this writer that are above the server
    /// watermark, whether or not they have finished uploading.
    pub lag_files: usize,
    /// Age of the oldest row that is not yet durable on staging: still in
    /// the buffer, or sealed and somewhere in the rotation pipeline (queued,
    /// rendering, compressing, uploading or retrying an upload). 0 = nothing
    /// pending. A storage outage shows up here first.
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
    /// Some(detail) once the put stage's order tripwire fired (see
    /// `check_order`): every append/flush/close then fails with
    /// StagingOrderViolation. Never reset, like `fenced`.
    order_fatal: std::sync::Mutex<Option<String>>,
    /// Priority of the most urgent stopped state already announced (see
    /// `fatal_priority`; `u8::MAX` = nothing announced yet). A priority
    /// rather than a flag, so a state more urgent than the last one still
    /// gets its line.
    alarm_raised: std::sync::atomic::AtomicU8,
    /// Latest consumption-lag sample (see [`LagSnapshot`]); None until the
    /// first background sample lands.
    lag: std::sync::Mutex<Option<LagSnapshot>>,
    /// Producer side of this writer's rotation pipeline (module docs).
    pipeline: PipelineHandle,
}

/// What `append` / `flush` / the ticker hold of the rotation pipeline.
struct PipelineHandle {
    /// Sealed files -> render stage. Bounded by `rotation_queue_depth`: a
    /// full queue is the backpressure that bounds memory.
    tx: mpsc::Sender<Sealed>,
    /// Taken across seal + send by everything that seals, so the order files
    /// enter the channel is the order their seqs were allocated. tokio's
    /// mutex, because the guard is held across the `send().await` that may
    /// wait for queue room.
    seal_lock: Mutex<()>,
    /// What the pipeline has made durable / where it is stuck; `flush` and
    /// `close` wait on this.
    progress: watch::Receiver<Progress>,
    /// The publishing side, shared with the stage tasks; the sealer uses it
    /// to record a pipeline that has gone away so a waiting `flush` fails
    /// instead of hanging.
    progress_tx: Arc<watch::Sender<Progress>>,
}

/// Pipeline progress as seen by `flush`/`close`.
#[derive(Clone, Debug, Default)]
struct Progress {
    /// serial_seq of the newest durable file. Files complete in seq order,
    /// so every seq at or below it is durable too.
    durable_seq: Option<i64>,
    /// A stage keeps failing on one file; cleared when that file gets past
    /// the stage (or becomes durable).
    failing: Option<Failing>,
}

#[derive(Clone, Debug)]
struct Failing {
    /// The file that is failing; a `flush` waiting on an EARLIER seq is not
    /// affected by it.
    seq: i64,
    /// Which stage reported it (`render` / `gzip` / `put`), named in the
    /// error a waiter gets for a permanent failure.
    stage: &'static str,
    attempts: u32,
    /// A deterministic failure (a row that cannot be rendered): retrying
    /// cannot fix it, so a waiter hears about it at once AND is told that
    /// waiting is not the remedy -- `RotationFailed`, not `StagingStalled`.
    permanent: bool,
    /// The underlying error, kept for a permanent failure so the caller sees
    /// what actually broke instead of a storage-shaped paraphrase. `Arc`:
    /// every waiter on this writer is handed the same one.
    cause: Option<Arc<Error>>,
    /// What kind of failure this is, which decides the error a waiter gets.
    kind: FailKind,
    last: String,
}

/// Declares `FailKind` and, for the tests, the full list of its variants.
///
/// One declaration site on purpose. The list is what
/// `a_landed_file_clears_stage_failures_but_not_terminal_states` walks, and
/// a hand-written second copy has already drifted once: `Rotation` was added
/// to the enum and the test went on checking the other three, so the very
/// invariant that commit was about went uncovered.
macro_rules! fail_kinds {
    ($($(#[$doc:meta])* $variant:ident),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum FailKind {
            $($(#[$doc])* $variant,)+
        }

        #[cfg(test)]
        const ALL_FAIL_KINDS: &[FailKind] = &[$(FailKind::$variant,)+];
    };
}

fail_kinds! {
    /// A stage keeps failing on the file: `StagingStalled` once the attempt
    /// threshold is reached (or at once when permanent).
    Stage,
    /// The stage tasks are gone (writer being dropped, or a bug):
    /// `WriterClosed`.
    PipelineGone,
    /// The writer was fenced and its stages stopped: `WriterFenced`. Without
    /// this a `flush` already waiting when the fence lands would never wake
    /// -- the writer itself holds a sender on the progress channel, so the
    /// channel does not close when the stages exit.
    Fenced,
    /// The put stage's order tripwire fired and the stages stopped:
    /// `StagingOrderViolation`, for the same reason as `Fenced`.
    OrderViolation,
    /// A stage gave up on a file whose data or schema was rejected:
    /// `RotationFailed`. Terminal like the three above, and for the same
    /// structural reason -- it has to survive the head-priority rule in
    /// `report_failure` and the clearing in `land()`, both of which apply
    /// only to `Stage`. Carried as a `Stage` entry, an earlier seq's
    /// transient failure overwrote it and a later landing erased it, after
    /// which the writer was dead and `fatal_error()` said nothing.
    Rotation,
}

impl FailKind {
    /// Whether the entry records a stopped writer rather than a file still
    /// being retried. Terminal entries are never overwritten by another
    /// failure and never cleared by a landing; a `Stage` entry is both.
    ///
    /// Exhaustive on purpose. The distinction used to be spelled
    /// `!= FailKind::Stage` at each of the four places that act on it, which
    /// is how a new terminal kind could be added without those places being
    /// reconsidered; now the compiler asks which side it is on, once.
    fn is_terminal(self) -> bool {
        match self {
            FailKind::Stage => false,
            FailKind::PipelineGone
            | FailKind::Fenced
            | FailKind::OrderViolation
            | FailKind::Rotation => true,
        }
    }
}

/// How much of a human a stopped state needs, lowest first -- the order
/// `fatal_state` reports them in, kept here so the alarm sorts by the same
/// rule. An order violation outranks the rest because it is the only one
/// that leaves a gap someone has to rewind Kafka offsets to fill.
fn fatal_priority(e: &Error) -> u8 {
    match e {
        Error::StagingOrderViolation(_) => 0,
        Error::WriterFenced(_) => 1,
        Error::SerialContractViolation(_) => 2,
        Error::RotationFailed { .. } => 3,
        _ => u8::MAX,
    }
}

/// Publish a terminal state on the progress channel unless one is already
/// there. The three terminal kinds are written from different tasks and can
/// race (a fence landing after the order tripwire fired, a gone pipeline
/// after either); the first one to land is the real cause and the one whose
/// remedy matters, so it stays. Returns whether this call wrote it.
fn publish_terminal(
    progress: &watch::Sender<Progress>,
    kind: FailKind,
    stage: &'static str,
    cause: Option<Arc<Error>>,
    last: String,
) -> bool {
    progress.send_if_modified(|p| match &p.failing {
        Some(f) if f.kind.is_terminal() => false,
        _ => {
            p.failing = Some(Failing {
                seq: i64::MIN,
                stage,
                attempts: 0,
                permanent: true,
                cause,
                kind,
                last,
            });
            true
        }
    })
}

/// The error a `Failing` entry stands for, once it qualifies (see
/// `WriterInner::stall_error`).
fn terminal_error(f: &Failing) -> Error {
    match f.kind {
        // Permanent means retrying cannot fix it: say so, and hand back the
        // error itself. StagingStalled promises the opposite ("keep
        // flushing, it will clear"), which would be an endless loop here.
        FailKind::Stage => match &f.cause {
            Some(cause) => Error::RotationFailed {
                stage: f.stage,
                source: Arc::clone(cause),
            },
            None => Error::StagingStalled {
                attempts: f.attempts,
                last: f.last.clone(),
            },
        },
        FailKind::PipelineGone => Error::WriterClosed,
        FailKind::Fenced => Error::WriterFenced(f.last.clone()),
        FailKind::OrderViolation => Error::StagingOrderViolation(f.last.clone()),
        FailKind::Rotation => Error::RotationFailed {
            stage: f.stage,
            source: match &f.cause {
                Some(c) => Arc::clone(c),
                None => Arc::new(Error::Config(f.last.clone())),
            },
        },
    }
}

/// A rotation unit as cut out of the buffer: identity, bookkeeping and the
/// batches. Immutable from here on -- see the module docs for why.
struct Sealed {
    meta: FileMeta,
    batches: Vec<RecordBatch>,
}

/// The file's identity and timings, carried through every stage.
struct FileMeta {
    file: StagedFile,
    serial_seq: i64,
    identifier: String,
    object_key: String,
    /// Rows in the buffer before dedup (the `deduped` log field).
    rows_in: usize,
    sealed_at: Instant,
    /// Wall time waiting for the render stage.
    queue_ms: u64,
    render_ms: u64,
    gzip_ms: u64,
}

/// Render stage output: the CSV body and how many rows survived dedup.
struct Rendered {
    meta: FileMeta,
    body: Arc<String>,
    rows: usize,
}

/// Gzip stage output: the bytes to put. `Buffer`, not `Vec`: a retry after a
/// credential refresh needs the bytes again and a `Buffer` clone is a
/// reference count, not a copy.
struct Compressed {
    meta: FileMeta,
    payload: opendal::Buffer,
    rows: usize,
    /// Rendered CSV size -- what the rotation threshold is calibrated on.
    bytes: usize,
    stored_bytes: usize,
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
    /// Every file this writer knows to be sealed or staged and not yet
    /// consumed, as (serial_seq, sealed_at_ms): one entry appended per seal
    /// (NOT per upload: a file stuck in the pipeline must count as lag, or a
    /// storage outage reads as "caught up"), pruned by the lag sampler as
    /// the server watermark passes them. The lag numbers are computed from
    /// this list, and `lag_seconds` is `now - sealed_at_ms` of the oldest
    /// entry -- so the time must be the FILE's, not the session epoch,
    /// which stays put for the life of the writer and would make the value
    /// read as "time since open". Entries seeded from the recovery listing
    /// at open carry the name's epoch instead (all the name has); those are
    /// older than the truth, so they over-report, never under-report.
    known_files: Vec<(i64, u64)>,
    /// Files sealed but not yet durable, as (serial_seq, sealed_at): what the
    /// `buffered_age_seconds` signal covers besides the buffer itself.
    in_flight: Vec<(i64, Instant)>,
    /// Rows currently buffered (for the CSV-byte size estimate).
    buffered_rows: usize,
    /// Average CSV bytes per surviving row, measured on the last rotation.
    /// None until the first rotation; the arrow memory estimate covers the
    /// first file.
    avg_row_bytes: Option<f64>,
    /// Highest Kafka end-offset that is durably staged (file on OSS).
    staged_offset: Option<i64>,
    /// serial_seq of the last file handed to the pipeline: what a `flush`
    /// that finds the buffer empty waits for.
    last_sealed_seq: Option<i64>,
    /// Highest Kafka end-offset appended to THIS writer instance (buffered
    /// or sealed). `append` refuses a batch that starts at or before it:
    /// offsets going backwards means the consumer was rewound, and the
    /// right move is to reopen the table and resume from the recovery
    /// plan, not to re-append into a writer that already holds those rows.
    /// Per instance on purpose -- a reopened writer starts at None so a
    /// replay of pre-crash offsets after recovery is not refused.
    max_end_offset: Option<i64>,
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
        let (seal_tx, seal_rx) = mpsc::channel(cfg.rotation_queue_depth.max(1));
        let (progress_tx, progress_rx) = watch::channel(Progress::default());
        let progress_tx = Arc::new(progress_tx);
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
                last_sealed_seq: None,
                max_end_offset: None,
                resume_persisted,
                known_files,
                in_flight: Vec::new(),
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
            order_fatal: std::sync::Mutex::new(None),
            alarm_raised: std::sync::atomic::AtomicU8::new(u8::MAX),
            lag: std::sync::Mutex::new(None),
            pipeline: PipelineHandle {
                tx: seal_tx,
                seal_lock: Mutex::new(()),
                progress: progress_rx,
                progress_tx: progress_tx.clone(),
            },
        });
        spawn_pipeline(&inner, seal_rx, progress_tx);
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
    ///
    /// **Call this serially for a given writer**, from one task, with
    /// offsets moving forward — the shape a partition's consume loop has
    /// anyway. Offsets are checked against the highest this writer has
    /// taken, so concurrent callers interleaving their ranges will see each
    /// other's batches rejected as a rewind ([`Error::Config`]). Feed
    /// several partitions through several writers, not one writer through
    /// several tasks.
    pub async fn append(
        &self,
        batch: RecordBatch,
        start_offset: i64,
        end_offset: i64,
    ) -> Result<()> {
        self.append_inner(batch, start_offset, end_offset)
            .await
            .map_err(|e| crate::error::log_input_error("append", e))
    }

    async fn append_inner(
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
        let (should_rotate, prev_max_end) = {
            let mut st = self.inner.state.lock().await;
            // Caller-side input, checked here and now rather than by the
            // pipeline's order tripwire (which guards SDK-internal seqs):
            // a rewound consumer must not silently re-append into a writer
            // that already holds those offsets.
            if let Some(max_end) = st.max_end_offset {
                if start_offset <= max_end {
                    return Err(Error::Config(format!(
                        "Kafka offsets went backwards: batch [{start_offset}, {end_offset}] starts \
                         at or before the last appended end offset {max_end}; the batch was not \
                         buffered. If the consumer was rewound (rebalance, seek), reopen the table \
                         and resume from RecoveryPlan::kafka_resume_offset instead of re-appending \
                         to this writer."
                    )));
                }
            }
            let prev_max_end = st.max_end_offset;
            st.max_end_offset = Some(end_offset);
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
            let rotate = est_bytes >= self.inner.cfg.rotate_size_bytes
                || st
                    .oldest_buffered_at
                    .map(|t| t.elapsed() >= self.inner.cfg.rotate_interval_max)
                    .unwrap_or(false);
            (rotate, prev_max_end)
        };
        if should_rotate {
            // Hands the buffer to the pipeline. Waits while the queue is
            // full (backpressure), never for the upload itself; if, while
            // waiting, the pipeline turns out to be stuck (see
            // StagingStalled) it gives up instead -- the common append*N ->
            // flush loop would otherwise park here and never reach the
            // flush that reports the stall. Nothing was sealed when that
            // happens, so this call's batch is taken back out: an Err from
            // append means "not accepted, retry later".
            if let Err(e) = self.inner.seal_and_send(false, true).await {
                let taken_back = self
                    .inner
                    .unbuffer(start_offset, end_offset, prev_max_end)
                    .await;
                // Could not take it back: a concurrent sealer already handed
                // this batch to the pipeline, so it IS accepted and saying
                // otherwise would have the caller append it twice. A stall
                // then surfaces at the next `flush`; a terminal writer state
                // still propagates, since that caller must stop either way.
                if taken_back || !matches!(e, Error::StagingStalled { .. }) {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Force-stage everything buffered and wait until it is durable on
    /// staging storage; returns the staged offset (highest Kafka end-offset
    /// durable) after the write. Fails with [`Error::StagingStalled`] when
    /// the pipeline is stuck (see there); nothing is lost and a later call
    /// waits again.
    pub async fn flush(&self) -> Result<Option<i64>> {
        self.inner.check_health()?;
        self.inner.flush_pipeline().await?;
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

    /// `Some(reason)` once this writer has stopped for good: every further
    /// `append` / `flush` / `close` will fail with the same thing.
    ///
    /// Covers all four ways a writer stops — the lease was lost to another
    /// process, the ordering tripwire fired, a rotation stage failed in a
    /// way retrying cannot fix, or the writer identity clashes with another
    /// one — so a monitor polling this catches a stopped stream even while
    /// the application is idle and not calling `append`. Background tasks
    /// (the lease heartbeat above all) reach these states on their own, so
    /// this is the signal that does not wait for the next call.
    ///
    /// Alerting guidance is in GUIDE.md; [`Self::lag`] is the separate,
    /// non-fatal "falling behind" signal.
    pub fn fatal_error(&self) -> Option<String> {
        self.inner.fatal_reason()
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
    ///
    /// `close` takes `self`, so the writer is gone whatever it returns.
    /// On an error there is nothing left to retry on: anything still
    /// buffered or in flight is dropped with the writer. That is safe as
    /// long as the contract is kept — Kafka offsets are committed only
    /// behind `staged_offset()` — because the next process resumes from
    /// [`RecoveryPlan::kafka_resume_offset`](crate::RecoveryPlan) and
    /// replays them. Call `flush()` first if you want to see (and wait out)
    /// a stalled pipeline while the writer is still usable.
    pub async fn close(self) -> Result<Option<i64>> {
        // Same gate as append/flush: fenced, order tripwire, serial fatal.
        self.inner.check_health()?;
        self.inner.flush_pipeline().await?;
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
        match self.fatal_state() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// The one place that decides whether this writer has stopped, so the
    /// error `append` returns and the string [`TableWriter::fatal_error`]
    /// reports can never disagree about it.
    ///
    /// Order matters when more than one state is set: the order violation
    /// comes first because it is the only one that needs a human to rewind
    /// Kafka offsets, and a fence landing afterwards must not hide that.
    /// `RotationFailed` is read from the pipeline's progress channel rather
    /// than a flag of its own -- that is where the stages publish it.
    fn fatal_state(&self) -> Option<Error> {
        if let Some(detail) = self.order_fatal.lock().unwrap().clone() {
            return Some(Error::StagingOrderViolation(detail));
        }
        if let Some(reason) = self.fenced.lock().unwrap().clone() {
            return Some(Error::WriterFenced(reason));
        }
        if let Some(msg) = self.notifier.fatal(&self.serial_group) {
            return Some(Error::SerialContractViolation(msg));
        }
        // A permanent stage failure: the file at the head cannot be staged
        // however many times it is retried, so the stream is stopped even
        // though the pipeline is still turning.
        let p = self.pipeline.progress.borrow();
        match self.stall_error(&p, i64::MAX) {
            Some(e @ Error::RotationFailed { .. }) => Some(e),
            _ => None,
        }
    }

    /// `fatal_state` rendered for [`TableWriter::fatal_error`].
    fn fatal_reason(&self) -> Option<String> {
        self.fatal_state().map(|e| e.to_string())
    }

    /// Cut the whole buffer into one file and give it the next seq. With
    /// `aged_only`, only when the oldest buffered row has aged past
    /// `rotate_interval_max` (the ticker's path). None when there is nothing
    /// to seal. Runs under the state lock the caller holds; does no IO.
    fn seal(&self, st: &mut WriterState, aged_only: bool) -> Result<Option<Sealed>> {
        if !self.sealable(st, aged_only) {
            return Ok(None);
        }
        if st.next_seq > MAX_SEQ {
            // Seq exhausted: roll to a fresh epoch.
            st.epoch_ms += 1;
            st.next_seq = 0;
        }
        let start = st.buffered.iter().map(|(_, s, _)| *s).min().unwrap();
        let end = st.buffered.iter().map(|(_, _, e)| *e).max().unwrap();
        let file = StagedFile {
            epoch_ms: st.epoch_ms,
            seq: st.next_seq,
            start_offset: start,
            end_offset: end,
            compressed: self.cfg.staging_compression == StagingCompression::Gzip,
        };
        // Encode the serial_seq and identifier BEFORE consuming anything: an
        // out-of-range epoch fails here with the buffer intact, not after a
        // put that would strand an orphan object the server was never told
        // about.
        let serial_seq = file.serial_seq()?;
        let identifier = file.identifier(&self.ident)?;
        let object_key = file.object_key(&self.ident);
        // The seq is spent even if the file needs retries: the pipeline
        // retries the SAME bytes under the same seq and key, so no second
        // object can ever claim it.
        st.next_seq += 1;
        let rows_in = st.buffered_rows;
        let batches = std::mem::take(&mut st.buffered)
            .into_iter()
            .map(|(b, _, _)| b)
            .collect();
        st.buffered_bytes_estimate = 0;
        st.buffered_rows = 0;
        st.oldest_buffered_at = None;
        st.last_sealed_seq = Some(serial_seq);
        // Visible to the lag sampler from this moment, not from the upload:
        // between seal and durable the rows are in neither the buffer nor
        // staging, and that window is exactly where a storage outage lives.
        // Stamped with the wall clock of the seal, not `file.epoch_ms`: the
        // epoch is fixed for the session, and lag_seconds measures file age.
        let sealed_at = Instant::now();
        st.known_files.push((serial_seq, crate::lock::now_ms()));
        st.in_flight.push((serial_seq, sealed_at));
        Ok(Some(Sealed {
            meta: FileMeta {
                file,
                serial_seq,
                identifier,
                object_key,
                rows_in,
                sealed_at,
                queue_ms: 0,
                render_ms: 0,
                gzip_ms: 0,
            },
            batches,
        }))
    }

    /// Whether `seal` would produce a file right now: a non-empty buffer,
    /// and with `aged_only` one whose oldest row has aged past
    /// `rotate_interval_max`.
    fn sealable(&self, st: &WriterState, aged_only: bool) -> bool {
        !st.buffered.is_empty()
            && (!aged_only
                || st
                    .oldest_buffered_at
                    .map(|t| t.elapsed() >= self.cfg.rotate_interval_max)
                    .unwrap_or(false))
    }

    /// Seal whatever is buffered and hand it to the pipeline. Returns the
    /// file's serial_seq; None when nothing was buffered.
    ///
    /// Queue room is reserved BEFORE the buffer is cut, so the hand-over
    /// itself never blocks and nothing ever has to be un-sealed. The wait
    /// for room is the backpressure. With `surface_stall`, a caller that
    /// would have to park -- the seal lock held by another sealer, or the
    /// queue full -- waits only until the pipeline reports a file that
    /// keeps failing, and then gives up with [`Error::StagingStalled`]
    /// rather than sit behind a storage outage indefinitely; while there is
    /// room the batch is taken regardless (that is what the queue is for).
    /// Without it (the ticker) the wait is unconditional.
    async fn seal_and_send(&self, aged_only: bool, surface_stall: bool) -> Result<Option<i64>> {
        let _order = match self.pipeline.seal_lock.try_lock() {
            Ok(g) => g,
            Err(_) if surface_stall => tokio::select! {
                g = self.pipeline.seal_lock.lock() => g,
                e = self.until_stalled(i64::MAX) => return Err(e),
            },
            Err(_) => self.pipeline.seal_lock.lock().await,
        };
        let anything = {
            let st = self.state.lock().await;
            self.sealable(&st, aged_only)
        };
        if !anything {
            return Ok(None);
        }
        let permit = match self.pipeline.tx.try_reserve() {
            Ok(p) => Ok(p),
            Err(TrySendError::Closed(())) => Err(()),
            Err(TrySendError::Full(())) if surface_stall => tokio::select! {
                p = self.pipeline.tx.reserve() => p.map_err(|_| ()),
                e = self.until_stalled(i64::MAX) => return Err(e),
            },
            Err(TrySendError::Full(())) => self.pipeline.tx.reserve().await.map_err(|_| ()),
        };
        let Ok(permit) = permit else {
            // The stages exit on a fence or a tripped tripwire too: name
            // that cause when it is the one, not "pipeline gone".
            self.check_health()?;
            return Err(self.pipeline_gone());
        };
        // The wait may have been long: a writer fenced meanwhile must not
        // cut and stage anything more.
        self.check_health()?;
        let sealed = {
            let mut st = self.state.lock().await;
            self.seal(&mut st, aged_only)?
        };
        // Only sealers touch the buffer and all of them hold seal_lock, so
        // the buffer seen above can only have grown; None is unreachable in
        // practice and harmless if it ever happens.
        let Some(sealed) = sealed else {
            return Ok(None);
        };
        let seq = sealed.meta.serial_seq;
        permit.send(sealed);
        Ok(Some(seq))
    }

    /// The ticker's path: seal an aged buffer without ever waiting. Skips
    /// the tick when another sealer is mid-handover or the queue is full;
    /// the next tick (or the next `append`) picks the buffer up.
    async fn seal_if_aged_nonblocking(&self) -> Result<()> {
        // Same gate as append/flush: a fenced writer must not stage anything
        // more, whatever path asks for it.
        self.check_health()?;
        let Ok(_order) = self.pipeline.seal_lock.try_lock() else {
            return Ok(());
        };
        // Full or closed: either way not this tick's problem.
        let Ok(permit) = self.pipeline.tx.try_reserve() else {
            return Ok(());
        };
        let sealed = {
            let mut st = self.state.lock().await;
            self.seal(&mut st, true)?
        };
        if let Some(sealed) = sealed {
            permit.send(sealed);
        }
        Ok(())
    }

    /// The render stage has exited, which only happens while this writer is
    /// being dropped (or on a bug). Nothing was sealed; make sure a `flush`
    /// waiting on an earlier file does not hang on a pipeline that will
    /// never answer.
    fn pipeline_gone(&self) -> Error {
        let mut err = Error::WriterClosed;
        self.pipeline
            .progress_tx
            .send_if_modified(|p| match &p.failing {
                // An earlier terminal state (fenced, tripwire) is the real
                // cause of the stages being gone: keep it and report it.
                Some(f) if f.kind.is_terminal() => {
                    err = terminal_error(f);
                    false
                }
                _ => {
                    p.failing = Some(Failing {
                        seq: i64::MIN,
                        stage: "pipeline",
                        attempts: 0,
                        permanent: true,
                        cause: None,
                        kind: FailKind::PipelineGone,
                        last: "rotation pipeline is gone".into(),
                    });
                    true
                }
            });
        err
    }

    /// Take one `append`'s batch back out of the buffer after its hand-over
    /// failed. Returns whether it was still there: a concurrent sealer (the
    /// ticker, or a `flush` on another task) may have taken the whole buffer
    /// — this batch included — while this call was waiting for queue room,
    /// and a batch that is already in the pipeline must NOT be handed back,
    /// or the caller would append it a second time.
    ///
    /// Searched from the back: with one producer per writer it is the last
    /// entry. The offset high-water mark is restored only together with the
    /// batch, and only when this batch is the one that set it.
    async fn unbuffer(
        &self,
        start_offset: i64,
        end_offset: i64,
        prev_max_end: Option<i64>,
    ) -> bool {
        let mut st = self.state.lock().await;
        let Some(i) = st
            .buffered
            .iter()
            .rposition(|(_, s, e)| *s == start_offset && *e == end_offset)
        else {
            return false;
        };
        let (batch, _, _) = st.buffered.remove(i);
        st.buffered_bytes_estimate = st
            .buffered_bytes_estimate
            .saturating_sub(batch.get_array_memory_size() as u64);
        st.buffered_rows = st.buffered_rows.saturating_sub(batch.num_rows());
        if st.buffered.is_empty() {
            st.oldest_buffered_at = None;
        }
        if st.max_end_offset == Some(end_offset) {
            st.max_end_offset = prev_max_end;
        }
        true
    }

    /// Seal what is buffered and wait until it -- and so everything sealed
    /// before it -- is durable.
    async fn flush_pipeline(&self) -> Result<()> {
        let target = match self.seal_and_send(false, true).await? {
            Some(seq) => Some(seq),
            None => self.state.lock().await.last_sealed_seq,
        };
        match target {
            Some(t) => self.await_durable(t).await,
            None => Ok(()),
        }
    }

    /// What a caller waiting on `target` (or on anything: `i64::MAX`) is
    /// owed once the pipeline is stuck: Some when the failing file is at or
    /// before `target` and has either failed for good or
    /// `staging_error_after_attempts` times in a row.
    fn stall_error(&self, p: &Progress, target: i64) -> Option<Error> {
        let f = p.failing.as_ref()?;
        if f.seq > target || !(f.permanent || f.attempts >= self.cfg.staging_error_after_attempts) {
            return None;
        }
        Some(terminal_error(f))
    }

    /// Resolves when the pipeline is stuck on a file at or before `target`
    /// (see `stall_error`), or when every stage task is gone.
    async fn until_stalled(&self, target: i64) -> Error {
        let mut rx = self.pipeline.progress.clone();
        loop {
            if let Some(e) = self.stall_error(&rx.borrow_and_update(), target) {
                return e;
            }
            // `changed()` fails only once every sender is gone, which cannot
            // happen while this writer lives (it holds one itself); the
            // stages report their exits through `failing` instead. The arm
            // handles the Result and gives the right answer if it ever
            // fires.
            if rx.changed().await.is_err() {
                return Error::WriterClosed;
            }
        }
    }

    /// Wait until the file with serial_seq `target` is durable, or the
    /// pipeline is stuck on it or a file before it (see `stall_error`).
    async fn await_durable(&self, target: i64) -> Result<()> {
        let mut rx = self.pipeline.progress.clone();
        loop {
            {
                let p = rx.borrow_and_update();
                if p.durable_seq.is_some_and(|d| d >= target) {
                    return Ok(());
                }
                if let Some(e) = self.stall_error(&p, target) {
                    return Err(e);
                }
            }
            // See `until_stalled` on why this arm is not expected to fire.
            if rx.changed().await.is_err() {
                return Err(Error::WriterClosed);
            }
        }
    }

    /// This writer lost its lease: record why, log it, and raise the alarm.
    /// The four call sites in the heartbeat all end the task right after, so
    /// this is the single place a fence becomes visible.
    fn fence(&self, reason: String) {
        tracing::error!("{reason}");
        {
            let mut slot = self.fenced.lock().unwrap();
            if slot.is_none() {
                *slot = Some(reason.clone());
            }
        }
        self.raise_stream_stopped();
    }

    /// Raise the stream-stopped alarm for the state `fatal_state` reports
    /// right now, rather than for whatever the calling task happened to
    /// discover.
    ///
    /// The alarm text and `fatal_error()` have to agree, because an operator
    /// acts on the alarm: a fence and an order violation carry opposite
    /// instructions -- "do not rewind" against "you must rewind" -- so
    /// announcing whichever arrived first could tell someone to leave a gap
    /// in place. Both therefore sort by `fatal_priority`.
    ///
    /// Hence a priority rather than a flag: normally one line per writer,
    /// but a state MORE urgent than the one already announced gets a second.
    /// Only ever an upgrade, so a writer cannot page repeatedly.
    fn raise_stream_stopped(&self) {
        use std::sync::atomic::Ordering;
        let Some(cause) = self.fatal_state() else {
            return;
        };
        let prio = fatal_priority(&cause);
        if self.alarm_raised.fetch_min(prio, Ordering::SeqCst) <= prio {
            return;
        }
        crate::alarm::stream_stopped(
            &format!("{}.{}", self.names.1, self.names.2),
            &self.ident.writer_id,
            &self.serial_group,
            &cause,
        );
    }

    /// The order tripwire fired: record why, so every append/flush/close
    /// fails with it from now on. Never reset -- the gap it names is not
    /// something a running writer can repair.
    fn trip_order_fatal(&self, detail: String) {
        tracing::error!(group = %self.serial_group, "{detail}");
        {
            let mut slot = self.order_fatal.lock().unwrap();
            if slot.is_none() {
                *slot = Some(detail.clone());
            }
        }
        // Wake anyone already parked in flush/close: the stages stop here
        // and nothing else will ever move the progress channel.
        publish_terminal(
            &self.pipeline.progress_tx,
            FailKind::OrderViolation,
            "pipeline",
            None,
            detail,
        );
        self.raise_stream_stopped();
    }

    /// The put returned, so the file is durable: record the offset, then
    /// announce the file. The caller publishes progress AFTER this, so a
    /// `flush` woken by it reads the advanced offset.
    async fn record_durable(
        &self,
        meta: &FileMeta,
        rows: usize,
        bytes: usize,
        stored_bytes: usize,
        put_ms: u64,
    ) {
        {
            let mut st = self.state.lock().await;
            st.staged_offset = Some(match st.staged_offset {
                Some(prev) => prev.max(meta.file.end_offset),
                None => meta.file.end_offset,
            });
            // known_files got this file at seal time; only the in-flight
            // bookkeeping ends here.
            st.in_flight.retain(|(s, _)| *s != meta.serial_seq);
        }
        tracing::info!(
            db = %self.names.0,
            table = %format!("{}.{}", self.names.1, self.names.2),
            writer_id = %self.ident.writer_id,
            object = %format!("{}/{}", self.url_base(), meta.object_key),
            start_offset = meta.file.start_offset,
            end_offset = meta.file.end_offset,
            rows,
            deduped = meta.rows_in.saturating_sub(rows),
            bytes,
            stored_bytes,
            serial_seq = meta.serial_seq,
            queue_ms = meta.queue_ms,
            render_ms = meta.render_ms,
            gzip_ms = meta.gzip_ms,
            put_ms,
            elapsed_ms = ms(meta.sealed_at.elapsed()),
            "staged file durable"
        );
        // Serial fields are unconditional: insert-only streams run with M=1
        // serialization too -- that is what gives them a server-side group
        // watermark for recovery pruning. Only the upsert load mode and the
        // intra-file PK dedup stay Upsert-specific. An enqueue failure means
        // the notify task is gone (process shutting down); the object is on
        // storage, so recovery backfill still owns it.
        if let Err(e) = self.notifier.enqueue(NotifyRequest {
            serial_group: self.serial_group.clone(),
            end_offset: meta.file.end_offset,
            identifier: meta.identifier.clone(),
            source_url: format!("{}/{}", self.url_base(), meta.object_key),
            target: self.ident.rel_oid,
            delimiter: self.cfg.csv.delimiter,
            upsert: self.cfg.stream_mode == StreamMode::Upsert,
            serial_seq: meta.serial_seq,
            retry_max: self.cfg.retry_max.or(Some(-1)),
        }) {
            tracing::error!(error = %e, serial_seq = meta.serial_seq, group = %self.serial_group,
                "staged file could not be handed to the notify queue; recovery backfill owns it");
        }
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
    ///   2. it is older than `gc_retain_days` (age = now - the epoch in its
    ///      name, i.e. the writer session's start; a long session makes
    ///      consumed files look OLDER than they are, so this only ever
    ///      reclaims early, never late);
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
            if inner.order_fatal.lock().unwrap().is_some() {
                tracing::warn!(group = %inner.serial_group,
                    "ticker stopped: staging order violation; buffered rows are not staged");
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
                // The fourth stopped state, and the only one a writer learns
                // by reading someone else's flag rather than by setting it:
                // the notify loop parks it on the group, and this tick is
                // where the writer notices. Raised here rather than there so
                // the alarm carries the table and writer_id, which the notify
                // loop does not know; the once-only guard makes a second
                // discoverer harmless.
                inner.raise_stream_stopped();
                break;
            }
            if let Err(e) = inner.seal_if_aged_nonblocking().await {
                tracing::warn!(error = %e, group = %inner.serial_group,
                    "age-triggered rotation failed; data stays buffered for the next attempt");
            }
            inner.persist_state_if_due().await;
        }
    });
}

// ---------------------------------------------------------------------------
// The rotation pipeline: render -> gzip -> put, one task per stage.
// ---------------------------------------------------------------------------

/// Start the three stage tasks. Each holds a `Weak` to the writer, so a
/// dropped writer ends them: the seal channel closes when `WriterInner`
/// drops, the render task exits, and the closure cascades down the chain.
/// A file whose put is in flight at that moment still lands, and recovery
/// backfill owns it (the object is on storage, the announcement is not).
fn spawn_pipeline(
    inner: &Arc<WriterInner>,
    rx: mpsc::Receiver<Sealed>,
    progress: Arc<watch::Sender<Progress>>,
) {
    // One slot between stages: with a single task per stage that is exactly
    // "file N+1 is waiting when the next stage finishes N", which is all the
    // overlap three stages can use.
    let (tx_gzip, rx_gzip) = mpsc::channel::<Rendered>(1);
    let (tx_put, rx_put) = mpsc::channel::<Compressed>(1);
    tokio::spawn(render_stage(
        Arc::downgrade(inner),
        rx,
        tx_gzip,
        progress.clone(),
    ));
    tokio::spawn(gzip_stage(
        Arc::downgrade(inner),
        rx_gzip,
        tx_put,
        progress.clone(),
    ));
    tokio::spawn(put_stage(Arc::downgrade(inner), rx_put, progress));
}

/// Stage 1: dedup + CSV on the blocking pool.
///
/// Whether a failure is permanent is decided by `is_permanent` on the error
/// itself, not by the stage: a rejection of the data or the schema cannot be
/// retried into a CSV, while a panic or a cancellation in the blocking task
/// may well succeed next time. A permanent one ends the stage -- it is
/// neither retried nor skipped, because skipping would drop rows and leave a
/// seq hole `flush` could never wait out. Everything else keeps retrying at
/// the capped backoff.
async fn render_stage(
    weak: Weak<WriterInner>,
    mut rx: mpsc::Receiver<Sealed>,
    tx: mpsc::Sender<Rendered>,
    progress: Arc<watch::Sender<Progress>>,
) {
    while let Some(Sealed { mut meta, batches }) = rx.recv().await {
        meta.queue_ms = ms(meta.sealed_at.elapsed());
        // Shared, not moved: a retry needs the batches again.
        let batches = Arc::new(batches);
        let mut attempt = 0u32;
        let (body, rows) = loop {
            let Some(inner) = weak.upgrade() else { return };
            if fenced_exit(&inner, "render", &meta) {
                return;
            }
            let started = Instant::now();
            match render_once(&inner, batches.clone()).await {
                Ok(out) => {
                    meta.render_ms = ms(started.elapsed());
                    if attempt > 0 {
                        clear_failure(&progress, meta.serial_seq);
                    }
                    break out;
                }
                Err(e) => {
                    attempt += 1;
                    let permanent = is_permanent(&e);
                    report_failure(&progress, &meta, "render", attempt, permanent, &e);
                    if permanent {
                        // Retrying cannot turn these bytes into a CSV, so the
                        // stage stops here rather than spinning: the writer is
                        // already fatal (`fatal_state` reads this very entry)
                        // and every caller now gets the cause.
                        // A protected terminal entry rather than a Stage one:
                        // an earlier seq's transient failure would overwrite a
                        // Stage entry and a later landing would clear it,
                        // leaving the writer dead with nothing to report.
                        publish_terminal(
                            &progress,
                            FailKind::Rotation,
                            "render",
                            Some(Arc::new(clone_error(&e))),
                            format!("render: {e}"),
                        );
                        inner.raise_stream_stopped();
                        return;
                    }
                    drop(inner);
                    tokio::time::sleep(backoff(attempt)).await;
                }
            }
        };
        // The arrow batches are dead weight from here on; release them
        // before waiting for the gzip stage to take the body.
        drop(batches);
        // Calibrate the size threshold on the measured row width right
        // away, not after the upload: the buffer refilling meanwhile is
        // judged against it.
        if rows > 0 {
            if let Some(inner) = weak.upgrade() {
                inner.state.lock().await.avg_row_bytes = Some(body.len() as f64 / rows as f64);
            }
        }
        if tx
            .send(Rendered {
                meta,
                body: Arc::new(body),
                rows,
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

/// One render attempt: gather what the blocking closure needs, run it off
/// the runtime.
async fn render_once(
    inner: &WriterInner,
    batches: Arc<Vec<RecordBatch>>,
) -> Result<(String, usize)> {
    let pk = if inner.cfg.stream_mode == StreamMode::Upsert {
        Some(inner.schema.pk_indices()?)
    } else {
        None
    };
    let names: Vec<String> = inner
        .schema
        .arrow
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let delimiter = inner.cfg.csv.delimiter;
    let avg_row_bytes = inner.state.lock().await.avg_row_bytes;
    tokio::task::spawn_blocking(move || {
        render(&batches, pk.as_deref(), &names, delimiter, avg_row_bytes)
    })
    .await
    .map_err(|e| Error::Config(format!("render task failed: {e}")))?
}

/// Dedup + serialize. Returns the CSV body and the surviving row count.
/// `avg_row_bytes` is the previous file's measurement (None for the first)
/// and only sizes the output buffer up front.
fn render(
    batches: &[RecordBatch],
    pk: Option<&[usize]>,
    names: &[String],
    delimiter: char,
    avg_row_bytes: Option<f64>,
) -> Result<(String, usize)> {
    // 1. Intra-file PK dedup, last write wins (hard requirement in upsert
    //    mode); insert-only keeps every row.
    let keep: Vec<Vec<usize>> = match pk {
        Some(pk) => dedup_last_wins(batches, pk)?,
        None => batches
            .iter()
            .map(|b| (0..b.num_rows()).collect())
            .collect(),
    };

    // 2. Serialize to CSV (header always on).
    let fmt = CsvFormatter::new(delimiter);
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut body = fmt.header(&name_refs);
    // Reserve the whole file up front from the measured average: without
    // it a 64MB body grows through ~20 reallocation + copy rounds.
    {
        let rows_total: usize = keep.iter().map(|r| r.len()).sum();
        let per_row = avg_row_bytes.unwrap_or(128.0).max(1.0);
        let est = (rows_total as f64 * per_row) as usize;
        body.reserve(est.min(MAX_CSV_RESERVE));
    }
    let mut kept_rows = 0usize;
    for (b, rows) in batches.iter().zip(keep.iter()) {
        kept_rows += rows.len();
        fmt.format_rows(b, rows, &mut body)?;
    }
    Ok((body, kept_rows))
}

/// Stage 2: gzip on the blocking pool (or a pass-through for plain staging).
async fn gzip_stage(
    weak: Weak<WriterInner>,
    mut rx: mpsc::Receiver<Rendered>,
    tx: mpsc::Sender<Compressed>,
    progress: Arc<watch::Sender<Progress>>,
) {
    while let Some(Rendered {
        mut meta,
        body,
        rows,
    }) = rx.recv().await
    {
        let bytes = body.len();
        let payload: Vec<u8> = if meta.file.compressed {
            let mut attempt = 0u32;
            let compressed = loop {
                let Some(inner) = weak.upgrade() else { return };
                if fenced_exit(&inner, "gzip", &meta) {
                    return;
                }
                drop(inner);
                let started = Instant::now();
                let body = body.clone();
                let out = tokio::task::spawn_blocking(move || gzip(&body))
                    .await
                    .map_err(|e| Error::Config(format!("gzip task failed: {e}")))
                    .and_then(|r| r);
                match out {
                    Ok(v) => {
                        meta.gzip_ms = ms(started.elapsed());
                        if attempt > 0 {
                            clear_failure(&progress, meta.serial_seq);
                        }
                        break v;
                    }
                    Err(e) => {
                        attempt += 1;
                        let permanent = is_permanent(&e);
                        report_failure(&progress, &meta, "gzip", attempt, permanent, &e);
                        if permanent {
                            let Some(inner) = weak.upgrade() else { return };
                            // A protected terminal entry rather than a Stage one:
                            // an earlier seq's transient failure would overwrite a
                            // Stage entry and a later landing would clear it,
                            // leaving the writer dead with nothing to report.
                            publish_terminal(
                                &progress,
                                FailKind::Rotation,
                                "gzip",
                                Some(Arc::new(clone_error(&e))),
                                format!("gzip: {e}"),
                            );
                            inner.raise_stream_stopped();
                            return;
                        }
                        tokio::time::sleep(backoff(attempt)).await;
                    }
                }
            };
            // The uncompressed CSV is dead weight from here on; release it
            // before waiting for the put stage to take the payload, or a
            // slow upload holds a file's worth of memory twice.
            drop(body);
            compressed
        } else {
            // Plain: the body IS the payload. Nothing else holds the Arc by
            // now, so this is a move, not a copy.
            Arc::try_unwrap(body)
                .map(String::into_bytes)
                .unwrap_or_else(|b| b.as_bytes().to_vec())
        };
        let stored_bytes = payload.len();
        if tx
            .send(Compressed {
                meta,
                payload: opendal::Buffer::from(payload),
                rows,
                bytes,
                stored_bytes,
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

/// gzip at level 6 (the default): the bandwidth/CPU sweet spot for CSV. The
/// backend is zlib-rs (see Cargo.toml), which is where most of a file's CPU
/// goes. The server sniffs the magic bytes, so no load option is needed.
fn gzip(body: &str) -> Result<Vec<u8>> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(
        Vec::with_capacity(body.len() / 4),
        flate2::Compression::default(),
    );
    enc.write_all(body.as_bytes())
        .and_then(|_| enc.finish())
        .map_err(|e| Error::Config(format!("gzip of staged CSV failed: {e}")))
}

/// Stage 3: the put, then -- the file being durable -- offset, log, notify,
/// progress. Storage errors are transient by assumption and retried with
/// backoff for as long as the writer lives; `flush` reports them through
/// [`Error::StagingStalled`] after `staging_error_after_attempts`.
async fn put_stage(
    weak: Weak<WriterInner>,
    mut rx: mpsc::Receiver<Compressed>,
    progress: Arc<watch::Sender<Progress>>,
) {
    // (epoch_ms, seq, end_offset) of the last file this stage made durable.
    let mut prev: Option<(u64, u32, i64)> = None;
    while let Some(Compressed {
        meta,
        payload,
        rows,
        bytes,
        stored_bytes,
    }) = rx.recv().await
    {
        {
            let Some(inner) = weak.upgrade() else { return };
            if fenced_exit(&inner, "put", &meta) {
                return;
            }
            // The tripwire sits here, right before the first externally
            // visible side effect, so an ordering bug anywhere upstream is
            // caught before it can reach storage or the server.
            if let Err(detail) = check_order(prev, &meta.file) {
                inner.trip_order_fatal(detail);
                return;
            }
        }
        let mut attempt = 0u32;
        let put_ms = loop {
            // Re-checked per attempt: a writer dropped during a storage
            // outage must not be kept alive by its own retries, and a fenced
            // one must not keep uploading under the old epoch.
            let Some(inner) = weak.upgrade() else { return };
            if fenced_exit(&inner, "put", &meta) {
                return;
            }
            let started = Instant::now();
            match put_once(&inner, &meta.object_key, payload.clone()).await {
                Ok(()) => {
                    if attempt > 0 {
                        clear_failure(&progress, meta.serial_seq);
                    }
                    break ms(started.elapsed());
                }
                Err(e) => {
                    // Storage errors are transient by nature, so this is
                    // almost always false; it goes through the same predicate
                    // so no stage has a rule of its own.
                    attempt += 1;
                    let permanent = is_permanent(&e);
                    report_failure(&progress, &meta, "put", attempt, permanent, &e);
                    if permanent {
                        // A protected terminal entry rather than a Stage one:
                        // an earlier seq's transient failure would overwrite a
                        // Stage entry and a later landing would clear it,
                        // leaving the writer dead with nothing to report.
                        publish_terminal(
                            &progress,
                            FailKind::Rotation,
                            "put",
                            Some(Arc::new(clone_error(&e))),
                            format!("put: {e}"),
                        );
                        inner.raise_stream_stopped();
                        return;
                    }
                    drop(inner);
                    tokio::time::sleep(backoff(attempt)).await;
                }
            }
        };
        // Gone or fenced between the put and here: the object is on storage
        // under the old epoch and the new holder's recovery listing owns it;
        // this writer must neither record nor announce it.
        let Some(inner) = weak.upgrade() else { return };
        if fenced_exit(&inner, "put (landed)", &meta) {
            return;
        }
        inner
            .record_durable(&meta, rows, bytes, stored_bytes, put_ms)
            .await;
        prev = Some((meta.file.epoch_ms, meta.file.seq, meta.file.end_offset));
        land(&progress, meta.serial_seq);
    }
}

/// One put with the managed-staging credential dance: a denied upload under
/// Relyt-managed staging is most likely a key the master has since rotated,
/// so refresh once and retry once only if that produced a different key.
/// Anything else propagates to the stage's retry loop.
async fn put_once(inner: &WriterInner, key: &str, payload: opendal::Buffer) -> Result<()> {
    if let Err(e) = inner.store().put(key, payload.clone()).await {
        let denied =
            matches!(&e, Error::Storage(s) if s.kind() == opendal::ErrorKind::PermissionDenied);
        if !(denied
            && crate::client::refresh_managed_staging(&inner.staging, "upload denied").await?)
        {
            return Err(e);
        }
        inner.store().put(key, payload).await?;
    }
    Ok(())
}

/// A copy of `err` that can be stored and handed to more than one waiter.
/// `Error` is not `Clone` (the storage and database variants wrap types that
/// are not), so the variants a rotation stage can actually produce are
/// reproduced and anything else keeps its rendered text.
fn clone_error(err: &Error) -> Error {
    match err {
        Error::Schema(m) => Error::Schema(m.clone()),
        Error::UnsupportedType { column, data_type } => Error::UnsupportedType {
            column: column.clone(),
            data_type: data_type.clone(),
        },
        Error::Naming(m) => Error::Naming(m.clone()),
        Error::Config(m) => Error::Config(m.clone()),
        other => Error::Config(other.to_string()),
    }
}

/// Whether retrying this file can ever succeed.
///
/// Judged on what the error IS, not on which stage produced it. Only a
/// rejection of the data or the schema is beyond retry: the file's bytes
/// cannot be turned into a CSV however many times we try, and the rows have
/// to reach a human. Everything else — a storage error, a panic in a
/// blocking task, a task cancelled while the runtime winds down — may well
/// succeed next time, and stopping the stream for it would turn a blip into
/// an outage.
///
/// A panic that really is a deterministic bug therefore retries forever
/// rather than stopping the stream. That is the deliberate trade: it stays
/// visible (a WARN per attempt, a growing `lag_seconds`, and `flush`
/// reporting `StagingStalled`) and it recovers by itself if the cause was
/// transient, whereas a wrong "permanent" verdict needs an operator to
/// restart a stream that would have healed.
fn is_permanent(err: &Error) -> bool {
    matches!(err, Error::Schema(_) | Error::UnsupportedType { .. })
}

/// Retry pacing for a failing stage: 1s, 2s, 4s, 8s, 16s, then 30s.
fn backoff(attempt: u32) -> Duration {
    let secs = 1u64 << attempt.saturating_sub(1).min(5);
    Duration::from_secs(secs.min(30))
}

/// A file is durable: publish it, and clear a stage failure it has caught
/// up with.
///
/// Only a stage failure. A terminal state (fenced, pipeline gone, order
/// tripped) carries seq `i64::MIN` and would match any comparison, but it is
/// exactly what a parked `flush` is waiting to be woken by -- clearing it
/// would leave that `flush` with nothing to wake it, since the writer holds
/// a progress sender of its own and the channel never closes. A fence
/// landing while the put stage was inside `record_durable` is that window.
fn land(progress: &watch::Sender<Progress>, seq: i64) {
    progress.send_modify(|p| {
        p.durable_seq = Some(seq);
        if p.failing
            .as_ref()
            .is_some_and(|f| !f.kind.is_terminal() && f.seq <= seq)
        {
            p.failing = None;
        }
    });
}

/// A stage failed on `meta`: log it and let `flush`/`close` see it.
fn report_failure(
    progress: &watch::Sender<Progress>,
    meta: &FileMeta,
    stage: &'static str,
    attempt: u32,
    permanent: bool,
    err: &Error,
) {
    // Two different operational situations, so two different levels: a
    // transient failure clears itself and is worth a WARN; a permanent one
    // needs someone to act and must not be filtered out with the noise.
    if permanent {
        tracing::error!(
            stage,
            serial_seq = meta.serial_seq,
            attempt,
            error = %err,
            "rotation stage failed permanently; retrying will not fix it and this stream \
             has stopped -- see the error for what to repair"
        );
    } else {
        tracing::warn!(
            stage,
            serial_seq = meta.serial_seq,
            attempt,
            error = %err,
            "rotation stage failed; the same file will be retried"
        );
    }
    let last = format!("{stage}: {err}");
    progress.send_if_modified(|p| {
        match &p.failing {
            // A terminal state (fenced, pipeline gone) is never overwritten.
            Some(f) if f.kind.is_terminal() => return false,
            // The slot reports the HEAD of the pipeline: a later file
            // failing behind a stuck earlier one must not hide the earlier
            // one from a flush waiting on it. Same seq updates in place.
            Some(f) if f.seq < meta.serial_seq => return false,
            _ => {}
        }
        p.failing = Some(Failing {
            seq: meta.serial_seq,
            stage,
            attempts: attempt,
            permanent,
            // Only a permanent failure carries its cause: a transient one is
            // reported as StagingStalled, whose contract is "keep waiting".
            cause: permanent.then(|| Arc::new(clone_error(err))),
            kind: FailKind::Stage,
            last,
        });
        true
    });
}

/// The file that was failing got past its stage.
fn clear_failure(progress: &watch::Sender<Progress>, seq: i64) {
    progress.send_if_modified(|p| match &p.failing {
        Some(f) if f.seq == seq => {
            p.failing = None;
            true
        }
        _ => false,
    });
}

/// A fenced writer was preempted: whatever it still holds in the pipeline
/// belongs to an epoch the new holder has moved past. Uploading or
/// announcing it would race the successor (and, insert-only, duplicate its
/// replay), so the stage drops the file and stops; the new holder's recovery
/// listing owns anything that already landed.
fn fenced_exit(inner: &WriterInner, stage: &str, meta: &FileMeta) -> bool {
    if !inner.is_fenced() {
        return false;
    }
    tracing::warn!(
        stage,
        serial_seq = meta.serial_seq,
        group = %inner.serial_group,
        "writer is fenced: dropping the in-flight file and stopping this stage"
    );
    // Wake anyone waiting in flush/close on this or a later file: the
    // pipeline will not deliver it. Guarded: an order violation that fired
    // first must keep the slot -- it needs a human to rewind offsets, a
    // fence does not, and the error a waiter gets decides which they do.
    let reason = inner
        .fenced
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| "writer was fenced".into());
    publish_terminal(
        &inner.pipeline.progress_tx,
        FailKind::Fenced,
        "pipeline",
        None,
        reason,
    );
    inner.raise_stream_stopped();
    true
}

/// The order tripwire, evaluated in the put stage right before the first
/// externally visible side effect. `prev` is the last file this pipeline
/// made durable, as (epoch_ms, seq, end_offset); None for the first.
///
/// One check, on SDK-internal state only: `(epoch, seq)` must be
/// CONTIGUOUS. Seqs are the dense sequence `seal` allocates under the state
/// lock, and in steady state no sealed file is ever dropped -- a failed put
/// retries the same bytes indefinitely, and the only early exits (a fenced
/// writer, a dead pipeline, this tripwire) end the stage rather than skip
/// ahead -- so a gap means a file was lost or overtaken. Anyone adding a
/// "skip this file on failure" rule must revisit this. Kafka offsets are
/// NOT judged here: they are caller input, validated synchronously in
/// `append` (they must move forward within a writer), so a rewound consumer
/// gets an error on that call instead of a poisoned writer; they only
/// appear in the message below to name the gap.
///
/// Correctly built, this never fires; it exists because the failure it
/// guards is silent -- an out-of-order file is swallowed by the server's
/// watermark gate as an already-consumed replay while `flush` returns Ok.
fn check_order(
    prev: Option<(u64, u32, i64)>,
    next: &StagedFile,
) -> std::result::Result<(), String> {
    let Some((epoch, seq, end)) = prev else {
        return Ok(());
    };
    let seq_ok = if next.epoch_ms == epoch {
        next.seq == seq.wrapping_add(1)
    } else {
        next.epoch_ms > epoch && next.seq == 0
    };
    if seq_ok {
        return Ok(());
    }
    let why = if next.epoch_ms == epoch && next.seq > seq + 1 {
        format!(
            "seqs {}..={} of epoch {epoch} never arrived",
            seq + 1,
            next.seq - 1
        )
    } else {
        format!(
            "expected seq {} of epoch {epoch} (or seq 0 of a later epoch)",
            seq + 1
        )
    };
    let gap = if next.start_offset > end + 1 {
        format!(
            "; Kafka offsets {}..={} are not in staging",
            end + 1,
            next.start_offset - 1
        )
    } else {
        String::new()
    };
    Err(format!(
        "file epoch {} seq {} (Kafka offsets [{}, {}]) reached the upload stage after epoch \
         {epoch} seq {seq} (offsets up to {end}): {}{gap}. A restart resumes after the \
         highest staged offset and will NOT re-stage that gap. Stop consuming this stream, \
         do not commit its Kafka offsets, roll back or upgrade the SDK, then rewind the \
         consumer to the start of the gap (GUIDE.md, \"Error handling\").",
        next.epoch_ms, next.seq, next.start_offset, next.end_offset, why
    ))
}

fn ms(d: Duration) -> u64 {
    d.as_millis() as u64
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
        writer_with("id", 3, ClientConfig::DEFAULT_ROTATE_SIZE)
    }

    /// Same wiring with a chosen primary-key column, queue depth and size
    /// threshold. A pk that is not a column makes the render stage fail for
    /// good, which stalls the pipeline without touching the network; a
    /// threshold of 1 makes every append seal its own file.
    fn writer_with(pk: &str, depth: usize, rotate_size: u64) -> TableWriter {
        let arrow = StdArc::new(schema(vec![Field::new("id", DataType::Int64, false)]));
        let table_schema = TableSchema {
            arrow: arrow.clone(),
            pk: vec![pk.to_string()],
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
        let mut cfg = ClientConfig::with_customer_staging(
            staging_cfg.clone(),
            "host=127.0.0.1 port=1 user=nobody dbname=nobody",
        );
        cfg.rotation_queue_depth = depth;
        cfg.rotate_size_bytes = rotate_size;
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
    async fn flush_with_nothing_buffered_returns_at_once() {
        // Nothing sealed, nothing to wait for: flush must not touch the
        // pipeline (whose store is unreachable here).
        let w = test_writer();
        assert_eq!(w.flush().await.unwrap(), None);
    }

    #[tokio::test]
    async fn seal_cuts_the_whole_buffer_under_increasing_seqs() {
        let w = test_writer();
        let b = one_row(w.schema());
        w.append(b.clone(), 0, 3).await.unwrap();
        w.append(b.clone(), 4, 7).await.unwrap();
        let mut st = w.inner.state.lock().await;
        let first = w
            .inner
            .seal(&mut st, false)
            .unwrap()
            .expect("two batches are buffered");
        assert_eq!(
            (first.meta.file.start_offset, first.meta.file.end_offset),
            (0, 7)
        );
        assert_eq!(first.batches.len(), 2);
        assert_eq!(first.meta.rows_in, 2);
        assert!(st.buffered.is_empty());
        assert_eq!(st.buffered_rows, 0);
        assert!(st.oldest_buffered_at.is_none());
        assert_eq!(st.last_sealed_seq, Some(first.meta.serial_seq));
        // Empty buffer: nothing to seal and no seq consumed.
        assert!(w.inner.seal(&mut st, false).unwrap().is_none());
        assert_eq!(st.next_seq, first.meta.file.seq + 1);
        drop(st);

        w.append(b, 8, 8).await.unwrap();
        let mut st = w.inner.state.lock().await;
        // Not aged yet: the ticker's variant leaves the buffer alone; a
        // full seal takes it under the next seq.
        assert!(w.inner.seal(&mut st, true).unwrap().is_none());
        let second = w.inner.seal(&mut st, false).unwrap().unwrap();
        assert_eq!(second.meta.file.seq, first.meta.file.seq + 1);
        assert!(second.meta.serial_seq > first.meta.serial_seq);
        assert_eq!(
            (second.meta.file.start_offset, second.meta.file.end_offset),
            (8, 8)
        );
    }

    /// Review finding on the first cut: flush took the same blocking
    /// hand-over path as append, so once a storage outage had filled the
    /// queue it could never reach the code that reports the failure.
    #[tokio::test]
    async fn flush_reports_a_stalled_pipeline_even_with_a_full_queue() {
        let w = writer_with("missing", 1, ClientConfig::DEFAULT_ROTATE_SIZE);
        let b = one_row(w.schema());
        w.append(b.clone(), 0, 0).await.unwrap();
        // File 1 is sealed; the render stage fails for good and keeps
        // retrying it. flush must report that, not wait. The failure is
        // permanent (the pk column does not exist), so it comes back as
        // RotationFailed rather than the retry-and-wait StagingStalled.
        let e = tokio::time::timeout(Duration::from_secs(10), w.flush())
            .await
            .expect("flush returns")
            .unwrap_err();
        assert!(matches!(e, Error::RotationFailed { .. }), "{e}");
        // The writer has stopped for good -- a permanent stage failure is
        // one of the fatal states -- so every further call reports the same
        // thing rather than accepting more rows.
        assert!(w.fatal_error().is_some());
        let e = w.append(b, 1, 1).await.unwrap_err();
        assert!(matches!(e, Error::RotationFailed { .. }), "{e}");
        let e = tokio::time::timeout(Duration::from_secs(10), w.flush())
            .await
            .expect("flush must not hang")
            .unwrap_err();
        assert!(matches!(e, Error::RotationFailed { .. }), "{e}");
    }

    /// A permanent failure now stops the stage instead of spinning on a
    /// file it can never render: the channel closes behind it, which is how
    /// a caller can tell the pipeline is done rather than still working.
    #[tokio::test]
    async fn a_permanent_failure_stops_the_pipeline() {
        let w = writer_with("missing", 1, ClientConfig::DEFAULT_ROTATE_SIZE);
        let b = one_row(w.schema());
        w.append(b, 0, 0).await.unwrap();
        w.inner.seal_and_send(false, false).await.unwrap();
        // One backoff is 1s; give the render stage room to fail, report and
        // exit, then confirm it did not queue up for another attempt.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(w.fatal_error().is_some(), "the writer is fatal");
        assert!(
            w.inner.pipeline.tx.is_closed(),
            "the render stage exited, closing the seal channel behind it"
        );
        // The failure it reported is the permanent kind, so a waiter is told
        // retrying will not help rather than to keep flushing.
        let e = w.flush().await.unwrap_err();
        assert!(matches!(e, Error::RotationFailed { .. }), "{e}");
    }

    /// A pipeline stalled on a TRANSIENT failure still takes rows while the
    /// queue has room, and once it is full `flush` reports rather than
    /// hanging -- the backpressure path, which a permanent failure
    /// short-circuits by stopping the stream instead.
    #[tokio::test]
    async fn a_transient_stall_still_accepts_rows_until_the_queue_is_full() {
        // The default size threshold, so `append` only buffers and the test
        // decides when a file is cut. Depth 1, so the queue fills as soon as
        // the stages behind it are each holding one.
        let w = writer_with("id", 1, ClientConfig::DEFAULT_ROTATE_SIZE);
        let b = one_row(w.schema());
        // The store is unreachable, so the put stage retries transiently and
        // everything backs up behind it. Seal while there is still room.
        for i in 0..24i64 {
            if w.inner.pipeline.tx.capacity() == 0 {
                break;
            }
            w.append(b.clone(), i, i).await.unwrap();
            w.inner.seal_and_send(false, false).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(w.inner.pipeline.tx.capacity(), 0, "the queue is full");
        // A retryable stall must not page anyone, and must not be fatal.
        assert!(w.fatal_error().is_none(), "a retryable stall is not fatal");
        // One more row stays buffered. flush has to come back rather than
        // hang on the full queue, and must not cut a buffer it cannot hand
        // over.
        w.append(b, 100, 100).await.unwrap();
        let e = tokio::time::timeout(Duration::from_secs(30), w.flush())
            .await
            .expect("flush must not hang on a full queue")
            .unwrap_err();
        assert!(matches!(e, Error::StagingStalled { .. }), "{e}");
        assert_eq!(w.inner.state.lock().await.buffered.len(), 1);
    }

    /// Review finding on MR 4: a permanent failure was recorded as an
    /// ordinary Stage entry, so an earlier seq's transient failure
    /// overwrote it and a later landing cleared it -- after which the
    /// writer was dead and `fatal_error()` reported nothing.
    #[tokio::test]
    async fn a_permanent_failure_survives_later_stage_traffic() {
        let w = test_writer();
        let tx = &w.inner.pipeline.progress_tx;
        let err = Error::Config("x".into());

        // The permanent record, as the stages now publish it.
        publish_terminal(
            tx,
            FailKind::Rotation,
            "render",
            Some(Arc::new(Error::Schema(
                "column `id` is not in the batch".into(),
            ))),
            "render: schema".into(),
        );
        assert!(w.fatal_error().is_some());

        // An EARLIER seq failing transiently must not take the slot.
        report_failure(tx, &meta(1), "put", 1, false, &err);
        assert_eq!(
            tx.borrow().failing.as_ref().map(|f| f.kind),
            Some(FailKind::Rotation)
        );
        // Nor may a landing clear it.
        land(tx, i64::MAX);
        assert_eq!(
            tx.borrow().failing.as_ref().map(|f| f.kind),
            Some(FailKind::Rotation)
        );
        // Still fatal, and still naming the real cause.
        let reason = w.fatal_error().expect("still fatal");
        assert!(reason.contains("not in the batch"), "{reason}");
    }

    /// Review finding on MR 4: the alarm kept whichever state arrived first
    /// while `fatal_error()` sorts them, so a fence landing before an order
    /// violation had the alarm saying "do not rewind" and the writer saying
    /// "you must".
    #[tokio::test]
    async fn the_alarm_upgrades_to_the_more_urgent_state() {
        let w = test_writer();
        // A fence first: the least urgent of the two.
        *w.inner.fenced.lock().unwrap() = Some("preempted".into());
        w.inner.raise_stream_stopped();
        assert_eq!(
            w.inner
                .alarm_raised
                .load(std::sync::atomic::Ordering::SeqCst),
            fatal_priority(&Error::WriterFenced(String::new()))
        );
        // The order violation outranks it, so it gets its own line.
        w.inner.trip_order_fatal("seqs 8..=8 never arrived".into());
        assert_eq!(
            w.inner
                .alarm_raised
                .load(std::sync::atomic::Ordering::SeqCst),
            fatal_priority(&Error::StagingOrderViolation(String::new()))
        );
        // And what a caller is told matches what was announced.
        assert!(matches!(
            w.inner.check_health(),
            Err(Error::StagingOrderViolation(_))
        ));
        // A third call adds nothing: only upgrades page.
        w.inner.raise_stream_stopped();
        assert_eq!(
            w.inner
                .alarm_raised
                .load(std::sync::atomic::Ordering::SeqCst),
            fatal_priority(&Error::StagingOrderViolation(String::new()))
        );
    }

    /// Review finding on the first cut: the stages checked only that the
    /// writer still existed, so a preempted writer kept uploading and
    /// announcing files under the epoch its successor had moved past.
    #[tokio::test]
    async fn fenced_writer_stops_its_pipeline() {
        let w = test_writer();
        let b = one_row(w.schema());
        w.append(b, 0, 0).await.unwrap();
        // Hand a file over, then fence before the render stage has run
        // (current-thread runtime: it runs only when this task yields).
        assert!(w.inner.seal_and_send(false, false).await.unwrap().is_some());
        *w.inner.fenced.lock().unwrap() = Some("preempted in test".into());
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The render stage saw the fence, dropped the file and exited, and
        // told the progress channel so that any waiter wakes with the fence.
        assert!(w.inner.pipeline.tx.is_closed());
        assert_eq!(
            w.inner
                .pipeline
                .progress
                .borrow()
                .failing
                .as_ref()
                .map(|f| f.kind),
            Some(FailKind::Fenced)
        );
        assert!(matches!(
            w.inner.await_durable(i64::MAX).await,
            Err(Error::WriterFenced(_))
        ));
        // Nothing was staged, and the writer refuses further work.
        assert_eq!(w.inner.state.lock().await.staged_offset, None);
        assert!(matches!(w.flush().await, Err(Error::WriterFenced(_))));
    }

    #[tokio::test]
    async fn sealed_files_count_as_lag_before_they_are_durable() {
        let w = writer_with("missing", 1, ClientConfig::DEFAULT_ROTATE_SIZE);
        let b = one_row(w.schema());
        w.append(b, 0, 0).await.unwrap();
        // Buffered rows: the age signal reads from the buffer.
        assert!(oldest_unstaged_age(&*w.inner.state.lock().await).is_some());
        let seq = w.inner.seal_and_send(false, false).await.unwrap().unwrap();
        let st = w.inner.state.lock().await;
        // Sealed, not durable (the render stage fails for good): the buffer
        // is empty, yet the file is registered for lag and still ages.
        assert!(st.buffered.is_empty() && st.oldest_buffered_at.is_none());
        assert_eq!(st.known_files.last().map(|(s, _)| *s), Some(seq));
        assert_eq!(st.in_flight.len(), 1);
        assert!(oldest_unstaged_age(&st).is_some());
        assert_eq!(lag_of(&mut st.known_files.clone(), None, u64::MAX).0, 1);
    }

    fn meta(seq: i64) -> FileMeta {
        FileMeta {
            file: file(1, seq as u32, 0, 0),
            serial_seq: seq,
            identifier: String::new(),
            object_key: String::new(),
            rows_in: 0,
            sealed_at: Instant::now(),
            queue_ms: 0,
            render_ms: 0,
            gzip_ms: 0,
        }
    }

    #[test]
    fn failing_slot_reports_the_head_and_keeps_terminal_states() {
        let (tx, rx) = watch::channel(Progress::default());
        let err = Error::Config("x".into());
        let seq_of = |rx: &watch::Receiver<Progress>| {
            rx.borrow().failing.as_ref().map(|f| (f.seq, f.attempts))
        };
        report_failure(&tx, &meta(7), "render", 1, false, &err);
        assert_eq!(seq_of(&rx), Some((7, 1)));
        // The earlier (head) file's failure takes the slot over.
        report_failure(&tx, &meta(5), "put", 1, false, &err);
        assert_eq!(seq_of(&rx), Some((5, 1)));
        // A later file's failure must not hide it.
        report_failure(&tx, &meta(9), "render", 1, false, &err);
        assert_eq!(seq_of(&rx), Some((5, 1)));
        // The same file's next attempt updates in place.
        report_failure(&tx, &meta(5), "put", 2, false, &err);
        assert_eq!(seq_of(&rx), Some((5, 2)));
        // Clearing only that file frees the slot; a later one can then report.
        clear_failure(&tx, 5);
        assert_eq!(seq_of(&rx), None);
        report_failure(&tx, &meta(9), "render", 1, false, &err);
        assert_eq!(seq_of(&rx), Some((9, 1)));
        // Terminal states win over any stage failure.
        tx.send_modify(|p| {
            p.failing = Some(Failing {
                seq: i64::MIN,
                stage: "pipeline",
                attempts: 0,
                permanent: true,
                cause: None,
                kind: FailKind::Fenced,
                last: "fenced".into(),
            })
        });
        report_failure(&tx, &meta(3), "put", 1, false, &err);
        assert_eq!(
            rx.borrow().failing.as_ref().map(|f| f.kind),
            Some(FailKind::Fenced)
        );
    }

    fn file(epoch_ms: u64, seq: u32, start: i64, end: i64) -> StagedFile {
        StagedFile {
            epoch_ms,
            seq,
            start_offset: start,
            end_offset: end,
            compressed: true,
        }
    }

    #[test]
    fn order_tripwire_accepts_contiguous_seqs_and_monotonic_offsets() {
        assert!(check_order(None, &file(5, 7, 100, 110)).is_ok());
        assert!(check_order(Some((5, 7, 110)), &file(5, 8, 111, 120)).is_ok());
        // Offset holes are legitimate (transaction markers, compaction).
        assert!(check_order(Some((5, 7, 110)), &file(5, 8, 500, 520)).is_ok());
        // Epoch rollover: a later epoch restarts at seq 0.
        assert!(check_order(Some((5, MAX_SEQ, 110)), &file(6, 0, 111, 111)).is_ok());
    }

    #[test]
    fn order_tripwire_rejects_gaps_overtakes_and_offset_regressions() {
        // A lost file: seq 8 never made it.
        let e = check_order(Some((5, 7, 110)), &file(5, 9, 130, 140)).unwrap_err();
        assert!(e.contains("seqs 8..=8 of epoch 5 never arrived"), "{e}");
        assert!(e.contains("offsets 111..=129 are not in staging"), "{e}");
        // Overtaken: an older file arrives after a newer one.
        assert!(check_order(Some((5, 8, 120)), &file(5, 7, 100, 110)).is_err());
        // Same seq twice.
        assert!(check_order(Some((5, 7, 110)), &file(5, 7, 111, 120)).is_err());
        // Epoch rollover must start at 0.
        assert!(check_order(Some((5, 7, 110)), &file(6, 1, 111, 120)).is_err());
        // Offsets are not the tripwire's business (append validates them).
        assert!(check_order(Some((5, 7, 110)), &file(5, 8, 105, 120)).is_ok());
    }

    /// Review finding on the second cut: the pipeline never woke a waiting
    /// flush when the tripwire fired, and close() skipped the health gate.
    #[tokio::test]
    async fn order_violation_wakes_waiters_and_fails_flush_and_close() {
        let w = test_writer();
        w.inner.trip_order_fatal("gap in test".into());
        assert!(matches!(
            w.inner.await_durable(i64::MAX).await,
            Err(Error::StagingOrderViolation(_))
        ));
        assert!(matches!(
            w.flush().await,
            Err(Error::StagingOrderViolation(_))
        ));
        assert!(matches!(
            w.close().await,
            Err(Error::StagingOrderViolation(_))
        ));
    }

    /// Review finding on the second cut: append parked forever on a full
    /// queue behind a stuck pipeline, so the usual append*N -> flush loop
    /// never reached the flush that reports the stall.
    #[tokio::test]
    async fn append_reports_a_stalled_pipeline_without_taking_the_batch() {
        let w = writer_with("missing", 1, 1);
        let b = one_row(w.schema());
        // Every append seals its own file. Yield so the render stage
        // (current-thread runtime: it runs only when this task yields) takes
        // file 1 and reports its failure before the next append.
        w.append(b.clone(), 0, 0).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        // The failure is permanent, so the writer has stopped: the next
        // append reports it instead of parking on the queue, and does not
        // take the batch.
        let e = tokio::time::timeout(Duration::from_secs(10), w.append(b.clone(), 1, 1))
            .await
            .expect("append must not park on a stalled pipeline")
            .unwrap_err();
        assert!(matches!(e, Error::RotationFailed { .. }), "{e}");
        {
            let st = w.inner.state.lock().await;
            assert!(st.buffered.is_empty(), "the batch was not taken");
            assert_eq!(st.max_end_offset, Some(0), "the water mark did not move");
        }
        // And it keeps reporting it rather than accepting rows.
        assert!(w.fatal_error().is_some());
        let e = w.append(b, 1, 1).await.unwrap_err();
        assert!(matches!(e, Error::RotationFailed { .. }), "{e}");
    }

    /// Review finding on the second cut: offsets are caller input, so a
    /// rewound consumer gets a synchronous error, not a poisoned writer.
    #[tokio::test]
    async fn append_rejects_offsets_going_backwards() {
        let w = test_writer();
        let b = one_row(w.schema());
        w.append(b.clone(), 0, 5).await.unwrap();
        let same_end = w.append(b.clone(), 5, 6).await.unwrap_err();
        assert!(matches!(same_end, Error::Config(_)), "{same_end}");
        let earlier = w.append(b.clone(), 3, 4).await.unwrap_err();
        assert!(matches!(earlier, Error::Config(_)), "{earlier}");
        // The writer is still usable; only the batch was refused.
        w.append(b, 6, 6).await.unwrap();
        assert_eq!(w.inner.state.lock().await.buffered.len(), 2);
    }

    /// Review finding on the sixth round: a permanent stage failure was
    /// reported as StagingStalled, whose documented remedy is "keep
    /// flushing" -- an endless loop for something retrying cannot fix.
    #[test]
    fn permanent_and_transient_stage_failures_map_to_different_errors() {
        let (tx, rx) = watch::channel(Progress::default());
        let cfg = ClientConfig::new("host=127.0.0.1 port=1 user=n dbname=n");
        let attempts = cfg.staging_error_after_attempts;

        // Transient: still "wait, it may clear".
        report_failure(
            &tx,
            &meta(1),
            "put",
            attempts,
            false,
            &Error::Config("net".into()),
        );
        let f = rx.borrow().failing.clone().unwrap();
        assert!(matches!(terminal_error(&f), Error::StagingStalled { .. }));

        // Permanent: names the stage and carries the cause through.
        clear_failure(&tx, 1);
        report_failure(
            &tx,
            &meta(2),
            "render",
            1,
            true,
            &Error::Schema("column type Float64 cannot be part of a primary key".into()),
        );
        let f = rx.borrow().failing.clone().unwrap();
        match terminal_error(&f) {
            Error::RotationFailed { stage, source } => {
                assert_eq!(stage, "render");
                assert!(source.to_string().contains("primary key"), "{source}");
            }
            other => panic!("expected RotationFailed, got {other}"),
        }
    }

    /// Review finding on the sixth round: a float primary key passed
    /// open_table and then failed forever inside the pipeline, where the
    /// rows are already sealed and the error cannot be acted on. The
    /// seventh round narrowed it: only floats, and only for upsert.
    #[test]
    fn float_primary_keys_are_refused_for_upsert_only() {
        let with_key = |dt: DataType| TableSchema {
            arrow: StdArc::new(schema(vec![Field::new("id", dt, false)])),
            pk: vec!["id".into()],
            db_oid: 1,
            rel_oid: 2,
        };

        // Every supported column type can be a key, binary included ...
        for ok in [
            DataType::Int64,
            DataType::Utf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::Boolean,
            DataType::Date32,
            DataType::Decimal128(18, 5),
        ] {
            let s = with_key(ok.clone());
            assert!(s.validate().is_ok(), "{ok:?}");
            assert!(s.validate_pk_for_dedup().is_ok(), "{ok:?}");
        }

        // ... except floats, whose equality does not agree across the two
        // sides. The column type itself stays supported.
        for bad in [DataType::Float64, DataType::Float32] {
            let s = with_key(bad.clone());
            assert!(crate::csv::is_supported_type(&bad), "{bad:?}");
            // The generic check stays silent: insert-only never dedups, so
            // such a table must keep working there.
            assert!(s.validate().is_ok(), "{bad:?}");
            let e = s.validate_pk_for_dedup().unwrap_err();
            assert!(
                matches!(&e, Error::UnsupportedType { data_type, .. } if data_type.contains("upsert")),
                "{bad:?}: {e}"
            );
        }
    }

    /// The permanence verdict is on the error, not on the stage that hit
    /// it: only a rejection of the data or the schema is beyond retry, so a
    /// panic in a blocking task (which is what a JoinError is) keeps being
    /// retried instead of stopping a stream that may well recover.
    #[test]
    fn only_data_and_schema_errors_are_beyond_retry() {
        // Cannot be turned into a CSV however often we try.
        assert!(is_permanent(&Error::Schema(
            "pk column not in batch".into()
        )));
        assert!(is_permanent(&Error::UnsupportedType {
            column: "c".into(),
            data_type: "Interval".into(),
        }));

        // A blocking task that panicked or was cancelled: this is the only
        // failure the render and gzip stages can actually reach through the
        // public API, and it must not stop the stream.
        assert!(!is_permanent(&Error::Config(
            "render task failed: task panicked".into()
        )));
        assert!(!is_permanent(&Error::Config(
            "gzip task failed: task was cancelled".into()
        )));

        // Storage and control-plane trouble: transient by nature.
        assert!(!is_permanent(&Error::Config("connection reset".into())));
        assert!(!is_permanent(&Error::WriterClosed));
    }

    /// `fatal_error()` used to report only one of the four ways a writer
    /// stops, while the guide told operators to build their dashboard on it
    /// -- so a fenced writer, a tripped tripwire or a permanent stage
    /// failure all showed green.
    #[tokio::test]
    async fn fatal_error_covers_every_stopped_state() {
        // Healthy: nothing to report.
        let w = test_writer();
        assert!(w.fatal_error().is_none());
        assert!(w.inner.check_health().is_ok());

        // 1. Preempted lease.
        let w = test_writer();
        *w.inner.fenced.lock().unwrap() = Some("taken over by another process".into());
        let reason = w.fatal_error().expect("fenced writer is fatal");
        assert!(reason.contains("taken over"), "{reason}");
        assert!(matches!(
            w.inner.check_health(),
            Err(Error::WriterFenced(_))
        ));

        // 2. Order tripwire.
        let w = test_writer();
        w.inner.trip_order_fatal("seqs 8..=8 never arrived".into());
        let reason = w.fatal_error().expect("order violation is fatal");
        assert!(reason.contains("8..=8"), "{reason}");
        assert!(matches!(
            w.inner.check_health(),
            Err(Error::StagingOrderViolation(_))
        ));

        // 3. Writer identity clash, recorded by the notify loop on the group
        // rather than set by this writer -- the state the ticker discovers.
        let w = test_writer();
        w.inner
            .notifier
            .arm_fatal_for_test(&w.inner.serial_group, "duplicate writer_id");
        let reason = w.fatal_error().expect("identity clash is fatal");
        assert!(reason.contains("duplicate writer_id"), "{reason}");
        assert!(matches!(
            w.inner.check_health(),
            Err(Error::SerialContractViolation(_))
        ));

        // 4. A stage failure retrying cannot fix.
        let w = test_writer();
        report_failure(
            &w.inner.pipeline.progress_tx,
            &meta(4),
            "render",
            1,
            true,
            &Error::Schema("column `id` is not in the batch".into()),
        );
        let reason = w.fatal_error().expect("permanent stage failure is fatal");
        assert!(reason.contains("not in the batch"), "{reason}");

        // A transient one is NOT fatal: it clears itself, and a monitor must
        // not page on it.
        let w = test_writer();
        report_failure(
            &w.inner.pipeline.progress_tx,
            &meta(4),
            "put",
            w.inner.cfg.staging_error_after_attempts,
            false,
            &Error::Config("connection reset".into()),
        );
        assert!(w.fatal_error().is_none(), "a retryable stall is not fatal");
    }

    /// Review finding on the fifth round: known_files carried the session
    /// epoch, so lag_seconds read "time since open", not file age.
    #[tokio::test]
    async fn sealed_files_are_stamped_with_their_own_time_not_the_epoch() {
        let w = test_writer(); // initial_epoch_ms is 1 in this fixture
        let b = one_row(w.schema());
        w.append(b, 0, 0).await.unwrap();
        let before = crate::lock::now_ms();
        let seq = w.inner.seal_and_send(false, false).await.unwrap().unwrap();
        let st = w.inner.state.lock().await;
        let (s, stamped) = *st.known_files.last().unwrap();
        assert_eq!(s, seq);
        assert!(
            stamped >= before && stamped <= crate::lock::now_ms(),
            "{stamped}"
        );
        // Would have been ~1 with the epoch, i.e. decades of "lag".
        assert!(stamped > 1_000_000_000_000);
    }

    /// Review finding on the fifth round: a fence landing after the order
    /// tripwire overwrote its terminal state and check_health hid it.
    #[tokio::test]
    async fn order_violation_survives_a_later_fence() {
        let w = test_writer();
        w.inner.trip_order_fatal("gap".into());
        *w.inner.fenced.lock().unwrap() = Some("preempted".into());
        // The stage-side exit must keep the earlier terminal state ...
        assert!(fenced_exit(&w.inner, "put", &meta(1)));
        assert_eq!(
            w.inner
                .pipeline
                .progress
                .borrow()
                .failing
                .as_ref()
                .map(|f| f.kind),
            Some(FailKind::OrderViolation)
        );
        // ... and so must the foreground gate.
        assert!(matches!(
            w.inner.check_health(),
            Err(Error::StagingOrderViolation(_))
        ));
        assert!(matches!(
            w.inner.await_durable(i64::MAX).await,
            Err(Error::StagingOrderViolation(_))
        ));
    }

    /// Review finding on the third cut: `unbuffer` rolled the offset
    /// high-water mark back even when a concurrent sealer had already taken
    /// the batch, so `append` reported "not accepted" for a batch that was
    /// in the pipeline and the caller would have appended it twice.
    #[tokio::test]
    async fn unbuffer_only_rolls_back_the_batch_it_takes() {
        let w = test_writer();
        let b = one_row(w.schema());
        w.append(b.clone(), 0, 5).await.unwrap();
        w.append(b, 6, 9).await.unwrap();
        // The batch is still buffered: taken back, water mark restored.
        assert!(w.inner.unbuffer(6, 9, Some(5)).await);
        {
            let st = w.inner.state.lock().await;
            assert_eq!(st.buffered.len(), 1);
            assert_eq!(st.max_end_offset, Some(5));
            assert_eq!(st.buffered_rows, 1);
        }
        // Someone else sealed it meanwhile: nothing to take back, and the
        // water mark must not move (it would let the caller re-append).
        assert!(!w.inner.unbuffer(10, 12, Some(5)).await);
        assert_eq!(w.inner.state.lock().await.max_end_offset, Some(5));
    }

    /// Review finding on the third cut: a file landing cleared any failing
    /// entry at or below its seq, and terminal states carry i64::MIN, so a
    /// fence that landed during `record_durable` was wiped and the flush it
    /// should have woken parked forever.
    ///
    /// Walks `ALL_FAIL_KINDS` rather than a list written out here: the
    /// earlier hand-written one silently stopped covering `Rotation` the
    /// moment that variant was added.
    #[tokio::test]
    async fn a_landed_file_clears_stage_failures_but_not_terminal_states() {
        let w = test_writer();
        let tx = &w.inner.pipeline.progress_tx;
        let current = || {
            w.inner
                .pipeline
                .progress
                .borrow()
                .failing
                .as_ref()
                .map(|f| f.kind)
        };

        report_failure(tx, &meta(5), "put", 1, false, &Error::Config("x".into()));
        land(tx, 5);
        assert_eq!(current(), None, "the stage failure is caught up with");

        for &kind in ALL_FAIL_KINDS {
            // seq i64::MIN as every terminal writer records it, which is what
            // made the landing comparison match them in the first place.
            tx.send_modify(|p| {
                p.failing = Some(Failing {
                    seq: i64::MIN,
                    stage: "pipeline",
                    attempts: 0,
                    permanent: true,
                    cause: None,
                    kind,
                    last: "terminal".into(),
                })
            });
            land(tx, 9);
            if kind.is_terminal() {
                assert_eq!(current(), Some(kind), "{kind:?} must survive a landing");
            } else {
                assert_eq!(current(), None, "{kind:?} is cleared by a landing");
            }
        }
    }

    /// Review finding on the second cut: pipeline_gone overwrote a Fenced
    /// terminal state, turning WriterFenced into WriterClosed.
    #[tokio::test]
    async fn pipeline_gone_keeps_an_earlier_terminal_state() {
        let w = test_writer();
        w.inner.pipeline.progress_tx.send_modify(|p| {
            p.failing = Some(Failing {
                seq: i64::MIN,
                stage: "pipeline",
                attempts: 0,
                permanent: true,
                cause: None,
                kind: FailKind::Fenced,
                last: "preempted".into(),
            })
        });
        assert!(matches!(w.inner.pipeline_gone(), Error::WriterFenced(_)));
        assert_eq!(
            w.inner
                .pipeline
                .progress
                .borrow()
                .failing
                .as_ref()
                .map(|f| f.kind),
            Some(FailKind::Fenced)
        );
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
                                inner.fence(reason);
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
                    inner.fence(reason);
                    return;
                }
                Ok(None) => {
                    let reason = format!(
                        "lease at `{}` was deleted (operator force-release) — stopping",
                        ident.lock_key()
                    );
                    inner.fence(reason);
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
                        inner.fence(reason);
                        return;
                    }
                }
            }
        }
    });
}

/// Consumption-lag sampler (GUIDE.md "Monitoring lag, and what to alert on"):
/// every `lag_sample_interval` (~30s), read the server group watermark over one
/// long-lived control connection and publish a [`LagSnapshot`] through
/// [`TableWriter::lag`].
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
                    oldest_unstaged_age(&st).map(|d| d.as_secs()).unwrap_or(0),
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

/// Age of the oldest row not yet durable on staging: the buffer's oldest
/// batch or the oldest sealed-but-not-durable file, whichever is older.
fn oldest_unstaged_age(st: &WriterState) -> Option<Duration> {
    st.oldest_buffered_at
        .iter()
        .chain(st.in_flight.iter().map(|(_, t)| t))
        .map(|t| t.elapsed())
        .max()
}

/// Drop from `known` every file at or below `watermark` (consumed) and
/// measure what is left: (how many files, age in seconds of the oldest by
/// the time it was sealed). `None` means the server has consumed nothing
/// yet, so everything counts.
fn lag_of(known: &mut Vec<(i64, u64)>, watermark: Option<i64>, now_ms: u64) -> (usize, u64) {
    known.retain(|(seq, _)| watermark.is_none_or(|w| *seq > w));
    let oldest = known.iter().map(|(_, sealed_at_ms)| *sealed_at_ms).min();
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
