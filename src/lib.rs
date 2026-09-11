//! relyt-ingest — Rust ingest SDK for Relyt tables.
//!
//! Data path: the client stages Arrow record batches as CSV objects on
//! OSS/S3, then submits async load jobs that the Relyt master executes as
//! parallel upsert loads; row data never crosses the Relyt master itself.
//! Ordering and exactly-once are enforced server-side by serial load groups
//! plus a per-group watermark.
//!
//! # Quickstart
//!
//! One writer per Kafka topic-partition:
//!
//! ```no_run
//! use relyt_ingest::{Client, ClientConfig, StreamMode};
//!
//! # async fn run(batch: arrow_array::RecordBatch) -> relyt_ingest::Result<()> {
//! // Staging is Relyt-managed by default: the client asks the server for the
//! // bucket and its credentials at connect time, so none of that lives here.
//! // For your own bucket, see ClientConfig::with_customer_staging.
//! let mut cfg = ClientConfig::new("host=... port=5432 user=ingest dbname=prod");
//! cfg.stream_mode = StreamMode::Upsert; // the default; StreamMode::InsertOnly skips dedup
//!
//! let client = Client::connect(cfg).await?;
//! let (writer, plan) = client.open_table("public.events", "kafka-p0").await?;
//!
//! // Seek Kafka to plan.kafka_resume_offset (the NEXT offset to consume);
//! // None means no prior state -- start from your own checkpoint.
//! writer.append(batch, /*start_offset*/ 0, /*end_offset*/ 41).await?;
//! writer.flush().await?; // durability point
//! // Commit Kafka offsets only after flush: writer.staged_offset() >= batch end.
//! # Ok(())
//! # }
//! ```
//!
//! # Writer lifecycle & contracts
//!
//! - **One live writer per `(table, writer_id)`.** `writer_id` names a
//!   stream — typically one Kafka topic-partition; include the topic in the
//!   name (`"orders-p0"`, not `"0"`) so two topics feeding one table cannot
//!   collide. Charset `[A-Za-z0-9._-]`, not starting with `_`, at most 64
//!   bytes. The rule is enforced by a lease object (`_meta/.../lock`):
//!   a second `open_table` on a live identity fails with `WriterLocked`,
//!   a lease untouched for `lock_lease_timeout` (default 180s) is taken
//!   over, and a writer that loses its lease fences itself (`WriterFenced`
//!   from `append`/`flush`) within one `lock_heartbeat_interval` (default
//!   30s). The lease is fast loud detection, not a mutex — the server-side
//!   same-seq gate (23P01) stays the correctness backstop.
//! - **Same key, same partition.** Writers are fully independent serial
//!   groups: the server keeps order *within* a writer, never *across* writers.
//!   For upsert last-write-wins to hold under one table x N partitions, every
//!   message for a given primary key must always land on the same Kafka
//!   partition (key-based producer partitioning -- Kafka's default). If keys
//!   scatter across partitions, two versions of one row load through two
//!   groups in undefined order and the table's final value is a silent race;
//!   nothing on the client or the server can detect it.
//! - **Offsets**: `RecoveryPlan::kafka_resume_offset` is the *next* offset
//!   to consume (`None` = no record, fall back to your own checkpoint);
//!   [`TableWriter::staged_offset`] is the *last* offset (inclusive) that
//!   is durably staged. Commit consumer offsets only when
//!   `staged_offset() >= end_offset` of the batch you are committing.
//! - **Durability**: `append` only buffers. Data is durable (and its load
//!   job submitted) after `flush`, or when the rotation thresholds spill a
//!   full object in the background.
//! - **Shutdown**: call [`TableWriter::close`] from your SIGTERM handler —
//!   it drains the buffer AND releases the writer lease so a successor
//!   starts instantly. A same-host successor also takes over instantly
//!   after a hard kill (provable-death check); only a cross-host restart
//!   after kill -9 waits out `lock_lease_timeout`.
//! - **Monitoring**: [`TableWriter::lag`] serves a ~30s-refreshed
//!   consumption-lag sample (`lag_seconds` is the alerting unit); the SDK
//!   also logs one structured line per staged file, per server
//!   confirmation, and a ~5min status heartbeat.
//! - **Concurrency**: `append`/`flush` on one writer must be called from a
//!   single task in order; different writers are fully independent (a stuck
//!   table only stalls its own writer's queue).
//! - **Types**: column types are validated against a whitelist at
//!   `open_table`: boolean, smallint/integer/bigint, real/double precision,
//!   numeric(p,s) with p <= 38 (-> `Decimal128(p, s)`, exact match required),
//!   text/varchar/char (-> `Utf8`), date, timestamp with/without time zone
//!   (-> `Timestamp(Microsecond, ..)`, tz-aware = UTC instants), bytea
//!   (-> `Binary`). Anything else (bare numeric, json, uuid, arrays, ...)
//!   fails loudly.
//!
//! # Deployment prerequisites
//!
//! - Relyt 3.55.0 or later, provisioned for ingest:
//!   `relyt_get_serial_group_watermark(text)`, `relyt_get_instance_id()` and
//!   `relyt_get_ingest_staging_config()` deployed, with terminal-job retention
//!   enabled (that retention is what makes the watermark gate idempotent).
//! - Cluster identity: `Client::connect` resolves the cluster id via
//!   `relyt_get_instance_id()`; if the master has no identity configured (`-1`) or the UDF is
//!   not deployed, `ClientConfig::cluster_id` must be set explicitly.
//! - Bucket lifecycle rules may be mounted on `<prefix>/staging/` **only**
//!   — never on `<prefix>/_meta/` (it holds the writers' resume state).
//!   The built-in GC (see [`ClientConfig`] `gc_*` fields) reclaims consumed
//!   staging objects; lifecycle rules are the backstop for orphans.
//! - The staging credentials need read+write+delete under both
//!   `<prefix>/staging/*` (GC) and `<prefix>/_meta/*` (lease release).
//!
//! # Failure & recovery semantics
//!
//! - Staging paths are keyed by *OIDs*, so `ALTER TABLE ... RENAME` is
//!   harmless. `DROP TABLE` + `CREATE TABLE` of the same name is a **new**
//!   table: new OID, new staging path; old objects become orphans reclaimed
//!   by the `staging/` lifecycle rule.
//! - Switching a stream between insert-only (`M=1`) and upsert (`M>1`)
//!   requires draining: stop the writer, wait until the group watermark
//!   reaches the last staged object, then reopen in the new mode.
//! - A poison object (job stuck at the head of the serial group) stalls
//!   that writer's watermark, GC, and queue; the operational SOP (skip +
//!   resume) lives in GUIDE.md and is exercised by the e2e suite.

mod client;
mod config;
mod csv;
mod dedup;
mod error;
mod recovery;
mod schema;
mod state;
mod table;

// Internal plumbing the e2e suite drives directly (object naming, manual job
// submission, staging-store poking). Not part of the supported API surface.
#[doc(hidden)]
pub mod lock;
#[doc(hidden)]
pub mod naming;
#[doc(hidden)]
pub mod notify;
#[doc(hidden)]
pub mod staging;

pub use client::Client;
pub use config::{
    ClientConfig, CsvConfig, Staging, StagingCompression, StagingConfig, StagingService, StreamMode,
};
pub use error::{Error, Result};
pub use recovery::RecoveryPlan;
pub use table::{LagSnapshot, TableWriter};
