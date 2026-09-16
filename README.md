# relyt-ingest

Rust ingest SDK for Relyt tables: Kafka → OSS/S3 CSV staging → async load jobs
the Relyt master executes as parallel upsert loads.
The data plane never crosses the Relyt master. API shape follows delta-rs.

- **User guide: [GUIDE.md](GUIDE.md)** — deployment topologies, latency-relevant
  knobs and their defaults, runnable examples under `examples/`
- Server-side prerequisites: Relyt **3.55.0 or later**, provisioned for ingest
  (`relyt_get_serial_group_watermark`, `relyt_get_instance_id`,
  `relyt_get_ingest_staging_config`; terminal-job retention enabled)

## Install

```toml
[dependencies]
relyt-ingest = "0.1"
arrow-array  = "58"   # RecordBatch crosses the API boundary: majors must match
```

Compatibility: arrow **58.x only** (the SDK is built against the `deltalake`
0.32 line; another arrow major does not compile against `append`), and Rust
**1.85** or newer (MSRV).

Staging stores: Alibaba OSS and AWS S3 are covered end to end. Tencent COS,
Kingsoft KS3, UCloud US3 and Volcengine TOS are recognised by endpoint and
signed as S3-compatible, but the integration suite does not run against them
yet.

## Usage sketch

```rust
use relyt_ingest::{Client, ClientConfig};

// Staging is Relyt-managed by default: Client::connect asks the server for the
// bucket and its credentials, so none of that appears here. For a customer-owned
// bucket use ClientConfig::with_customer_staging(StagingConfig { .. }, dsn).
let cfg = ClientConfig::new("host=... user=... dbname=...");
let client = Client::connect(cfg).await?;

// One writer per Kafka partition; writer_id == partition identity.
let (table, plan) = client.open_table("public.events", "kafka-p0").await?;
// Seek Kafka to plan.kafka_resume_offset (recovery protocol), then:
table.append(batch, start_offset, end_offset).await?; // buffered, non-blocking
table.flush().await?;                                  // durable staging point
// Contract: commit Kafka offsets only after flush() returned.
```

## Module map

| Module | Contents |
|---|---|
| `client` | `Client::connect` / `open_table` (recovery handshake) |
| `table` | `TableWriter`: buffered append, double-threshold sealing, render → gzip → put pipeline, flush |
| `csv` | POSTGRESQL_CSV formatter (quote-all-non-null, NULL bare, NUL strip, decimal plain, header) |
| `dedup` | intra-file PK dedup, last write wins (poison-file prevention) |
| `naming` | file naming, serial_seq encoding (`epoch<<20\|seq`), identifier/serial_group limits |
| `recovery` | pure recovery-plan computation (resume offset, backfill, epoch anti-rollback) |
| `notify` | background FIFO notify task → `zdb_add_async_load_job` (15-arg), infinite retry |
| `staging` | OpenDAL put/list on OSS/S3 (SigV4 region derived from AWS endpoints) + the client-side confirmation watermark |
| `schema` | schema fetch, PK check, arrow type whitelist |

## Semantics worth knowing

**Stream modes.** `StreamMode::Upsert` (default) requires the target table to
have a primary key: the Relyt master upserts on it,
and the SDK deduplicates by PK inside each staged file (last write wins — a
duplicate key within one load statement is a hard server-side error, so this is
not optional). `StreamMode::InsertOnly` serves tables without a primary key; no
dedup happens and duplicate rows are preserved as-is.

**Same key, same partition (multi-writer contract).** Writers are independent
serial groups; the server orders loads *within* a group only. With one table fed
by N partitions, upsert last-write-wins requires that every message for a given
primary key always lands on the same Kafka partition (key-based producer
partitioning). Keys scattered across partitions make the final row value a
silent race that neither side can detect.

**Recovery watermark.** `open_table` re-notifies every staged file above the
server's group watermark (`relyt_get_serial_group_watermark`). All streams —
insert-only included (M=1) — submit under serial groups, so the watermark
exists for both modes. A watermark that lags reality only costs a redundant
(idempotent) re-notify; the submission gate swallows anything at or below it.

**Retention invariants.** Three retention windows interact, and the SDK does not
enforce them — they are deployment settings:

1. staging objects must outlive the longest crash window, otherwise recovery
   cannot replay what was never notified;
2. the Relyt master's terminal-job retention must outlive the staging objects.
   If the watermark rows are
   GC-ed while old objects remain, a restart can replay stale files over newer
   data. The client-side watermark covers this for a writer that keeps running
   with its staging prefix intact, but a wiped prefix falls back to the server.

**Rotation runs in a per-writer pipeline; `append` only buffers.** When a
threshold trips, the buffer is sealed under the next seq and handed to three
background stages (render → gzip → put + notify) joined by bounded channels;
the CPU stages run on the blocking pool. Files still reach the server in seq
order (one task per stage, hand-over under a lock), `staged_offset` advances
only after the put returned, and a failed put retries the same bytes to the
same key rather than returning rows to the buffer. `append` waits only when
`rotation_queue_depth` sealed files are already queued (backpressure), and
once the pipeline is stuck it returns without taking the batch --
`Error::StagingStalled` while retrying may still clear it,
`Error::RotationFailed` when it cannot. `flush()` waits for its file to be
durable and reports the same two the same way. Offsets must
move forward within a writer (`append` refuses a rewound batch with a
`Config` error; reopen the table instead).

## Deployment sizing (multi-writer)

Per-writer isolation is deliberate (a stuck table stalls only its own queue),
so two resources scale linearly with W = tables x partitions per process:

| Resource | Formula | Why |
|---|---|---|
| master connections (resident) | **1 (control) + 2W**: one notify connection and one lag-sampler connection per writer. Short-lived on top: one GC connection per writer per hour, and under managed staging one credential refresh per **process** every 5 minutes | each serial_group lazily owns a notify loop with its own connection; the lag sampler keeps one rather than forking a backend every 30s |
| memory upper bound | **W x (6 + `rotation_queue_depth`) x `rotate_size_bytes`** ((6 + 3) x 64MB = 576MB per writer by default) | one buffer filling + `rotation_queue_depth` (default 3) sealed files queued + up to ~5 file-equivalents in flight across the render / gzip / put stages and the slots between them; a full queue blocks `append`, so this is a hard bound and actual residency is usually far below it |
| background tasks | 7W tokio tasks (ticker / GC / lease heartbeat / lag sampler / render / gzip / put) + W notify loops | negligible |
| tokio blocking threads | up to **2 per writer** (render, gzip; held only while working) against tokio's default cap of 512 | past ~256 busy writers raise the runtime's `max_blocking_threads`; exceeding it queues rather than fails, and shows up as throughput loss. Their stacks are outside the memory formula above |

Example: 10 tables x 32 partitions = 320 writers -> ~641 resident connections
(budget against the master's `max_connections`) and a 320 x 576MB ~ 180GB
memory bound; at `rotate_size_bytes` = 16MB it is ~45GB (or rely on the 15s
time threshold to keep files small).
Connection multiplexing for hundreds of writers is a tracked follow-up.

## Tests

```sh
cargo test    # unit tests, no external dependencies
```

The integration suite under `tests/` drives a live Relyt master and an
object-storage bucket; it is bound to Relyt's internal test infrastructure and
is not part of the published crate.

## Open items

- float rendering: `NaN`/`±inf` render as Rust's `NaN`/`inf`; the Relyt master
  expects `NaN`/`Infinity`.
- Schema cache fallback; see the review follow-up issues for the rest.
