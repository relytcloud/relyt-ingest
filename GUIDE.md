# relyt-ingest user guide

**English** | [简体中文](GUIDE.zh-CN.md)

For teams writing into Relyt heap tables in real time from a Rust program —
typically a Kafka consumer. This document covers how to integrate, three
deployment topologies, the knobs that decide how soon data becomes visible, and
the internals you can safely ignore. For API details see `cargo doc` (the
rustdoc in `lib.rs`).

## How it works, in one sentence

The SDK accumulates Arrow batches client-side into CSV files on OSS/S3 staging,
then tells Relyt to load them server-side in parallel (tables with a primary key
go through upsert) — **row data never crosses the master**. Relyt guarantees
that the files of one writer are loaded strictly in order and exactly once:
duplicate notifications, process crashes and redelivered messages cause neither
loss nor duplication.

## Prerequisites

- Relyt cluster **3.55.0 or later** (the server-side capabilities the SDK needs
  ship from that version);
- **Staging bucket: you normally do not provide one.** `Client::connect` asks
  the Relyt server for the bucket and its credentials, so no secret ever appears
  in your configuration or repository. Only if you require the data to transit
  through a bucket in your own account do you switch to customer-owned staging,
  which needs an OSS or S3 bucket plus an AK/SK with read, write and delete
  permission on `<prefix>/staging/*` and `<prefix>/_meta/*`. The two modes are
  compared under [Who owns the staging bucket](#who-owns-the-staging-bucket);
- Bucket lifecycle rules: attach them to `<prefix>/staging/` only, **never** to
  `<prefix>/_meta/` (that is where resume state lives). Relyt configures this in
  the default mode; you configure it when you own the bucket;
- The target table is a heap table. Upsert mode requires a PRIMARY KEY;
  insert-only mode does not;
- **Rust and Arrow versions**: `append` takes an Arrow `RecordBatch` directly,
  and arrow types are incompatible across major versions, so the SDK pins
  `arrow-array` / `arrow-schema` to **58.x** (matching `deltalake` 0.32.x). Your
  project must use the same major; talk to us before upgrading. The SDK's
  minimum supported Rust version is **1.85** (CI compiles against it every run)
  with no upper bound; in practice a higher dependency in your project usually
  sets the real floor — `deltalake` 0.32.x, for instance, requires **1.91.1**,
  which cargo enforces as a hard error;
- Database account: use the ingest account your Relyt administrator provides
  (least privilege, no superuser needed).

## Adding the dependency

```toml
[dependencies]
relyt-ingest = "0.1"
arrow-array  = "58"   # append takes RecordBatch directly; the arrow major must match the SDK
```

The crate is published on crates.io and its API documentation on docs.rs. The
arrow and Rust version constraints are in [Prerequisites](#prerequisites) above.

## Integration checklist: who provides what, and where it goes

| Information | Provided by | Where it goes in the SDK |
|---|---|---|
| Relyt control connection: host / port / dbname + the **ingest account** username and password (least privilege, no superuser) | Relyt administrator | the single argument of `ClientConfig::new(control_dsn)`, in tokio-postgres connection-string form `host=... port=... user=... password=... dbname=...` |
| Staging bucket | **Relyt provides it by default** and the SDK fetches it at connect time, so you fill in nothing. Only with customer-owned staging do you supply endpoint, bucket, prefix and AK/SK (the region is derived automatically for AWS, Tencent COS, Kingsoft KS3, UCloud and Volcengine TOS endpoints; MinIO, R2 and similar need it spelled out. **The e2e suite currently covers Alibaba OSS and AWS S3 only; the other vendors are recognised by endpoint and signed as S3-compatible, but have no test coverage yet**) | default mode: nothing; customer-owned: `ClientConfig::with_customer_staging(StagingConfig { .. }, dsn)` |
| Bucket lifecycle rules | whoever owns the bucket, in the cloud console: attach to `<prefix>/staging/`, **not** to `_meta/` | none (the SDK is unaware of them) |
| Relyt instance id | usually **not needed** — the SDK fetches it from the server; supplied by the Relyt administrator only when the instance has none configured | `ClientConfig::cluster_id` (`Option<String>`) |
| Target table | you create it (heap table; upsert needs a PRIMARY KEY; column types under [Supported column types](#supported-column-types)) | `open_table("schema.table", writer_id)` |
| Version floor | Relyt cluster ≥ 3.55.0; your project on arrow 58 and Rust ≥ 1.85 (this SDK's floor, no upper bound; `deltalake` 0.32.x itself wants ≥ 1.91.1 and usually decides the real floor) | compile time |

**The SDK reads no environment variables** (the lease identity reads `HOSTNAME`
for a machine name, and that is all) — everything above is passed in code
through the `ClientConfig` struct.

### Who owns the staging bucket

| | Default (Relyt-managed) | Customer-owned |
|---|---|---|
| How you write it | `ClientConfig::new(dsn)` | `ClientConfig::with_customer_staging(staging, dsn)` |
| Bucket and credentials | fetched from the server at connect time; **no secret in your configuration or repository** | you supply and safeguard them |
| Lifecycle rules | configured by Relyt | configured by you; staging growth during a backlog lands on your bill |
| Region | Relyt guarantees the same region as the instance | you must keep it in the same cloud and region as the Relyt instance, which reads it directly |
| Security posture | your people and configuration systems never touch a secret | **your keys necessarily reach the Relyt server**: the master must be able to read the objects you wrote, so the credentials travel with the load job and land in its job record. This differs from the default mode — choose it knowingly |

Both are values of the same `staging` field (`Staging::Relyt` and
`Staging::Customer(..)`), so the halfway state of "filled in my own bucket but
forgot to switch ownership" does not exist in the type system.

```rust
// Default: no credentials at all
let cfg = ClientConfig::new(env("RELYT_DSN"));

// Customer-owned bucket
let cfg = ClientConfig::with_customer_staging(
    StagingConfig {
        service: StagingService::Oss,
        endpoint: env("RELYT_STAGING_ENDPOINT"),
        bucket: env("RELYT_STAGING_BUCKET"),
        prefix: env("RELYT_STAGING_PREFIX"),
        access_key_id: env("RELYT_STAGING_AK"),
        secret_access_key: env("RELYT_STAGING_SK"),
        region: std::env::var("RELYT_STAGING_REGION").ok(),
    },
    env("RELYT_DSN"),
);
// Client::connect validates: ownership against configuration, empty credentials
// or ones containing `,` or `"`, an S3 region it cannot determine, out-of-range
// parameters — all reported here rather than at the first upload
```

In the default mode `connect` can fail three ways: the instance has no ingest
staging provisioned (contact your Relyt administrator), the ingest account may
not call that interface (likewise), or the server is too old (upgrade the
instance, or switch to a customer-owned bucket).

## Credentials and rotation

**In the default mode you hold no staging credentials**, which leaves only the
DSN to protect: keep it out of source and configuration repositories, and inject
it through a K8s Secret, a 0400 file or a secret manager. Relyt rotates the
staging keys and **you neither reconfigure nor restart**: the SDK re-fetches
them every 5 minutes (`staging_refresh_interval`) and refreshes immediately when
an upload is rejected with 403. Relyt keeps the previous key valid for at least
24 more hours, which covers both the refresh cycle and the retry window of
in-flight load jobs.

With a customer-owned bucket the job is yours:

- keep the AK/SK out of source and configuration repositories as well;
- issue them with **least privilege** (that staging prefix only) and do not
  reuse them across systems;
- **use a static AK/SK rather than STS temporary credentials**: the credentials
  are handed to the server with every load job so it can read the staging files,
  a job may retry for hours, and an expired temporary credential fails it;
- **rotation procedure**: issue the new key → switch configuration and restart
  gracefully (`close()`, then `open_table` in the new process — no write gap) →
  **keep the old key for at least 24 hours** before revoking it.

In both modes the SDK redacts credentials from logs and `Debug` output.

## Quick start (one table, one partition)

One Kafka partition maps to one writer. Complete, compilable example:
[`examples/kafka_partition_writer.rs`](examples/kafka_partition_writer.rs).

```rust
use relyt_ingest::{Client, ClientConfig, StreamMode};

// Staging is Relyt-managed by default: connect obtains the bucket and its
// credentials, so nothing secret is needed here. For your own bucket use
// ClientConfig::with_customer_staging(staging, dsn) instead.
let mut cfg = ClientConfig::new("host=... port=5432 user=ingest dbname=prod");
cfg.stream_mode = StreamMode::Upsert;                // the default; use InsertOnly without a PK

let client = Client::connect(cfg).await?;
let (writer, plan) = client.open_table("public.orders", "orders-p0").await?;

// 1. Seek the Kafka consumer to plan.kafka_resume_offset (the next offset to
//    consume; None means no history, so use your own checkpoint).
// 2. Consume loop: append every batch. You do not need to flush per batch —
//    files are sealed automatically by the rotation thresholds.
writer.append(batch, start_offset, end_offset).await?;

// 3. Gate for committing Kafka offsets: writer.staged_offset() >= that batch's
//    end_offset. (staged_offset is the last durable offset, inclusive.
//    Committing periodically is fine.)
// 4. On exit — including SIGTERM during a rolling upgrade — call close(): it
//    drains the tail and releases the writer lease at once, so the successor
//    process starts with no wait:
writer.close().await?;
```

**`append` on one writer must be called serially** — one task calling in
increasing offset order, which is normally just that partition's consume loop.
The SDK requires offsets to move strictly forward within a writer; concurrent
calls interleave two runs of data and surface as a `Config` error reporting a
rewound offset. Give each partition its own writer rather than sharing one
across tasks.

**Naming a writer_id**: it identifies one stream and must be identical across
restarts. `"<topic>-p<partition>"` (for example `"orders-p0"`) is a good shape —
including the topic avoids a collision when two topics write the same table.
The character set is `[A-Za-z0-9._-]`, it may not start with `_`, and it is at
most 64 bytes. **Only one process may run a given writer_id at a time** — the
SDK enforces this with a lease on staging: a second process calling `open_table`
gets `WriterLocked`, and after the original crashes its lease expires in roughly
3 minutes and the new process takes over automatically.

## Three deployment topologies

### 1. One table, one partition

The quick start above: one process, one `Client`, one writer.

### 2. One table, many partitions

**One writer per partition** — usually one process or container each, though
several tokio tasks in one process are semantically equivalent, since every
writer has its own connection, notify queue and lease identity.

> **Usage contract: every message for a given primary key must always land on
> the same Kafka partition** — that is, the producer partitions by primary or
> business key, which is Kafka's default behaviour. Writers are not ordered
> against each other, only strictly ordered internally. If two updates to one
> key reach two partitions, two writers load them in parallel, the order is
> undefined, and the final row value becomes a silent race that neither the SDK
> nor the server can detect. Honour the contract and each writer naturally
> touches a disjoint key set, which makes concurrent writes to one table safe.

Complete example:
[`examples/single_table_multi_partition.rs`](examples/single_table_multi_partition.rs).

```rust
// Run per partition, in its own process or task:
let client = Client::connect(cfg.clone()).await?;
let (writer, plan) = client
    .open_table("public.orders", &format!("orders-p{partition}"))
    .await?;
// ... each consumes its own partition and resumes from its own plan
```

After a partition split or a consumer rebalance, the new process simply calls
`open_table` with the same writer_id and continues from the checkpoint —
resume position, backfill and deduplication all happen automatically.

### 3. Many tables, many partitions

One writer per `(table, partition)` pair; streams of different tables do not
affect each other (a stuck table stalls only its own notify queue). One process
can hold writers for several tables, or you can split by table across processes.
Complete example, which also demonstrates both stream modes:
[`examples/multi_table_multi_partition.rs`](examples/multi_table_multi_partition.rs).

```rust
tokio::join!(
    run_stream("public.orders", "orders", 0, StreamMode::Upsert),
    run_stream("public.orders", "orders", 1, StreamMode::Upsert),
    run_stream("public.clicks", "clicks", 0, StreamMode::InsertOnly), // table without a PK
    run_stream("public.clicks", "clicks", 1, StreamMode::InsertOnly),
);
```

### Capacity planning (multi-writer)

Giving every writer its own connection and buffer is deliberate fault isolation.
The cost is that these resources grow linearly with W = tables × partitions in
one process, so size the deployment before rolling it out:

| Resource | Formula | Notes |
|---|---|---|
| Resident Relyt connections | **1 + 2W**: one control connection plus two per writer (one notify queue, one lag sampler). Short-lived on top: one GC connection per writer per hour, and in managed mode one credential refresh per **process** every 5 minutes | check against the instance's connection quota |
| Memory upper bound | one writer: **(6 + `rotation_queue_depth`) × `rotate_size_bytes`**; W writers: **W × (6 + `rotation_queue_depth`) × `rotate_size_bytes`** | with the defaults: (6 + 3) × 64 MB = **576 MB per writer**. The 6 is one buffer being filled plus the three rotation stages and the slots between them, rounded up conservatively. A full queue blocks `append`, so this is a hard bound and real residency is usually far below it |
| CPU | **at most 2 cores per writer**; linear across writers | only rendering and compression consume CPU, each on one thread handling one file at a time, which caps a single writer at 2 cores — sustained use is typically 1–2 cores depending on throughput. **Compression dominates**; turning it off (`staging_compression = Plain`) lowers CPU noticeably at the cost of several times more upload bytes and object storage |
| Background tasks | 7W tokio tasks (ticker / GC / lease heartbeat / lag sampler / render / gzip / put) plus W notify loops | negligible |
| tokio blocking threads | **2 per writer** at peak (one render, one compress, held only while working) against tokio's default cap of 512 | past W > 256 with most writers saturated, raise the runtime's `max_blocking_threads`. Exceeding it queues rather than fails, and shows up as lower throughput and rising `lag_seconds`. These stacks are outside the memory formula above |

Example: 10 tables × 32 partitions = 320 writers → roughly 641 resident
connections and a memory bound of 320 × 576 MB ≈ 180 GB; at
`rotate_size_bytes` = 16 MB it is about 45 GB. For large W, lower it like this
(or lean on the 15-second time threshold to seal files) and confirm the
connection quota with your Relyt administrator.

**Sizing a container for a single writer**: the ingest path itself follows the
formula above (576 MB by default) with a CPU ceiling of 2 cores. Two things to
keep in mind when you pick a container size:

- that CPU figure covers **the write path only**. It excludes your application
  consuming messages, deserialising them into `RecordBatch` and applying
  business transformations. A thin data path is fine on 2 cores; anything that
  decodes and transforms should get 4, or the two contend for the same cores.
- lowering `rotate_size_bytes` reduces memory but **does not reduce CPU** —
  rendering and compression cost scales with bytes. For more throughput add
  writers (CPU grows with them) rather than giving one writer more cores.

## Supported column types

`open_table` validates every column of the target table against a whitelist and
fails with `UnsupportedType` before writing anything. Each column of the
`RecordBatch` you pass to `append` must carry the matching arrow type — building
it from `writer.schema()` aligns them automatically:

| Relyt column type | Arrow type in the batch | Notes |
|---|---|---|
| `boolean` | `Boolean` | rendered as `t`/`f` |
| `smallint` / `integer` / `bigint` | `Int16` / `Int32` / `Int64` | |
| `real` / `double precision` | `Float32` / `Float64` | ordinary values are exact; `NaN` and `±Infinity` are not yet guaranteed to match the server's convention, so avoid writing them |
| `numeric(p,s)` / `decimal(p,s)`, p ≤ 38 | `Decimal128(p, s)` | **precision and scale must match the table definition exactly**; bare `numeric` without precision is unsupported |
| `text` / `varchar` / `varchar(n)` / `char(n)` | `Utf8` | any Unicode (CJK, emoji, newlines, quotes, the delimiter itself) is written verbatim; only `\0` is stripped |
| `date` | `Date32` | |
| `timestamp` (without time zone) | `Timestamp(Microsecond, None)` | |
| `timestamp with time zone` | `Timestamp(Microsecond, Some("UTC"))` | the value is a UTC instant; the SDK writes a `+00` offset so the server does not shift it by session time zone |
| `bytea` | `Binary` | written in hex form prefixed with `\x` |

**Unsupported** (talk to us if you need them): `json`/`jsonb`, `uuid`, arrays
(including nested shapes such as `array(row(...))`), `interval`, `time`, `inet`
and similar; `numeric` with precision > 38.

**Primary-key column types** (upsert mode only): any type in the table above may
be a primary key **except floating point** (`real` / `double precision`). Floats
are excluded because equality for `NaN` and `±0.0` differs between client and
server, so the same data could deduplicate differently; such a table fails at
`open_table` with `UnsupportedType` naming the column. Insert-only mode does not
deduplicate and therefore has no such restriction — a table with a float primary
key writes normally.

## Parameters you should care about

### Rotation thresholds (these decide how soon data is visible)

Data travels from `append` to queryable in three steps: **buffering → file lands
on staging → server-side load**. A file is sealed and uploaded when either of
two conditions trips first:

| Parameter | Default | Valid range | Meaning |
|---|---|---|---|
| `rotate_size_bytes` | **64 MB** | (0, 512 MiB] | seal a file once the buffer reaches this size (estimated from the rendered CSV bytes); the ceiling reflects the implementation, which renders a whole file in memory and uploads it in one call |
| `rotate_interval_max` | **15 s** | [1 s, 6 h] | seal a file once the oldest buffered row reaches this age (a background safety net for low-traffic streams) |
| `rotation_queue_depth` | **3** | [1, 16] | how many sealed files may wait for the background pipeline (render → gzip → upload); once full, the `append` that triggers the next seal blocks (backpressure). One slot keeps the pipeline busy, the rest absorb bursts; each slot costs one `rotate_size_bytes` of memory (see [Capacity planning](#capacity-planning-multi-writer)) |

All parameters are range-checked at `Client::connect`, so a unit mistake (bytes
written as megabytes, say) fails immediately with the valid range rather than
misbehaving at run time.

**Visible latency ≈ min(time to fill 64 MB, 15 s) + server-side load time
(usually seconds).** With the defaults a low-traffic stream is therefore visible
after about 15 seconds plus load time, while a high-traffic stream becomes
visible at the cadence of one 64 MB file. Tuning directions:

- For lower latency, lower `rotate_interval_max` (5 s, for example). The cost is
  more and smaller files and therefore more server-side load jobs; going below
  2–3 seconds is not recommended.
- For higher throughput or fewer files, keep or raise `rotate_size_bytes`.
  64 MB balances load efficiency against latency and rarely needs changing.
- The batch size per `append` call does not affect correctness, only call
  overhead; passing the Kafka poll batch straight through is common.
- When something must be visible right away (before exit, or in a test), call
  `writer.flush()` explicitly.

### Other parameters worth setting or knowing

| Parameter | Default | Notes |
|---|---|---|
| `staging` | `Staging::Relyt` | where the bucket comes from. By default the server supplies bucket and credentials. `Staging::Customer(StagingConfig { endpoint/bucket/prefix/AK/SK/service/region })` means your own bucket, where `region` is only needed for endpoints it cannot derive, such as MinIO or R2 (AWS, Tencent COS, Kingsoft KS3, UCloud and Volcengine TOS are derived automatically; e2e covers OSS and AWS S3 only). See [Who owns the staging bucket](#who-owns-the-staging-bucket) |
| `control_dsn` | required | the Relyt control connection string (metadata and notifications only; no row data) |
| `stream_mode` | `Upsert` | `Upsert` for tables with a primary key (the last write for a key wins); `InsertOnly` for tables without one (duplicate rows are preserved). **Every writer of one table must use the same mode**; stop and drain before switching |
| `staging_compression` | `Gzip` | staged CSV files are gzip-compressed before upload (objects named `.csv.gz`); the Relyt server detects and decompresses by content, so **no server-side configuration is needed**. `Plain` produces readable `.csv` objects, which helps when troubleshooting. Rotation thresholds always apply to the uncompressed CSV size. The trade-off: the ratio depends on your data (wide string tables often compress several times over) and what you save is upload bandwidth and staging storage, while the cost is **client CPU** — compression is the heaviest stage of the write path. Decompression on the Relyt side is cheap, **so this trades client CPU against bandwidth and storage, and barely involves the server** |
| `cluster_id` | `None` | the Relyt instance id (one Relyt instance is one DWSU with one id), used to namespace the staging path so several instances can share a bucket. Normally unset, since the SDK fetches it from the server; specify it only when the instance has none configured, which `connect` tells you |

### Error handling

Start by asking whether you have to stop. **Every error is returned directly
from `connect` / `open_table` / `append` / `flush` / `close`** — no callback or
polling is needed — and a stopped stream has two more channels described in A.

#### A. The stream has stopped — you must stop consuming it

These four do not heal. The stream is closed for writing on the SDK side, and
`append` / `flush` / `close` return the same error from then on.

| Error | Meaning | Action |
|---|---|---|
| `WriterFenced` | another process took the lease | stop consuming this stream in this process and check for a duplicate deployment. **Do not rewind**: the writer holding the lease is carrying the stream on |
| `StagingOrderViolation` | files did not reach the upload stage in seq order (an SDK-internal invariant broke). The message names **the missing seq and the Kafka offset range** | stop consuming, **do not commit offsets**, and escalate. After rolling back or upgrading the SDK, **rewind manually to the range in the message** and restart. A restart alone does not heal it |
| `RotationFailed` | the **data or table definition** behind a sealed file was rejected, and no number of retries will render the CSV | stop consuming, do not commit offsets; fix the root cause named in the message and restart, and the recovery handshake replays it — **no rewind needed** |
| `SerialContractViolation` | two writers used the same writer_id, or the epoch went backwards | stop consuming and resolve the identity conflict before restarting |

The same condition reaches you through **three channels**; pick whichever suits
your architecture, or use several:

1. the return value of `append` / `flush` / `close` — known at call time;
2. `writer.fatal_error()` returning `Some(reason)` — **this is the one that
   matters when the application is idle and does not call `append` for a while**,
   because background tasks such as the lease heartbeat notice first;
3. the `RELYT_OBSERVE_ALARM` line in the log, for log-based alerting (below).

#### B. Integration or configuration problems — fix, then restart

A parameter, a configuration or a table definition does not satisfy the
contract, so **retrying the same input fails the same way**. The SDK logs an
`ERROR` alongside returning the error, so the cause survives an application that
swallows return values.

| Error | Typical cause |
|---|---|
| `Config` | a rewound or invalid offset, an out-of-range parameter, a credential containing a comma or a quote |
| `Schema` | the data passed to `append` does not match the table; or upsert mode against a table without a primary key |
| `UnsupportedType` | an unsupported column type; or a float primary key under upsert |
| `Naming` | an invalid or overlong writer_id |

Action: **treat these as integration defects** — do not retry the same input;
fix the code, the table definition or the operational step and restart. Among
them, `append` returning `Config` for a rewound offset is the only one triggered
by an operational action: a rebalance or a seek moved the consumer back into
a range already written. **The data never entered the buffer and the writer is
still usable**; the right move is to reopen the table (`open_table`) and resume
from `RecoveryPlan::kafka_resume_offset`.

#### C. Retry later — no need to stop

| Error | Meaning | Action |
|---|---|---|
| `StagingStalled` | staging has been unreachable for a while; the pipeline is still retrying the same bytes with a 1 s → 30 s backoff. When `append` returns it, **that batch was not accepted** | from `append`: retry the same batch later. From `flush`: keep flushing later. In both cases do not reopen the writer, and do not commit the corresponding offsets meanwhile |
| `WriterLocked` (from `open_table`) | that writer_id still has a live process; a rolling upgrade shows this briefly while the old and new processes overlap | do not force a start. Once the old process is confirmed dead, the lease expires in about 3 minutes and the takeover is automatic |

#### D. Nothing to handle

Upload jitter, notification retries, staging cleanup and checkpoint writes fail
from time to time; the SDK retries them internally and **never surfaces them as
errors** — they appear only as `WARN` in the log. Your consume loop needs no
code for them.

---

**About `close()`**: `close(self)` consumes the writer by value, so **whatever it
returns, the writer is finished** and its buffer and in-flight files are
discarded. Therefore do not commit the corresponding Kafka offsets on any error;
after a restart the recovery handshake resumes from
`RecoveryPlan::kafka_resume_offset`. If you want to watch the pipeline recover
while the writer is still usable, call `flush()` first.

### When you have to rewind Kafka manually

The SDK's automatic recovery can only express "continue after the last file that
landed" (the resume offset is the maximum end offset across all staged files,
plus one). It cannot express "there is still a hole behind the frontier". Hence:

| Situation | Rewind? |
|---|---|
| `StagingOrderViolation` | **Yes.** The gap sits behind the frontier and the recovery handshake would skip it |
| Restart after `StagingStalled` or `RotationFailed` | No. The rows that never landed never entered staging, and the resume offset falls exactly at their start |
| Writer preempted (`WriterFenced`), successor continues | No. Objects that landed without a notification are backfilled by the successor's LIST |
| Process crash and restart | No. The recovery handshake covers it |

**How to rewind**: touch **only the Kafka consumer offset on your side** — either
`reset-offsets` for the consumer group to the start of the range named in the
error, or `seek` inside the application before starting. **The server needs no
action and should not be touched**: the replayed data is submitted by a new
writer session and loads as usual.

**Mode difference**: replay is safe under `Upsert` (the last value for a key
wins). Under `InsertOnly` a replay re-inserts the already-loaded rows after the
gap, so you need an idempotency key on the business side, or an assessment of
the duplicate blast radius first.

## Parameters you can leave alone

These relate to server-side load jobs and background maintenance. The defaults
come from production experience and **you neither need nor are advised to change
them**; they are listed so the behaviour is understandable:

| Parameter | Default | What it does (for information) |
|---|---|---|
| `retry_max` | 15 | the retry budget of one load job on the server (about 5 hours before it moves to a manual state that Relyt operations handle by SOP). Making it unlimited would let a bad file retry forever |
| `gc_interval` / `gc_retain_days` / `gc_retain_min_files` | 1 h / 7 days / 50000 | cadence and floor for cleaning consumed files off staging |
| `lock_heartbeat_interval` / `lock_lease_timeout` | 30 s / 180 s | heartbeat and takeover deadline of the writer lease — the source of the "about 3 minutes" above; see the note below |
| `csv.delimiter` | `,` | the CSV column delimiter, agreed with the server; do not change |
| `staging_refresh_interval` | 5 min | how often managed mode re-fetches staging credentials (a 403 upload also refreshes immediately); ignored with a customer-owned bucket |
| `staging_error_after_attempts` | 3 | after this many consecutive failures on the head-of-queue file, `flush()` / `close()` stop waiting and return `StagingStalled` (returned as the third failure is reported, after a 1 s + 2 s ≈ 3 s backoff, excluding the attempts themselves). The pipeline keeps retrying the same bytes with a 1 s → 30 s backoff and loses no rows, and a later `flush()` waits again. Do not commit the corresponding Kafka offsets when you get it |

**About the two lease parameters**: they exist only to enforce "one process per
writer_id at a time". They are **not on the data path and do not affect visible
latency** — the heartbeat is an independent background task, and append, flush
and rotation never wait on it. Takeover time falls into three tiers depending on
how the previous process exited:

| How it exited | Successor waits |
|---|---|
| called `writer.close()` (recommended: call it on SIGTERM, which is the rolling-upgrade case) | **no wait** |
| killed with `-9` or crashed, successor restarts on the **same machine or container** | **no wait** (the SDK sees that the lock holder's process is gone and takes over immediately) |
| killed with `-9` or lost power, successor starts on **another machine** | up to `lock_lease_timeout` = 3 minutes (it cannot prove the predecessor is dead, so it waits out the lease) |

Only that partition's ingest pauses meanwhile; **data already written stays
visible**. Shorten the lease (to 10 s / 30 s, say; the floor is a heartbeat ≥ 1 s
and a lease ≥ 3 × heartbeat) only if the third tier is time-critical for you. The
cost is that a long GC pause or a network hiccup exceeding the lease is mistaken
for death and the stream is handed over — the data stays safe, but the process
has to reopen its writer.

## Logging

The SDK emits structured logs through `tracing`, so any tracing subscriber can
collect them. The key events are one line each: **every file landing** (database,
table, writer, object path, start–end offset, rows, bytes, duration), **every
server acknowledgement** (round-trip time), **the startup recovery summary**
(resume offset, backfill count, watermark), **lease events** (acquire, take over,
release, preempted), the GC round summary, and a status heartbeat roughly every
5 minutes. Normal paths are INFO, self-healing anomalies WARN, a stopped stream
ERROR. Logs never contain credentials.

## Operational notes

- **A failed load file** (typically caused by a batch with invalid data types):
  that writer's loads stop in a failed state once retries are exhausted, and
  **subsequent files of that stream queue behind it while other tables and
  partitions are unaffected**. Contact your Relyt administrator to skip or repair
  it by SOP; on the SDK side `fatal_error()` detects the state.
- **Dropping and recreating a table of the same name**: the new table is a fresh
  identity, the old staging files become orphans and are reclaimed by the
  `staging/` lifecycle rule, and no manual work is needed. The writer starts from
  scratch afterwards.
- **Renaming a table** (`ALTER TABLE ... RENAME`): no impact; the checkpoint
  carries on.

## Monitoring lag, and what to alert on

Data crosses three segments from Kafka to queryable in Relyt: **consumption
(Kafka → SDK) → buffer and upload (SDK → staging) → server-side load**. Three
layers of signal, one per segment, let any degradation be pinned to a segment
immediately:

| Signal | How to collect | Suggested threshold | Meaning |
|---|---|---|---|
| Kafka consumer lag | standard Kafka monitoring (consumer group lag) | per your business tolerance | consumption is behind production: the SDK process lacks CPU or network, or it died |
| Durability lag = consumer's current offset − `writer.staged_offset()` | sample periodically in the application (exported every 30 s, say) | converted to time, > 3 × `rotate_interval_max` (≈ 45 s by default) for two consecutive samples | buffering or uploading is impeded: a staging network or credential problem, or oversized batches |
| **Load lag `lag_seconds`** (built into the SDK) = age of the oldest unloaded file | `writer.lag()` — a background task samples the server watermark every 30 s over one resident connection and subtracts the files this process has sealed but the watermark does not yet cover. A file counts from the moment it is sealed, including files still uploading or retrying, so when staging is unreachable this grows together with `buffered_age_seconds`; after a restart the recovery listing refills it. Export it to your monitoring system; the SDK also logs a status heartbeat about every 5 minutes | > 120 s for two consecutive samples | server-side loading is impeded (including a failed file stuck at the head of the queue, where this value keeps climbing) |
| End-to-end visible latency = now − newest event time in the table | if the table has an event-time column, probe from the query side with `SELECT max(event_time)` at low frequency, such as once a minute | > 5 × (`rotate_interval_max` + 1 min) | the final verdict on whole-chain health, covering the server-side load segment |

**States that must alert immediately** (not lag — the stream has stopped and
needs a human):

- **`writer.fatal_error()` returns `Some(...)`**: that stream has stopped writing
  and will not recover. It covers all four causes — lease taken by another
  process (`WriterFenced`), internal ordering check tripped
  (`StagingOrderViolation`), a permanent failure in one rotation stage
  (`RotationFailed`), and a writer identity conflict
  (`SerialContractViolation`) — and the returned string is the reason.
  **Poll it periodically and surface it on a dashboard**: background tasks such
  as the lease heartbeat notice before your business calls do, so reporting only
  on a failed `append` misses it while the application is idle. Handling for each
  state is under [Error handling](#error-handling).
- The same batch failing to load repeatedly (typically a load file rejected for
  invalid data types): durability lag looks normal while end-to-end visible
  latency keeps growing — the third signal catches it. Contact your Relyt
  administrator.

**On the log side**: a retryable failure in a rotation stage logs `WARN` (it
recovers on its own, covering storage jitter and internal hiccups), while
**a rejection of the data or the table definition, which cannot be retried, logs
`ERROR`** with wording that says retrying will not help, and the stream stops.

### Alerting on a single keyword (recommended)

When any of the four stop conditions occurs, the SDK logs one line in a fixed
format carrying the keyword **`RELYT_OBSERVE_ALARM`**, the same format other
Relyt components use, so one grep rule covers all of them:

```
RELYT_OBSERVE_ALARM:[ALARM_LEVEL=Fatal,ALARM_LOG_TIME=2030-01-02 03:04:05,ALARM_LOG_MODULE=INGEST-SDK],ALARM_MSG=ingest stream stopped and will not resume on its own. table=public.orders writer_id=orders-p0 serial_group=... cause=... action=...
```

- `ALARM_LOG_MODULE=INGEST-SDK` marks it as coming from this SDK; grepping for
  `RELYT` alone also matches.
- `ALARM_MSG` carries **the table, writer_id, serial_group, the cause and what to
  do**, so you can identify which stream stopped, why, and the next step without
  consulting the Relyt side.
- **Normally one line per writer per stop** (several background tasks may notice
  the same state, and only the first of them logs). A state **more urgent** than
  the one already announced adds a second line — the ranking is
  `StagingOrderViolation` > `WriterFenced` > `SerialContractViolation` >
  `RotationFailed`, and it only ever escalates, so a writer cannot page
  repeatedly. **When several lines arrive for one writer, act on the last one**:
  a fence says "do not rewind" while an order violation says "you must rewind",
  so the upgrade exists precisely to correct the earlier instruction.
- The level is always `Fatal`: none of the four recovers on its own.

**The ordering violation (`StagingOrderViolation`) matters most here**, because it
is the only case needing a manual Kafka rewind, and the alert body names the gap
directly so operators can act from it:

```
cause=... seqs 8..=8 of epoch 1789460671000 never arrived; Kafka offsets 111..=129 are not
in staging. A restart resumes after the highest staged offset and will NOT re-stage that gap.
action=stop consuming this stream, do NOT commit its Kafka offsets, then rewind the consumer
to the offset range named above and restart; ...
```

That is: rewind the consumer group to **111** and restart. The procedure and its
caveats are under
[When you have to rewind Kafka manually](#when-you-have-to-rewind-kafka-manually).

**Prerequisite: the SDK emits through `tracing`, so your application needs a
subscriber installed** (`tracing_subscriber::fmt().init()`, for example) before
any log appears — including this alert line.

**Why a keyword rather than a log level:**

| | The question it answers | Frequency |
|---|---|---|
| `WARN` | the SDK is handling it and will recover (upload retries, notify reconnects, a skipped cleanup round) | possibly many lines |
| `ERROR` | the SDK cannot handle it | few |
| `RELYT_OBSERVE_ALARM` | **a running stream stopped and needs a human** | one per writer per stop, plus one more if a more urgent state follows |

Two things catch people out:

- **The alert line is at `ERROR` level**, so even the common `RUST_LOG=warn`
  receives it. Matching on the keyword rather than the level is about
  **precision**, not about escaping a level filter.
- **An `ERROR` does not mean the stream stopped**: the category B integration
  errors above (where the stream never started) also log `ERROR`, and a
  retryable rotation failure may log before it recovers. **Alerting on the
  `ERROR` level therefore produces false positives**, while the keyword picks out
  exactly the "stream has stopped" subset of `ERROR`.

Recommendation: **match only `RELYT_OBSERVE_ALARM` (or simply grep `RELYT`), then
read the same writer's `ERROR` and `WARN` context for detail.**

**What is promised about the format**: the `RELYT_OBSERVE_ALARM` keyword itself is
stable and safe to use as an alert match string. The fields inside the brackets
and the `ALARM_MSG` body will change across versions (new fields, reworded text),
so **do not parse fields and do not match on the body text** — read the
surrounding log lines of the same writer when you need detail.

**Do not** use the file count in the staging directory as a lag signal: consumed
files are retained for 7 days by default (`gc_retain_days`) before cleanup, so
their presence does not mean they are unconsumed, and the pile has no stable
relationship to lag.
