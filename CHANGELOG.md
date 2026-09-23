# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and versions follow
[Semantic Versioning](https://semver.org/). While the major version is 0, a
minor release may contain breaking changes; each one is called out here.

## [Unreleased]

- The `failpoints` cargo feature exists for Relyt's own fault-injection tests
  and is off by default: leave it off. With it on, the crate pulls in `fail`
  and compiles named injection points into the staging and lease paths, where
  a configured point makes a call fail or aborts the process deliberately.
  The default build carries neither the dependency nor the checks.
- Fixed: `close()` could leave the writer lease behind. It released the lease
  by read-then-delete while the lease heartbeat renewed it by read-then-write,
  with nothing keeping the two apart; a renewal that straddled the release
  wrote the lease back, under a log line saying it had been released. A
  process that exited right after `close()` never cleaned that up, and a
  writer on another host then waited out the whole `lock_lease_timeout`.
  `close()` now stops the heartbeat and waits for it before touching the
  lease (at most one storage round trip), retries the release, and logs
  "lease released" only when it was.
- Initial release: one writer per Kafka partition stages Arrow record batches
  as CSV objects on OSS/S3 and hands them to the Relyt master as serially
  ordered upsert loads. Staging location and credentials are supplied by the
  Relyt master (nothing secret in the application's configuration), recovery
  resumes from the staging state after a crash, and duplicate primary keys
  inside one staged file are collapsed last-write-wins before upload.
- Requires Relyt 3.55.0 or later on the server side.
- `TableWriter::fatal_error()` reports every way a stream stops (a lost
  lease, an ordering violation, a permanent rotation failure, a writer
  identity clash), so a monitor polling it catches a stopped stream while
  the application is idle. Each of those also emits one
  `RELYT_OBSERVE_ALARM` log line naming the stream, the cause and the
  operator action -- including the Kafka offsets to rewind to when that is
  the remedy. Requires a `tracing` subscriber in the application.
- gzip of staged objects uses flate2's zlib-rs backend: 2.6x less CPU per
  file than the default backend at the same level and ratio.
- Rotation runs in a per-writer background pipeline (render -> gzip -> put,
  CPU stages on the blocking pool) instead of inside the `append` that trips
  the threshold: `append` no longer pays the upload, and consecutive files
  overlap their stages. Files still reach the server in seq order and
  `flush()` still means durable. New tunables `rotation_queue_depth` (default 3) and
  `staging_error_after_attempts` (default 3); new error `StagingStalled` (returned by
  `append`/`flush`/`close` while a file keeps failing to stage -- nothing is
  dropped, an `append` that gets it did not take its batch, the pipeline
  retries), `RotationFailed` (the data or the schema of a sealed file was
  rejected, so retrying cannot help and the stage stops; storage trouble and
  in-process faults keep being retried as `StagingStalled` instead) and `StagingOrderViolation` (an order tripwire in
  the upload stage; a broken SDK invariant, with the Kafka offset range to
  rewind to in the message). An upsert stream whose primary key is a
  floating-point column is refused by `open_table` instead of failing
  inside the pipeline; binary keys are supported. The per-writer memory bound becomes
  (6 + `rotation_queue_depth`) x `rotate_size_bytes` (was ~2x); size
  `rotate_size_bytes` down for many writers per process.
