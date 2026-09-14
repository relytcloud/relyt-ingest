# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and versions follow
[Semantic Versioning](https://semver.org/). While the major version is 0, a
minor release may contain breaking changes; each one is called out here.

## [Unreleased]

- Initial release: one writer per Kafka partition stages Arrow record batches
  as CSV objects on OSS/S3 and hands them to the Relyt master as serially
  ordered upsert loads. Staging location and credentials are supplied by the
  Relyt master (nothing secret in the application's configuration), recovery
  resumes from the staging state after a crash, and duplicate primary keys
  inside one staged file are collapsed last-write-wins before upload.
- Requires Relyt 3.55.0 or later on the server side.
- gzip of staged objects uses flate2's zlib-rs backend: 2.6x less CPU per
  file than the default backend at the same level and ratio.
