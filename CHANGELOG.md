# Changelog

All notable changes will be documented here. The project follows Semantic
Versioning after v1; all pre-v1 APIs and persisted formats may change with a
documented migration path.

## Unreleased

### Changed

- Reset the reported version to `0.1.0-dev` until production gates pass.
- Publish results now report the acknowledgement actually achieved by the
  backing store and reject confirmation levels the store cannot provide.
- Added explicit node, cluster, storage, security, listener, performance
  profile, acknowledgement, and durability contract types.
- Tenant-scoped principals are enforced by publishing and message-store paths.
- Pool idle detection now accounts for work between dequeue and execution.

### Fixed

- Removed data races in Raft state transitions and the memory transport test.
- Drop-oldest pool backpressure now terminates and accounts for the evicted job.

### Security

- Added a private vulnerability reporting policy and multi-module vulnerability
  scanning workflow.
