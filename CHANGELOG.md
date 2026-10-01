# Changelog

All notable changes will be documented here. The project follows Semantic
Versioning after v1; all pre-v1 APIs and persisted formats may change with a
documented migration path.

## Unreleased

### Changed

- Project license finalized as Apache License 2.0 (confirmed by project
  owner); README and Tasks.md no longer say "no license selected."
- `brokerd` no longer requires `--insecure-dev` to enable the native BST1
  stream listener (`-native-addr`). It already has a token/mTLS handshake via
  the shared ACL; the CLI now permits enabling it in production mode when
  `-token` or `-require-client-cert` is set, matching the AMQP gate. Updated
  README and flag help text, which previously and incorrectly described the
  listener as unauthenticated.
- Added `deploy/kubernetes/` manifests and a `deploy/helm/broker` Helm chart
  (StatefulSet, Services, PodDisruptionBudget, NetworkPolicy, ConfigMap,
  Secret template). **Not yet validated against a real cluster** — see
  `deploy/README.md` and `Tasks.md` P5 for caveats, including that `brokerd`
  currently has no peer/cluster flags, so a multi-replica install is N
  independent standalone brokers rather than an actual cluster.
- Raised test coverage: `pkg/queue` 0% -> 100%, `pkg/client` 12.4% -> 85.6%.
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
- `AdminServer`'s `/v1/dlq/*` route forwarded requests with the `/v1` prefix
  still attached, so `dlqQueue()`'s prefix-stripping logic never matched and
  every `/v1/dlq/*` call 404'd. `pkg/client`'s `Client.DLQ()` and
  `ReplayDLQ()` were unusable against a real broker. Fixed by stripping `/v1`
  before delegating, matching the existing `/v1/streams/` handling.

### Security

- Added a private vulnerability reporting policy and multi-module vulnerability
  scanning workflow.
