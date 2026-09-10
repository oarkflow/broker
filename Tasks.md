# Broker production-hardening tracker

This is the authoritative readiness tracker. Historical feature claims were
removed because several described APIs rather than verified guarantees.

## Current verdict

- **Version:** `0.1.0-dev`
- **Embedded/single-node:** functional pre-v1; not yet production-supported.
- **Distributed broker:** experimental; not production-safe.
- **Public stable release:** blocked by the gates below and by the missing
  owner-selected license.

## Verified in the current hardening pass

- [x] Root tests and vet pass.
- [x] Repository-wide race testing passes after the current fixes.
- [x] `cmd`, `contrib`, and `examples` modules compile/test locally.
- [x] Fixed Raft state and memory-transport race reports.
- [x] Fixed pool idle detection across the dequeue/execution transition.
- [x] Drop-oldest jobs are explicitly terminated and accounted for.
- [x] Memory publishing reports only `enqueued` acknowledgement.
- [x] local WAL publishing reports `persisted` with `local_fsync` durability.
- [x] Unachievable requested confirmation levels fail before publish mutation.
- [x] Added explicit public node, cluster, storage, security, listener,
  performance-profile, acknowledgement, and durability types.
- [x] Added tenant enforcement to publish and core message-store paths.
- [x] Added multi-module CI, race/vet checks, vulnerability scanning, security
  reporting policy, changelog, migration notes, and explicit release gates.
- [x] Corrected README protocol and production-readiness claims.
- [x] Added a versioned protobuf/HTTP2 gRPC service and a distinct RFC 6455
  WebSocket listener with authentication and tenant-aware handlers.
- [x] Added secure-by-default AMQP PLAIN/EXTERNAL authentication and fixed the
  AMQP 0-9-1 protocol-header parser.
- [x] Added hashed API credentials with expiry, revocation, and safe rotation
  through concurrent credential addition.
- [x] Added verified-client-certificate identity mapping for HTTP, gRPC,
  WebSocket, and SSE ACL principals.
- [x] Tenant-filtered store statistics, stream lag/offsets, notifications,
  tasks, sagas, and SSE broadcasts; tenant-scoped credentials are fenced from
  global routes, audit, metrics, consumers, and consumer groups.
- [x] Durable publish appends are prepared and synced before becoming visible;
  forced WAL failures cannot expose an unacknowledged ghost message.
- [x] Added backup-preserving BWL1 verify/tail-repair plus verified JSON
  export and exclusive import tooling.
- [x] Added codec fuzz targets and CI smoke execution.
- [x] Added and locally verified reproducible command builds plus a Go 1.26
  distroless, non-root broker image with a writable non-root WAL directory.
- [x] Vulnerability scanning reports no known Go vulnerabilities in all four
  public modules; CycloneDX 1.7 image SBOM generation is scripted and locally
  verified.
- [x] Added the etcd/raft `RawNode` foundation with safe election/quorum defaults;
  the durable controller/partition group runtime is still pending.

## P0 — correctness and truthful contracts

- [x] Make the complete repository race-free under the current repository-wide run.
- [ ] Eliminate memory-before-WAL mutation windows for every durable operation.
  Publish append is WAL-first; lease, acknowledgement, nack, movement, purge,
  update, and offset mutation paths still require the same ordering.
- [x] Add persistent store health/fencing after disk and `fsync` failures.
- [ ] Finish tenant enforcement for every admin, stream-offset, notification,
  task, schema, route, and control-plane read/write operation.
- [ ] The legacy outbox has been renamed/documented as an in-memory retry buffer;
  provide the real transactional integration contract.
- [ ] Establish 80% core and 90% critical-path coverage. Current package
  coverage is 44.1% root, 33.2% cluster, 22.1% AMQP, 12.4% client, and 42.2%
  pool; generated API and queue packages remain at 0%.
- [x] Removed the machine-specific public examples replacement; the optional
  private dagflow example is isolated in its own nested module.

## P1 — segmented durable storage

- [ ] Implement BWL2/BST2 segmented logs with manifests and sparse indexes.
- [ ] Add clean-shutdown markers, checkpoints, atomic snapshots, and BWL1
  read-only migration.
- [ ] Integrate safe rotation, retention, TTL cleanup, compaction, archival,
  and disk accounting into the primary binary store.
- [ ] Handle torn writes, middle-record corruption, disk full, permission loss,
  interrupted compaction, and encryption-key rotation.
- [x] Add non-destructive offline verify, tail repair, export, and exclusive
  import commands for BWL1. BWL2 repair/migration remains pending.
- [x] Make SQLite mutations transactional with a registered supported driver
  and restart/rollback integration tests.

## P2 — production distributed broker

- [ ] Replace the custom production consensus path with the audited etcd Raft
  engine; keep third-party code limited to correctness-critical dependencies.
- [ ] Add a controller quorum for membership, placement, ACLs, schemas, and
  transaction coordination.
- [ ] Add independently replicated message partitions with RF=3 and quorum
  `fsync` as the durable default.
- [ ] Implement epochs/fencing, snapshots, catch-up, joint membership changes,
  safe leader transfer, failover, and request routing to partition leaders.
- [ ] Implement online reassignment, draining, rolling upgrades, and protocol
  version negotiation.
- [ ] Certify 3, 5, 9, and 25-node clusters without a hard-coded node ceiling.

## P3 — delivery, profiles, and adaptive operation

- [ ] Persist producer IDs, epochs, and sequences for idempotent publishing.
- [ ] Add transactional consume-offset-plus-produce with durable commit/abort
  markers and coordinator recovery.
- [x] Define and test `durable`, `latency`, `throughput`, and `lightweight`
  policy defaults; adaptive tuning for those profiles remains pending.
- [ ] Add observable adaptive batching, overload admission control, lag-aware
  assignment, hot-partition detection, balancing, and bounded fairness.

## P4 — protocol and security conformance

- [x] Make `brokerd` supervise every enabled listener and stop on listener failure.
- [x] Add protobuf/HTTP2 gRPC; retain JSON/TCP only as accurately named native RPC.
- [x] Add RFC 6455 WebSocket; keep SSE separate.
- [ ] Pass standard-client AMQP 0-9-1 and MQTT 3.1.1 interoperability suites,
  including reconnect, flow control, confirms, sessions, and QoS state recovery.
- [ ] Centralize listener authentication, tenant-aware RBAC, credential hashing,
  rotation/revocation, certificate mapping, quotas, deadlines, and frame limits.
  Hashing, rotation/revocation, certificate mapping, core RBAC, deadlines, and
  frame limits are implemented; cross-protocol per-principal quotas remain.
- [x] Refuse anonymous or unsafe plaintext `brokerd` production operation;
  insecure operation requires the explicit development flag.

## P5 — release evidence

- [ ] Pass 12-hour nightly fuzzing and a 72-hour mixed-workload release soak.
- [ ] Pass deterministic crash, disk, corruption, partition, clock, upgrade,
  and simultaneous-failure suites while preserving documented invariants.
- [ ] Publish equivalent benchmarks against Kafka, NATS JetStream, and RabbitMQ;
  one declared profile must lead each selected workload/metric category.
- [ ] Resolve every critical/high independent security-review finding.
- [ ] Produce reproducible signed binaries/images, checksums, SBOMs, upgrade and
  rollback procedures, capacity limits, and backup/restore drills.
- [ ] Owner selects and commits a license; implementers must not choose it.

See `docs/RELEASE_GATES.md` for the exact stable-release policy.
