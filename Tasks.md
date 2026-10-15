# Broker production-hardening tracker

This is the authoritative readiness tracker. Historical feature claims were
removed because several described APIs rather than verified guarantees.

## Current verdict

- **Version:** `0.1.0-dev`
- **Embedded/single-node:** pre-v1, hardened. BWL2 segmented WAL is now the
  recommended durable store (`-store wal2`); several crash-safety and
  data-loss bugs in it were found and fixed in the 2026-10-01 pass (see P1).
  `govulncheck` is clean across all four modules and a 45s fuzz smoke of the
  WAL/message codecs found no crashes. Still short of the project's own
  production bar: root/cluster/AMQP coverage remain below the stated 80/90%
  targets (see P0), there is no AMQP/MQTT interoperability evidence against
  real clients, no multi-hour fuzz/soak run has been published, and no
  independent security review has occurred.
- **Distributed broker:** experimental; not production-safe. Out of scope for
  the current owner-directed hardening pass (single-node is the target
  deployment); see P2.
- **Public stable release:** blocked by the gates below. License is resolved
  (Apache License 2.0, confirmed by project owner).

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
- [x] Closed the remaining memory-before-WAL window: `SweepLeases` was still
  mutating memory only. Both `FileWALStore` and `BinaryFileWALStore` now
  prepare the deterministic expiry set, persist it (`sweep_leases`), then apply
  it, and the BWL1 codec carries the new operation without renumbering the
  released operation codes.
- [x] Hardened durable control-plane ordering: `PauseQueue`, `ResumeQueue`,
  `PurgeQueue`, `Update` and `UpdateBatch` validate their target before
  appending a WAL record. Previously a typo'd queue name appended a record that
  could not be applied, and `FileWALStore` recovery treats apply errors as fatal,
  so the store failed to open on the next restart. Replay is now also tolerant
  of records whose subject has since disappeared.
- [x] Closed the `Lease` tenant-isolation bypass: the lease paths filtered only
  on the caller-supplied `req.TenantID`, so a credential scoped to one tenant
  could lease another tenant's messages by naming that tenant. `Lease` and
  `prepareLease` now resolve the request tenant against the credential and
  enforce `tenantAllowed` per message.
- [x] Added `MemorySchemaRegistry.RegisterContext` with the same
  tenant-scoped-credentials-may-not-mutate-shared-config rule used by queue,
  topic and stream declarations.

## P0 — correctness and truthful contracts

- [x] Make the complete repository race-free under the current repository-wide run.
- [x] Eliminate memory-before-WAL mutation windows for every durable operation.
  Publish append, lease, acknowledgement, nack, movement, purge, update, offset
  and lease-sweep mutation paths all persist before applying to memory in both
  WAL stores.
- [x] Add persistent store health/fencing after disk and `fsync` failures.
- [x] Finish tenant enforcement for every admin, stream-offset, notification,
  task, schema, route, and control-plane read/write operation. Lease was the
  last bypass; schema registration is now guarded as well.
- [x] Provide a real transactional application outbox: `sqlitestore.NewOutbox`
  enqueues using an application-owned SQL transaction and dispatches with durable
  retry state, leased claims and fenced completion. Rollback, restart recovery,
  append-before-completion interruption and competing workers are tested.
  Legacy in-memory workers remain compatibility APIs and are documented as such.
  See `docs/TRANSACTIONAL_OUTBOX.md` for the at-least-once contract.
- [ ] Establish 80% core and 90% critical-path coverage. Current package
  coverage: 52.8% root, 42.1% cluster, 31.5% AMQP, 85.6% client (raised from
  12.4%), 42.7% pool, 100% queue (raised from 0%); generated API package
  remains at 0% (generated code, not hand-written logic).
- [x] Fixed a real bug found while raising client coverage: `AdminServer`'s
  `/v1/dlq/*` route forwarded the unmodified request (with the `/v1` prefix
  still on the path) to `dlqQueue()`, which expects the prefix already
  stripped, so every `/v1/dlq/*` call 404'd. `pkg/client`'s `Client.DLQ()`
  and `ReplayDLQ()` were unusable against a real broker. Fixed in
  `dataplane.go` by stripping `/v1` before delegating, mirroring the existing
  `/v1/streams/` case. (2026-10-01)
- [x] The native BST1 stream listener already had a token/mTLS handshake via
  the shared ACL (see `native_stream.go`, `auth_bst1_test.go`), but
  `cmd/brokerd/main.go` unconditionally required `--insecure-dev` to enable
  it at all, and README/flag help text still described it as
  "unauthenticated." Relaxed the gate to match the AMQP pattern (require
  `-token` or `-require-client-cert` outside `--insecure-dev`) and corrected
  the stale docs. (2026-10-01)
- [x] Removed the machine-specific public examples replacement; the optional
  private dagflow example is isolated in its own nested module.

## P1 — segmented durable storage

- [x] Implement BWL2 segmented logs with manifests and sparse indexes
  (`wal_segment.go`, `wal_sparse_index.go`). Selectable via `-store wal2`.
  BST2 (a segmented *network stream* format) remains explicitly out of scope
  — see the BST2 note under docs/storage-formats.md; it is unrelated to BWL2.
- [x] Checkpoints and BWL1/JSON read-only migration were already implemented
  and tested; fixed during this pass (2026-10-01):
  - Persisted segment metadata (`SizeBytes`, `Records`, `ClosedAt`,
    `CleanClose`) was never synced from the live in-memory segment handle
    back into `manifest.Segments` after the segment was created, so
    `manifest.json` silently diverged from reality the moment a segment
    rotated. This broke age-based retention outright (it reads `ClosedAt`,
    which was always zero) and made the persisted manifest unreliable for
    any external tooling. Fixed in `wal_segment.go`
    (`syncSegmentMetaLocked`/`saveManifestLocked`).
  - Compaction deleted pruned segment files *before* persisting the updated
    manifest; a crash in between left the manifest pointing at files that no
    longer existed, which recovery then silently treated as zero records
    (real data loss on top of a crash). Reordered to save-then-delete, and
    added orphan-file cleanup on open for files that were pruned from the
    manifest but never got deleted (`wal_compaction_active.go`,
    `cleanupOrphanFilesLocked`).
  - A manifest entry with no file on disk was silently treated as 0 records
    instead of failing closed; fixed to return an error wrapping
    `ErrStorageUnavailable`.
  - Added `-wal-retention`/`BROKER_WAL_RETENTION_SEC` and
    `-wal-compaction-interval` flags to actually expose the (already
    implemented) retention/interval config through `brokerd`.
  - Regression tests: `TestSegmentedWALManifestMetadataAccuracy`,
    `TestSegmentedWALRetentionPruning`,
    `TestSegmentedWALCompactionSurvivesInterruptedDeletion`,
    `TestSegmentedWALMissingReferencedSegmentFailsClosed`.
- [x] Rotation, count/age-based retention, and checkpoint-based compaction
  are implemented and tested for BWL2 (`SegmentCompactor`). "Archival"
  (moving pruned segments to cold storage instead of deleting) is not
  implemented — pruned segments are deleted, not archived.
- [x] Torn writes (active segment only) and a manifest/file mismatch are
  handled and tested. Disk-full and permission-loss surface through the
  existing group-commit fault path (`groupCommitWriter.faultErr`, shared with
  BWL1) which fails closed and reports via `StoreHealth()`/the health
  callback; this is structural, not separately fault-injection-tested for
  BWL2 in this pass. Mid-segment (non-tail) corruption in a *closed* segment
  still fails the whole store open rather than being auto-repaired — this is
  a deliberate fail-closed choice (see docs/storage-formats.md), not an
  oversight, but there is no operator tool yet to quarantine a single bad
  segment and resume from the last checkpoint. Encryption-key rotation is
  not implemented (BWL2 reuses the WAL codec's single configured key, same
  as BWL1).
- [x] Extended `broker-wal` with `-dir` to run non-destructive BWL2
  verification (`VerifySegmentedWAL`); it previously only understood BWL1
  single files. A BWL2 store self-heals a torn active-segment tail
  automatically on open, so there is no separate BWL2 `-repair-tail`.
- [x] Add non-destructive offline verify, tail repair, export, and exclusive
  import commands for BWL1.
- [x] Make SQLite mutations transactional with a registered supported driver
  and restart/rollback integration tests.

## P2 — production distributed broker

- [!] Confirmed gap: no shipped binary (`cmd/brokerd` or otherwise) wires up
  the `cluster/` package at all — `brokerd` exposes no peer/cluster/Raft
  flags. The distributed broker exists only as a library (`distributed.go`,
  `cluster/*`) with no runnable daemon entry point today. A multi-replica
  deployment of `brokerd` today runs N independent standalone brokers with
  separate WAL and identity, not a cluster. Wiring an actual clustered daemon
  (peer discovery, ConfChange-driven membership, controller quorum) is
  prerequisite to every other item below being reachable outside tests.
  (2026-10-01)
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
  Single-store/shard offset-plus-produce now commits in one durable operation,
  with expected-offset fencing, crash recovery and SQLite rollback tests. The
  embedded, HTTP and Go client APIs are documented in `docs/STREAM_TRANSACTIONS.md`.
  Multi-shard prepared transactions and coordinator recovery remain pending.
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
- [x] Native BST1 stream: the authenticated token/mTLS handshake already
  existed in `native_stream.go`; `brokerd` now permits enabling it outside
  `--insecure-dev` when `-token` or `-require-client-cert` is set, instead of
  unconditionally requiring `--insecure-dev`. (2026-10-01)

## P5 — release evidence

- [ ] Pass 12-hour nightly fuzzing and a 72-hour mixed-workload release soak.
- [ ] Pass deterministic crash, disk, corruption, partition, clock, upgrade,
  and simultaneous-failure suites while preserving documented invariants.
- [ ] Publish equivalent benchmarks against Kafka, NATS JetStream, and RabbitMQ;
  one declared profile must lead each selected workload/metric category.
- [ ] Resolve every critical/high independent security-review finding.
- [ ] Produce reproducible signed binaries/images, checksums, SBOMs, upgrade and
  rollback procedures, capacity limits, and backup/restore drills.
- [x] Owner selects and commits a license; implementers must not choose it. (Apache License 2.0, confirmed 2026-10-01.)
- [!] Added initial `deploy/kubernetes/` manifests and a `deploy/helm/broker`
  chart (StatefulSet, headless/client Services, PDB, NetworkPolicy, ConfigMap,
  Secret template). These are new and have NOT been validated against a real
  cluster (no `kubectl`/`helm` cluster access in the authoring environment —
  only YAML syntax and template balance were checked). Because `brokerd` has
  no peer/cluster flags (see P2), a multi-replica install today is N
  independent standalone brokers, not a cluster. Run `helm lint`/`helm
  template` and `kubectl apply --dry-run=client` against a real cluster, and
  treat this as unvalidated until someone does. See `deploy/README.md`.
  (2026-10-01)

See `docs/RELEASE_GATES.md` for the exact stable-release policy.
