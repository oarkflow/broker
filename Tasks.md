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
- [x] Encode tenant and consumer-group offset keys injectively, persist the
  tenant explicitly in new WAL offset records, and migrate delimiter-based
  snapshot/WAL state during restore/replay.
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
- [x] Topic/pattern subscriptions own uniquely named router entries and remove
  them when stopped, preventing stale routes and unbounded router growth.
- [x] Consumers do not invoke handlers if persisting the processing state fails;
  the leased message or batch is requeued and the storage error is surfaced.
- [x] Ack/Nack handles become settled only after the store commits the operation;
  failed settlements remain retryable and auto-ack errors reach the worker.
- [x] Local and remote progress operations surface message/task persistence
  errors and emit success notifications only after required writes succeed.
- [x] Cancelling a task restored from persistent state hydrates the local task
  record, stops local execution, and returns task/message persistence failures.
- [x] Task submission rejects reused IDs and stops before publishing if the
  initial task record cannot be persisted; post-publish status failures include
  the task handle and ID so callers can recover the accepted work.
- [x] Retrying a failed task moves its dead-lettered message back to the task
  queue, resets attempts/error state, and persists the task's queued status.
- [x] Task start, progress, retry, failure, and completion transitions hydrate
  records from the state store after restart instead of silently updating only
  an empty process-local task map; durable state is written before in-memory
  transitions, hooks, and terminal waiter notifications are exposed.
- [x] Task submission preserves a consumer's faster `running` or terminal
  transition after publish instead of writing a stale `queued` status over it.
- [x] Task cancellation is serialized with completion and cannot overwrite a
  succeeded, failed, or dead-lettered terminal result; repeated cancellation is
  idempotent. A cancelled task is acknowledged without invoking its handler,
  including when cancellation wins between lease and handler startup; the
  remote client retains early cancellation events until that delivery starts.
- [x] Consumer retry/dead-letter task transitions and their hooks now happen
  only after the store accepts the corresponding Nack; task state is persisted
  before hooks and waiter notifications, and Nack/task-store failures are
  returned to the caller. `RespondError` only emits the response, so a retryable
  handler error cannot prematurely mark a task terminal before settlement.
- [x] `TaskHandle.Wait` returns an already-terminal persisted task immediately
  after restart instead of waiting on an in-memory notification that cannot
  arrive, and removes timed-out waiters so they do not accumulate until task
  completion.
- [x] Consumer offset reads reject missing stream/group identifiers and honor
  canceled contexts, matching the commit path's input and cancellation checks.
- [x] Tenant-wide offset display keys fall back to injective encoded keys when
  a tenant or group contains the legacy NUL separator, avoiding cross-tenant
  collisions in unrestricted stats and listings.
- [x] The legacy outbox is documented as an in-memory retry buffer, and a
  database-local transactional outbox contract is available through SQLite.
  - [x] `DurableOutbox` now persists dispatch entries through the optional
    `OutboxStateStore` interface, restores in-flight work after restart, and
    includes a SQLite implementation. This provides at-least-once restart
    recovery when the state store is durable; it does not atomically commit
    with an application database, and a crash after append can cause a
    duplicate delivery.
  - [x] Added `EnqueueTx` and the SQLite `OutboxTransaction` adapter so an
    application can commit its business rows and outbox row in one caller-owned
    SQL transaction. This is database-local atomicity; broker message dispatch
    still happens after commit and remains at-least-once.
  - [ ] Add equivalent transaction adapters for other database backends as
    needed; the legacy `PublishRetryBuffer` remains process-local.
- [x] Add outbox claim leases with token-fenced completion so multiple dispatchers
  can share SQLite or memory outbox state and recover expired claims. Delivery
  remains at-least-once if a worker appends just before its lease expires.
  - [x] Reject claim leases shorter than the sequential batch dispatch budget
    plus grace, preventing a configured lease from expiring during normal
    worst-case dispatch and allowing a second worker to reclaim live work.
  - [x] Outbox dispatch returns both the original failure and any failure to
    persist pending recovery state, using a bounded recovery-write context.
- [x] Make the outbox worker lifecycle safe for concurrent Start/Stop calls and
  reject restart on an already stopped instance; persisted work resumes through
  a newly constructed outbox.
- [x] Add persisted exponential retry scheduling and a retryable dead-letter
  phase. If DLQ publication fails, retry the DLQ phase without resending the
  original message.
- [x] Add bounded retention pruning for published and dead-lettered outbox
  records so the in-memory index and persistent state do not grow indefinitely.
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

- [x] Persist producer IDs, epochs, and sequences for idempotent publishing;
  deduplication, idempotency, and producer state indexes are tenant-scoped,
  and version-1 snapshots migrate their legacy global indexes during restore.
- [ ] Add transactional consume-offset-plus-produce with durable commit/abort
  markers and coordinator recovery.
  - [x] Added `Broker.CommitOffsetAndAppend` for memory, SQLite, and the
    single-node WAL stores. The append batch and offset are one locked
    in-memory operation; WAL stores persist one combined record, while SQLite
    saves both in its transactional snapshot.
  - [x] Added a single replicated command so the offset and output batch apply
    together on each broker state machine.
  - [x] Added optional `OffsetCommit.TransactionID` deduplication. The bounded
    outcome cache is part of snapshots and WAL/replicated records; reusing an ID
    with different input fails with `ErrTransactionConflict`.
  - [x] Consumer offset lag reads the committed offset from the store, so a
    restarted or externally committed consumer group does not report stale lag
    from the process-local compatibility cache. The cache is tenant-scoped, and
    context-aware reads preserve tenant isolation.
  - [x] `GetAllOffsetsContext` uses an optional durable store lister where
    available, including memory, WAL, replicated, partitioned WAL, and SQLite
    stores; custom stores without the extension retain the cache fallback.
  - [ ] Add a distributed transaction coordinator and recovery protocol for
    failures across multiple resources/partitions.
- [x] Define and test `durable`, `latency`, `throughput`, and `lightweight`
  policy defaults, including default batch-consumer sizes and profile-specific
  adaptive batch-wait starting points, floors, and adjustment rates.
- [ ] Add observable adaptive batching, overload admission control, lag-aware
  assignment, hot-partition detection, balancing, and bounded fairness.
  - [x] WAL group commit drains requests already queued before honoring its
    coalescing timer, preserving the timer wait when the queue is empty.
  - [x] WAL group-commit coalescing wait adapts between 50 and 300 microseconds
    based on whether a batch reaches its previous batch-size target; expose the
    current wait through store stats and Prometheus.
  - [x] Queue and topic rate-limit settings now govern publish admission with
    configured token refill and burst sizes; count rejected publishes in
    Prometheus. Multi-destination publishes charge all distinct destination
    buckets atomically, so a rejected publish spends no quota on destinations
    that had capacity.
  - [x] Expose queue-depth backpressure rejections alongside rate-limit
    rejections in Prometheus and OTLP, including store-level races where an
    append loses the capacity check after preflight.
  - [x] Atomic `Publisher.Tx` commits run the same queue/topic admission and
    tenant publish-quota checks as single-message and batch publishing.
  - [x] Least-loaded assignment uses stable consumer ordering, rotates ties,
    and includes outstanding dispatch assignments in load scores so a batch
    of concurrent queue polls does not choose from the same stale load snapshot.
  - [x] Topic partition stats identify hot partitions by depth skew and expose
    per-partition skew ratios; the hot threshold is configurable.
  - [x] Configured partition reassignment now transfers queued messages with
    one store move, rechecks their queued state atomically, preserves offsets
    when requested, wakes destination consumers, and skips a transfer when the
    destination queue has reached its configured depth limit. Move validates
    source membership under the store lock so overlapping passes cannot append
    the same globally keyed message more than once.
  - [x] Add least-depth partition selection for unkeyed publishes; preserve
    stable hash placement for keyed messages and rotate equal-depth choices.
  - [x] Batch subscriptions honor `BatchWait` and cap lease size to available
    `MaxInFlight` slots; configured wait is kept below the visibility timeout.
  - [x] If the worker pool rejects a leased message or batch, restore the
    in-flight count and requeue the lease without incrementing its attempt.
  - [x] RateLimiter treats non-positive rates as unlimited, defaults a missing
    burst to the rate, and avoids zero-rate panics or impossible `WaitN` loops.
  - [x] Queue/topic rate-limit maps are copied when the limiter is created, so
    later caller mutations cannot race with publish admission.
  - [x] Expose batch-consumer batch/message totals, maximum batch size, and
    message collection time in Prometheus.
  - [x] Adapt each batch consumer's collection wait within its configured
    `BatchWait` ceiling based on batch fill and report the mean current wait
    for active consumers.
  - [x] Durable outbox dispatch sorts the full in-memory due set by creation
    time and ID before selecting a batch, preventing arbitrary map iteration
    from repeatedly excluding older entries.
  - [x] Export durable outbox dispatch attempts, failures, successful publishes,
    and dead-letter completions through Prometheus and OTLP.

## P4 — protocol and security conformance

- [x] Make `brokerd` supervise every enabled listener and stop on listener failure.
- [x] Add protobuf/HTTP2 gRPC; retain JSON/TCP only as accurately named native RPC.
- [x] Add RFC 6455 WebSocket; keep SSE separate.
- [ ] Pass standard-client AMQP 0-9-1 and MQTT 3.1.1 interoperability suites,
  including reconnect, flow control, confirms, sessions, and QoS state recovery.
  - [x] AMQP consumer goroutines now stop on cancel/channel/connection close;
    unsettled deliveries are requeued on close, ACK/NACK use the broker's
    lease token, auto-ack consume/get paths acknowledge after delivery, and
    content frame sequences are serialized per channel across concurrent
    consumers and `basic.get`. `basic.qos` prefetch count is enforced per
    consumer or across the channel when the global flag is set.
    Standard-client interoperability remains unverified.
  - [x] MQTT subscription consumers now stop on unsubscribe/disconnect, are
    restored on reconnect, and old same-client-ID connections are closed
    before the replacement attaches. Subscription metadata persists through
    the optional `MQTTSessionStore` interface (SQLite and memory stores
    implement it); `+` filters map to the broker wildcard and malformed
    CONNECT/SUBSCRIBE/UNSUBSCRIBE packets are rejected. Durable queued delivery
    still depends on the message store. Inbound QoS 2 handshakes persist their
    in-flight payload/sequence and use a stable, per-session-generation message
    idempotency key, but duplicate suppression depends on the message store
    retaining that key through recovery. Standard-client interoperability
    remains unverified. CONNECT Last Will payloads now publish on unexpected
    disconnect; graceful DISCONNECT and broker shutdown suppress the Will.
    MQTT publish contexts carry the authenticated principal, tenant-scoped
    topic routes are tenant-fenced, and multi-tenant credentials are refused
    because MQTT 3.1.1 has no tenant selector. ACL `message:publish` and
    `message:read` permissions gate inbound publishes and subscriptions.
    RETAIN publications now replace tenant-scoped retained state, empty retained
    payloads clear it, and matching subscriptions replay it with RETAIN set.
    Memory and SQLite state stores persist retained values; other state stores
    use a server-local fallback. Outbound QoS 1 packets wait for `PUBACK`, are
    persisted with packet identifiers for session recovery, and resume with
    the DUP flag. Outbound QoS 2 persists its PUBLISH/PUBREL phase, resumes with
    the same packet identifier, and completes the PUBREC/PUBREL/PUBCOMP handshake.
- [ ] Centralize listener authentication, tenant-aware RBAC, credential hashing,
  rotation/revocation, certificate mapping, quotas, deadlines, and frame limits.
  Hashing, rotation/revocation, certificate mapping, core RBAC, deadlines,
  frame limits, and shared per-principal quotas are implemented. Unary
  requests, streaming messages, and long-lived protocol operations now charge
  the same ACL rate bucket; open connections share the concurrency limit.
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
