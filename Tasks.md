# Broker production-hardening tracker

This is the authoritative readiness tracker. Historical feature claims were
removed because several described APIs rather than verified guarantees.

## Current verdict

- **Version:** `0.1.0-dev`
- **Embedded/single-node:** pre-v1, hardened. BWL2 segmented WAL is now the
  recommended durable store (`-store wal2`); several crash-safety and
  data-loss bugs in it were found and fixed in the 2026-10-01 pass (see P1).
  A prior `govulncheck` run reported clean results across all four modules and
  a 45s fuzz smoke of the WAL/message codecs found no crashes. The project is
  still short of its own production bar: root/cluster/AMQP coverage remain
  below the stated 80/90% targets (see P0), there is no AMQP/MQTT
  interoperability evidence against real clients, no multi-hour fuzz/soak run
  has been published, and no independent security review has occurred.
  Selected recent Go advisories have
  been reviewed against pinned versions, but a full current-DB `govulncheck`
  scan has not completed because the environment cannot reach `vuln.go.dev`;
  the dependency vulnerability gate remains open (see P2).
- **Distributed broker:** controller, partition replication, transactions,
  reassignment, and routed clients are implemented, but remain experimental and
  not production-safe until multi-node, failure, interoperability, and release
  certification gates in P2–P5 pass.
- **Public stable release:** blocked by the gates below. License is resolved
  (Apache License 2.0, confirmed by project owner).

## Verified in the current hardening pass

- [x] `go vet ./...` and `go build ./...` pass in the current worktree.
- [x] Distributed offset-append recovery now has an end-to-end test that
  commits an output, interrupts before source resolution, removes the output
  message to simulate retention, and recovers the source outcome. The test
  exposed and fixed coordinator-ID validation that rejected recovery.
- [x] BWL1 and BWL2 key rotation now have regression tests for post-rotation
  verification and restart using only the new key; BWL1 backup preservation is
  checked. Crash injection at intermediate key-rotation steps remains open.
- [x] Optional BWL2 cold archival is implemented with fail-closed compaction:
  segments remain locally referenced if archive transfer fails.
- [x] etcd/raft in-memory replication smoke coverage now exercises 3, 5, 9, and
  25 voters. Deployed cluster certification remains open.
- [!] A fresh `govulncheck ./...` attempt could not fetch the vulnerability
  database (`vuln.go.dev` DNS resolution failed), so dependency security remains
  open. The targeted coverage run also could not complete for AMQP/client tests
  because this sandbox denies loopback listener binds.
- [!] The latest `go test ./... -count=1` run could not complete in this
  sandbox: tests that bind loopback listeners fail with `operation not
  permitted`. The run also found a task-result regression, now fixed and
  covered by `TestTaskSubmitAndResult`, plus a stale cluster snapshot test
  assertion, now corrected; targeted root transaction tests and the complete
  cluster package pass. Rerun the full suite where loopback binds are permitted.
- [x] Repository-wide race testing passes after the earlier hardening fixes.
- [x] `cmd`, `contrib`, and `examples` modules compile/test locally.
- [x] Fixed Raft state and memory-transport race reports.
- [x] Fixed pool idle detection across the dequeue/execution transition.
- [x] Drop-oldest jobs are explicitly terminated and accounted for.
- [x] Memory publishing reports only `enqueued` acknowledgement.
- [x] HTTP batch publish endpoints return `207 Multi-Status` when item errors
  accompany the results, and preserve top-level broker errors when no item
  error is present.
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
  global route/resource listings, broker lifecycle controls, cluster topology,
  audit, metrics, consumers, and consumer groups.
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
- [x] A prior vulnerability scan reported no known Go vulnerabilities in all
  four public modules; CycloneDX 1.7 image SBOM generation is scripted and
  locally verified. This does not replace the current-DB scan gate below.
- [x] Added the etcd/raft `RawNode` foundation with safe election/quorum defaults;
  durable controller and partition-group runtimes are implemented below.
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
- [x] Reject message IDs already present in the store and duplicate IDs within
  an atomic batch before mutation, including generated fan-out copy IDs. This
  prevents one map entry from being overwritten while queue/topic indexes keep
  multiple references to the same identifier.
- [x] Preflight every recovered atomic append batch for message IDs and
  destination validity before applying its first entry, preventing a malformed
  later entry from leaving a partially installed batch.
- [x] Add persistent store health/fencing after disk and `fsync` failures.
- [x] Serialize broker lifecycle transitions: repeated `Start` is idempotent,
  overlapping start/stop transitions fail without duplicating background loops,
  and `Stop` can be retried after a canceled drain.
- [x] Finish tenant enforcement for every admin, stream-offset, notification,
  task, schema, route, and control-plane read/write operation. Lease was the
  last bypass; schema registration is now guarded as well.
- [x] Reject tenant-scoped credentials on scalar and partition queue
  pause/resume/purge routes because queue control state and purges affect the
  shared queue across tenants.
- [x] Keep schema lookup and version listings consistent when a version is
  replaced or deprecated, and return defensive copies of required-field lists.
- [x] Consumer subroutes verify the registered consumer's tenant before
  exposing event history or applying control operations.
- [x] Consumer registration, heartbeat, subscription, state changes, ban/unban,
  and unregister enforce tenant ownership atomically with registry mutations;
  a scoped registration cannot take over another tenant's consumer ID, and
  topic routes created for a consumer are fenced to that consumer's tenant.
- [x] Remote subscription creation returns queue declaration failures before
  mutating the consumer registry or installing a route; queue declaration also
  surfaces dead-letter queue creation failures. Route identity includes both
  filter and destination, so one consumer can bind a filter to multiple queues;
  reconnects reuse persisted generated queue names across the naming upgrade.
- [x] Remote consumer registry mutations persist proposed consumer state before
  making it visible in memory; state-store failures leave the prior registry
  entry intact, canceled operations stop before mutation, and unregister
  persists deletion before removing the entry.
  Reconnects hydrate tenant-checked persisted registrations first so stored
  subscriptions and generated queue names survive broker restarts. Startup
  restores persisted consumers as disconnected before the broker becomes ready.
- [x] Memory state-store operations now check cancellation after acquiring the
  lock and consistently return `ErrClosed` after shutdown across broker,
  consumer, task, publisher, AMQP, MQTT, retained-message, and outbox records.
- [x] Topic/pattern subscriptions own uniquely named router entries and remove
  them when stopped, preventing stale routes and unbounded router growth.
- [x] Router entries and consumer registrations own defensive copies of their
  caller-provided maps, slices, nested routing rules, and strategy configs;
  returned routes and consumer records cannot mutate registry state outside its
  lock.
- [x] Router matching evaluates cloned route snapshots after releasing the
  registry lock; user conditions and strategy callbacks can reconfigure routes
  without deadlocking writers or mutating the routed message.
- [x] Notification data is deep-copied when stored in history or consumer
  events, when returned to readers, and separately for each callback, so nested
  payload maps and slices cannot mutate another delivery or race with history.
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
  - [x] Add a driver-independent `database/sql` outbox transaction adapter
    with question-mark and dollar-numbered placeholders, and use it from the
    SQLite contrib store. Other SQL backends can pair it with their own
    `OutboxStateStore` and compatible table schema. The legacy
    `PublishRetryBuffer` remains process-local.
- [x] Add outbox claim leases with token-fenced completion so multiple dispatchers
  can share SQLite or memory outbox state and recover expired claims. Delivery
  remains at-least-once if a worker appends just before its lease expires.
  - [x] Reject claim leases shorter than the sequential batch dispatch budget
    plus grace, preventing a configured lease from expiring during normal
    worst-case dispatch and allowing a second worker to reclaim live work.
  - [x] Release not-yet-attempted claims after cancellation or a batch dispatch
    failure using a bounded uncancelled context; keep the current entry fenced
    when its append outcome may be ambiguous.
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
- [x] Persist registered task-handler results in the task's terminal state when
  the delivery is acknowledged, so `TaskHandle.Result` returns the handler's
  encoded value. `TestTaskSubmitAndResult` covers the returned payload.
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
  BST2 is the native stream's handshake negotiation envelope, not a BWL2
  storage format; no BST2 data-frame codec is implemented.
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
  are implemented and tested for BWL2 (`SegmentCompactor`). Optional cold
  archival is available through `WALSegmentArchiver`; pruned segments are
  streamed to it before local manifest changes or deletion. Archival failures
  preserve the local segment. Archive writes should be idempotent by filename
  because a crash can cause a retry. `TestSegmentedWALArchivesPrunedSegmentsBeforeRemoval`
  covers success and failure behavior.
- [!] BWL2 compaction checkpoints now use AES-GCM with the configured WAL key;
  legacy plaintext checkpoints are encrypted in place on open, and missing or
  malformed or structurally incomplete referenced checkpoints fail closed.
  Queue head/depth bounds and resource-name/index consistency are checked
  before restore.
  `broker-wal -dir` verifies the referenced checkpoint and reports legacy
  plaintext checkpoints without modifying the store; recovery and verification
  reject unexpected checkpoint and segment filenames from the manifest. This
  checkpoint writer now stages through an exclusive, unpredictable
  same-directory temporary file, preventing a stale `.tmp` symlink from being
  followed and cleaning up failed writes. `TestBinaryWALKeyRotationPreservesBackupAndRecords`
  and `TestSegmentedWALKeyRotationPreservesRecoverableState` cover successful
  BWL1/BWL2 rotation, new-key-only verification, backups, and restart recovery.
  Dedicated crash-injection coverage around the key-rotation commit point
  remains open.
- [!] BWL2 supports a bounded decryption keyring (16 previous keys) while new
  writes use the configured primary key. `broker-wal -dir ... -rotate-key`
  performs offline rotation by replaying with the old keyring, checkpointing
  with the new key, persisting the manifest, then pruning prior segments;
  `-recursive` discovers all manifested child stores beneath a partition-WAL
  root, and each directory is verified with only the new key after rotation.
  BWL1 now has a matching offline `broker-wal -path ... -rotate-key` flow:
  verify source records, rewrite them under the new key in a same-directory
  temporary file, verify record count, canonical record-content digest, and
  new-key readability, save a synced backup, and atomically replace the
  original. Plaintext-to-encrypted migration
  and legacy-frame conversion are supported. Stop all brokers. Rotate each
  BWL1 broker WAL file separately; for BWL2 rotate each broker store and its
  partition-WAL root. Retain old keys for backups and if a recursive run stops
  partway. BWL1 and BWL2 rotation have successful-path regression tests;
  crash-injection coverage around intermediate rotation failures remains open.
- [x] Torn writes (active segment only) and a manifest/file mismatch are
  handled and tested. Disk-full and permission-loss surface through the
  existing group-commit fault path (`groupCommitWriter.faultErr`, shared with
  BWL1) which fails closed and reports via `StoreHealth()`/the health
  callback; this is structural, not separately fault-injection-tested for
  BWL2 in this pass. Mid-segment (non-tail) corruption in a *closed* segment
  still fails the whole store open rather than being auto-repaired — this is
  a deliberate fail-closed choice (see docs/storage-formats.md), not an
  oversight, but there is no operator tool yet to quarantine a single bad
  segment and resume from the last checkpoint.
- [x] Extended `broker-wal` with `-dir` to run non-destructive BWL2
  verification (`VerifySegmentedWAL`); it previously only understood BWL1
  single files. A BWL2 store self-heals a torn active-segment tail
  automatically on open, so there is no separate BWL2 `-repair-tail`.
- [x] Add non-destructive offline verify, tail repair, export, and exclusive
  import commands for BWL1. Import validates operation names against the WAL
  codec's operation registry, so newer offset, transaction, and lease-sweep
  records accepted by export can also be restored without a second drift-prone
  whitelist. Imports reject unknown JSON fields, stage and verify all records,
  and publish with an atomic no-overwrite link. Tail repair rejects non-regular
  files, verifies the repaired prefix and unchanged source before replacement,
  and syncs the original backup and repaired directory entry.
- [x] Make SQLite mutations transactional with a registered supported driver
  and restart/rollback integration tests.
- [x] Fix `PartitionedBinaryWALStore.ReadTopic` pagination to scan each shard in
  bounded pages and merge the next page in stable order. The prior path called
  `ListTopic` with a zero limit, which defaulted to the first 100 messages and
  made later cursors appear empty; it also materialized the entire topic.
  `PartitionedBinaryWALStore.ListTopic` now returns `ErrTopicNotFound` when no
  shard has that topic and uses a deterministic tie-breaker for equal creation
  timestamps.
  `MemoryStore.ReadTopic` now returns `ErrCursorExpired` for removed or
  tenant-inaccessible cursors and `ErrInvalidMessage` when the cursor belongs
  to a different topic, instead of restarting from the beginning; SQLite
  inherits this behavior.

## P2 — production distributed broker

- [x] Add an opt-in `brokerd -cluster` entry point for the existing etcd/raft
  distributed broker, with explicit peer IDs/addresses, separate Raft WAL,
  and mandatory peer mTLS whose certificate SAN is checked against the claimed
  peer address. Committed membership changes update the transport allowlist.
  Raft message sender/destination IDs are checked against the authenticated
  transport peer and local node. Handshake identities and inbound Raft frames
  are size-bounded. The authenticated admin API returns the assigned peer ID,
  and the daemon can add/remove voters and start an already-added node without
  bootstrapping a new group.
  The authenticated admin API adds joiners as learners, returns the peer ID,
  and promotes only after the leader confirms catch-up. `/cluster/status` reports
  the Raft leader, term, commit/applied indexes, joint voter groups, and
  leader-local replication lag for each peer.
  This makes the library path runnable from the shipped daemon. Controller
  quorum, partition replication, and safe membership changes are implemented
  below; the system remains experimental until multi-node failure and release
  validation pass. The library constructor also rejects multi-node etcd/raft
  without mutual TLS, and dynamic multi-node groups require strict peer identity
  checks.
- [x] Replace the custom production consensus path with etcd/raft. The shipped
  daemon selects etcd/raft, and library callers must explicitly opt in to the
  legacy custom engine with `AllowExperimentalCustomRaft`.
- [x] Keep consensus correctness delegated to the audited etcd/raft engine.
- [ ] Complete the third-party dependency audit with a current, reachability-aware
  vulnerability scan across supported modules.
  - [x] Align the daemon and contrib SQLite driver to modernc.org/sqlite
    v1.63.0 and its matching libc/memory versions; examples use the same
    versions. The module files pass offline builds for the daemon and contrib.
  - [x] The public CI vulnerability matrix scans the root, daemon, contrib,
    and examples modules. The optional pipeline-consumer module is excluded
    because it depends on private dagflow and cannot pass public CI.
  - [x] CI runs the four-module vulnerability matrix nightly against the
    current Go vulnerability database; regular push and pull-request scans
    remain enabled. The latest scheduled result must still be reviewed before
    marking the dependency-audit gate complete.
  - [ ] Run the vulnerability scan against the current Go vulnerability DB and
    resolve any findings; local govulncheck cannot reach vuln.go.dev here.
    - [!] Checked the current Go vulnerability database on 2026-11-24: the
      known `klauspost/compress` two-block-frame panic is fixed in v1.20.2,
      and the reviewed 2026 gRPC transport/xDS advisories are fixed before
      v1.85.0. The checked-in v1.20.5 and v1.85.0 pins are beyond those fixed
      versions. `govulncheck ./...` could not complete: its default build cache
      was inaccessible in the sandbox, and a retry with a writable cache could
      not resolve `vuln.go.dev`. This advisory review does not replace the full
      reachability-aware scan across all modules.
    - [!] Rechecked selected published Go advisory records on 2026-11-25:
      `golang.org/x/net` v0.63.0 is beyond the v0.62.0 fix in
      GO-2026-6866, `golang.org/x/sys` v0.50.0 is beyond the v0.44.0 fix in
      GO-2026-5024, and `google.golang.org/protobuf` v1.36.12 is beyond the
      v1.33.0 fix in GO-2024-2611.
      This selected-package review is not a complete module or reachability
      scan; local govulncheck still cannot resolve `vuln.go.dev`.
- [x] Implement the controller quorum for membership, placement, ACLs,
  schemas, and transaction coordination.
  - [x] Controller metadata timestamps are assigned before proposal and carried
    in the replicated command, keeping metadata state deterministic across
    replicas and snapshots. Replay still applies pre-fencing committed leader
    changes, while new proposals require Raft transfer and an expected-leader
    epoch fence.
  - [x] Controller metadata reads return deep copies in stable ID order, so
    callers cannot mutate quorum state through returned maps or slices.
    Linearizable read APIs are available for nodes, partitions, and transaction
    records; transaction execution and recovery establish a quorum read point
    before inspecting coordinator state. `/cluster/status` reports controller
    metadata counts only after that quorum barrier succeeds. ReadIndex calls
    without a caller deadline use a configurable five-second timeout by default,
    configurable in brokerd with `-cluster-read-index-timeout` or
    `BROKER_CLUSTER_READ_INDEX_TIMEOUT`, so quorum loss does not leave
    control-plane reads waiting indefinitely. Followers with a known leader
    forward ReadIndex requests and wait for the returned point to apply locally.
  - [x] Ready-processing persistence and state-machine failures from inbound
    messages, campaigning, proposals, and ReadIndex mark the Raft controller
    fatal, release outstanding proposal/read waiters, and stop its event loop.
    Shutdown synchronizes with in-flight RawNode calls; peer steps, campaigns,
    and proposals check stopped/fatal state while holding the Raft node lock.
  - [x] Validate partition assignments, ISR membership, leader transfers, and
    node removal before proposal so malformed topology cannot enter metadata.
    New placement proposals also require every logical replica to be registered;
    the distributed controller verifies that replica addresses map to distinct
    authenticated Raft peers, and its admin path enforces the configured
    replication factor before the assignment is committed.
  - [x] Bind metadata proposals to the leader term used during validation, so
    leadership changes cannot forward stale decisions to a new leader.
  - [x] Share proposal serialization across controller wrappers that use the
    same metadata object, preventing concurrent validation from observing and
    committing conflicting local mutations.
  - [x] The etcd/raft distributed broker now snapshots and replays broker data
    and controller metadata through one versioned composite state machine;
    `DistributedBroker.ControllerQuorum()` exposes the metadata API, and the
    authenticated `/cluster/status` endpoint reports bounded metadata and
    transaction-recovery counts.
  - [x] Replicate schema definitions, registration timestamps, and deprecation
    state in controller metadata snapshots. Linearizable list/read APIs and
    authenticated register/read/deprecate routes are available; distributed
    brokers use the quorum-backed schema validator by default when no custom
    registry is configured. Publish validation observes committed versions.
  - [x] Replicate ACL principal hashes, certificate identities, permissions,
    tenant scopes, quotas, expiry, and revocation in controller snapshots.
    `brokerd` seeds an empty registry from configured credentials, then uses
    locally applied quorum policy for broker and listener authentication. A
    partial bootstrap proposal is retried by adding only missing names, while
    existing revoked names remain authoritative. ACL revisions persist in
    snapshots so authentication refreshes avoid copying the principal table
    when no policy has changed; successful startup sync stops retrying.
    Authenticated linearizable admin routes support list, create/replace,
    inspect, and revoke; response bodies never expose token hashes.
  - [x] Authenticated admin routes can list and assign partition metadata,
    update ISR membership, and advance a partition leader epoch; tenant-scoped
    credentials are denied access. Retrying an identical assignment is
    idempotent and does not advance the epoch; changing an existing replica set
    or assigned leader is rejected until the safe reassignment protocol exists.
    Partition leader changes transfer Raft leadership to a caught-up voter in
    the ISR before committing controller metadata, with the prior leader and
    epoch checked during that commit; retrying after a controller-quorum outage
    completes the metadata step after the Raft transfer. The partition `/isr`
    detail route now has a leader-only GET view sourced from live Raft progress,
    and its PUT operation accepts only an exact match to that live set before
    replicating ISR metadata. Partition-specific not-leader responses carry the
    partition leader address and Raft term so clients do not follow the
    unrelated controller-group leader. The partition `/route` endpoint returns
    a quorum-confirmed assigned leader address and, on assigned replicas, the
    locally observed Raft leader and term for comparison, plus whether the
    local group is running and ready. The Go client exposes a validated
    `PartitionRoute` lookup for this endpoint. Logical node records can now
    advertise a separate HTTP(S) client address; routes expose it as
    `leader_client_address`, and the Go client validates it through
    `PartitionLeaderURL` instead of mistaking a Raft peer address for an HTTP
    endpoint. Distributed assignment commands carry a routability requirement
    through the controller log and reject any replica without a client address;
    snapshots retain that requirement, and assigned nodes cannot clear their
    client address. Auto-placement excludes nodes without an advertised client
    endpoint.
    Partition list and detail reads
    wait for an etcd/raft ReadIndex quorum barrier and local application through
    the read point before returning metadata.
  - [x] `/cluster/metadata/nodes` exposes the replicated logical node registry
    with quorum-confirmed listing and authenticated register/deregister
    proposals, separate from Raft voter membership. Node registration uses
    deterministic joined/updated timestamps, defaults to `joining`, and
    validates lifecycle state values before replication and during snapshot
    restore. A node cannot be marked `left` while assigned to a partition, and
    later assignments cannot reference a registered node marked `left`. New
    placements cannot add a draining/failed node or transfer leadership to a
    non-active registered node. Initial replicas must also map to distinct
    authenticated controller voters; learners cannot enter assignment metadata.
    Current assignments can remain while draining.
    In brokerd, the controller leader registers current peers, including
    learners added after startup, as `joining`; it reuses an existing logical
    ID by address and preserves lifecycle state on restart. Operators
    explicitly mark healthy nodes `active`.
    Controller admin errors now translate leader/quorum, timeout, missing
    partition, node-in-use, invalid-placement, and unavailable-node failures to
    stable broker wire codes and HTTP statuses; invalid node lifecycle states
    report as invalid requests. Raft membership endpoints also
    map missing peers, pending joint changes, stale terms, and closed controllers
    to stable client-facing errors.
- [x] Add independently replicated message partitions with RF=3 and quorum
  `fsync` as the durable default.
  - [x] The shipped `brokerd -cluster` defaults to RF=3 and majority MinISR;
    each local partition uses a BWL2 store, and successful writes wait for the
    configured MinISR to match the committed Raft index. The etcd/raft Ready
    path syncs HardState, entries, and snapshots before sending peer messages
    or applying committed entries. Operator-provided values can lower these
    defaults, and full multi-node failure certification remains open.
  - [x] Add group IDs to etcd/raft peer messages, a shared-transport
    `RaftMultiplexer` and `RaftGroupHost`, group-scoped transports, and
    per-group durable storage namespaces. The distributed broker now routes
    its existing controller through the multiplexer and exposes
    `AddRaftGroup`/`RemoveRaftGroup` for independent application state machines
    on that authenticated listener. Membership-size reads use the controller's
    synchronized peer map, and idempotent same-address membership updates do
    not leak identity refs. Authenticated `/cluster/status` also returns stable,
    per-group leader, term, commit/applied indexes, membership, and replication
    lag. Partition groups additionally expose live ISR, RF/MinISR, and local
    leader-side MinISR wait counts, failures, and cumulative wait time. Dynamic
    group creation validates the local address
    against the shared transport identity and rejects duplicate peer addresses.
    `ReconcilePartitionGroups` provisions missing local groups from a single
    quorum-confirmed topology snapshot, mapping logical node addresses through
    authenticated controller peers; it refuses existing membership changes
    and reports groups whose local node was removed from metadata assignments.
    Existing groups are checked for voter roles and agreement with the
    controller-assigned leader before reconciliation reports success. Group
    assignments must meet the configured replication factor (default three).
    Reconciliation also builds a `PartitionGroup` fencing/ISR view and maps
    controller-local Raft peer IDs to stable logical replica IDs; this mapping
    is needed to refresh progress correctly when replica IDs are not addresses.
    `NewPartitionStateMachine` adapts the standard broker command state machine
    to a snapshot-capable store. `PartitionStoreFactory` lets reconciliation
    construct and own these adapters, rejects reusing a store pointer across
    groups, and closes stores with their Raft groups. Each partition must
    receive its own store because snapshot restore replaces that store's state.
    `NewSegmentedWALPartitionStoreFactory` creates stable, hash-keyed BWL2
    directories keyed by resource kind, name, and partition. It preserves
    single-kind legacy directories and fails clearly when a legacy directory
    contains both a same-named topic and stream.
    `brokerd -cluster` configures this factory with a separate partition WAL
    root and periodically reconciles local partition groups once assignments
    exist; the interval and root are configurable. Shutdown cancels and joins
    the reconciliation loop before closing Raft groups. `/readyz` now checks a
    quorum-confirmed topology and fails while assigned local groups are missing,
    stopped, fatal, stale, or inconsistent with assigned membership or leader.
    Operators can configure replication factor, minimum ISR, maximum lag, and
    replica inactivity timeout through `DistributedConfig` and brokerd's
    `-cluster-replication-factor`, `-cluster-min-isr`,
    `-cluster-max-replication-lag`, and `-cluster-replication-timeout` flags.
    The etcd/raft Ready path syncs entries before sending acknowledgements, and
    publish results now report `quorum_fsync` for writes confirmed by that
    replicated store. Partition writes now wait until the committed entry is
    durably replicated to the configured MinISR of assigned voters, covering
    configurations where MinISR exceeds Raft's majority quorum; the wait is
    bounded by the configured replication timeout and caller deadline. RF=3
    behavior still needs multi-node crash and failure certification.
    Topic append and same-partition atomic append now use the assigned partition
    Raft group when its metadata exists; writes spanning destinations or
    partitions are rejected instead of silently using the broker-wide group.
    A write to a partition without an assignment now fails when that resource
    already has assigned partitions, preventing partial assignment from splitting
    the resource between partition groups and the legacy broker-wide group.
    This guard uses a quorum-confirmed resource-existence check without cloning
    or sorting the full assignment table on the publish path.
    `WithPartition` and HTTP `partition` fields let ordinary publisher calls
    select nonzero topic partitions; negative indexes fail before append.
    Topic `ListTopic` and `ReadTopic` use local partition stores after a
    per-group ReadIndex barrier and merge messages in stable creation-time
    order. `ReadTopic` scans in bounded pages instead of loading all retained
    messages. `/v1/topics/{topic}/partitions/{partition}/messages` serves one
    partition, and `Client.ReadPartitionTopic` resolves its advertised client
    endpoint while restricting credential forwarding to configured broker
    origins. `Client.ListTopic` fans out a bounded first-page read across all
    assigned partitions; `Client.ReadTopicPage` continues from an opaque
    per-partition cursor, rotates its starting partition, and advances only
    across messages returned in that page. Partition and full-topic reads reject
    cursor IDs that belong to another topic/partition; a checkpoint removed by
    retention returns HTTP 410 with the stable `cursor_expired` error code
    instead of silently replaying from the beginning. Cursor input/output is
    capped at 4 MiB, cursor position maps cannot exceed the current partition
    assignment size, and client page fan-out is capped at 10,000 partitions.
    Stream partitions now have typed controller assignments and independent
    Raft groups/stores. Stream appends route to their assigned group, the broker
    can read the first page across locally hosted partitions, and the HTTP/Go
    client APIs expose partition reads plus a merged cursor page with one local
    offset per partition. Partition-scoped consumer offset read/commit APIs now
    route checkpoints through the assigned stream Raft group, and replicated
    offset-plus-append supports outputs confined to that same stream partition,
    exposed through the partition HTTP endpoint and Go client with retry IDs.
    Cross-partition offset-plus-append coordination now uses durable controller
    decisions and partition recovery, subject to the supported destination limits
    listed under P3. Server-side assigned-stream lag aggregation now gathers
    leader reports over the authenticated broker identity and preserves each
    partition's local head and committed offset. Partial results retain failed
    partition details and use HTTP 207; one scalar offset across partitions
    remains undefined. Queue assignments, enqueue, lease, ack, nack, and lease
    extension now use assigned queue groups, with a Go client partition poll and
    per-operation configuration synchronization. Partition-local listing,
    pause/resume, and purge APIs are also available. `Client.PollAssignedQueue`
    fans out within an exact aggregate lease budget and returns partial leases
    with partition errors; `ListAssignedQueue` merges a bounded per-partition
    sample (not a cursor-based global FIFO page). Client whole-queue
    pause/resume/purge methods expose a transaction ID, completed partitions,
    and per-partition purge counts.
    - [x] Server-side assigned queue listing now uses
      authenticated broker mTLS forwarding, bounded concurrent fan-out,
      rotating per-partition page shares, and explicit completed/failure lists
      while preserving successful messages when another partition fails. The
      Go client consumes that server report. Internal partition responses are
      size-bounded and must be one strict JSON object with no unknown fields
      or trailing values.
    - [x] Assigned queue pause/resume/purge now use one durable coordinator
      transaction across all assigned partitions. Every partition must prepare
      before the coordinator records commit; participant resolution is retried
      during recovery. Results include the transaction ID and per-partition
      purge counts captured from replicated apply. A committed transaction may
      be temporarily visible on only some partitions while resolution catches
      up; the durable decision prevents a failed prepare from applying only a
      subset.
    Topic and stream client pages cap aggregate per-partition fetches
    at 10,000 messages so high partition counts do not multiply the page limit
    into unbounded buffered results.
    the broker itself must host every partition to return a local full-topic
    result.
    Reconciliation must be invoked on each assigned broker, with either an
    operator-supplied state-machine factory or a
    `DistributedConfig.PartitionStoreFactory` that opens a distinct
    snapshot-capable store for each local partition.
    DistributedBroker rejects new groups after shutdown or failed startup
    cleanup; failed startup and shutdown stop and unregister all groups so the
    multiplexer releases their peer identity references.
- [x] Implement partition epochs/fencing, Raft snapshots and log catch-up,
  joint membership changes, safe leader transfer, and request routing to
  partition leaders.
  - [x] Partition write requests must carry the current fencing epoch; the
    epoch follows the Raft term so leadership changes invalidate old requests,
    future or otherwise mismatched epochs are rejected, and leader/term are
    rechecked atomically with Raft proposal admission. `DistributedBroker`
    exposes `ProposePartition`, which checks a quorum-confirmed assignment,
    ensures the local group leader matches controller metadata, and validates
    the maximum of metadata epoch and Raft term. Metadata-only epoch bumps are
    rejected; epoch changes follow a Raft leadership transfer.
  - [x] Partition progress updates reject unassigned replica IDs and backwards
    indexes, reject follower reports ahead of the current leader, compare lag
    against that leader rather than the fastest reported peer, and return ISR
    and replica snapshots in stable order. Partition group construction also
    rejects invalid topic/partition IDs, duplicate or empty replicas, and
    replication-factor/minimum-ISR mismatches. ISR membership now expires when
    replica acknowledgements exceed the configured replication timeout; the
    local leader's applied index refreshes its progress only on the leader.
    Reconciled partition groups now refresh their ISR view from the local Raft
    leader's peer match indexes and recent-activity flags, and leader transfers
    require the target to remain in that live ISR.
  - [x] Voter add/remove proposals use etcd/raft `ConfChangeV2` joint consensus;
    removed-peer transport identities remain allowed until the automatic leave
    entry commits, and overlapping changes are rejected while joint state is
    active. Peer draining and catch-up orchestration beyond Raft log replication
    remain open.
  - [x] Daemon joins add non-voting learners first. Promotion is rejected until
    the learner's replicated match index reaches the leader's commit index; the
    leader promotes caught-up learners automatically and manual promotion is
    idempotent.
  - [x] Expose leader transfer through the authenticated admin API, reject
    targets that have not matched the leader's commit index, and wait for the
    target to become leader. Transfers are blocked during a joint change.
  - [x] Partition-group not-leader errors resolve the elected Raft peer through
    quorum-confirmed node metadata and advertise its broker client endpoint;
    private Raft transport addresses are never returned as client redirect URLs.
    ReadIndex and proposal races that return `leader lost`/`not leader` now use
    the same resource-specific routing hint.
  - [x] Partition-directed Go client calls retry one typed leader redirect when
    the advertised broker origin is already configured by the caller; they
    rebuild the target from the configured base and never forward credentials
    to an untrusted origin.
  - [x] Admin error mapping preserves a resource-specific partition leader
    hint instead of replacing it with the controller leader, and resolves
    controller Raft leaders to advertised broker endpoints for retry hints.
- [x] Add opt-in automated peer draining and catch-up orchestration beyond
  Raft log replication. Each broker advances reassignment for its locally led
  groups and replaces replicas on draining, failed, or left nodes using the
  durable learner, catch-up, ISR, leadership-transfer, and cleanup phases.
- [ ] Certify partition failover under simultaneous failures on a multi-node
  deployment; the local reassignment tests do not establish failure-domain or
  production-scale behavior.
- [x] Implement online reassignment, safe node draining, version inventory and
  upgrade planning, and BST2 handshake negotiation.
  - [x] Add a linearizable, read-only reassignment plan that validates desired
    replicas against active logical nodes, advertised client addresses, and
    authenticated Raft peers, then reports learner/catch-up,
    leadership-transfer, removal phases, and blockers. During an active
    transition it validates the durable source/target membership and reports
    the persisted phase, remaining catch-up/removal work, and abort cleanup,
    without falsely treating the transition's extra members as a mismatch.
    Learner addition and promotion lists now reflect live Raft roles, and
    missing or duplicate logical-node address mappings return validation errors.
    A target-only node that has become inactive does not block planning the
    durable abort cleanup that removes it.
    It does not mutate assignments or membership. The client resolves the
    partition leader for planning and leader-changing reassignment, then follows
    one typed redirect only to a configured broker origin after leadership moves.
  - [x] Add a linearizable node removal plan that lists every partition
    assigned to or referenced by a pending reassignment on the peer's logical
    node; the voter-removal guard consumes the same plan and rejects removal
    while either remains. It also reports
    whether leadership transfer, an in-progress joint change, or routing the
    request to the current controller leader blocks removal. A peer's logical
    node must be marked `left` before removal; left records are terminal,
    duplicate controller addresses are rejected, and records cannot be
    deregistered while their Raft membership remains. This fences new
    assignments and identities between preflight and voter removal.
  - [x] Add an authenticated drain-start operation that maps a Raft peer to its
    logical node and commits `draining` through the controller quorum, fencing
    new placement while existing assignments are moved. It does not execute
    reassignment or mark the node `left` automatically.
  - [x] Add a drain-completion operation that requires the logical node to be
    draining and absent from both active assignments and pending reassignment
    source/target sets, then commits `left`. The replicated metadata apply path
    rechecks those references, so a racing transition either commits first and
    blocks completion or follows `left` and is rejected.
  - [x] Add Go client methods for learner addition/promotion, controller
    leadership transfer, and guarded Raft peer removal so operators can drive
    the existing authenticated membership endpoints without raw HTTP calls.
  - [x] Reconciliation closes a stale local partition group only after the
    local peer is absent from its Raft membership; it keeps the group and
    reports an error while that peer is still a voter or learner.
  - [x] Persist a snapshot-safe reassignment transition with source/target sets,
    expected leader epoch, and copying/cleanup phases. Reconciliation provisions
    target-local groups from that metadata, and the authenticated `/reassign`
    route plus Go client advances replica and leader changes through learner
    addition, catch-up, promotion, metadata commit, old-member removal, and
    cleanup in retryable steps. Controller metadata writes from followers now
    establish a quorum read point before validation, then use etcd/raft proposal
    forwarding over the authenticated peer transport. Reassignment can transfer
    partition leadership to the desired replica after catch-up, verify the
    desired set against live Raft progress, and commit the observed ISR without
    requiring the partition and controller leaders to be co-located.
    A copying phase can be durably cancelled, including after a pre-commit
    leadership transfer: cancellation first returns leadership to the original
    source leader when the source ISR still meets MinISR, then brokers stop
    provisioning targets, remove added learners/voters, and clear the transition
    only after Raft membership returns to the original set and every original
    replica is confirmed as a voter.
  - [x] Add a BST2 negotiation envelope that selects the highest common
    supported native stream version before authentication. New clients use it
    by default; servers continue accepting legacy BST1 handshakes, and clients
    can explicitly select legacy mode for older servers. The current frame
    codec negotiates version 1 only; broader compatibility certification and
    rolling-upgrade orchestration remain open.
  - [x] `brokerd` now stamps release builds through the workflow's
    `main.version` linker variable and self-reports that build in replicated
    `metadata.broker_version`. Operators can inspect mixed versions through
    `GET /cluster/nodes`; this is inventory only and does not assert protocol
    or storage compatibility. The version-report command reuses the existing
    node-registration envelope for older controller binaries; newer state
    machines merge only that metadata key into the current node record.
    `brokerd -version` prints the embedded version and exits before startup
    validation. `docs/OPERATIONS.md` includes both inventory paths in the
    upgrade preflight.
  - [x] Added a read-only `/cluster/upgrade-plan?target_version=...` endpoint
    and Go client method. It lists unknown versions and non-active peers as
    blockers, requires a stable controller term/membership and no pending
    partition reassignments, confirms fresh live progress for every assigned
    replica through broker mTLS, then orders learners, follower voters, and the
    controller leader. It blocks stable-release downgrade targets and flags
    non-release strings for manual version ordering. The response explicitly
    leaves storage and protocol compatibility to the documented manual checks.
  - [x] `brokerctl cluster` now exposes status, node inventory, upgrade plan,
    drain start/completion, voter-removal plan, and controller leadership
    transfer commands. Control-plane CLI responses are capped at 64 MiB so
    large node or plan responses cannot be read without a bound.
- [ ] Implement rolling-upgrade orchestration and certify cross-version
  protocol/storage compatibility. BST2 negotiation currently selects version 1
  because only that frame codec is implemented.
- [ ] Certify 3, 5, 9, and 25-node deployed clusters and failure behavior without
  a hard-coded node ceiling.
  - [x] Add an in-memory etcd/raft replication smoke test at 3, 5, 9, and 25
    voters. `TestEtcdRaftScaleConfigurations` verifies leader election and
    committed-entry delivery to every simulated peer. It is useful scale
    regression coverage, but does not replace deployment/network/failure
    certification at those cluster sizes.

## P3 — delivery, profiles, and adaptive operation

- [x] Persist producer IDs, epochs, and sequences for idempotent publishing;
  deduplication, idempotency, and producer state indexes are tenant-scoped,
  and version-1 snapshots migrate their legacy global indexes during restore.
- [x] Add transactional consume-offset-plus-produce with durable commit/abort
  markers and coordinator recovery.
  - [x] Added `Broker.CommitOffsetAndAppend` for memory, SQLite, and the
    single-node WAL stores. The append batch and offset are one locked
    in-memory operation; WAL stores persist one combined record, while SQLite
    saves both in its transactional snapshot.
  - [x] Added a single replicated command so the offset and output batch apply
    together on each broker state machine.
  - [x] Partitioned streams now store and commit per-partition checkpoints in
    their assigned Raft group. Offset-plus-append can atomically checkpoint and
    append when all outputs target that same stream partition; writes to any
    other assigned topic, stream, or queue partition are coordinated through
    the controller transaction protocol. The multi-partition API requires a
    transaction ID, one assigned destination per output, and outputs distinct
    from the source checkpoint partition; mixed assigned/unassigned writes are
    rejected rather than split across durable and non-durable participants.
  - [x] Added optional `OffsetCommit.TransactionID` deduplication. The bounded
    outcome cache is part of snapshots and WAL/replicated records; reusing an ID
    with different input fails with `ErrTransactionConflict`.
  - [x] Offset-plus-append now applies publish backpressure, message-size,
    schema, and tenant message/queue quota checks before committing. Its HTTP
    route requires stream-commit permission and publish permission when outputs
    are present.
  - [x] Consumer offset lag reads the committed offset from the store, so a
    restarted or externally committed consumer group does not report stale lag
    from the process-local compatibility cache. The cache is tenant-scoped, and
    context-aware reads preserve tenant isolation.
  - [x] `GetAllOffsetsContext` uses an optional durable store lister where
    available, including memory, WAL, replicated, partitioned WAL, and SQLite
    stores; custom stores without the extension retain the cache fallback.
  - [x] Add a distributed transaction coordinator and recovery protocol for
    failures across multiple resources/partitions.
    - [x] Controller-quorum transaction records now persist participant sets,
      prepared votes, deadlines, immutable commit/abort decisions, and per-
      participant resolution. Recovery aborts expired undecided work and
      retries idempotent participant decisions until resolution is recorded.
      Snapshot restore validates coordinator, partition, and node metadata
      invariants before replacing live state. Participant counts and IDs are
      bounded and UTF-8 validated before replication. Replicated begin, prepare,
      decision, and resolution transitions reject missing timestamps so malformed
      log entries cannot poison transaction snapshot state.
      `BeginTransactionWithFingerprint` and
      `ExecuteTransactionWithFingerprint` persist a caller-supplied request
      fingerprint, rejecting transaction-ID reuse with different request data
      before preparing participants.
    - [x] `ExecuteTransaction` sequences participant prepare, durable decision,
      and resolution replay; participant implementations retain staged work by
      transaction ID and provide idempotent prepare/commit/abort handlers.
      Retrying an undecided transaction replays prepare for already-recorded
      votes so participants can verify reconstructed payloads against durable
      staged work before continuing.
      Prepare runs in participant-ID order; commit now resolves in reverse order
      and stops after the first failed commit so dependent effects cannot pass
      an unresolved earlier effect.
      Single-transaction execution now recovers only its own participant set;
      cluster-wide recovery remains an explicit sweep with a resolver that
      covers all pending participants; `RunTransactionRecovery` now provides
      an immediate, leader-only retry loop with cancellation and error reporting.
      `DistributedConfig.TransactionResolver` now lets `DistributedBroker`
      manage that loop with a five-second default interval, reports failures
      through an optional callback, and cancels/joins recovery before closing
      local partition stores. Partition state machines now stage atomic append
      and offset participant payloads durably in the Raft log and composite
      snapshots, apply commit/abort through idempotent resolution commands, and
      clear outcome markers after coordinator resolution. Prepare commands now
      persist a payload digest with the staged work; retries reuse the staged
      prepared messages instead of recalculating IDs or offsets, and the leader
      verifies the coordinator record before accepting peer prepare requests.
      Built-in participant
      constructors and a partition resolver are available. Proposals to a
      remote partition leader now use its registered HTTPS client address and
      the broker's mTLS client certificate; the receiving admin endpoint
      requires an ACL principal with `cluster:write`. Daemon resolver
      configuration is no longer required for built-in partition participants;
      the distributed broker's leader-only recovery loop uses the built-in
      resolver and delegates other participant IDs to the configured fallback.
      If a concurrent executor wins the
      durable decision race, the losing caller reads and resolves that actual
      decision instead of leaving its staged work for deadline recovery.
      Prepare failures, vote-persistence failures, and caller cancellation now
      make a bounded uncancelled attempt to persist abort and resolve staged
      participants.
    - [x] Wire `Broker.CommitOffsetAndAppend` to coordinate one output per
      assigned topic, stream, or queue partition. Output participants
      prepare and commit before the source checkpoint participant; that final
      participant also stores compact retry results. The API rejects fanout,
      source-partition output mixed with other participants, unassigned
      destinations, and archive/dead-letter outputs. Partition state machines
      reserve bounded-queue capacity when staging append participants; ordinary
      appends account for reservations, abort releases them, commit consumes
      them atomically, and snapshots rebuild reservations from staged work.
      Raft replay ignores outputs already restored from the partition WAL,
      avoiding duplicate append failures after a crash between store apply and
      Raft progress persistence. Prepare is durably rejected if the queue
      cannot reserve every required slot.
      - [x] Preserve final output-stream offsets in the caller result and
        source-partition retry cache when another append commits between
        participant prepare and commit. The durable source outcome carries
        destination references; output participants retain committed stream
        messages, including their partition identity, in consensus snapshots
        until the coordinator has resolved every participant. Source resolution
        fetches those results via authenticated
        partition-leader reads and applies final offsets to the retry cache in
        its replicated resolution command. A crash before source resolution
        leaves that participant staged so recovery can repeat the lookup from
        another broker. Coordinator cleanup is deferred until all participants
        are resolved, then forgets retained participant results. Queue
        reservations are released on abort and restored from staged work after
        recovery.
      - [x] Verify interleaving and crash recovery with targeted tests, including
        output commit before source resolution and retry after retention removes
        the output message. `TestDistributedOffsetAppendRecoversSourceAfterOutputRetention`
        commits the output, injects a one-time source-resolution failure,
        simulates retention deleting the output message, then recovers the
        source outcome from the retained participant result. It also verifies
        the durable offset and coordinator resolution. The test exposed and
        fixed source-participant validation conflating the caller idempotency
        key with the coordinator transaction ID. The lower-level
        `TestPartitionStreamTransactionResultSurvivesInterleavingAndSnapshot`
        covers output commit-time offsets, nonzero partition identity, snapshot
        restore, and retained lookup. Coordinator cleanup ordering is covered
        by `TestRecoveryForgetsParticipantsOnlyAfterAllCommitsResolve`.
    - [x] The coordinator state machine is included in the distributed broker's
      etcd/raft log and composite snapshots. Concrete broker partition staging
      and resolution adapters now persist work in partition snapshots, and
      prepare/resolve commands forward to partition leaders over the
      authenticated HTTPS admin peer endpoint.
    - [x] Add authenticated, quorum-confirmed lookup for one transaction by ID
      and a Go client method, exposing votes, decision, and participant
      resolution without returning an unbounded transaction listing.
      A cursor-based paginated view and Go client method now expose the recovery
      backlog in stable transaction-ID order with a bounded page size. A sorted
      transaction-ID index is maintained through apply, prune, and snapshot
      restore so pages and the recovery sweep avoid copying or rescanning the
      full transaction table. Operators can prune fully resolved records older
      than an explicit cutoff through a bounded replicated command and
      authenticated admin route; undecided or unresolved transactions are
      never eligible.
    - [x] The transaction-prune admin route rejects unknown JSON fields,
      trailing JSON values, zero cutoffs, and negative limits; its request-body
      bound maps oversized requests to HTTP 413.
- [x] Define and test `durable`, `latency`, `throughput`, and `lightweight`
  policy defaults, including default batch-consumer sizes and profile-specific
  adaptive batch-wait starting points, floors, and adjustment rates.
- [x] Add observable adaptive batching, overload admission control, lag-aware
  assignment, hot-partition detection, balancing, and bounded fairness.
  - [x] Initial topic, stream, and queue partition placement can be previewed
    and assigned deterministically from active authenticated Raft voters,
    preferring distinct `NodeInfo.Metadata["failure_domain"]` values when
    enough domains are configured, then balancing replica counts relative to
    optional `NodeInfo.Metadata["capacity_weight"]` (default 1), preferring
    known, recently active voters with lower controller-log lag, and using
    weighted leader counts on remaining ties. The plan reports each node's
    weight, failure domain, and unknown progress
    explicitly. A node's failure domain is immutable while it is assigned to a
    partition, preventing metadata edits from silently changing the topology
    represented by existing assignments. Failure-domain labels are bounded to
    256 bytes and validated as UTF-8 without control characters on registration
    and snapshot restore. Node updates that omit the metadata map preserve the
    existing map; an explicit empty map requests clearing it where allowed.
    Existing assignments are
    returned unchanged on retry, and concurrent auto-assignments serialize at
    the controller quorum so each scores against the preceding committed
    assignment; automatic reassignment runs as a separate opt-in sweep. The reassignment plan
    blocks when live ISR falls below the configured minimum or live Raft
    membership/leadership differs from the controller assignment.
  - [x] `/cluster/status` exposes leader-observed partition Raft match indexes,
    per-replica lag, recent activity, and ISR eligibility for partitions led
    by the serving broker. The mTLS-protected placement signal endpoint also
    reports local partition-store and per-partition message and lease counts,
    plus used and available disk capacity for the configured storage root or
    stores that implement `StorageUsageReporter`. Workload and disk signals
    carry observation timestamps and reports older than ten seconds are
    discarded. Initial placement gathers every active
    candidate's available signals, validates full per-partition leader
    coverage, and uses maximum replica lag, disk availability, and workload
    counts as tie-breakers. Partial or unavailable reports are discarded.
    Capacity weight remains operator-provided and relative. The opt-in daemon
    sweep advances durable reassignment for replicas on nodes marked draining,
    failed, or left, and moves healthy replicas when a source has at least two
    more assignments than an eligible target. It selects active authenticated
    voters by replica load and failure-domain diversity. Fresh, complete,
    per-partition workload snapshots can also trigger a move only when the
    projected source/target workload gap improves by at least 100 message/lease
    units and 20%; complete partition-progress snapshots prefer recent, in-sync targets
    with lower lag. Telemetry-informed balancing makes at most one move per
    sweep. Fresh complete per-partition disk reports can also trigger a move
    from a source that is at least ten utilization points fuller when the move
    reduces source/target utilization skew and leaves the target with a 20%
    partition-size reserve. Disk-informed balancing makes at most one move per
    sweep.
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
  - [x] Batch and native-frame publish paths reserve tenant message and queue
    quotas for the full per-tenant batch before appending, so unchanged store
    stats cannot let a batch exceed its message cap.
  - [x] Least-loaded assignment uses stable consumer ordering, rotates ties,
    and includes outstanding dispatch assignments in load scores so a batch
    of concurrent queue polls does not choose from the same stale load snapshot.
  - [x] Client polling across assigned queue partitions redistributes unused
    per-partition capacity to partitions that filled their share, without
    exceeding the aggregate lease limit; remainder allocation rotates.
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
  - [x] Expose aggregate automatic partition-reassignment start, completion,
    error, and pending-observation counters through Prometheus and OTLP without
    adding partition IDs as metric labels.
  - [x] Add opt-in automatic partition reassignment for nodes explicitly
    marked draining, failed, or left, plus bounded healthy balancing when a
    source has at least two more assigned replicas than an eligible target.
    Each broker advances only its locally led partition groups, replacing
    unavailable replicas with active authenticated voters selected by replica
    load and failure-domain diversity. Healthy balancing retains the current
    leader and moves one non-leader replica at a time. Reassignment uses the
    existing durable learner/catch-up/ISR/leader-transfer protocol; the daemon
    interval defaults to disabled.

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
    consumers and `basic.get`. Unknown settlement tags now close the channel,
    storage settlement failures close it so outstanding deliveries can be
    requeued, and channel close retries buffered transactional settlements
    after commit or rollback failures so their leases are not silently dropped.
    Publisher confirm sequence numbers are independent of consumer delivery
    tags. `basic.qos`
    prefetch count is enforced per
    consumer or across the channel when the global flag is set. `queue.declare`
    replies report tenant-visible ready-message counts and active AMQP
    consumers visible to the caller's tenant scope; passive declarations check
    existence without creating the queue, durable declarations require a
    persistent/replicated store, and unsupported exclusive/auto-delete queues
    fail explicitly. `basic.get-ok` reports the remaining ready-message count,
    and `basic.get-empty` encodes its reserved short string correctly.
    Declare/Bind/Purge honor their no-wait response flag;
    queue topology and AMQP channel flow, `basic.qos`, `consume`, `cancel`,
    `publish`, `get`, settlements, recovery, and confirm selection validate
    every method argument before mutating state. Content properties reject
    truncation/trailing bytes, body frames cannot exceed the declared content
    size, and publishes exceeding the broker message limit are rejected from
    the content header before body allocation. Missing queues return
    `NOT_FOUND` instead of being treated as empty names or empty queues. AMQP
    timestamps round-trip at their full 64-bit wire width through durable
    message metadata;
    malformed field-table lengths return parse errors instead of slicing past
    the input buffer, allocating from an untrusted field-value length, or
    recursing without a nesting bound.
    Method tables, nested table values, and Basic.Properties headers now decode
    the single AMQP field-table length prefix correctly. The legacy Basic
    `cluster-id` API field uses its reserved 0-9-1 property bit rather than the
    continuation bit; unsupported extended property-flag words fail explicitly;
    message priority values 0–9 now flow into the broker scheduler and back
    through pushed and polled AMQP deliveries;
    unsupported Queue.Delete now reports `NOT_IMPLEMENTED` instead of false
    success. `basic.recover` and `basic.recover-async` requeue all unsettled
    deliveries when requested and honor transactional settlement buffering;
    `requeue=false` redelivers active pushed deliveries on the same channel with
    the redelivered flag set, including the original consumer tag. AMQP bindings
    are now scoped by exchange and tenant, queue unbind removes the matching
    binding, and mandatory unroutable publishes return the content before
    publisher confirms; the unsupported immediate flag fails explicitly.
    Direct, topic, fanout, and headers exchange bindings now use their respective
    routing key rules; AMQP header tables retain their original value types
    across broker persistence and consumer delivery, including explicit empty
    tables. Content encoding, delivery mode, expiration, user/app IDs, and the
    legacy reserved field also survive persistence and delivery. AMQP
    expiration is parsed as milliseconds and enforced by the broker TTL path;
    malformed and out-of-range values return `PRECONDITION_FAILED`. Passive
    declarations, exchange type mismatches, missing
    exchanges, and unused exchange deletion are handled explicitly. Headers
    exchange bindings support `x-match` all/any and all/any-with-x, presence-only
    and scalar-value criteria; `amq.match` and `amq.headers` are predeclared.
    AMQP field tables now decode and encode void values, and consumer deliveries
    preserve headers in the content properties. Durable exchange declarations
    persist and reload through the optional AMQP exchange state-store interface,
    including SQLite; they fail explicitly when no persistent exchange store is
    configured. Exchange and queue topology methods reject truncated argument
    frames with `SYNTAX_ERROR` rather than treating missing fields as empty values.
    Exchange-to-exchange bindings now route recursively with cycle protection,
    and exchange deletion removes bindings in either direction. Links between
    durable exchanges persist and reload through the optional binding store.
    Queue durability is now retained in local and replicated queue configuration;
    bindings between durable exchanges and durable queues also persist and reload.
    Bindings involving non-durable resources remain process-local.
    Explicit queue bind/unbind against the default exchange now fails instead of
    acknowledging a no-op; queues are attached to it implicitly by queue name.
    Auto-delete, internal, and argument-based exchanges remain unsupported.
    Standard-client interoperability remains unverified.
  - [x] AMQP PLAIN advertises ACL-backed credentials when configured, and an
    ACL rejection cannot fall through to an unauthenticated login. TLS listener
    validation accepts ACL credentials as an authentication method.
  - [x] AMQP method handling now enforces shared ACL permissions for message,
    queue, and topic operations, propagates authenticated principals into broker
    contexts for tenant checks, requires ACL certificate mapping for EXTERNAL
    authentication. MQTT and AMQP reject incomplete configured static
    username/password pairs at listener startup.
  - [x] MQTT subscription consumers now stop on unsubscribe/disconnect, are
    restored on reconnect, and old same-client-ID connections are closed
    before the replacement attaches. Subscription metadata persists through
    the optional `MQTTSessionStore` interface (SQLite and memory stores
    implement it); `+` filters map to the broker wildcard and malformed
    CONNECT/SUBSCRIBE/UNSUBSCRIBE packets plus non-canonical remaining-length
    encodings are rejected. MQTT UTF-8 validation rejects nulls, control
    characters, and Unicode noncharacters in protocol strings. Durable queued
    delivery still depends on the message store. A clean-session reconnect
    now purges prior subscription queues before deleting session metadata and
    rejects CONNECT when a purge fails, allowing the client to retry instead
    of silently retaining messages from the old session. Inbound QoS 2
    handshakes persist their in-flight payload/sequence and use a stable,
    per-session-generation message idempotency key, but duplicate suppression
    depends on the message store retaining that key through recovery.
    Standard-client interoperability remains unverified. CONNECT Last Will
    payloads publish on unexpected disconnect; broker shutdown attempts to
    publish active wills before cancellation under a bounded deadline, and
    graceful DISCONNECT suppresses
    the Will. Pending Wills use optional `MQTTWillStore` persistence and are
    replayed before the listener accepts clients; memory and SQLite state
    stores implement it. Transient publish and record-delete failures retry
    with bounded exponential backoff; stable publish IDs allow retries after
    publish/delete ambiguity when the message store retains idempotency records. Custom state
    stores without this extension retain only the active connection's Will.
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
    Inbound QoS 2 closes a connection when its in-flight limit is reached or
    durable handshake state cannot be stored, allowing a retry instead of
    leaving the client waiting indefinitely for PUBREC. An occupied packet ID
    is acknowledged only for a matching retransmission with DUP set; conflicting
    reuse closes the connection instead of acknowledging and dropping new data.
- [x] Centralize listener authentication, tenant-aware RBAC, credential hashing,
  rotation/revocation, certificate mapping, quotas, deadlines, and frame limits.
  Hashing, rotation/revocation, certificate mapping, core RBAC, deadlines,
  frame limits, and shared per-principal quotas are implemented. Unary
  requests, streaming messages, and long-lived protocol operations now charge
  the same ACL rate bucket; open connections share the concurrency limit.
  The native stream listener now rejects production startup without both TLS
  and an ACL, and anonymous handshakes are accepted only in explicit development
  mode. MQTT listener startup likewise requires TLS and a configured
  authentication mechanism unless `AllowInsecure` is explicitly enabled; the
  daemon ties that switch to `--insecure-dev`. Legacy Native RPC now has the
  same explicit development switch and requires TLS plus a token or verified
  client certificates in production. RFC 6455 requests now return an
  authentication-unavailable response when production mode has no ACL; its
  explicit development mode supports a nil ACL without panicking. gRPC
  streaming rate admission also skips safely when ACL is intentionally absent
  in development mode.
  Distributed operational API membership changes now require an explicitly
  configured authorizer and fail closed when it is absent; `metrics:read`,
  `stats:read`, and `cluster:read` separately authorize global metrics, detailed
  statistics, and cluster-state reads, while
  unauthenticated GET/HEAD health and readiness probes stay available and
  return no internal failure details. The standalone operational HTTP server
  now applies configurable request-body bounds and read/write/idle timeouts,
  returns 413 for oversized JSON membership requests, reports bind failures
  synchronously, and rejects duplicate or post-shutdown starts. The admin API
  also maps bounded-body overflow to HTTP 413 with the stable
  `message_too_large` error code. The daemon
  admin API gates cluster reads on `cluster:read`, voter changes on
  `cluster:write`, and rejects tenant-scoped credentials. Go client JSON
  responses are read through a
  configurable 64 MiB default bound; oversized replies return
  `client.ErrResponseTooLarge` before JSON decoding.
  - [x] Centralize the production TLS-and-authentication startup gate shared by
    MQTT, Native RPC, gRPC V1, and BST1 listeners while retaining
    protocol-specific authentication checks. HTTP admin, WebSocket, gRPC, and
    Native RPC use certificate-first, token-fallback principal resolution.
    Native RPC maps verified certificate identities or bearer tokens to ACL
    principals, applies method permissions, tenant scope, per-request admission,
    and per-connection concurrency limits; request deadlines remain live until
    the handler returns. `brokerd` exposes least-privilege certificate identity
    and permission flags for mTLS principal mapping. Production listener gates
    now reject empty, expired, or revoked ACLs, and certificate-only ACLs count
    as authentication only when the TLS listener verifies client certificates.
    The HTTP admin listener uses an empty trust pool on invalid client-CA input,
    and brokerd rejects missing or invalid client-CA configuration before
    starting when client certificates are required.
    `brokerd` validates certificate identity prefixes and IP values and refuses
    identity mappings unless client-certificate verification is enabled.
    In distributed mode, brokerd shares one ACL across listeners and broker
    operations, seeds the controller registry from bootstrap configuration,
    and refreshes authentication from committed replicated principals.
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
  - [x] The tagged GitHub release workflow now pins Go to 1.26.11 (the Go 1.26
    security release), disables VCS build metadata, clears the linker
    build ID, verifies duplicate command builds, validates plain semver tags,
    and normalizes tar/zip timestamps and ownership. It signs the checksum
    manifest with Cosign keyless signing. It also creates a CycloneDX SBOM for
    each binary using pinned Syft whose release checksum signature is verified
    against Syft's exact `release.yaml` workflow identity before installation;
    the final checksum manifest covers those SBOMs.
    Before publication, the workflow checks every listed asset and verifies
    the checksum signature against the tagged release workflow identity.
    `docs/RELEASE_GATES.md` documents verification of the checksum signature
    against the exact workflow identity and requires every listed asset to be
    present. The workflow now also publishes the broker image for linux/amd64
    and linux/arm64 to GHCR, records its manifest-list digest in the signed
    release assets, signs that digest with Cosign, creates per-platform
    CycloneDX SBOM attestations using verified Syft, and verifies the image
    signature and attestation identity before GitHub Release publication.
    The image build now uses the source commit timestamp for BuildKit output
    metadata and disables implicit BuildKit provenance/SBOM generation, keeping
    the published index focused on the reproducible image and separately
    verified attestations. The Docker builder now uses the official
    multi-architecture Go 1.26.11 image index by digest; runtime stages use
    the Distroless Debian 13 nonroot image by digest, and the workflow requires
    its keyless signature to verify before building. An independent image
    rebuild comparison has not been done; container image reproducibility and
    release drills remain open.
  - [x] A local reproducibility check built the current worktree's Linux/amd64
    `brokerd` twice with the release linker/build flags and separate empty Go
    build caches; both binaries had SHA-256
    `b05f74375771c982a905d187f03bcc97d5995e187838e63623b3e94ec7b2655a` and
    compared byte-for-byte. The local toolchain is Go 1.27.4, not the pinned
    release Go 1.26.11, so this does not verify the release toolchain or image.
  - [x] Third-party GitHub Actions in CI and release workflows are pinned to
    immutable commit SHAs, with their release tags retained as comments for
    review.
- [x] Owner selects and commits a license; implementers must not choose it. (Apache License 2.0, confirmed 2026-10-01.)
- [!] Added initial `deploy/kubernetes/` manifests and a `deploy/helm/broker`
  chart (StatefulSet, headless/client Services, PDB, NetworkPolicy, ConfigMap,
  Secret template). Helm now has opt-in experimental cluster wiring with
  StatefulSet-derived Raft IDs, headless peer addresses, mTLS, and separated
  persistent WAL directories. With that option disabled, replicas remain
  standalone. Clustered Helm rendering now rejects `persistence.enabled: false`
  because that would place broker, Raft, and partition WALs on ephemeral
  `emptyDir` storage; it also requires a quorum-safe PodDisruptionBudget and
  hard topology spreading for clustered replicas, and a shared immutable
  SHA-256 image digest for every Raft peer. These deployment changes have NOT
  been validated: Helm is not installed in the authoring environment, and no
  live cluster is available.
  Run `helm lint`, `helm template`, and `kubectl apply --dry-run=client`, then
  exercise a disposable multi-node cluster before relying on it. See
  `deploy/README.md`.
  (2026-10-01)

See `docs/RELEASE_GATES.md` for the exact stable-release policy.
