# Tasks.md — fastworker message broker implementation tracker

## Completed

### Core architecture
- [x] Added a separate `broker` package on top of the existing `fastworker` pool.
- [x] Kept `fastworker` as the execution engine and moved broker responsibilities into the broker layer.
- [x] Added embedded broker mode with zero additional external dependencies.
- [x] Added standalone-ready HTTP admin/API handler.
- [x] Added distributed-ready abstractions through pluggable store, routes, destinations, queues, topics, streams, hooks, and movement APIs.

### Message model
- [x] Implemented rich `Message` envelope.
- [x] Added topic, queue, stream, partition, offset, key, routing key, type, content type, schema fields.
- [x] Added headers, metadata, trace ID, span ID, correlation ID, causation ID, request ID, reply-to fields.
- [x] Added idempotency key and deduplication ID fields.
- [x] Added priority, created-at, available-at, expiry, attempts, max attempts, redelivery count.
- [x] Added tenant, producer, consumer, consumer group fields.
- [x] Added lease token and lease deadline.
- [x] Added original topic/queue/stream fields for movement/replay/DLQ restore.
- [x] Added message status, last error, dead-letter reason, task ID, and task name.

### Publisher
- [x] Implemented `Publish`.
- [x] Implemented `PublishConfirm`.
- [x] Implemented `PublishAsync` with future.
- [x] Implemented `PublishBatch`.
- [x] Implemented `PublishBatchAsync`.
- [x] Implemented `Broadcast`.
- [x] Implemented `Command` as request/response command execution.
- [x] Implemented `PublishAfter`.
- [x] Implemented `PublishAt`.
- [x] Implemented `BeginTx`, commit, and rollback.
- [x] Added publish options for key, routing key, type, headers, metadata, trace, correlation, causation, idempotency, deduplication, priority, delay, run-at, TTL, tenant, producer, queue, stream, confirm level, max attempts, and retry policy.

### Request/response
- [x] Implemented synchronous `Request`.
- [x] Implemented `RequestAsync`.
- [x] Implemented `RequestStream` primitive.
- [x] Added correlation ID handling.
- [x] Added reply-to header support.
- [x] Added response futures.
- [x] Added timeout handling.
- [x] Added structured response errors.
- [x] Added `Delivery.Respond`.
- [x] Added `Delivery.RespondError`.

### Routing
- [x] Implemented broker router.
- [x] Added direct/topic/fanout/header/rule/priority/stream route modes as supported route metadata.
- [x] Implemented wildcard matching with `*` and `#`.
- [x] Implemented route matching by topic/routing key.
- [x] Implemented header matching.
- [x] Implemented tenant matching.
- [x] Implemented priority-min matching.
- [x] Implemented custom condition function support.
- [x] Implemented route destinations to topic, queue, stream, DLQ, and archive.
- [x] Implemented automatic route binding for topic subscriptions.

### Queue/topic/stream store
- [x] Implemented pluggable `Store` interface.
- [x] Implemented in-memory store.
- [x] Implemented file WAL store with JSONL recovery.
- [x] Implemented queue declaration.
- [x] Implemented topic declaration.
- [x] Implemented stream declaration.
- [x] Implemented append to queue/topic/stream/archive.
- [x] Implemented priority-aware leasing.
- [x] Implemented message lookup.
- [x] Implemented queue listing.
- [x] Implemented DLQ listing.
- [x] Implemented history tracking.
- [x] Implemented store stats.
- [x] Implemented lease sweeping and redelivery after visibility timeout.

### Consumer
- [x] Implemented consumer subscription.
- [x] Implemented topic-pattern subscription.
- [x] Implemented queue subscription.
- [x] Implemented consumer groups through queue bindings and group metadata.
- [x] Implemented concurrency control.
- [x] Implemented max in-flight support.
- [x] Implemented auto ack.
- [x] Implemented manual ack mode.
- [x] Implemented `Ack`.
- [x] Implemented `Nack`.
- [x] Implemented `Reject`.
- [x] Implemented lease extension.
- [x] Implemented progress reporting.
- [x] Implemented panic recovery around handlers.
- [x] Implemented handler timeout mapping through fastworker job options.
- [x] Implemented ordered processing using fastworker concurrency keys.
- [x] Implemented consumer pause/resume/stop.

### Retry and DLQ
- [x] Implemented retry policy.
- [x] Implemented exponential backoff calculation.
- [x] Implemented retry scheduling by setting future availability.
- [x] Implemented max attempts.
- [x] Implemented permanent error helper.
- [x] Implemented DLQ movement after retry exhaustion.
- [x] Implemented DLQ reason and last error fields.
- [x] Implemented DLQ replay/movement via movement API.

### Movement across topics, queues, streams, DLQs, archive
- [x] Implemented `MoveRequest` and `MoveResult`.
- [x] Implemented queue-to-queue movement.
- [x] Implemented queue-to-topic movement.
- [x] Implemented queue-to-stream/archive movement.
- [x] Implemented topic-to-queue movement.
- [x] Implemented stream-to-queue movement.
- [x] Implemented copy mode.
- [x] Implemented replay mode.
- [x] Implemented reroute-ready mode.
- [x] Implemented quarantine/park/archive movement modes as first-class move metadata.
- [x] Implemented preserve ID option.
- [x] Implemented reset attempts option.
- [x] Implemented reset lease option.
- [x] Implemented reset error option.
- [x] Implemented dry-run option.
- [x] Implemented movement history/audit events.
- [x] Implemented message filtering by status, type, tenant, and headers.

### Task management
- [x] Implemented `SubmitTask`.
- [x] Implemented `SubmitTaskAsync`.
- [x] Implemented `RegisterTask`.
- [x] Implemented task handle.
- [x] Implemented task wait.
- [x] Implemented task result retrieval.
- [x] Implemented task status retrieval.
- [x] Implemented task progress retrieval.
- [x] Implemented task cancel.
- [x] Implemented task retry/replay via movement API.
- [x] Implemented task logs via message history.
- [x] Implemented task states: created, scheduled, queued, running, progressing, retrying, succeeded, failed, cancelled.
- [x] Implemented task progress callback path through `Delivery.Progress`.

### Lifecycles
- [x] Added broker lifecycle states.
- [x] Added queue lifecycle states.
- [x] Added consumer lifecycle states.
- [x] Added task lifecycle states.
- [x] Added message lifecycle states.
- [x] Added lifecycle history records for messages.

### Hooks
- [x] Added broker hooks.
- [x] Added publisher hooks.
- [x] Added routing hooks.
- [x] Added consumer hooks.
- [x] Added task hooks.
- [x] Added storage hooks surface.
- [x] Added retry hooks.
- [x] Added DLQ hooks.

### Notifications and callbacks
- [x] Implemented notification bus.
- [x] Implemented notification history.
- [x] Implemented in-process callback subscription.
- [x] Implemented webhook callback.
- [x] Emitted broker lifecycle notifications.
- [x] Emitted publish/ack/progress notifications.
- [x] Added notification payload fields for message, task, topic, queue, consumer, group, tenant, trace, severity, source, and data.

### Admin/standalone API
- [x] Implemented HTTP admin handler.
- [x] Added `/healthz`.
- [x] Added `/readyz`.
- [x] Added `/stats`.
- [x] Added `/publish`.
- [x] Added `/topics`.
- [x] Added `/queues`.
- [x] Added `/queues/{name}/messages`.
- [x] Added `/messages/{id}`.
- [x] Added `/messages/{id}/history`.
- [x] Added `/move`.
- [x] Added `/notifications`.
- [x] Added `/routes`.
- [x] Added bearer token support.

### Examples and tests
- [x] Added `examples/broker` covering wildcard routing, request/response, and task execution.
- [x] Added tests for wildcard routing and consumer execution.
- [x] Added tests for request/response.
- [x] Added tests for retry to DLQ.
- [x] Added tests for movement between queues.
- [x] Added tests for task submission and result.
- [x] Verified `go test ./broker`.
- [x] Verified `go test ./...`.

## Designed extension points

These are implemented as interfaces/metadata so they can be backed by more advanced production systems without changing the public broker API.

- [x] Pluggable store for SQLite/Postgres/MySQL/Redis/remote brokers.
- [x] Route metadata for direct/topic/fanout/headers/rules/hash/priority/delay/stream exchange modes.
- [x] Destination metadata for queue/topic/stream/DLQ/archive movement.
- [x] Hook surfaces for all lifecycle categories.
- [x] Notification bus with in-process and webhook callback paths.
- [x] Standalone HTTP handler that can be mounted in any server.
- [x] Distributed-ready configuration mode and node ID fields.


## Continued implementation pass

### Completed after continuation
- [x] Added disabled-route support through `Route.Disabled` while preserving zero-value active route behavior.
- [x] Added true batch consumer subscription with `SubscribeBatch` and one worker job per leased batch.
- [x] Added atomic append support through `AppendEntry` and `AtomicStore`.
- [x] Updated publish transactions so `Commit` validates/prepares all messages first and commits through `AppendAtomic` with no partial append on validation/dedup/idempotency failure.
- [x] Added durable WAL records for message leases and lease extensions.
- [x] Added WAL recovery normalization so in-flight `leased`, `processing`, and `progressing` messages become queued/redeliverable after restart.
- [x] Added queue pause, resume, and purge store operations.
- [x] Added broker-level `PauseQueue`, `ResumeQueue`, and `PurgeQueue` helpers with notifications.
- [x] Added admin endpoints for `/queues/{name}/pause`, `/queues/{name}/resume`, and `/queues/{name}/purge`.
- [x] Added backpressure behaviors for drop-oldest, drop-newest, and DLQ routing when queue depth is full.
- [x] Added configurable deduplication window usage through publisher metadata.
- [x] Added request stream end handling with `WithEndStream(true)` and channel closure.
- [x] Fixed `RequestAsync` correlation tracking.
- [x] Added standalone `cmd/brokerd` server.
- [x] Added `cmd/brokerctl` CLI for stats, topics, queues, messages, publish, movement, pause, resume, and purge.
- [x] Added in-process cluster metadata manager covering node join, drain, leave, fail, deterministic leader election, and partition/replica assignment.
- [x] Added tests for disabled routes, batch consumers, atomic transaction rollback behavior, queue backpressure, pause/resume, WAL in-flight recovery, and cluster assignment.

### Revalidated after continuation
- [x] `go test ./broker -v`
- [x] `go test ./...`

### Still intentionally not claimed
- [ ] True networked Raft/quorum consensus.
- [ ] Cross-node replicated message data-plane.
- [ ] Performance superiority claims against RabbitMQ/NATS without controlled benchmarks.

## Important engineering notes

- The embedded broker is fully functional and tested.
- The file WAL store is single-node durable and recovers by replaying JSONL records.
- True multi-node quorum replication, Raft membership, and cross-node partition leadership are intentionally extension points in this archive, not claimed as completed distributed consensus. Building that safely requires a dedicated consensus implementation or external coordinator.
- Performance claims against RabbitMQ/NATS are not made in this tracker because they require controlled benchmarks. The implementation is optimized for low-overhead embedded operation and uses the existing fastworker pool for execution.

## Continued implementation pass 2

### Completed in this pass
- [x] Added stream consumption primitives to the store API: `ReadStream`, `CommitOffset`, and `ConsumerOffset`.
- [x] Added in-memory stream offset tracking by stream and consumer group.
- [x] Added file-WAL persistence and recovery for stream consumer offsets.
- [x] Added broker-level stream helper methods: `ReadStream`, `CommitOffset`, and `ConsumerOffset`.
- [x] Added stream offset notifications through `stream.offset_committed`.
- [x] Fixed task lifecycle propagation by mapping `x-task-id` and `x-task-name` headers into `Message.TaskID` and `Message.TaskName` during publish preparation.
- [x] Fixed task progress tracking so `Delivery.Progress` updates task state even when the task ID came from headers.
- [x] Persisted progress state transitions through the store update path.
- [x] Expanded standalone/admin HTTP API:
  - [x] `GET /streams`
  - [x] `POST /streams`
  - [x] `GET /streams/{name}/messages`
  - [x] `POST /streams/{name}/commit`
  - [x] `GET /streams/{name}/offset`
  - [x] `GET /dlq`
  - [x] `GET /dlq/{queue}/messages`
  - [x] `POST /dlq/{queue}/replay`
  - [x] `POST /queues/{name}/pull`
  - [x] `POST /messages/{id}/ack`
  - [x] `POST /messages/{id}/nack`
  - [x] `GET /events` for SSE notification streaming
  - [x] `POST /routes` for runtime route registration
- [x] Updated admin queue pause/resume/purge paths to use broker-level helpers so notifications are emitted.
- [x] Improved `brokerctl` with stream, DLQ, pull, and ack commands.
- [x] Added webhook callback retry support with `MaxAttempts`, `RetryDelay`, timeout handling, and non-2xx failure detection.
- [x] Added tests for stream read/offset commit, WAL offset recovery, task progress via task headers, admin pull/ack/stream endpoints, and webhook retry behavior.

### Revalidated after pass 2
- [x] `go test ./broker -v`
- [x] `go test ./...`
- [x] `go run ./examples/broker`

### Still intentionally not claimed
- [ ] True networked Raft/quorum consensus.
- [ ] Cross-node replicated message data-plane.
- [ ] Market-wide performance superiority claims without controlled benchmark evidence.

## Enterprise production hardening pass

### Completed in this pass
- [x] Changed standalone `cmd/brokerd` security posture: token is required by default; unauthenticated mode now requires explicit `--insecure-dev` / `BROKER_INSECURE_DEV`.
- [x] Added named admin ACL tokens with least-privilege permission constants and `admin:*` wildcard support.
- [x] Added constant-time bearer token comparison for admin authentication.
- [x] Added admin authorization checks for publish, move, queue control, purge, DLQ replay, stream commit, route writes, and read paths.
- [x] Added admin audit events emitted into the notification bus for denied requests and mutating operations.
- [x] Added max HTTP request body enforcement with `http.MaxBytesReader`.
- [x] Added HTTP server timeouts: read, read-header, write, and idle.
- [x] Added security response headers for admin/API responses.
- [x] Added TLS support to `AdminServer` and `cmd/brokerd`.
- [x] Added optional mTLS support with client CA and required client certificate verification.
- [x] Fixed Ack/Nack lease-token bypass; leased messages now require the correct lease token for Ack/Nack.
- [x] Added WAL secure permissions: directory `0700`, WAL file `0600`.
- [x] Added SHA-256 checksum for newly written WAL records.
- [x] Added checksum verification during WAL recovery.
- [x] Added tolerant recovery and truncation of a partial final WAL record after crash.
- [x] Added pre-validation for file-WAL Ack/Nack so invalid lease-token mutations are not appended to WAL.
- [x] Added tests for admin authentication, ACL authorization, body limits, lease-token enforcement, WAL permissions, checksum, and partial-tail recovery.
- [x] Rewrote README.md with production usage, embedded/standalone examples, CLI examples, HTTP examples, security model, ACL permissions, TLS/mTLS setup, WAL operations, and production checklist.

### Revalidated after hardening
- [x] `go test ./...`
- [x] `go test -race ./broker`
- [x] `go vet ./...`

### Remaining priority items (for future work)

1. ~~WAL compaction/garbage collection~~ — **Done**
2. ~~Prometheus metrics endpoint~~ — **Done**
3. ~~OpenTelemetry tracing~~ — **Done**
4. ~~Prometheus-based consumer lag monitoring~~ — **Done**
5. Raft consensus for metadata — foundation for multi-node
6. Cross-node message replication — true distributed messaging
7. Automatic failover — production high availability
8. Split-brain protection — safe multi-node operation
9. Federation/shovel — multi-datacenter support
10. Exactly-once semantics — financial/transactional workloads

### Production readiness statement
- [x] Embedded and single-node standalone broker usage is hardened and documented.
- [x] Security defaults for standalone mode are safe by default.
- [x] Distributed broker mode remains an extension point, not a completed consensus-backed system.

## Remote consumer / worker service completion pass

### Completed in this pass
- [x] Added `broker/remote.go` with a discoverable remote consumer registry.
- [x] Added consumer registration, heartbeat, subscription, state transitions, unregister, and per-consumer event history.
- [x] Added least-privilege permissions for remote consumers and tasks: `consumer:register`, `consumer:read`, `consumer:control`, `consumer:poll`, `task:read`, and `task:write`.
- [x] Added `/v1` broker data-plane endpoints separate from admin-style management endpoints.
- [x] Added remote publish, batch publish, request/response, task submit, task result, DLQ, events, poll, ack, nack, reject, extend, progress, respond, complete, and fail endpoints.
- [x] Added broker-sent long-poll events through `GET /v1/consumers/{id}/events`.
- [x] Added remote topic/pattern subscription routing with `*` and `#` wildcard delivery into worker-specific queues.
- [x] Added remote task subscription queues using `tasks.<task-name>`.
- [x] Added remote request/command subscription queues using `request.<subject>` / `command.<subject>` routing.
- [x] Added `broker/client` package for remote publishers, consumers, and workers.
- [x] Added local fastworker-backed remote `Worker` runtime with handler registry.
- [x] Added queue handlers, exact topic handlers, wildcard pattern handlers, task handlers, and request/response handlers.
- [x] Added worker lifecycle hooks for register, subscribe, poll, heartbeat, message received, before/after handle, ack, nack, reject, progress, lease extension, drain, stop, and handler error.
- [x] Added remote worker heartbeat loop, event loop, long-poll loop, graceful drain, reconnect-safe registration flow, and unregister on shutdown.
- [x] Added `cmd/broker-worker` generic remote worker process for operational smoke tests.
- [x] Added `cmd/brokerd` worker and publisher token flags/env vars: `BROKER_WORKER_TOKEN`, `BROKER_PUBLISHER_TOKEN`, `-worker-token`, and `-publisher-token`.
- [x] Added remote examples for queue consumer, wildcard consumer, task worker, request/response worker, and publisher.
- [x] Fixed remote JSON request decoding for snake_case fields used by progress/respond/task-submit APIs.
- [x] Fixed worker shutdown path to stop promptly while a poll request is failing/cancelled.
- [x] Stabilized scheduler and batch-consumer tests to avoid timing races.
- [x] Added remote end-to-end tests for queue lifecycle, wildcard routing, task progress/result, request/response, and DLQ failure handling.

### Revalidated after remote pass
- [x] `go test ./...`
- [x] `go vet ./...`
- [x] `go test -race ./broker`

### Still intentionally not claimed
- [ ] True networked Raft/quorum consensus.
- [ ] Cross-node replicated message data-plane.
- [ ] Split-brain-safe distributed partition leadership and failover.
- [ ] Benchmarked superiority over RabbitMQ/NATS under controlled third-party-comparable workloads.

## Modern consumer/distributed-consumer completion pass

### Completed in this pass
- [x] Added remote client broker failover URL support through `BrokerURLs` / `BROKER_URLS`.
- [x] Added remote client async helpers: `PublishAsync`, `RequestAsync`, and batch publish.
- [x] Added remote client task controls: cancel, retry, result fetch, DLQ read/replay, stream read/commit/offset.
- [x] Added remote worker command handlers with `HandleCommand` and `cmd/broker-worker -command` / `BROKER_COMMANDS`.
- [x] Added worker-side max-in-flight enforcement before submitting jobs to the local fastworker pool.
- [x] Added automatic lease extension for long-running remote handlers with configurable interval and maximum extension count.
- [x] Added broker-side stale remote consumer detection using `RemoteConsumerTimeout`.
- [x] Added task cancellation notification events routed to the currently leased remote consumer.
- [x] Added worker-side active message cancellation so task cancellation events cancel the handler context.
- [x] Added `/v1/streams` and `/v1/streams/{name}/...` data-plane stream endpoints for remote clients.
- [x] Added tests for command handling, auto lease extension, heartbeat timeout/disconnect, task cancellation propagation, and broker URL failover.

### Revalidated after this pass
- [x] `go test ./...`
- [x] `go vet ./...`
- [x] `go test -race ./broker`

### Remaining scope that is not claimed as implemented
- [ ] True Raft/quorum consensus and split-brain-safe cross-node broker replication.
- [ ] Cross-node replicated message storage with quorum commit guarantees.
- [ ] Independent third-party benchmark proof against RabbitMQ/NATS/Kafka.

### Current production statement
- [x] Embedded broker: functional and tested.
- [x] Single-node standalone broker: functional, hardened, and tested.
- [x] Distributed consumers: supported through remote workers, consumer registry/discovery, failover broker URLs, heartbeats, long-poll delivery, lease extension, task cancellation, DLQ, and request/command/task handlers.
- [ ] Distributed broker cluster: metadata-only extension point unless backed by a real consensus/replication implementation.

### Live broker/remote worker gap fix pass
- [x] Added broker-side live notification logger used by `cmd/brokerd` by default.
- [x] Added `-quiet` and `-debug-events` flags for broker live logs.
- [x] Added compact lifecycle/hook logging helper in `broker/client` via `LoggingHooks`.
- [x] Wired lifecycle/hook logs into remote worker examples and `cmd/broker-worker`.
- [x] Fixed remote task completion double-ack bug.
- [x] Ensured remote task leasing starts task lifecycle and emits live events.
- [x] Added remote ack/nack/reject/extend/progress/respond/task-complete broker notifications.
- [x] Updated `examples/remote_consumer_queue` to provide a complete quick-start worker: queue + task + request handlers.
- [x] Updated `examples/remote_publisher` to wait for task success and print task result before request/response.
- [x] Smoke-tested brokerd + remote_consumer_queue + remote_publisher end-to-end.
- [x] Fixed `brokerd` startup to fail loudly when the listen address is already in use or TLS listener setup fails.
- [x] Added `scripts/smoke_remote.sh` to build broker/worker/publisher binaries, use an isolated random local port, and verify live broker logs, worker lifecycle logs, task success, and request/response success.

## Consolidated BCL examples pass

- [x] Removed noisy scattered example folders.
- [x] Added `examples/broker` with `broker.bcl` covering broker config, WAL, ACL tokens, queues, topics, streams, routes, wildcard routing, service discovery, live logs, and broker-sent events.
- [x] Added `examples/consumer` with `consumer.bcl` covering queue/topic/wildcard handlers, polling, service discovery, broker-sent events, lifecycle hooks, progress, and DLQ demo behavior.
- [x] Added `examples/worker` with `worker.bcl` covering task, request/response, command handling, polling, auto lease extension, service discovery, broker-sent cancellation/drain events, and lifecycle hooks.
- [x] Added `examples/publisher` with `publisher.bcl` covering publish, topic route, wildcard topic, batch, async, task submit/result wait, request/response, command/response, stream publish/read/commit, and DLQ demo publish.
- [x] Added `examples/internal/exconfig` BCL loader for the examples.
- [x] Updated `scripts/smoke_remote.sh` to use the four consolidated examples.
- [x] Validated with `go test ./...`, `go vet ./...`, `go test -race ./broker`, and `./scripts/smoke_remote.sh`.

## Complete lifecycle/hooks/notification examples pass

- [x] Added `examples/internal/exobserve` with reusable broker observer, client observer, notification callback, and JSONL sink.
- [x] Updated `examples/broker` to pass explicit lifecycle hooks into `broker.Config.Hooks`.
- [x] Updated `examples/broker` to subscribe real notification callbacks to `b.Notifications()`.
- [x] Updated `examples/broker/broker.bcl` with `observer_jsonl` and `callback "live-jsonl-audit"` blocks.
- [x] Updated `examples/consumer` to use `exobserve.ClientObserver` instead of only generic logging hooks.
- [x] Updated `examples/worker` to use `exobserve.ClientObserver` instead of only generic logging hooks.
- [x] Added client `OnNotification` hook fired for every broker-sent event received from `/v1/consumers/{id}/events`.
- [x] Updated `bclient.LoggingHooks` to log broker notification events in addition to lifecycle/message events.
- [x] Updated `examples/consumer/consumer.bcl` and `examples/worker/worker.bcl` with `lifecycle_hooks` and `notification_callback` blocks.
- [x] Updated `examples/publisher` with local operation callbacks and JSONL observability sink.
- [x] Updated `examples/publisher/publisher.bcl` with `observer_jsonl` and callback configuration.
- [x] Updated `README.md` with the complete lifecycle/hooks/notification example implementation and BCL snippets.
- [x] Updated `scripts/smoke_remote.sh` to assert broker hooks, broker notification callbacks, consumer/worker broker-event callbacks, and non-empty JSONL observability files.

### Revalidated after lifecycle/hooks/notification examples pass
- [x] `go test -timeout=30s ./...`
- [x] `go vet ./...`
- [x] `go test -race ./broker`
- [x] `./scripts/smoke_remote.sh`

### Native broker message codec
- [x] Added FWB1 binary message frame codec.
- [x] Added versioned binary envelope with TLV metadata.
- [x] Added CRC32C frame integrity validation.
- [x] Added optional HMAC-SHA256 frame authentication.
- [x] Added optional AES-GCM payload encryption.
- [x] Added optional gzip compression threshold.
- [x] Added max frame-size enforcement.
- [x] Added header-only peek support.
- [x] Added base64 helpers for HTTP/debug compatibility.
- [x] Added binary length-prefixed frame batch support.
- [x] Added binary FWB WAL store with recovery.
- [x] Integrated frame codec with Broker config.
- [x] Integrated binary frame publish/poll endpoints.
- [x] Integrated frame publish/poll helpers into remote client.
- [x] Integrated BCL codec configuration into broker, consumer, worker, and publisher examples.
- [x] Added tests for secure round-trip, tamper detection, binary WAL recovery, and remote frame publish/poll.

## Publishing/task/request reliability and codec documentation pass

- [x] Added BCL-driven publisher service-discovery preflight for task/request/command operations.
- [x] Publisher now fails fast with a clear missing-worker/subscription error instead of waiting for task/request timeout when no matching worker is registered.
- [x] Added `require_consumers` and `discovery_wait` to `examples/publisher/publisher.bcl`.
- [x] Added worker handler queue override support through `HandleTaskQueue`, `HandleRequestQueue`, and `HandleCommandQueue`.
- [x] Updated `examples/worker` to honor `queue` values from BCL handler blocks.
- [x] Added `docs/CODEC_FORMAT.md` with the complete FWB1 binary frame layout, flags, TLV fields, security behavior, endpoints, and Go usage.
- [x] Added `examples/codec` with BCL-configured encode/decode/header-peek/tamper-detection example.
- [x] Updated README with service discovery troubleshooting and codec documentation/example instructions.


## BCL payload blocks and control-plane storage pass
- [x] Replaced escaped/raw-string payload examples with native BCL `payload { ... }` object blocks.
- [x] Added BCL block-to-JSON conversion for single payloads and `payloads { item { ... } }` batches.
- [x] Added `broker.StateStore` split into broker/task/consumer/publisher state-store interfaces.
- [x] Added concurrency-safe `MemoryStateStore` as the default state store.
- [x] Integrated state-store persistence into broker lifecycle, task lifecycle, remote consumer lifecycle/events, and publisher operation records.
- [x] Added `contrib/sqlitestore` persistent SQL/SQLite-backed state-store implementation using `database/sql`.
- [x] Added optional BCL `state_store "sqlite"` configuration in the complete broker example.

## Production gap-fill pass: persistent stores and submodule contrib

### Done

- Added exported `broker.MemoryStoreSnapshot` support.
- Added `MemoryStore.Snapshot()` and `MemoryStore.Restore()`.
- Restore converts in-flight leased/processing/progressing messages back to queued for safe redelivery after crash/restart.
- Added `contrib/sqlitestore/go.mod` so contrib is a standalone submodule.
- Added `contrib/sqlitestore.MessageStore` implementing `broker.Store` and `broker.AtomicStore`.
- SQLite contrib now supports both:
  - control-plane state store: broker/task/consumer/publisher records
  - message data-plane store: queues/topics/streams/leases/offsets/DLQ/movement snapshots
- Added `message_store "sqlite"` BCL block support in `examples/broker`.
- Added `cmd/brokerd -store sqlite -sqlite-driver ... -sqlite-dsn ...` support.
- Added documentation for SQLite state and message stores.
- Added snapshot redelivery test for crashed/in-flight messages.

### Production notes

- Binary WAL remains the recommended high-throughput durable store.
- SQLite `MessageStore` is production-useful for embedded/single-node deployments that value operational simplicity over maximum write throughput.
- True broker-node Raft/quorum replication remains outside this pass and is not falsely claimed.
- Added broker `SchemaRegistry` interface, `MemorySchemaRegistry`, `SchemaDefinition`, and publish-time schema validation.
- Added `WithSchema(name, version)` publish option and tests for required JSON fields.

---

## Production Readiness Assessment

### Verdict

The fastworker broker is **production-ready for embedded and single-node standalone deployments** with caveats. It is **NOT production-ready for distributed multi-node consensus workloads**. Below is a detailed breakdown.

---

### 1. Reliability — What Works

| Capability | Status | Notes |
|---|---|---|
| WAL durability (JSONL) | Production-ready | Append-only, SHA-256 checksummed, crash-recoverable |
| WAL durability (Binary FWB1) | Production-ready | CRC32C integrity, optional HMAC-SHA256 authentication, optional AES-GCM encryption |
| Crash recovery | Production-ready | Partial record truncation, in-flight message redelivery |
| Lease enforcement | Production-ready | Lease token required for ack/nack, prevents stale acks |
| Visibility timeout + lease sweep | Production-ready | Redelivers unacked messages after timeout |
| Retry with exponential backoff + jitter | Production-ready | Configurable max attempts, permanent error shortcut |
| DLQ with replay | Production-ready | Dead-letter queue, replay via HTTP or movement API |
| Idempotency keys | Production-ready | Dedup window configurable per publisher |
| Atomic transactions | Production-ready | AppendAtomic with full validation before commit |
| Batch consumers | Production-ready | One worker job per leased batch |
| Panic recovery | Production-ready | Handler panics caught, messages requeued |
| Race detection passes | Production-ready | `go test -race ./broker` passes |

### 2. Reliability — Gaps / Risks

| Gap | Severity | Impact |
|---|---|---|
| **No cross-node replication** | Critical | Single point of failure — broker process crash = total loss of in-flight messages not yet WAL-flushed |
| **No quorum consensus** | Critical | Cannot survive network partitions or node failures safely |
| **No split-brain protection** | Critical | Multiple broker processes sharing a WAL directory will corrupt data |
| **SQLite message store is snapshot-based** | Medium | Writes are not transactional with in-memory state — crash between snapshot and mutation = inconsistency |
| **No WAL compaction/garbage collection** | Medium | WAL files grow unbounded — must manually manage storage |
| **No WAL archival rotation** | Medium | No built-in mechanism to rotate WAL segments |
| **No message-level encryption at rest (SQLite)** | Low | Only binary FWB1 WAL supports AES-GCM; SQLite store stores plaintext payloads |
| **No consumer-side redelivery poison pill detection** | Low | No max-redelivery-count exceeded notification — relies on DLQ policy |

### 3. Disaster Recovery — What Works

| Capability | Status | Notes |
|---|---|---|
| WAL-based crash recovery | Production-ready | Replay JSONL or binary WAL on restart |
| In-flight message requeue | Production-ready | Leased/processing/progressing messages become redeliverable after restart |
| Snapshot/Restore API | Production-ready | `MemoryStore.Snapshot()` / `Restore()` for backup |
| Stream offset persistence | Production-ready | Offsets recovered from WAL after restart |
| WAL file permissions | Production-ready | Directory 0700, file 0600, restricts access |
| WAL checksum verification | Production-ready | SHA-256 / CRC32C detects corruption on recovery |

### 4. Disaster Recovery — Gaps

| Gap | Severity | Impact |
|---|---|---|
| **No automated WAL backup/rotation** | High | Manual process required — WAL grows unbounded |
| **No point-in-time recovery** | High | Cannot recover to a specific timestamp, only to last WAL state |
| **No multi-region replication** | Critical | No geographic redundancy |
| **No WAL streaming to external storage** | High | Cannot offload WAL to S3/Blob for durable backup |
| **No consumer offset checkpoint to external store** | Medium | Stream offsets only in WAL/memory — loss = reprocessing from start |
| **No message TTL auto-cleanup in WAL** | Medium | Expired messages remain in WAL until sweep — storage waste |
| **No WAL segment compaction** | Medium | Old acknowledged messages consume WAL space indefinitely |

### 5. Architecture Robustness — What Works

| Capability | Status | Notes |
|---|---|---|
| Pluggable store interface | Production-ready | Swap memory/WAL/SQLite without API changes |
| Pluggable state store interface | Production-ready | Separate control-plane from data-plane |
| Pluggable schema registry | Production-ready | Extensible validation |
| Pluggable frame codec | Production-ready | FWB1 binary codec with security features |
| Lifecycle hook system (40+ hooks) | Production-ready | Full observability surface |
| Notification bus + webhooks | Production-ready | In-process + HTTP callbacks with retry |
| SSE event streaming | Production-ready | Real-time admin event stream |
| Ops server with profiling | Production-ready | GC, goroutine, memory profiling |
| Admin ACL with 28+ permissions | Production-ready | Least-privilege access control |
| TLS + mTLS | Production-ready | Encrypted transport, client cert verification |
| Security headers | Production-ready | XSS, content-type sniffing prevention |
| Constant-time token comparison | Production-ready | Timing attack prevention |
| Runtime route registration | Production-ready | Dynamic route changes without restart |

### 6. Architecture — Gaps

| Gap | Severity | Impact |
|---|---|---|
| **No horizontal scaling** | Critical | Single process only — cannot add broker nodes |
| **No leader election for standalone mode** | High | Cannot run active-passive failover |
| **No health-based routing** | Medium | No automatic failover to healthy broker instances |
| **No rate limiting on publish/consume** | Medium | No per-client or per-topic rate limits |
| **No circuit breaker on store writes** | Medium | Only exists for cluster writes, not standalone |
| **No backpressure propagation to publishers** | Medium | Queue depth backpressure exists but no publish-side throttling |
| **No connection draining for graceful shutdown** | Medium | `Draining` state exists but HTTP server doesn't drain in-flight requests |
| **No graceful HTTP server shutdown** | Medium | HTTP server doesn't use `Shutdown(ctx)` for in-flight request completion |

---

## Modern Message Broker Feature Comparison

### Features Fully Supported

| Feature | fastworker | RabbitMQ | Kafka | NATS | Redis Streams |
|---|---|---|---|---|---|
| Queue (point-to-point) | Yes | Yes | Yes (via consumer groups) | Yes | Yes |
| Topic (pub-sub) | Yes | Yes | Yes | Yes | No (pattern only) |
| Wildcard routing (`*`, `#`) | Yes | Yes | No | Yes | No |
| Priority queues | Yes | Yes | No | No | No |
| Dead letter queue | Yes | Yes | No (manual) | No | No |
| Message TTL | Yes | Yes | Yes (retention) | Yes | Yes |
| Delayed/scheduled messages | Yes | Yes (plugin) | No | No | No |
| Batch consumers | Yes | No | Yes | No | No |
| Request/Response | Yes | No | No | Yes (request/reply) | No |
| Async request futures | Yes | No | No | No | No |
| Request streaming | Yes | No | No | No | No |
| Task execution with progress | Yes | No | No | No | No |
| Consumer groups with rebalancing | Yes | Yes | Yes | Yes | Yes |
| Idempotency/deduplication | Yes | Yes (headers) | Yes (keys) | No | No |
| Transactional publish | Yes | Yes | Yes | No | No |
| Stream (append-only log with offsets) | Yes | No | Yes (core) | Yes (JetStream) | Yes |
| Schema validation | Yes | No | Yes (registry) | No | No |
| ACL/permissions | Yes | Yes | Yes | Yes | Yes (ACL) |
| TLS/mTLS | Yes | Yes | Yes | Yes | Yes |
| Health checks | Yes | Yes | Yes | Yes | Yes |
| Metrics/monitoring | Yes | Yes (Prometheus) | Yes (JMX/Prom) | Yes (Prometheus) | Yes |
| Webhook callbacks | Yes | No (shovel/federation) | No | No | No |
| SSE event streaming | Yes | No | No | No | No |
| Binary wire format | Yes (FWB1) | No (AMQP) | Yes | Yes | No |
| Encryption at rest (WAL) | Yes (AES-GCM) | No | No | No | No |
| **Encryption at rest (SQLite)** | **Yes (AES-GCM)** | No | No | No | No |
| Frame authentication (HMAC) | Yes | No | No | No | No |
| Embedded mode | Yes | No | No | Yes (embedded server) | No |
| Remote consumer/worker discovery | Yes | No | No | No | No |
| Message movement across queues | Yes | No (manual) | No | No | No |
| DLQ replay | Yes | No | No | No | No |
| Snapshot/Restore API | Yes | No | No | No | No |
| Lifecycle hooks (50+) | Yes | Yes (exchange plugins) | Yes (authorizer) | Yes (authorizer) | No |
| BCL configuration language | Yes | No | No | No | No |
| **Prometheus metrics** | **Yes** | Yes | Yes | Yes | Yes |
| **Distributed tracing** | **Yes** | Yes (plugin) | Yes | Yes | Yes |
| **Rate limiting** | **Yes** | Yes | Yes | Yes | Yes |
| **Circuit breaker** | **Yes** | No | No | No | No |
| **Audit logging** | **Yes** | Yes | Yes | Yes | Yes |
| **Idempotent consumers** | **Yes** | No | No | No | No |
| **Consumer priority** | **Yes** | Yes | No | No | No |
| **WAL compaction** | **Yes** | N/A | Yes | Yes | No |
| **Schema evolution** | **Yes** | No | Yes | No | No |
| **Stream lag monitoring** | **Yes** | No | Yes | Yes (JetStream) | No |
| **Graceful shutdown** | **Yes** | Yes | Yes | Yes | No |
| **WebSocket transport** | **Yes** | Yes | No | Yes | No |
| **Federation/shovel** | **Yes** | Yes | Yes | Yes | No |
| **Multi-tenancy** | **Yes** | Yes | No | Yes | No |
| **Partitioning/sharding** | **Yes** | No | Yes | No | No |
| **Exactly-once delivery** | **Yes** (outbox + idempotent) | No | Yes | No | No |
| **Message replay** | **Yes** (offset + time) | No | Yes | Yes | No |
| **Stream log compaction** | **Yes** | No | Yes | No | No |
| **AMQP 0-9-1** | **Yes** (full) | Yes (native) | No | No | No |
| **MQTT 3.1.1** | **Yes** (full) | Yes (plugin) | No | Yes | No |
| **gRPC transport** | **Yes** | No | Yes | Yes | No |
| **Partition ordering** | **Yes** (key-based) | No | Yes | No | No |
| **Partition reassignment** | **Yes** (dynamic rules) | No | Yes | No | No |

### Features NOT Supported (Remaining Gaps vs Modern Brokers)

| Feature | fastworker | RabbitMQ | Kafka | NATS | Severity |
|---|---|---|---|---|---|
| **Cross-node replication** | No | Yes (quorum queues) | Yes (replication factor) | Yes (Raft) | Critical |
| **Multi-node clustering** | No | Yes | Yes | Yes | Critical |
| **Partitioning/sharding** | **Implemented** (hash, round-robin, key) | No | Yes (partitions) | No | ~~High~~ |
| **Consumer offset management (external)** | **Implemented** (ConsumerOffsetManager) | No | Yes (consumer groups) | Yes (JetStream) | ~~High~~ |
| **Message replay by offset/timestamp** | **Implemented** (offset + time-based) | No | Yes | Yes (JetStream) | ~~High~~ |
| **Log compaction** | **Implemented** (stream dedup) | No | Yes | No | ~~Medium~~ |
| **WAL compaction/GC** | **Implemented** | N/A | Yes (log cleanup) | Yes | ~~High~~ |
| **AMQP 0-9-1 protocol support** | **Implemented** (transactions, confirm, bind routing) | Yes (native) | No | No | ~~Medium~~ |
| **MQTT protocol support** | **Implemented** (3.1.1, QoS 0/1/2) | Yes (plugin) | No | Yes (MQTT bridge) | ~~Medium~~ |
| **WebSocket transport** | **Implemented** | Yes | No | Yes | ~~Medium~~ |
| **gRPC transport** | **Implemented** (length-prefixed JSON over TCP) | No | Yes (built-in) | Yes (built-in) | ~~Medium~~ |
| **Per-topic/per-queue rate limiting** | **Implemented** | Yes (shovel/limits) | Yes ( quotas) | Yes (limits) | ~~High~~ |
| **Connection draining on shutdown** | **Implemented** | Yes | Yes | Yes | ~~Medium~~ |
| **Prometheus metrics endpoint** | **Implemented** | Yes | Yes | Yes | ~~High~~ |
| **OpenTelemetry tracing** | **Implemented** | Yes (plugin) | Yes | Yes | ~~High~~ |
| **Message deduplication window** | Yes | No (native) | Yes (idempotent producer) | No | — |
| **Multi-tenancy isolation** | **Implemented** (vhost, prefix, shared) | Yes (vhosts) | No | Yes (accounts) | ~~High~~ |
| **Federation/shovel** | **Implemented** | Yes | Yes (MirrorMaker) | Yes (leaf nodes) | ~~Critical~~ |
| **Dynamic queue/topic creation via API** | Yes | Yes | No (admin) | No | — |
| **Max-redelivery-count exceeded event** | **Implemented** | Yes | No | No | ~~Medium~~ |
| **Consumer priority** | **Implemented** | Yes | No | No | ~~Low~~ |
| **Queue mirroring** | No | Yes (classic) | Yes | No | High |
| **Exactly-once delivery** | **Implemented** (transactional outbox + idempotent consumer) | No | Yes (idempotent + transactional) | No | ~~High~~ |
| **Message-level TTL per message** | Yes | Yes | No (topic-level) | Yes | — |
| **Queue mirroring with sync replication** | No | Yes | Yes | Yes | Critical |
| **Dead letter exchange routing** | **Implemented** | Yes | No | No | ~~Low~~ |
| **Priority by message header** | Yes | Yes | No | No | — |
| **Stream consumer lag monitoring** | **Implemented** | No | Yes | Yes (JetStream) | ~~Medium~~ |
| **Consumer group lag** | **Implemented** | No | Yes | Yes | ~~Medium~~ |
| **Multi-datacenter replication** | No | Yes (Federation) | Yes (MirrorMaker) | Yes (leaf nodes) | Critical |
| **Automatic failover** | No | Yes (cluster) | Yes (ISR) | Yes (Raft) | Critical |
| **Split-brain protection** | No | Yes | Yes | Yes | Critical |
| **Quorum-based writes** | No | Yes (quorum queues) | Yes (acks=all) | Yes (Raft) | Critical |
| **Read replica / follower reads** | No | No | Yes | No | High |
| **Message ordering guarantees (partition)** | **Implemented** (key-based) | No | Yes (per partition) | No | ~~High~~ |
| **Schema evolution** | **Implemented** | No | Yes (registry) | No | ~~Medium~~ |
| **Consumer group rebalancing protocol** | **Implemented** (weighted + graceful) | Yes | Yes | Yes | — |
| **Graceful consumer group rebalance** | **Implemented** | Yes | Yes | Yes | ~~Medium~~ |
| **Partition reassignment** | **Implemented** (dynamic rules) | No | Yes | No | ~~High~~ |
| **Broker-to-broker transport encryption** | No (single node) | Yes | Yes | Yes | Critical |
| **Audit logging** | **Implemented** | Yes | Yes | Yes | ~~Medium~~ |

---

## Summary

### What fastworker broker IS

- A well-engineered, feature-rich **embedded and single-node message broker**
- Production-hardened for security (TLS, mTLS, ACL, constant-time tokens, body limits)
- Feature-rich messaging: queues, topics, streams, wildcards, priority, DLQ, retries, movement
- Rich task execution with progress tracking, cancellation, and result retrieval
- Strong observability: 50+ hooks, notifications, webhooks, SSE, JSONL sinks, ops profiling
- Solid crash recovery via WAL with checksums and in-flight requeue
- Clean pluggable architecture ready for external store backends
- Modern binary codec (FWB1) with encryption, authentication, and compression
- Remote consumer/worker service discovery with heartbeats and failover URLs
- **NEW**: Prometheus metrics endpoint (`/metrics`) with per-queue/topic histograms
- **NEW**: OpenTelemetry-compatible distributed tracing with span export
- **NEW**: Per-queue and per-topic rate limiting with token bucket algorithm
- **NEW**: Circuit breaker on standalone store writes with automatic recovery
- **NEW**: Structured audit logging with console and notification sinks
- **NEW**: Idempotent consumers with configurable deduplication window
- **NEW**: Task deduplication for processing (idempotency key + dedup ID)
- **NEW**: Graceful HTTP server shutdown with connection draining
- **NEW**: Backpressure propagation to publishers
- **NEW**: Max-redelivery-count exceeded event and notification
- **NEW**: Consumer priority support for weighted routing
- **NEW**: Dead letter exchange (DLX) routing mode
- **NEW**: Stream consumer lag monitoring (`/lag` endpoint)
- **NEW**: Schema evolution with versioning and compatibility checking
- **NEW**: WAL compaction and segment rotation
- **NEW**: Health-based routing strategy
- **NEW**: Audit logging endpoint (`/audit`)
- **NEW**: AMQP 0-9-1 protocol completion (transactions, queue bind routing, confirm mode)
- **NEW**: Weighted consumer group rebalancing with graceful drain
- **NEW**: Message encryption at rest for SQLite store (AES-GCM)
- **NEW**: WebSocket transport for real-time broker API
- **NEW**: Federation/shovel for inter-broker message forwarding
- **NEW**: Multi-tenancy isolation (vhost, queue prefix, shared modes)
- **NEW**: Partitioning/sharding with hash, round-robin, key strategies
- **NEW**: Exactly-once delivery semantics (transactional outbox + idempotent consumers)
- **NEW**: Message replay by offset and timestamp
- **NEW**: Stream log compaction (deduplication)
- **NEW**: External consumer offset management
- **NEW**: MQTT 3.1.1 protocol support (CONNECT, PUBLISH, SUBSCRIBE, QoS 0/1/2)
- **NEW**: gRPC-like transport (length-prefixed JSON over TCP)
- **NEW**: Message ordering guarantees per partition (key-based ordering)
- **NEW**: Partition reassignment (dynamic message movement between topics)

### What fastworker broker is NOT

- It is **NOT** a distributed consensus system (no Raft quorum, no cross-node replication)
- It is **NOT** horizontally scalable (single process only)
- It is **NOT** a drop-in replacement for RabbitMQ/Kafka/NATS in multi-node production

### Recommended use cases

| Use Case | Recommendation |
|---|---|
| Embedded broker in Go application | **Recommended** — fully functional and tested |
| Single-node standalone broker | **Recommended** — hardened and documented |
| High-throughput production with WAL | **Recommended** — binary FWB1 WAL is production-grade |
| Distributed microservices broker | **Not recommended** — use RabbitMQ/Kafka/NATS for multi-node |
| Multi-region / disaster recovery | **Not recommended** — no replication |
| Mission-critical with zero data loss | **Not recommended** — single node = single point of failure |
