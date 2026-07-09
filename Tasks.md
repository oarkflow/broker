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

### Remaining scope that is not falsely claimed
- [ ] True networked Raft/quorum consensus.
- [ ] Cross-node replicated message data-plane.
- [ ] Split-brain-safe distributed partition leadership and failover.
- [ ] Benchmarked superiority over RabbitMQ/NATS under controlled third-party-comparable workloads.

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
- [x] Added `examples/complete_broker` with `broker.bcl` covering broker config, WAL, ACL tokens, queues, topics, streams, routes, wildcard routing, service discovery, live logs, and broker-sent events.
- [x] Added `examples/complete_consumer` with `consumer.bcl` covering queue/topic/wildcard handlers, polling, service discovery, broker-sent events, lifecycle hooks, progress, and DLQ demo behavior.
- [x] Added `examples/complete_worker` with `worker.bcl` covering task, request/response, command handling, polling, auto lease extension, service discovery, broker-sent cancellation/drain events, and lifecycle hooks.
- [x] Added `examples/complete_publisher` with `publisher.bcl` covering publish, topic route, wildcard topic, batch, async, task submit/result wait, request/response, command/response, stream publish/read/commit, and DLQ demo publish.
- [x] Added `examples/internal/exconfig` BCL loader for the examples.
- [x] Updated `scripts/smoke_remote.sh` to use the four consolidated examples.
- [x] Validated with `go test ./...`, `go vet ./...`, `go test -race ./broker`, and `./scripts/smoke_remote.sh`.

## Complete lifecycle/hooks/notification examples pass

- [x] Added `examples/internal/exobserve` with reusable broker observer, client observer, notification callback, and JSONL sink.
- [x] Updated `examples/complete_broker` to pass explicit lifecycle hooks into `broker.Config.Hooks`.
- [x] Updated `examples/complete_broker` to subscribe real notification callbacks to `b.Notifications()`.
- [x] Updated `examples/complete_broker/broker.bcl` with `observer_jsonl` and `callback "live-jsonl-audit"` blocks.
- [x] Updated `examples/complete_consumer` to use `exobserve.ClientObserver` instead of only generic logging hooks.
- [x] Updated `examples/complete_worker` to use `exobserve.ClientObserver` instead of only generic logging hooks.
- [x] Added client `OnNotification` hook fired for every broker-sent event received from `/v1/consumers/{id}/events`.
- [x] Updated `bclient.LoggingHooks` to log broker notification events in addition to lifecycle/message events.
- [x] Updated `examples/complete_consumer/consumer.bcl` and `examples/complete_worker/worker.bcl` with `lifecycle_hooks` and `notification_callback` blocks.
- [x] Updated `examples/complete_publisher` with local operation callbacks and JSONL observability sink.
- [x] Updated `examples/complete_publisher/publisher.bcl` with `observer_jsonl` and callback configuration.
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
- [x] Added `require_consumers` and `discovery_wait` to `examples/complete_publisher/publisher.bcl`.
- [x] Added worker handler queue override support through `HandleTaskQueue`, `HandleRequestQueue`, and `HandleCommandQueue`.
- [x] Updated `examples/complete_worker` to honor `queue` values from BCL handler blocks.
- [x] Added `docs/CODEC_FORMAT.md` with the complete FWB1 binary frame layout, flags, TLV fields, security behavior, endpoints, and Go usage.
- [x] Added `examples/complete_codec` with BCL-configured encode/decode/header-peek/tamper-detection example.
- [x] Updated README with service discovery troubleshooting and codec documentation/example instructions.


## BCL payload blocks and control-plane storage pass
- [x] Replaced escaped/raw-string payload examples with native BCL `payload { ... }` object blocks.
- [x] Added BCL block-to-JSON conversion for single payloads and `payloads { item { ... } }` batches.
- [x] Added `broker.StateStore` split into broker/task/consumer/publisher state-store interfaces.
- [x] Added concurrency-safe `MemoryStateStore` as the default state store.
- [x] Integrated state-store persistence into broker lifecycle, task lifecycle, remote consumer lifecycle/events, and publisher operation records.
- [x] Added `contrib/sqlitestore` persistent SQL/SQLite-backed state-store implementation using `database/sql`.
- [x] Added optional BCL `state_store "sqlite"` configuration in the complete broker example.
