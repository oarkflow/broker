# fastworker Broker (pre-v1)

An experimental message broker for Go with embedded, single-node, and distributed prototypes. Correctness, durability, protocol conformance, and security hardening are still in progress.

## What This Is

| Capability | Status |
|------------|--------|
| Embedded broker (in-process) | Pre-v1; functional, hardening in progress |
| Standalone broker (HTTP/native server) | Pre-v1; functional, hardening in progress |
| Distributed broker (multi-node) | Pre-v1 etcd/raft implementation; release gates remain |
| Encryption at rest (AES-GCM) | Optional, configurable |
| Encryption in transit (TLS/mTLS) | Listener-dependent; conformance in progress |

> **Release status**: No current mode has a stable production-support contract. See the [release gates](docs/RELEASE_GATES.md) and [pre-v1 migration notes](docs/MIGRATION_PRE_V1.md).

---

## Table of Contents

- [Quick Start](#quick-start)
- [Complete Examples](#complete-examples)
- [Installation](#installation)
- [Broker Modes](#broker-modes)
- [Features](#features)
- [Protocols](#protocols)
- [Performance](PERFORMANCE.md)
- [Encryption](#encryption)
- [Security](#security)
- [Configuration](#configuration)
- [CLI Usage](#cli-usage)
- [HTTP API](#http-api)
- [Publisher](#publisher)
- [Consumer](#consumer)
- [Routing](#routing)
- [Queues](#queues)
- [Streams](#streams)
- [Retries and DLQ](#retries-and-dlq)
- [Tasks](#tasks)
- [Multi-Tenancy](#multi-tenancy)
- [Federation and Shovel](#federation-and-shovel)
- [Partitioning](#partitioning)
- [Schema Evolution](#schema-evolution)
- [Observability](#observability)
- [Production Checklist](#production-checklist)
- [Package Layout](#package-layout)

---

## Quick Start

```bash
# Install
go get github.com/oarkflow/broker

# Run tests
go test ./...
go test -race ./broker
go vet ./...
```

### Embedded (in-process)

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/oarkflow/broker"
)

func main() {
    ctx := context.Background()
    b := broker.MustNew(broker.Config{
        DefaultQueue: "jobs",
        DefaultVisibility: 30 * time.Second,
    })
    b.Start(ctx)
    defer b.Stop(context.Background())

    b.Subscribe(ctx, broker.ConsumerConfig{
        Name: "worker", Queue: "email.send", AckMode: broker.AckAuto,
    }, func(ctx context.Context, d broker.Delivery) error {
        fmt.Println("received:", string(d.Message.Payload))
        return nil
    })

    b.Publisher().Publish(ctx, "email.created", map[string]string{
        "to": "user@example.com",
    }, broker.WithQueue("email.send"))
}
```

### Standalone (HTTP server)

```bash
# Start broker
export BROKER_TOKEN='your-secret-token'
go run ./cmd/brokerd -addr :8090 -wal ./.broker-data -token "$BROKER_TOKEN"

# Publish a message
curl -X POST -H "Authorization: Bearer $BROKER_TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/publish \
  -d '{"topic":"events","queue":"jobs","payload":{"task":"send-email"}}'

# Pull a message
curl -X POST -H "Authorization: Bearer $BROKER_TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/queues/jobs/pull \
  -d '{"consumer_id":"worker-1","limit":1}'
```

---

## Installation

```bash
go get github.com/oarkflow/broker
```

### Build binaries

```bash
go build -o brokerd ./cmd/brokerd/
go build -o brokerctl ./cmd/brokerctl/
go build -o broker-worker ./cmd/broker-worker/
```

---

## Broker Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Embedded** | Runs in your Go process as a library | High-performance in-process messaging |
| **Standalone** | HTTP server with auth/TLS/ACL | Microservices, API gateway |
| **Distributed** | Durable etcd/raft replication, snapshots, membership, and leader-gated reads | Pre-v1 clustered evaluation |

## Complete Examples

Runnable, fail-fast examples for embedded workflows, durable WAL restart, and
a real three-node etcd/Raft cluster are in
[`examples/complete`](examples/complete/README.md). From the `examples` module:

```bash
go run ./complete embedded
go run ./complete durable
go run ./complete distributed
./complete/check.sh
```

---

## Features

### Messaging

| Feature | Description |
|---------|-------------|
| Queues | Point-to-point with FIFO/priority leasing |
| Topics | Pub-sub with wildcard routing (`*`, `#`) |
| Streams | Append-only logs with consumer offsets |
| Request/Response | Sync, async futures, streaming |
| Commands | RPC-style request/response |
| Tasks | Managed submission with progress tracking |
| Saga | Multi-step workflow orchestration |

### Reliability

| Feature | Description |
|---------|-------------|
| WAL durability | JSONL or native packed BWL1 records with CRC32C |
| Crash recovery | Replay WAL, in-flight requeue |
| Retry policy | Exponential backoff with jitter |
| DLQ | Dead-letter queue with replay |
| Idempotency | Publisher and consumer dedup |
| Delivery semantics | At-least-once queues with bounded idempotency helpers |
| Circuit breaker | Automatic failure detection and recovery |
| Rate limiting | Per-queue and per-topic token bucket |
| Backpressure | Block, reject, drop-oldest, drop-newest, DLQ |

### Operations

| Feature | Description |
|---------|-------------|
| Prometheus metrics | `/metrics` endpoint with histograms |
| Distributed tracing | OpenTelemetry-compatible spans |
| Audit logging | Structured audit trail with sinks |
| Webhooks | Event callbacks with retry |
| SSE streaming | Real-time event stream |
| Schema evolution | Versioned schema registry |
| Stream lag monitoring | Consumer lag per stream |
| WAL compaction | Experimental detached JSON-WAL utility; not active-store compaction |
| Consumer priority | Weighted routing based on consumer capacity |
| Consumer group rebalancing | Weighted, graceful drain |
| Partitioning | Hash, round-robin, key-based routing |
| Multi-tenancy | Tenant-scoped core data paths; full control-plane isolation is pending |
| Federation | Inter-broker message forwarding |
| Shovel | Queue-to-queue message movement |
| Graceful shutdown | HTTP connection draining |

---

## Protocols

| Protocol | Transport | Port | Status |
|----------|-----------|------|--------|
| HTTP/JSON | TCP | 8090 | Pre-v1; daemon-integrated |
| Native stream | TCP/TLS | configurable | Pre-v1; authenticated (token/mTLS) handshake, see below |
| AMQP 0-9-1 | TCP/TLS | configurable | Experimental; conformance not established |
| MQTT 3.1.1 | TCP/TLS | configurable | Experimental; persistent sessions remain incomplete |
| Native RPC | TCP/TLS | configurable | Experimental JSON/TCP; not gRPC |
| SSE | HTTP(S) | configurable | Experimental event stream, separate from WebSocket |
| WebSocket | RFC 6455 | configurable | Pre-v1; protobuf-independent JSON data plane |
| gRPC | protobuf/HTTP2 | configurable | Pre-v1 versioned `broker.v1` API |

`brokerd` supervises every enabled listener. Protocol listeners are opt-in by
address; production mode requires TLS and authentication on every listener,
including the native BST1 stream, which enforces a token or mTLS handshake
unless `--insecure-dev` is set.

---

## Encryption

### At Rest

| Component | Encryption | How to Enable |
|-----------|-----------|---------------|
| JSONL WAL | AES-256-GCM | `-frame-encryption-key <hex-key>` |
| Binary WAL (BWL1) | AES-256-GCM + optional HMAC-SHA256 | `-frame-encryption-key`, `-frame-signing-key` |
| FWB1/FWB2 network codecs | AES-GCM + HMAC-SHA256 | `WithFrameEncryptionKey()`, `WithFrameSigningKey()` |
| SQLite | AES-256-GCM | `NewEncryptedMessageStore(db, key)` |

### In Transit

| Transport | TLS | mTLS | How to Enable |
|-----------|-----|------|---------------|
| HTTP Admin | ✅ | ✅ | `-tls-cert`, `-tls-key`, `-client-ca`, `-require-client-cert` |
| MQTT | ✅ | ✅ | `MQTTConfig.TLSCertFile`, etc. |
| Native JSON/TCP RPC | ✅ | ✅ | `NativeRPCConfig.TLSCertFile`, etc.; this is not gRPC |
| AMQP | ✅ | ✅ | `ListenTLS()`, `EXTERNAL` auth |
| gRPC v1 | ✅ | ✅ | `GRPCV1Config.TLSConfig` |
| RFC 6455 WebSocket | ✅ | via HTTPS client identity | `RFC6455Config` behind a TLS HTTP server |
| Client SDK | ✅ | ✅ | `Config.TLSCertFile`, etc. |

At-rest encryption remains opt-in. `brokerd` rejects plaintext production mode;
plaintext listeners require the explicit `--insecure-dev` flag.

---

## Security

### Authentication

| Method | Protocol | How |
|--------|----------|-----|
| Bearer token | HTTP | `Authorization: Bearer <token>` |
| ACL tokens | HTTP | Named tokens with 28+ permissions |
| MQTT password | MQTT | `BrokerToken` in `MQTTConfig` |
| AMQP PLAIN | AMQP | Username/password |
| AMQP EXTERNAL | AMQP | Client certificate (mTLS) |

### Authorization (HTTP)

| Permission | Description |
|------------|-------------|
| `admin:*` | All admin/API actions |
| `broker:read` | health/ready/stats |
| `message:publish` | publish messages |
| `message:ack/nack` | ack/nack messages |
| `message:move` | move/copy/replay |
| `queue:read/write/control/purge` | queue management |
| `topic:read/write` | topic management |
| `stream:read/write/commit` | stream management |
| `dlq:read/replay` | DLQ management |
| `route:read/write` | route management |
| `consumer:register/read/control/poll` | consumer management |
| `task:read/write` | task management |
| `notification:read` | notification/SSE access |

### TLS/mTLS

```bash
# TLS
brokerd -addr :8443 -tls-cert server.crt -tls-key server.key

# mTLS
brokerd -addr :8443 -tls-cert server.crt -tls-key server.key \
  -client-ca ca.crt -require-client-cert
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BROKER_ADDR` | `:8090` | Listen address |
| `BROKER_WAL` | - | WAL directory (empty = memory) |
| `BROKER_WAL_FORMAT` | `binary` | `binary` or `json` |
| `BROKER_TOKEN` | - | Admin token (required unless `--insecure-dev`) |
| `BROKER_READ_TOKEN` | - | Read-only token |
| `BROKER_WORKER_TOKEN` | - | Worker token |
| `BROKER_PUBLISHER_TOKEN` | - | Publisher token |
| `BROKER_TLS_CERT` | - | TLS certificate |
| `BROKER_TLS_KEY` | - | TLS key |
| `BROKER_CLIENT_CA` | - | Client CA for mTLS |
| `BROKER_REQUIRE_CLIENT_CERT` | `false` | Require mTLS |
| `BROKER_MAX_BODY` | `1048576` | Max request body (bytes) |
| `BROKER_FRAME_SIGNING_KEY` | - | HMAC signing key |
| `BROKER_FRAME_ENCRYPTION_KEY` | - | AES encryption key (hex) |
| `BROKER_INSECURE_DEV` | `false` | Disable auth (dev only) |

### CLI Flags

```bash
brokerd \
  -addr :8090 \
  -wal ./.broker-data \
  -wal-format binary \
  -token "your-secret-token" \
  -tls-cert server.crt \
  -tls-key server.key \
  -client-ca ca.crt \
  -require-client-cert \
  -max-body 1048576 \
  -frame-signing-key "hex-signing-key" \
  -frame-encryption-key "hex-encryption-key" \
  -frame-compress-threshold 1024
```

---

## CLI Usage

```bash
export BROKER_TOKEN='your-secret-token'
export BROKER_ADDR='http://127.0.0.1:8090'

# Stats
brokerctl stats

# List queues
brokerctl queues

# Publish
brokerctl publish -topic orders.created -queue orders.process \
  -type order -payload '{"id":"order-1","amount":100}'

# Pull
brokerctl pull email.send -consumer cli -group cli -limit 1

# Ack
brokerctl ack <message-id> -queue email.send -lease <lease-token> -consumer cli

# Move (DLQ replay)
brokerctl move -from-type queue -from email.send.dlq \
  -to-type queue -to email.send -mode replay
```

---

## HTTP API

### Health and Stats

```bash
curl http://localhost:8090/healthz           # Health check
curl http://localhost:8090/readyz            # Readiness check
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/stats                # Broker stats
curl http://localhost:8090/metrics           # Prometheus metrics
curl http://localhost:8090/lag?stream=audit  # Stream lag
```

### Publish

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/publish \
  -d '{
    "topic": "orders.created",
    "queue": "orders.process",
    "type": "order",
    "payload": {"id": "order-1", "amount": 100}
  }'
```

### Queue Operations

```bash
# Create queue
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/queues \
  -d '{"name":"orders","visibility_timeout":30000000000,"max_depth":100000}'

# Pull messages
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/queues/orders/pull \
  -d '{"consumer_id":"worker-1","consumer_group":"workers","limit":1}'

# Pause/Resume/Purge
curl -X POST -H "Authorization: Bearer $TOKEN" http://localhost:8090/queues/orders/pause
curl -X POST -H "Authorization: Bearer $TOKEN" http://localhost:8090/queues/orders/resume
curl -X POST -H "Authorization: Bearer $TOKEN" http://localhost:8090/queues/orders/purge
```

### Message Operations

```bash
# Ack
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/messages/<id>/ack \
  -d '{"queue":"orders","consumer_id":"worker-1","lease_token":"<token>"}'

# Nack with retry
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/messages/<id>/nack \
  -d '{"queue":"orders","consumer_id":"worker-1","lease_token":"<token>","action":"retry","delay":5000000000}'
```

### Movement

```bash
# Replay DLQ
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/move \
  -d '{
    "source_type":"queue","source":"orders.dlq",
    "destination_type":"queue","destination":"orders",
    "mode":"replay","reset_attempts":true,"reset_error":true
  }'
```

### SSE Events

The standalone SSE listener is secure by default: configure its ACL (or call
`SetToken` for the pre-v1 compatibility path), or explicitly set
`InsecureDev` for isolated development. Tenant-scoped connections receive only
tenant-tagged broadcasts for an allowed tenant; unowned global events are not
forwarded.

```bash
curl -N -H "Authorization: Bearer $TOKEN" http://localhost:8090/events
```

---

## Publisher

```go
pub := b.Publisher()

// Fire-and-forget
pub.Publish(ctx, "orders.created", payload, broker.WithQueue("orders"))

// Confirmed publish
pub.PublishConfirm(ctx, "orders.created", payload, broker.WithQueue("orders"))

// Async publish
future := pub.PublishAsync(ctx, "events.raw", payload, broker.WithQueue("events"))
res, _ := future.Wait(ctx)

// Batch publish
pub.PublishBatch(ctx, "events.raw", []any{e1, e2, e3}, broker.WithQueue("events"))

// Transactional publish
tx, _ := pub.BeginTx(ctx)
tx.Publish("order.created", order, broker.WithQueue("orders"))
tx.Publish("audit.event", audit, broker.WithStream("audit"))
tx.Commit(ctx)

// Delayed publish
pub.PublishAfter(ctx, "email.reminder", payload, 30*time.Minute, broker.WithQueue("email"))

// Scheduled publish
pub.PublishAt(ctx, "billing.renew", payload, renewAt, broker.WithQueue("billing"))
```

---

## Consumer

```go
// Auto-ack
b.Subscribe(ctx, broker.ConsumerConfig{
    Name: "worker", Queue: "jobs", AckMode: broker.AckAuto, Concurrency: 32,
}, func(ctx context.Context, d broker.Delivery) error {
    // process
    return nil
})

// Manual ack
b.Subscribe(ctx, broker.ConsumerConfig{
    Name: "worker", Queue: "jobs", AckMode: broker.AckManual,
}, func(ctx context.Context, d broker.Delivery) error {
    if err := process(d); err != nil {
        return d.Nack(ctx, broker.WithNackAction(broker.NackRetry), broker.WithNackDelay(5*time.Second))
    }
    return d.Ack(ctx)
})

// Batch consumer
b.SubscribeBatch(ctx, broker.ConsumerConfig{
    Name: "batcher", Queue: "events", BatchSize: 500, BatchWait: 50*time.Millisecond,
}, func(ctx context.Context, ds []broker.Delivery) error {
    // process batch
    return nil
})

// Idempotent consumer (deduplicates messages)
b.Subscribe(ctx, broker.ConsumerConfig{
    Name: "idempotent-worker", Queue: "orders",
    Idempotent: true, DeduplicationWindow: 5*time.Minute,
}, func(ctx context.Context, d broker.Delivery) error {
    // automatically deduplicates by idempotency_key
    return nil
})
```

---

## Routing

```go
// Wildcard routes
b.AddRoute(broker.Route{
    Name: "tenant-invoices", Match: "tenant.*.invoice.created",
    To: []broker.Destination{{Kind: broker.DestinationQueue, Name: "invoice.process"}},
})

// Fanout
b.AddRoute(broker.Route{
    Name: "all-events", Match: "events.*", Mode: broker.RouteFanout,
    To: []broker.Destination{
        {Kind: broker.DestinationQueue, Name: "analytics"},
        {Kind: broker.DestinationQueue, Name: "audit"},
    },
})

// Add route via HTTP
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/routes \
  -d '{"name":"my-route","match":"orders.*","to":[{"kind":"queue","name":"orders.process"}]}'
```

---

## Queues

```go
b.DeclareQueue(ctx, broker.QueueConfig{
    Name: "orders", VisibilityTimeout: 30*time.Second, MaxDepth: 100000,
    RetryPolicy: broker.DefaultRetryPolicy(), DLQ: "orders.dlq",
})
```

Features: FIFO/priority leasing, delayed availability, TTL, manual/auto ack, visibility timeout, lease extension, redelivery, pause/resume/purge, max-depth backpressure.

---

## Streams

```go
b.DeclareStream(ctx, broker.StreamConfig{Name: "audit.events"})

// Publish to stream
pub.Publish(ctx, "audit.login", event, broker.WithStream("audit.events"))

// Read from stream
msgs, _ := b.ReadStream(ctx, broker.StreamReadRequest{Stream: "audit.events", ConsumerGroup: "analytics", Limit: 100})

// Commit offset
b.CommitOffset(ctx, broker.OffsetCommit{Stream: "audit.events", ConsumerGroup: "analytics", Offset: msgs[len(msgs)-1].Offset})

// Check lag
lag, _ := b.StreamLag(ctx, "audit.events", "analytics")
```

---

## Retries and DLQ

```go
// Retry on error
return fmt.Errorf("temporary failure")

// Permanent error (skip retry, go to DLQ)
return broker.Permanent(fmt.Errorf("invalid payload"))

// Manual retry
return d.Nack(ctx, broker.WithNackAction(broker.NackRetry), broker.WithNackDelay(10*time.Second))

// Reject to DLQ
return d.Reject(ctx, "invalid customer id")

// Replay DLQ
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/dlq/orders.dlq/replay?queue=orders
```

---

## Tasks

```go
// Register task handler
b.RegisterTask(ctx, "image.resize", func(ctx context.Context, d broker.TaskDelivery) (any, error) {
    d.Progress(ctx, broker.Progress{Current: 1, Total: 2, Percent: 50})
    return resize(d.Message.Payload)
})

// Submit task
h, _ := pub.SubmitTask(ctx, "image.resize", req, broker.WithTaskQueue("image.jobs"))
result, _ := h.Wait(ctx)
```

---

## Multi-Tenancy

```go
b := broker.MustNew(broker.Config{
    Tenancy: &broker.TenancyConfig{
        Enabled: true, IsolationLevel: broker.IsolationQueuePrefix, QueuePrefix: "t.",
    },
})

tenantMgr := b.TenantManager()
tenantMgr.EnsureTenant("tenant-a")

// Tenant-isolated publish
tenantMgr.TenantAwarePublish(ctx, "tenant-a", "orders.created", payload)

// Tenant-isolated subscribe
tenantMgr.TenantAwareSubscribe(ctx, "tenant-a", broker.ConsumerConfig{Queue: "orders"}, handler)
```

---

## Federation and Shovel

```go
// Shovel: move messages from queue A to queue B
b.ShovelManager().StartShovel(broker.ShovelConfig{
    Source: "source-queue", Destination: "dest-queue", BatchSize: 100,
})

// Federation: link to upstream broker
b.FederationManager().AddLink(broker.FederationConfig{
    Upstream: "http://upstream:8090", Queues: []string{"orders"},
})
```

---

## Partitioning

```go
b := broker.MustNew(broker.Config{
    Partition: &broker.PartitionConfig{Partitions: 4, Strategy: broker.PartitionHash},
})

router := broker.NewPartitionRouter(broker.PartitionConfig{Partitions: 4, Strategy: broker.PartitionHash})
partition := router.SelectPartition(broker.Message{Key: "user-123"})
queue := fmt.Sprintf("events.p%d", partition)
pub.Publish(ctx, "events.user", payload, broker.WithQueue(queue))
```

---

## Schema Evolution

```go
registry := broker.NewMemorySchemaRegistry(
    broker.SchemaDefinition{Name: "invoice", Version: "v1", Required: []string{"id", "amount"}},
    broker.SchemaDefinition{Name: "invoice", Version: "v2", Required: []string{"id", "amount", "currency"}, CompatibleWith: "v1"},
)
b := broker.MustNew(broker.Config{SchemaRegistry: registry})

pub.Publish(ctx, "invoice.created", payload, broker.WithSchema("invoice", "v2"))
```

---

## Observability

### Prometheus Metrics

```bash
curl http://localhost:8090/metrics
```

Metrics: `broker_messages_published_total`, `broker_messages_delivered_total`, `broker_messages_acked_total`, `broker_queue_depth{queue}`, `broker_topic_published_total{topic}`, latency histograms.

### Tracing

```go
b := broker.MustNew(broker.Config{
    EnableTracing: true,
    TracerConfig: &broker.TracerConfig{ServiceName: "my-service"},
})
```

### Audit Logging

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8090/audit
```

---

## Production Checklist

- [ ] Set `BROKER_TOKEN` or `-token`; never use `--insecure-dev`
- [ ] Enable TLS; use mTLS for sensitive environments
- [ ] Use named ACL tokens with least-privilege permissions
- [ ] Set `-max-body` to a sane value
- [ ] Put WAL on reliable storage and back it up
- [ ] Enable encryption at rest if storing sensitive data
- [ ] Run behind a private network or authenticated ingress
- [ ] Monitor `/stats`, `/metrics`, `/lag`, queue depth, DLQ count
- [ ] Alert on DLQ growth and callback failure
- [ ] Use idempotency keys for unsafe operations
- [ ] Use manual ack for operations requiring confirmation
- [ ] Keep visibility timeout longer than handler time or extend leases
- [ ] Run `scripts/release_gate.sh` and archive its artifacts for the exact commit
- [ ] Complete `DURATION=72h scripts/soak.sh` on production-like hardware
- [ ] Perform the backup, restore, upgrade, rollback, and alert drills in [Operations](docs/OPERATIONS.md)
- [ ] Review the [threat model](docs/THREAT_MODEL.md) and close deployment-specific risks
- [ ] Verify required third-party clients against the [protocol compatibility scope](docs/PROTOCOL_COMPATIBILITY.md)
- [ ] Obtain independent security review and explicit release sign-off

---

## Package Layout

```
broker/
  broker.go              Core broker runtime
  publisher.go           Publisher/request/task APIs
  consumer.go            Consumers, delivery, ack/nack
  router.go              Routes and wildcard matching
  store.go               Store interface, memory store, WAL stores
  task.go                Task state/result/wait/progress
  saga.go                Saga orchestration
  notification.go        Notification bus and webhooks
  security.go            Auth, ACL, audit
  hooks.go               Lifecycle hooks
  types.go               Types and config
  health.go              Health checks, metrics
  admin.go               HTTP admin/API server
  dataplane.go           /v1/* data-plane endpoints
  remote.go              Remote consumer registry
  distributed.go         Distributed broker and replicated store adapter
  rebalance.go           Consumer group rebalancing
  strategy.go            14 routing strategies
  schema.go              Schema registry
  errors.go              Error types
  mqtt.go                MQTT 3.1.1 server
  grpc_transport.go      Experimental native JSON/TCP RPC transport
  websocket.go           Server-Sent Events (SSE) transport
  federation.go          Federation and shovel
  tenancy.go             Multi-tenancy
  exactly_once.go        Legacy in-memory publish retry buffer and replay helpers
  partition_ordering.go  Partition routing
  wal_compaction.go      WAL segment management
  metrics.go             Prometheus metrics
  tracing.go             Distributed tracing
  ratelimit.go           Rate limiting
  circuit_breaker.go     Circuit breaker
  audit.go               Audit logging
  frame_codec.go         FWB1 network message codec
  compact_codec.go       FWB2 compact network codec
  wal_record_codec.go    BWL1 packed mutation codec
  binary_wal_store.go    Native BWL1 WAL store

  amqp/                  AMQP 0-9-1 protocol server
  cluster/               Durable etcd/raft consensus and transports
  contrib/sqlitestore/   SQLite state + message stores
  cmd/brokerd/           Standalone broker server
  cmd/brokerctl/         CLI client
  cmd/broker-worker/     Remote worker process
  pkg/client/            Client SDK
  pkg/pool/              Worker pool (execution engine)
  examples/              BCL-driven examples
```

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for the full text.

## High-throughput BST1 stream mode

For Kafka-equivalent append/fetch workloads use the native stream listener rather than queue leasing:

```bash
brokerd -wal .broker-data/queue -native-addr :8091 \
  -native-dir .broker-data/native-stream -native-partitions 8 \
  -native-sync=true -insecure-dev
```

The native stream listener supports an authenticated handshake (bearer token or
mTLS, enforced via the same ACL used by the HTTP admin API). Outside
`--insecure-dev`, `brokerd` requires `-token` or `-require-client-cert` to be
set before it will start the listener. `--insecure-dev` disables that
enforcement (anonymous/auto-admin access) and must only be used for local
development.

The `native-bench` command uses persistent TCP clients, compact record batches, partition offsets and restart-safe segmented logs. See `NATIVE_STREAM_PERFORMANCE.md` and `scripts/benchmark_native_vs_kafka.sh`.
