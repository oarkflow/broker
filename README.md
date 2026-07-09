# fastworker Broker

`fastworker` is still the worker-pool execution engine. The `broker` package added in this repository is a production-oriented message broker layer built on top of that pool.

The broker provides embedded and single-node standalone messaging with queues, topics, streams, request/response, task execution, retries, DLQs, scheduling, movement, lifecycle hooks, notifications, durable WAL storage, and hardened HTTP administration.

> Honest scope: this archive is production-hardened for embedded and single-node standalone deployments. It includes cluster metadata and extension points, but it does **not** claim completed Raft/quorum consensus or cross-node replicated message storage. Do not market it as RabbitMQ/NATS/Kafka-equivalent distributed consensus until a real replicated data plane and benchmark suite are added.

---

## Contents

- [What is included](#what-is-included)
- [Security hardening](#security-hardening)
- [Quick start](#quick-start)
- [Embedded broker](#embedded-broker)
- [Standalone broker](#standalone-broker)
- [CLI usage](#cli-usage)
- [HTTP API examples](#http-api-examples)
- [Publisher APIs](#publisher-apis)
- [Request/response](#requestresponse)
- [Consumers](#consumers)
- [Batch consumers](#batch-consumers)
- [Routing and wildcard topics](#routing-and-wildcard-topics)
- [Queues](#queues)
- [Streams and offsets](#streams-and-offsets)
- [Retries and DLQ](#retries-and-dlq)
- [Movement across queues/topics/streams/DLQ](#movement-across-queuestopicsstreamsdlq)
- [Tasks](#tasks)
- [Scheduling](#scheduling)
- [Hooks, notifications, and callbacks](#hooks-notifications-and-callbacks)
- [Durable WAL store](#durable-wal-store)
- [Admin auth, ACL, TLS, and mTLS](#admin-auth-acl-tls-and-mtls)
- [Production checklist](#production-checklist)
- [Testing and validation](#testing-and-validation)
- [Package layout](#package-layout)

---

## What is included

### Broker modes

| Mode | Status | Description |
|---|---:|---|
| Embedded | Complete | Broker runs inside your Go process. |
| Standalone | Complete, single-node | `cmd/brokerd` exposes HTTP API with auth/TLS/ACL hardening. |
| Distributed | Extension-ready | Cluster metadata primitives exist; true consensus/data replication is not claimed. |

### Core features

- Publisher API
- Async publish
- Publish confirm
- Batch publish
- Request/response
- Async request futures
- Request stream
- Command API
- Managed task submission
- Queue consumers
- Topic-pattern consumers
- Batch consumers
- Manual ack
- Auto ack
- Nack/requeue/retry/DLQ/reject
- Visibility timeout / leases
- Lease extension
- Lease sweeping / redelivery
- Priority queue leasing
- Queue pause/resume/purge
- Retry policy with exponential backoff
- DLQ replay
- Wildcard routing with `*` and `#`
- Header/tenant/priority route matching
- Runtime route registration
- Topic, queue, stream destinations
- Movement/copy/replay/reroute across queues/topics/streams/DLQ/archive
- Stream read and committed offsets
- Task lifecycle, result, wait, progress, cancel, retry/replay
- Hooks
- Notifications
- Webhook callbacks with retry
- SSE admin event stream
- In-memory store
- File-WAL durable store
- Standalone HTTP API
- `brokerctl` CLI
- Security hardening and tests

---

## Security hardening

The latest hardening pass added:

- Standalone `brokerd` refuses to start without a token unless `--insecure-dev` is explicitly provided.
- Admin HTTP API supports named ACL tokens with exact permissions or `admin:*`.
- Bearer token comparison uses constant-time comparison.
- Ack/Nack now require the correct lease token when a message is leased.
- Admin request body size is limited with `http.MaxBytesReader`.
- Admin server has read, read-header, write, and idle timeouts.
- TLS support via `--tls-cert` and `--tls-key`.
- Optional mTLS client certificate verification via `--client-ca` and `--require-client-cert`.
- Admin mutation audit events are emitted to the notification bus.
- Security headers are set on admin responses.
- WAL directory is created with `0700` permissions.
- WAL file is created with `0600` permissions.
- WAL records include SHA-256 checksums.
- WAL recovery tolerates and truncates a partial final record after crash.
- Race test passes for the broker package.

---

## Quick start

```bash
unzip fastworker_broker_production.zip
cd fastworker_broker

go test ./...
go test -race ./broker
go vet ./...
```

Run the consolidated BCL-driven examples:

```bash
# Terminal 1: broker with BCL routes, queues, streams, ACL tokens, logs, service discovery, and broker-sent events
go run ./examples/complete_broker -config examples/complete_broker/broker.bcl

# Terminal 2: consumer for queue/topic/wildcard message handling
go run ./examples/complete_consumer -config examples/complete_consumer/consumer.bcl

# Terminal 3: worker for task/request/command handling
go run ./examples/complete_worker -config examples/complete_worker/worker.bcl

# Terminal 4: publisher covering publish, async, batch, task, request, command, stream, and DLQ demo
go run ./examples/complete_publisher -config examples/complete_publisher/publisher.bcl
```

Or run the full smoke flow:

```bash
./scripts/smoke_remote.sh
```

The smoke script also asserts:

```text
HOOK before_publish
CALLBACK live-jsonl-audit notification=message.published
CALLBACK broker_event type=message.leased
non-empty broker/consumer/worker/publisher JSONL observability files
```

Run the standalone broker securely:

```bash
export BROKER_TOKEN='change-me-long-random-token'
go run ./cmd/brokerd -addr :8090 -wal ./.broker-data
```

Development-only unauthenticated mode:

```bash
go run ./cmd/brokerd -addr :8090 --insecure-dev
```

Do not use `--insecure-dev` outside local development.

---

## Embedded broker

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/oarkflow/fastworker/broker"
)

func main() {
    ctx := context.Background()

    b := broker.MustNew(broker.Config{
        Mode:              broker.ModeEmbedded,
        DefaultQueue:      "jobs",
        DefaultVisibility: 30 * time.Second,
        PollInterval:      time.Millisecond,
    })

    if err := b.Start(ctx); err != nil {
        panic(err)
    }
    defer b.Stop(context.Background())

    _, err := b.Subscribe(ctx, broker.ConsumerConfig{
        Name:        "email-worker-1",
        Queue:       "email.send",
        AckMode:     broker.AckAuto,
        Concurrency: 16,
    }, func(ctx context.Context, d broker.Delivery) error {
        var req struct {
            To      string `json:"to"`
            Subject string `json:"subject"`
        }
        if err := d.Decode(&req); err != nil {
            return err
        }
        fmt.Println("send email", req.To, req.Subject)
        return nil
    })
    if err != nil {
        panic(err)
    }

    _, err = b.Publisher().Publish(ctx, "email.created", map[string]string{
        "to":      "user@example.com",
        "subject": "Welcome",
    }, broker.WithQueue("email.send"))
    if err != nil {
        panic(err)
    }
}
```

---

## Standalone broker

### Secure single-node with WAL

```bash
export BROKER_TOKEN='use-a-long-random-admin-token'
export BROKER_READ_TOKEN='optional-read-only-token'

go run ./cmd/brokerd \
  -addr :8090 \
  -wal ./.broker-data \
  -token "$BROKER_TOKEN" \
  -read-token "$BROKER_READ_TOKEN" \
  -max-body 1048576
```

### TLS

```bash
go run ./cmd/brokerd \
  -addr :8443 \
  -wal ./.broker-data \
  -token "$BROKER_TOKEN" \
  -tls-cert ./certs/server.crt \
  -tls-key ./certs/server.key
```

### mTLS

```bash
go run ./cmd/brokerd \
  -addr :8443 \
  -wal ./.broker-data \
  -token "$BROKER_TOKEN" \
  -tls-cert ./certs/server.crt \
  -tls-key ./certs/server.key \
  -client-ca ./certs/ca.crt \
  -require-client-cert
```

### Environment variables

| Variable | Purpose |
|---|---|
| `BROKER_ADDR` | Listen address, default `:8090`. |
| `BROKER_WAL` | WAL directory. Empty means memory store. |
| `BROKER_TOKEN` | Admin bearer token. Required unless insecure dev mode. |
| `BROKER_READ_TOKEN` | Optional read-only token. |
| `BROKER_DEFAULT_QUEUE` | Default queue name. |
| `BROKER_INSECURE_DEV` | Set true only for local unauthenticated dev. |
| `BROKER_TLS_CERT` | TLS certificate path. |
| `BROKER_TLS_KEY` | TLS key path. |
| `BROKER_CLIENT_CA` | Client CA for mTLS. |
| `BROKER_REQUIRE_CLIENT_CERT` | Require verified client certificates. |
| `BROKER_MAX_BODY` | Max admin request body size in bytes. |

---

## CLI usage

```bash
export BROKER_TOKEN='change-me-long-random-token'
export BROKER_ADDR='http://127.0.0.1:8090'
```

Stats:

```bash
go run ./cmd/brokerctl stats
```

Create/list queues:

```bash
go run ./cmd/brokerctl queues
```

Publish:

```bash
go run ./cmd/brokerctl publish \
  -topic email.created \
  -queue email.send \
  -type send-email \
  -payload '{"to":"user@example.com","subject":"Welcome"}'
```

Pull a message:

```bash
go run ./cmd/brokerctl pull email.send -consumer cli -group cli -limit 1
```

Ack a pulled message:

```bash
go run ./cmd/brokerctl ack <message-id> -queue email.send -lease <lease-token> -consumer cli
```

Move messages:

```bash
go run ./cmd/brokerctl move \
  -from-type queue \
  -from email.send.dlq \
  -to-type queue \
  -to email.send \
  -mode replay
```

---

## HTTP API examples

Use this helper:

```bash
TOKEN='change-me-long-random-token'
BASE='http://127.0.0.1:8090'
AUTH=(-H "Authorization: Bearer $TOKEN")
```

Health and readiness:

```bash
curl -s "$BASE/healthz"
curl -s "$BASE/readyz"
```

Stats:

```bash
curl -s "${AUTH[@]}" "$BASE/stats" | jq .
```

Create queue:

```bash
curl -s -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  "$BASE/queues" \
  -d '{"name":"email.send","visibility_timeout":30000000000,"max_depth":100000}'
```

Publish:

```bash
curl -s -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  "$BASE/publish" \
  -d '{
    "topic":"email.created",
    "queue":"email.send",
    "type":"send-email",
    "headers":{"tenant":"acme"},
    "payload":{"to":"user@example.com","subject":"Welcome"}
  }'
```

Pull:

```bash
curl -s -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  "$BASE/queues/email.send/pull" \
  -d '{"consumer_id":"worker-1","consumer_group":"email-workers","limit":1}'
```

Ack:

```bash
curl -s -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  "$BASE/messages/<message-id>/ack" \
  -d '{"queue":"email.send","consumer_id":"worker-1","lease_token":"<lease-token>"}'
```

Nack and retry later:

```bash
curl -s -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  "$BASE/messages/<message-id>/nack" \
  -d '{"queue":"email.send","consumer_id":"worker-1","lease_token":"<lease-token>","action":"retry","delay":5000000000,"reason":"temporary SMTP failure"}'
```

Pause/resume/purge queue:

```bash
curl -s -X POST "${AUTH[@]}" "$BASE/queues/email.send/pause"
curl -s -X POST "${AUTH[@]}" "$BASE/queues/email.send/resume"
curl -s -X POST "${AUTH[@]}" "$BASE/queues/email.send/purge"
```

SSE events:

```bash
curl -N "${AUTH[@]}" "$BASE/events"
```

---

## Publisher APIs

```go
pub := b.Publisher()
```

### Fire-and-forget / persisted publish

```go
res, err := pub.Publish(ctx, "orders.created", order,
    broker.WithQueue("orders.process"),
    broker.WithKey(order.ID),
    broker.WithTenantID("tenant-a"),
    broker.WithIdempotencyKey("order:"+order.ID),
)
```

### Confirmed publish

```go
res, err := pub.PublishConfirm(ctx, "orders.created", order,
    broker.WithQueue("orders.process"),
    broker.WithConfirmLevel(broker.ConfirmPersisted),
)
```

### Async publish

```go
future := pub.PublishAsync(ctx, "metrics.raw", payload, broker.WithQueue("metrics"))
res, err := future.Wait(ctx)
```

### Batch publish

```go
batch, err := pub.PublishBatch(ctx, "events.raw", []any{event1, event2, event3},
    broker.WithQueue("events.ingest"),
)
```

### Transactional publish

```go
tx, err := pub.BeginTx(ctx)
if err != nil {
    return err
}
if err := tx.Publish("order.created", order, broker.WithQueue("orders")); err != nil {
    _ = tx.Rollback(ctx)
    return err
}
if err := tx.Publish("audit.event", audit, broker.WithStream("audit")); err != nil {
    _ = tx.Rollback(ctx)
    return err
}
_, err = tx.Commit(ctx)
return err
```

Transaction commit validates all entries first and uses `AppendAtomic` when the store supports it, preventing partial commit on validation, deduplication, or idempotency failures.

---

## Request/response

### Consumer side

```go
_, err := b.Subscribe(ctx, broker.ConsumerConfig{
    Name:    "user-lookup",
    Queue:   "user.lookup",
    AckMode: broker.AckAuto,
}, func(ctx context.Context, d broker.Delivery) error {
    var req struct{ ID string `json:"id"` }
    if err := d.Decode(&req); err != nil {
        return err
    }
    return d.Respond(ctx, map[string]any{
        "id":   req.ID,
        "name": "Sujit",
    })
})
```

### Publisher side

```go
var out struct {
    ID   string `json:"id"`
    Name string `json:"name"`
}
err := b.Publisher().Request(ctx, "user.lookup", map[string]string{"id":"u1"}, &out,
    broker.WithTimeout(3*time.Second),
)
```

### Async request

```go
future := b.Publisher().RequestAsync(ctx, "report.generate", req, broker.WithTimeout(30*time.Second))
resp, err := future.Wait(ctx)
```

### Request stream

```go
stream, err := b.Publisher().RequestStream(ctx, "logs.query", req)
if err != nil {
    return err
}
defer stream.Close()
for stream.Next() {
    msg := stream.Message()
    _ = msg
}
if err := stream.Err(); err != nil {
    return err
}
```

---

## Consumers

### Auto-ack consumer

```go
_, err := b.Subscribe(ctx, broker.ConsumerConfig{
    Name:        "worker-1",
    Queue:       "jobs",
    ConsumerGroup: "workers",
    Concurrency: 32,
    AckMode:     broker.AckAuto,
}, func(ctx context.Context, d broker.Delivery) error {
    var payload MyPayload
    if err := d.Decode(&payload); err != nil {
        return err
    }
    return process(payload)
})
```

### Manual ack consumer

```go
_, err := b.Subscribe(ctx, broker.ConsumerConfig{
    Name:    "manual-worker",
    Queue:   "jobs",
    AckMode: broker.AckManual,
}, func(ctx context.Context, d broker.Delivery) error {
    if err := processMessage(d.Message()); err != nil {
        return d.Nack(ctx, broker.WithNackAction(broker.NackRetry), broker.WithNackDelay(5*time.Second), broker.WithNackReason(err.Error()))
    }
    return d.Ack(ctx)
})
```

Ack/Nack security rule: when a message has a lease token, the correct lease token must be supplied. A caller with only a message ID cannot ack or nack a leased message.

---

## Batch consumers

```go
_, err := b.SubscribeBatch(ctx, broker.ConsumerConfig{
    Name:      "batcher",
    Queue:     "events.ingest",
    BatchSize: 500,
    BatchWait: 50 * time.Millisecond,
    AckMode:   broker.AckAuto,
}, func(ctx context.Context, ds []broker.Delivery) error {
    records := make([]MyRecord, 0, len(ds))
    for _, d := range ds {
        var r MyRecord
        if err := d.Decode(&r); err != nil {
            return err
        }
        records = append(records, r)
    }
    return bulkInsert(records)
})
```

---

## Routing and wildcard topics

Wildcards:

| Pattern | Meaning |
|---|---|
| `*` | exactly one token |
| `#` | zero or more tokens |

Examples:

| Pattern | Matches |
|---|---|
| `email.*` | `email.created`, `email.deleted` |
| `email.#` | `email.created`, `email.marketing.sent` |
| `tenant.*.invoice.created` | `tenant.acme.invoice.created` |
| `tenant.#` | `tenant.acme.invoice.created`, `tenant.demo.email.sent` |

### Add route in Go

```go
b.AddRoute(broker.Route{
    Name:  "tenant-invoices",
    Match: "tenant.*.invoice.created",
    To: []broker.Destination{{
        Kind: broker.DestinationQueue,
        Name: "invoice.process",
    }},
})
```

### Add route by HTTP

```bash
curl -s -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  "$BASE/routes" \
  -d '{
    "name":"tenant-invoices",
    "match":"tenant.*.invoice.created",
    "to":[{"kind":"queue","name":"invoice.process"}]
  }'
```

---

## Queues

Queue features:

- FIFO/priority leasing
- delayed availability
- TTL expiry
- manual and auto ack
- visibility timeout
- lease extension
- redelivery after lease expiry
- pause/resume/purge
- max-depth backpressure
- drop-oldest/drop-newest/reject/DLQ full-queue policies
- retry and DLQ policy

Declare queue:

```go
err := b.DeclareQueue(ctx, broker.QueueConfig{
    Name:              "email.send",
    VisibilityTimeout: 30 * time.Second,
    MaxDepth:          100000,
    RetryPolicy:       broker.DefaultRetryPolicy(),
    DLQ:               "email.send.dlq",
})
```

---

## Streams and offsets

Streams are append-only logs with offsets and consumer-group commits.

```go
err := b.DeclareStream(ctx, broker.StreamConfig{Name: "audit.events"})
_, err = b.Publisher().Publish(ctx, "audit.login", event, broker.WithStream("audit.events"))
```

Read from stream:

```go
msgs, err := b.ReadStream(ctx, broker.StreamReadRequest{
    Stream:        "audit.events",
    ConsumerGroup: "analytics",
    Limit:         100,
})
```

Commit offset:

```go
err = b.CommitOffset(ctx, broker.OffsetCommit{
    Stream:        "audit.events",
    ConsumerGroup: "analytics",
    Offset:        msgs[len(msgs)-1].Offset,
})
```

Get committed offset:

```go
off, err := b.ConsumerOffset(ctx, broker.OffsetCommit{
    Stream:        "audit.events",
    ConsumerGroup: "analytics",
})
```

---

## Retries and DLQ

Return a normal error to retry based on policy:

```go
return fmt.Errorf("temporary dependency failure")
```

Return a permanent error to skip retry and move to DLQ:

```go
return broker.Permanent(fmt.Errorf("invalid payload"))
```

Manual retry via Nack:

```go
return d.Nack(ctx,
    broker.WithNackAction(broker.NackRetry),
    broker.WithNackDelay(10*time.Second),
    broker.WithNackReason("rate limited"),
)
```

Reject to DLQ:

```go
return d.Reject(ctx, "invalid customer id")
```

Replay DLQ by HTTP:

```bash
curl -s -X POST "${AUTH[@]}" "$BASE/dlq/email.send.dlq/replay?queue=email.send"
```

---

## Movement across queues/topics/streams/DLQ

The movement manager supports:

- move
- copy
- replay
- reroute-ready mode
- quarantine
- park
- archive
- dry-run
- reset attempts
- reset lease
- reset error
- filter by status/type/tenant/headers

Go API:

```go
res, err := b.Store().Move(ctx, broker.MoveRequest{
    SourceType:      broker.DestinationQueue,
    Source:          "email.send.dlq",
    DestinationType: broker.DestinationQueue,
    Destination:     "email.send",
    Mode:            broker.MoveReplay,
    ResetAttempts:   true,
    ResetLease:      true,
    ResetError:      true,
    Reason:          "manual replay after SMTP fix",
    Actor:           "admin",
})
```

HTTP API:

```bash
curl -s -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  "$BASE/move" \
  -d '{
    "source_type":"queue",
    "source":"email.send.dlq",
    "destination_type":"queue",
    "destination":"email.send",
    "mode":"replay",
    "reset_attempts":true,
    "reset_lease":true,
    "reset_error":true,
    "reason":"manual replay",
    "actor":"admin"
  }'
```

---

## Tasks

Register a task worker:

```go
_, err := b.RegisterTask(ctx, "image.resize", func(ctx context.Context, d broker.TaskDelivery) (any, error) {
    var req ResizeRequest
    if err := d.Decode(&req); err != nil {
        return nil, err
    }
    _ = d.Progress(ctx, broker.Progress{Current: 1, Total: 2, Percent: 50, Message: "resizing"})
    out, err := resize(req)
    if err != nil {
        return nil, err
    }
    return out, nil
})
```

Submit and wait:

```go
h, err := b.Publisher().SubmitTask(ctx, "image.resize", ResizeRequest{FileID: "f1"},
    broker.WithTaskQueue("image.jobs"),
    broker.WithTaskMaxAttempts(3),
)
if err != nil {
    return err
}
res, err := h.Wait(ctx)
```

Inspect:

```go
info, err := h.Info(ctx)
progress, err := h.Progress(ctx)
result, err := h.Result(ctx)
```

Cancel:

```go
err := h.Cancel(ctx, "user requested cancellation")
```

---

## Scheduling

Delayed publish:

```go
_, err := b.Publisher().PublishAfter(ctx, "email.reminder", payload, 30*time.Minute,
    broker.WithQueue("email.send"),
)
```

Publish at timestamp:

```go
_, err := b.Publisher().PublishAt(ctx, "subscription.renew", payload, renewAt,
    broker.WithQueue("billing.jobs"),
)
```

Internally, scheduled messages are stored with `AvailableAt` in the future and become leaseable after that time.

---

## Hooks, notifications, and callbacks

### Hooks

```go
b := broker.MustNew(broker.Config{
    Hooks: broker.Hooks{
        BeforePublish: func(ctx context.Context, e broker.PublishEvent) error {
            // validate, enrich, authorize, etc.
            return nil
        },
        AfterPublish: func(ctx context.Context, e broker.PublishEvent) {
            // custom audit
        },
        OnHandlerError: func(ctx context.Context, e broker.MessageEvent) {
            // alert
        },
    },
})
```

### Notifications

```go
b.Notifications().Subscribe("logger", broker.CallbackFunc(func(ctx context.Context, n broker.Notification) error {
    fmt.Println(n.Type, n.MessageID, n.Queue)
    return nil
}))
```

### Webhook callbacks

```go
b.Notifications().Subscribe("webhook", broker.WebhookCallback{
    URL:         "https://ops.example.com/broker-events",
    Token:       "webhook-secret",
    Timeout:     3 * time.Second,
    MaxAttempts: 3,
    RetryDelay:  250 * time.Millisecond,
})
```

---

## Durable WAL store

Create a durable single-node store:

```go
st, err := broker.NewFileWALStore("./.broker-data")
if err != nil {
    panic(err)
}
b := broker.MustNew(broker.Config{Store: st})
```

WAL behavior:

- append-only JSONL mutations
- file permissions `0600`
- directory permissions `0700`
- SHA-256 checksum per new record
- legacy WAL records without checksums are accepted for upgrade
- crash recovery replays records into memory indexes
- partial final record is truncated during recovery
- in-flight leased/processing/progressing messages are made queueable again after restart
- stream offsets are persisted and recovered

Operational notes:

- Put the WAL directory on reliable local disk.
- Back it up like a database.
- Avoid sharing the same WAL directory between multiple broker processes.
- For multi-node production, implement a real replicated store or an external database-backed store.

---

## Admin auth, ACL, TLS, and mTLS

### Single admin token

```go
srv := broker.AdminServer{
    Broker:       b,
    Token:        "long-random-admin-token",
    MaxBodyBytes: 1 << 20,
}.ListenAndServe(":8090")
```

`Token` is a convenience option that grants `admin:*`.

### Named ACL tokens

```go
srv := broker.AdminServer{
    Broker: b,
    ACL: broker.NewACL(
        broker.AdminToken{
            Name:        "admin",
            Token:       "long-random-admin-token",
            Permissions: []string{broker.PermissionAdminAll},
        },
        broker.AdminToken{
            Name:  "reader",
            Token: "read-only-token",
            Permissions: []string{
                broker.PermissionBrokerRead,
                broker.PermissionQueueRead,
                broker.PermissionTopicRead,
                broker.PermissionStreamRead,
                broker.PermissionMessageRead,
                broker.PermissionDLQRead,
                broker.PermissionRouteRead,
                broker.PermissionNotification,
            },
        },
    ),
}.ListenAndServe(":8090")
```

### Permission constants

| Permission | Meaning |
|---|---|
| `admin:*` | All admin/API actions |
| `broker:read` | health/ready/stats |
| `message:publish` | publish messages |
| `message:read` | inspect messages/history |
| `message:ack` | ack messages |
| `message:nack` | nack messages |
| `message:move` | move/copy/replay messages |
| `queue:read` | list/pull/read queue messages |
| `queue:write` | create queues |
| `queue:control` | pause/resume queues |
| `queue:purge` | purge queues |
| `topic:read` | list topics |
| `topic:write` | create topics |
| `stream:read` | list/read streams |
| `stream:write` | create streams |
| `stream:commit` | commit stream offsets |
| `dlq:read` | inspect DLQ |
| `dlq:replay` | replay DLQ |
| `route:read` | list routes |
| `route:write` | create routes |
| `notification:read` | read/SSE notification events |

### TLS/mTLS in Go

```go
srv := broker.AdminServer{
    Broker:            b,
    Token:             "long-random-admin-token",
    TLSCertFile:       "./certs/server.crt",
    TLSKeyFile:        "./certs/server.key",
    ClientCAFile:      "./certs/ca.crt",
    RequireClientCert: true,
}.ListenAndServe(":8443")
```

---

## Production checklist

Before using standalone broker in production:

- [ ] Run with `BROKER_TOKEN` or `-token`; never use `--insecure-dev`.
- [ ] Use TLS; use mTLS for sensitive environments.
- [ ] Use named ACL tokens and least-privilege permissions.
- [ ] Set `-max-body` to a sane value for your workload.
- [ ] Put WAL on reliable storage and back it up.
- [ ] Ensure WAL directory permissions are restricted.
- [ ] Configure OS service restart policy.
- [ ] Run behind a private network or authenticated ingress.
- [ ] Monitor `/stats`, `/notifications`, queue depth, DLQ count, and oldest message age.
- [ ] Alert on DLQ growth and repeated callback failure.
- [ ] Use idempotency keys for unsafe operations.
- [ ] Use manual ack for operations that must be confirmed after durable side effects.
- [ ] Keep visibility timeout longer than expected handler time or extend leases.
- [ ] Use task cancellation and deadlines for long-running work.
- [ ] Run `go test ./...`, `go test -race ./broker`, and `go vet ./...` in CI.
- [ ] Add your own soak/load benchmarks for your payload sizes and durability settings.

For distributed production:

- [ ] Implement a consensus-backed metadata plane or use an external coordinator.
- [ ] Implement replicated message storage or database-backed transactional store.
- [ ] Add partition leadership, failover, split-brain protection, and cross-node consumer rebalancing.

---

## Testing and validation

Commands run during this package build:

```bash
go test ./...
go test -race ./broker
go vet ./...
```

Important test coverage added:

- lease-token enforcement for ack/nack
- admin authentication and ACL authorization
- admin body-size limits
- WAL permissions
- WAL checksum and partial-tail recovery
- stream read and committed offsets
- WAL stream offset recovery
- task progress propagation
- admin pull/ack/stream endpoints
- webhook retry behavior
- disabled routes
- batch consumers
- atomic transactions
- backpressure and queue pause/resume
- in-flight WAL redelivery after restart

---

## Package layout

```text
broker/
  admin.go          HTTP admin/standalone API
  broker.go         Broker runtime
  publisher.go      Publisher/request/task APIs
  consumer.go       Consumers, delivery, ack/nack/respond/progress
  router.go         Routes and wildcard matching
  store.go          Store interface, memory store, file WAL store
  task.go           Task state/result/wait/progress
  notification.go   Notification bus and webhook callbacks
  security.go       Admin auth, ACL, audit, constant-time token checks
  hooks.go          Lifecycle hook surfaces
  types.go          Message/config/lifecycle types

cmd/brokerd/        Standalone broker server
cmd/broker-worker/  Generic remote worker CLI
cmd/brokerctl/      CLI client
examples/complete_broker/     Complete BCL-driven broker example
examples/complete_consumer/   Complete BCL-driven consumer example
examples/complete_worker/     Complete BCL-driven worker/task service example
examples/complete_publisher/  Complete BCL-driven publisher example
examples/internal/exconfig/   Small BCL loader used by the examples
Tasks.md            Implementation tracker
```

---

## Final notes

This implementation is now hardened for serious embedded and single-node standalone usage. It is intentionally honest about remaining distributed-system scope: true Raft/quorum replication and cross-node data-plane guarantees require a separate implementation phase and cannot be safely faked with metadata alone.

---

## Consolidated BCL-driven examples

The examples folder has been intentionally reduced to four complete examples plus a tiny internal BCL loader used by those examples:

```text
examples/
  complete_broker/
    broker.bcl
    main.go
  complete_consumer/
    consumer.bcl
    main.go
  complete_worker/
    worker.bcl
    main.go
  complete_publisher/
    publisher.bcl
    main.go
  internal/exconfig/
    bcl.go
```

The old scattered examples were removed to avoid noise. The four remaining examples cover the complete standalone flow with configuration-first setup.

### 1. Complete broker example

`examples/complete_broker/broker.bcl` configures:

- listen address
- standalone mode
- durable WAL path
- insecure local-dev mode or token-based ACL mode
- named admin/publisher/worker tokens
- queue declarations
- topic declarations
- stream declarations
- wildcard routes
- topic-to-queue routes
- topic-to-stream archive copies
- default visibility timeout
- remote consumer heartbeat timeout
- live broker logs
- explicit broker lifecycle hooks
- explicit publish/task/consumer/DLQ hook callbacks
- broker notification callback subscriber
- JSONL observability sink configured by BCL
- broker-sent notification/event channel
- consumer service discovery through `/v1/consumers`

Run:

```bash
go run ./examples/complete_broker -config examples/complete_broker/broker.bcl
```

Production-style token run:

```bash
BROKER_INSECURE_DEV=false \
BROKER_TOKEN='admin-secret' \
BROKER_PUBLISHER_TOKEN='publisher-secret' \
BROKER_WORKER_TOKEN='worker-secret' \
go run ./examples/complete_broker -config examples/complete_broker/broker.bcl
```

### 2. Complete consumer example

`examples/complete_consumer/consumer.bcl` configures:

- broker URL/failover URL list
- worker token
- consumer ID
- consumer group
- local concurrency
- max in-flight
- heartbeat interval
- visibility timeout
- auto lease extension
- hook/lifecycle logs
- polling delivery
- broker-sent events
- service discovery registration
- queue handler for `email.send`
- wildcard handler for `tenant.*.invoice.created`
- queue handler for `invoice.events`
- DLQ demo behavior through `fail_on_contains "force_dlq"`

Run:

```bash
go run ./examples/complete_consumer -config examples/complete_consumer/consumer.bcl
```

### 3. Complete worker example

`examples/complete_worker/worker.bcl` configures the task/RPC/command worker service:

- broker URL/failover URL list
- worker token
- worker/consumer ID
- group
- concurrency
- heartbeat
- visibility timeout
- auto lease extension
- lifecycle hook logs
- broker-sent cancellation/drain events
- task handler `pdf.generate`
- request handler `payment.authorize`
- command handler `payment.capture`

Run:

```bash
go run ./examples/complete_worker -config examples/complete_worker/worker.bcl
```

### 4. Complete publisher example

`examples/complete_publisher/publisher.bcl` configures publisher operations for all primary modes:

- direct queue publish
- topic publish routed by broker route
- wildcard topic publish
- batch publish
- async publish
- task submission and result wait
- request/response
- command/response
- stream publish/read/commit
- DLQ demonstration message

Run:

```bash
go run ./examples/complete_publisher -config examples/complete_publisher/publisher.bcl
```

### Complete local flow

Use four terminals:

```bash
# terminal 1
go run ./examples/complete_broker -config examples/complete_broker/broker.bcl

# terminal 2
go run ./examples/complete_consumer -config examples/complete_consumer/consumer.bcl

# terminal 3
go run ./examples/complete_worker -config examples/complete_worker/worker.bcl

# terminal 4
go run ./examples/complete_publisher -config examples/complete_publisher/publisher.bcl
```

Automated smoke:

```bash
./scripts/smoke_remote.sh
```

The smoke script also asserts:

```text
HOOK before_publish
CALLBACK live-jsonl-audit notification=message.published
CALLBACK broker_event type=message.leased
non-empty broker/consumer/worker/publisher JSONL observability files
```

Successful publisher output includes:

```text
published message_id=msg-... destinations=...
batch published count=2 errors=0 err=<nil>
async published message_id=msg-... destinations=... err=<nil>
task status=succeeded progress=100 result={...}
request response={...} err=<nil>
command response={...} err=<nil>
stream committed offset=... group=publisher-demo
```

### BCL configuration notes

The examples use BCL files instead of hardcoded setup. The example loader supports the subset used by these files:

- named blocks: `broker "complete" { ... }`
- nested blocks: `queue "email.send" { ... }`
- string, int, bool, duration, and list values
- environment values: `env("BROKER_TOKEN", "dev-token")`
- route expressions such as `to queue "email.send"` and `copy_to stream "audit.events"`

This keeps the examples self-contained and dependency-light while still using BCL-style configuration for broker, consumer, worker, and publisher setup.

### Broker-sent events and service discovery

Remote consumers and workers register with the broker and are discoverable through:

```text
GET /v1/consumers
GET /v1/consumers/{id}
GET /v1/consumers/{id}/events
```

The worker runtime consumes the event endpoint for lifecycle signals such as:

- `task.cancelled`
- `consumer.draining`
- `broker.draining`
- `broker.stopping`

The examples attach `exobserve.ClientObserver`, which wraps `bclient.LoggingHooks(...)` and also writes every hook/callback event to JSONL. Consumer and worker examples print and persist lifecycle, poll, heartbeat, broker notification, receive, before-handle, progress, ack/nack/reject, lease-extension, error, drain, and stop events.

### Complete lifecycle/hooks/notification implementation

The examples now include real code paths for the observability surfaces instead of comments-only configuration:

```text
examples/internal/exobserve/
  observe.go   Broker hooks, client hooks, notification callbacks, JSONL sink
```

Broker example implementation:

- creates `exobserve.BrokerObserver` from `broker.bcl`
- passes `observer.Hooks()` into `broker.Config.Hooks`
- subscribes `observer.NotificationCallback(...)` to `b.Notifications()`
- writes hook and notification callback events to `BROKER_OBSERVER_JSONL`
- logs hook events such as `HOOK before_publish`, `HOOK task.progress`, `HOOK dlq.dead_lettered`
- logs notification callbacks such as `CALLBACK live-jsonl-audit notification=message.published`

Consumer and worker implementation:

- create `exobserve.ClientObserver` from `consumer.bcl` / `worker.bcl`
- pass `observer.Hooks()` into `bclient.Config.Hooks`
- consume broker-sent events through `/v1/consumers/{id}/events`
- invoke `OnNotification` for each broker event received
- write hook/callback events to `CONSUMER_OBSERVER_JSONL` and `WORKER_OBSERVER_JSONL`
- log callback events such as `CALLBACK broker_event type=message.leased`

Publisher implementation:

- configures a local callback sink with `PUBLISHER_OBSERVER_JSONL`
- records `publisher.operation.start`, `publisher.operation.succeeded`, and `publisher.operation.failed`

Example BCL observability fields:

```bcl
observer_jsonl env("BROKER_OBSERVER_JSONL", "./.broker-data/observability/broker-events.jsonl")

callback "live-jsonl-audit" {
  enabled true
  type "notification"
  sink "jsonl"
  events ["broker.*", "consumer.*", "message.*", "task.*", "queue.*", "stream.*", "dlq.*"]
}

lifecycle_hooks "all" {
  enabled true
  callbacks ["jsonl", "console"]
  events ["connecting", "registered", "subscribed", "polling", "message_received", "before_handle", "progress", "ack", "nack", "reject", "broker_notification", "stopped"]
}
```

The smoke script validates these logs and JSONL sinks, not only message success.

### Data-plane paths used by the examples

```text
POST /v1/publish
POST /v1/publish/batch
POST /v1/request
POST /v1/tasks/submit
GET  /v1/tasks/{id}/result
POST /v1/consumers/register
POST /v1/consumers/{id}/subscribe
POST /v1/poll
POST /v1/messages/{id}/ack
POST /v1/messages/{id}/nack
POST /v1/messages/{id}/progress
POST /v1/messages/{id}/respond
GET  /v1/streams/{stream}/messages
POST /v1/streams/{stream}/commit
GET  /v1/dlq/{queue}/messages
```

### Validation

The consolidated examples were validated with:

```bash
go test ./...
go vet ./...
go test -race ./broker
./scripts/smoke_remote.sh
```

The smoke script also asserts:

```text
HOOK before_publish
CALLBACK live-jsonl-audit notification=message.published
CALLBACK broker_event type=message.leased
non-empty broker/consumer/worker/publisher JSONL observability files
```

## Final notes

This implementation is hardened for embedded and single-node standalone broker usage with distributed remote consumers/workers. The examples now demonstrate the intended production shape without noisy scattered demos. True broker-node Raft/quorum replication remains an explicit future phase and is not falsely claimed as complete.
