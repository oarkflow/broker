# Broker Performance Architecture and Benchmarking

This document describes the durable high-throughput path implemented in this repository, how to run it, and what the current measurements do and do not prove.

## What changed

### Publish path

- Batch endpoints now prepare and append a batch once instead of calling single-message publish repeatedly.
- `OwnedAtomicStore` transfers ownership of freshly decoded payloads, headers, and metadata to the store. This removes unnecessary deep copies on the minimal-response hot path.
- `Prefer: return=minimal` returns HTTP `204` and skips result construction, message cloning, publisher history, and notification history when those features are disabled.
- Native frame batches are length-prefixed and accepted by `/v1/publish/frame/batch`.
- Publish hooks, schema validation, routing, idempotency, deduplication, max-depth checks, and normal result responses continue to use the complete semantic path when enabled.

### Durable WAL

- BWL1 encodes mutations directly as packed binary operation records. The durable hot path no longer marshals JSON and no longer wraps mutations in a generic message frame.
- Message presence bitmaps omit empty fields; append/lease/update batches encode repeated message and destination fields once; ack batches encode common queue/consumer fields once.
- Ordinary uncompressed records use one output allocation and one linear encode pass.
- CRC32C, optional HMAC-SHA256, optional AES-GCM, bounded decompression, version checks, and legacy FWB/JSON recovery are preserved.
- Concurrent writes use group commit: queued records are buffered, flushed, and `fsync`ed together while each caller waits for the durability result.
- Atomic publish batches are represented by one WAL operation rather than one durable operation per message.
- Lease, update, and acknowledgement batches use one durable operation per batch.
- WAL write and `fsync` failures are returned to callers instead of being silently ignored.
- Recovery accepts bounded records up to 128 MiB and replays batch append, lease, update, and acknowledgement operations.
- Optional partitioned binary WALs provide independent memory locks, files, group-commit goroutines, and `fsync` pipelines. Messages with the same ordering key, idempotency key, deduplication key, or routing key remain on one shard.

> Cross-shard append is atomic per shard, not globally atomic across all shard files. Use one shard when a batch must have single-file atomicity, or use an outbox/transaction coordinator for cross-shard transactions.

### Queue and consumer path

- FIFO queues use a logical head cursor and active depth counter. Successful acknowledgements are O(1), rather than scanning and deleting from the middle of a slice.
- Priority sorting is bypassed for queues that have never received priority messages.
- Remote polling no longer performs a second durable update immediately after the durable lease batch.
- Batch acknowledgement uses one store lock and one WAL record.
- Completed payloads can be evicted while bounded completion tombstones preserve duplicate-ack idempotency.
- Per-message history, publisher tracking, notification history, and consumer event tracking are independently configurable and disabled by default in `brokerd`'s performance-oriented configuration.

### Wire protocol and benchmark client

The load generator supports:

- `-protocol=json`: JSON HTTP batch publishing and JSON polling/acknowledgement.
- `-protocol=frame`: FWB frame batch publishing, frame polling, and compact binary acknowledgement batches.
- Separate publish, consume, and combined message-rate reporting.
- `publish`, `pull`, and bounded `roundtrip` modes.

## Compact backlog (`-compact-backlog`)

A queue holding millions of undelivered messages normally keeps each one as a 752-byte `Message` struct plus strings, maps and payload, about 1.1 KB and eight pointer-bearing heap objects per message, all scanned on every GC cycle. With `-compact-backlog` (or `MemoryStore.SetCompactBacklog(true)`), a freshly queued message is kept as its lossless BWL1 encoding plus a small summary, and is decoded back into a normal message the first time anything leases or changes it.

Measured on 1M retained 256-byte messages: live heap 1,167 to 610 B/message, GC cycle 106 ms to 23 ms. In the daemon, publish-only into a durable WAL went from 143k to 171k msg/s, with about 500 B/message live heap. Steady-state publisher plus consumers is unchanged within noise at about half the RSS.

Notes:
- Only queue messages that have never been leased are compacted; streams, topics, leased and dead-lettered messages stay in normal form.
- Resident memory also depends on Go GC headroom. Set `GOMEMLIMIT` to turn the smaller live heap into a smaller RSS.
- Decoded times come back in UTC (as they already do after WAL recovery).
- It is off by default. `BROKER_TEST_COMPACT=1 go test ./...` runs the whole suite against it.

## Recommended single-node configuration

```bash
./brokerd \
  -addr :8090 \
  -store wal \
  -wal ./.broker-data/wal \
  -wal-format binary \
  -wal-shards 1 \
  -message-history 0 \
  -notification-history 0 \
  -quiet \
  -max-body 67108864 \
  -token "$BROKER_TOKEN"
```

For benchmark-only local development, `-insecure-dev` may replace `-token`. Do not use it in production.

Use `-wal-shards N` only when the shard directories can benefit from independent storage queues or volumes and the workload has enough independent keys. Multiple WAL files on the same constrained disk can be slower because every batch may require several `fsync` operations.

Enable optional observability only when needed:

```text
-message-history=N
-notification-history=N
-publisher-tracking
-consumer-event-tracking
-retain-completed
```

`-retain-completed=false` removes completed message bodies from memory after durable acknowledgement. A bounded tombstone set still makes recent duplicate acknowledgements idempotent.

## Benchmark commands

Build:

```bash
cd cmd
go build -o ../bin/brokerd ./brokerd
go build -o ../bin/loadgen ./loadgen
cd ..
```

Durable publish workload:

```bash
./bin/loadgen \
  -addr http://127.0.0.1:8090 \
  -token "$BROKER_TOKEN" \
  -mode publish \
  -protocol json \
  -c 32 \
  -duration 15s \
  -payload 256 \
  -batch 200 \
  -queue bench.q
```

Durable consume workload with a pre-filled queue:

```bash
./bin/loadgen \
  -addr http://127.0.0.1:8090 \
  -token "$BROKER_TOKEN" \
  -mode pull \
  -protocol json \
  -c 32 \
  -duration 15s \
  -pull-limit 256 \
  -queue bench.q
```

Bounded publish-and-drain workload:

```bash
./bin/loadgen \
  -addr http://127.0.0.1:8090 \
  -token "$BROKER_TOKEN" \
  -mode roundtrip \
  -protocol json \
  -c 32 \
  -duration 15s \
  -payload 256 \
  -batch 200 \
  -pull-limit 256 \
  -queue bench.q
```

Native frame workload:

```bash
./bin/loadgen \
  -addr http://127.0.0.1:8090 \
  -token "$BROKER_TOKEN" \
  -mode roundtrip \
  -protocol frame \
  -c 16 \
  -duration 15s \
  -payload 256 \
  -batch 50 \
  -pull-limit 64 \
  -queue bench.q
```

Direct durable-store benchmark:

```bash
go test . \
  -run '^$' \
  -bench BenchmarkBinaryWALPublishBatch50 \
  -benchtime=3000x \
  -benchmem
```

## Measurements from this workspace

Environment:

- Linux x86-64 container
- AMD EPYC 9V74 host CPU
- Go 1.23.2
- `GOMAXPROCS=16` for HTTP tests
- One broker process and one load-generator process
- One binary WAL shard
- Every successful batch waited for WAL group commit and `fsync`
- 256-byte payloads
- No Docker runtime was available in this workspace, so these are not an apples-to-apples rerun of the supplied Docker topology

| Workload | Batch / pull | Concurrency | Result | p50 |
|---|---:|---:|---:|---:|
| Durable HTTP publish | 200 | 32 | 61,675 msg/s | 96.49 ms per 200-message request |
| Durable HTTP consume + ack | 256 | 32 | 46,405 msg/s | 3.60 ms per poll/ack cycle |
| Durable bounded roundtrip | 200 / 256 | 32 | 32,649 publish msg/s + 32,649 consume msg/s | 196.12 ms per publish+poll+ack cycle |
| Direct binary-WAL append | 50 | benchmark loop | 85,661–100,491 msg/s across three fixed-count runs | 497–584 µs per 50-message durable batch |

The supplied earlier Docker result was 9,155 publish msg/s and 7,084 combined consume msg/s at its highest-throughput setting. The measurements above show a materially higher implementation ceiling, but hardware, filesystem, container networking, payload representation, and benchmark topology differ.

## Kafka comparison: honest boundary

These changes do **not** by themselves prove that this broker outperforms Kafka under an equivalent production configuration.

A valid comparison must hold constant:

- Message bytes per second, not only message count
- Producer acknowledgement and durability rules
- Replication factor and replica acknowledgement
- Number of partitions
- Compression algorithm and batch size
- Retention and log-cleaning work
- TLS, authentication, and authorization
- Consumer processing and offset semantics
- Disk type, filesystem, network, CPU limits, and container limits

The direct durable append path now reaches approximately the 100K-message/s magnitude on this host, while the measured HTTP end-to-end path remains below 100K msg/s. Kafka commonly benefits from a mature record-batch format, partition leaders spread across disks/nodes, zero-copy fetch, long-lived multiplexed binary connections, and replication pipelines. Claiming a Kafka win requires running the same reproducible suite against both systems.

## Native BWL1 codec measurements

A three-run benchmark on Linux amd64 with Go 1.23.2 and a 50-message append batch measured:

| Path | Median time per encoded batch | Allocations | Encoded size |
|---|---:|---:|---:|
| Legacy JSON operation payload | 103.718 µs | 207 | 15,757 bytes |
| Native BWL1 operation record | 10.429 µs | 1 | 1,892 bytes |

That is approximately **9.95× faster codec execution**, **99.5% fewer allocations**, and an **88.0% smaller record** for this workload.

The complete durable `AppendAtomicOwned` benchmark, including memory-store work, group commit, flush, and `fsync`, improved from a median **119,339 msg/s** with the legacy JSON-in-frame format to **223,392 msg/s** with BWL1 in this workspace—about **1.87×**. Filesystem and virtualization behavior can materially change absolute results. See [`docs/BWL1_BENCHMARK_RESULTS.md`](docs/BWL1_BENCHMARK_RESULTS.md) for the commands and complete measurements.

## Next highest-leverage work

The WAL serialization ceiling is removed. The next major work is outside the record encoder: long-lived multiplexed binary connections, partition-aware producers and consumers, segment indexing, zero-copy fetch, record-batch compression with a faster algorithm than gzip, replicated commit pipelines, and equivalent Kafka-vs-broker benchmark profiles.
