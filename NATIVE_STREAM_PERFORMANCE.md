# BST1 native stream data plane

The broker now has two intentionally separate delivery models:

- Queue mode: visibility leases, per-message ack/nack, retry and DLQ semantics.
- Stream mode: partition offsets, sequential segmented logs and grouped durable commits. This is the Kafka-equivalent performance path.

## Architecture

BST1 uses persistent TCP connections and compact length-delimited record batches. A produce request encodes the stream and partition once, followed by record lengths and payloads. Each stream partition owns a bounded append channel and a single writer. The writer coalesces concurrent producer batches, writes one contiguous buffer per segment, and performs one grouped `fsync` before acknowledging when sync mode is enabled.

Storage is isolated by stream and partition:

```
native-stream/streams/<stream>/partition-0000/
  00000000000000000000.log
  00000000001000000000.log
consumer.offsets
```

Recovery scans each segment, rebuilds the sparse in-memory position index and truncates an incomplete tail record. Consumer groups persist monotonically increasing partition offsets.

## Matched local comparison with Kafka

Four fresh one-million-record runs on OrbStack 29.8.2 (Apple M2 Pro, 10 host
CPUs, 8 GiB VM). Both brokers were single-node ARM64 containers capped at 4 CPUs
and 4 GiB. Each run used 8 partitions, 256-byte records, 16 producer clients,
1,000 records per producer batch, no compression, and `acks=all` for Kafka.
Broker used sync-before-ack; Kafka 4.3.1 used a one-replica topic and
`log.flush.interval.messages=1000` so it also flushed at batch scale. Kafka's
flush threshold is per partition; it does not add replication or HA. The Kafka
image is pinned in the benchmark script.

| Durable publish metric | Broker | Kafka | Broker lead |
|---|---:|---:|---:|
| Median throughput (4 runs) | 1,530,649 msg/s | 476,810 msg/s | 3.21x |
| Throughput range | 1.30–1.81M msg/s | 0.45–0.52M msg/s | Broker led all 4 runs |
| Median batch p99 | 25.7 ms | 336.6 ms | 13.1x lower |

Every run published exactly 1,000,000 records with zero reported errors.
Per-run logs and JSON summaries are written beneath `benchmark-results/`.
Reproduce with:

```bash
./scripts/benchmark_native_vs_kafka.sh
```

The consume numbers are not a like-for-like durability result: Broker used 8
workers and synced offset commits; Kafka's bundled consumer performance tool
used one consumer and reports fetch rate separately from rebalance time. Treat
those figures as fetch diagnostics only.

This demonstrates a repeatable win for the tested single-node durable publish
profile. It is not evidence that Broker is faster for every Kafka workload,
cluster size, message shape, or failure guarantee, and it does not complete the
project's production release gates.

## Run

```bash
./scripts/benchmark_native_vs_kafka.sh
```

Useful parameters:

```bash
NUM_RECORDS=5000000 PAYLOAD_SIZE=256 PARTITIONS=8 \
PUBLISH_BATCH=1000 PRODUCER_CONCURRENCY=16 \
FETCH_BATCH=4000 CONSUMER_CONCURRENCY=8 \
./scripts/benchmark_native_vs_kafka.sh
```
