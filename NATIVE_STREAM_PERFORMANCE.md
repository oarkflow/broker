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

## Local validation result

Environment: isolated Linux runtime, one broker process, 8 partitions, 256-byte records, sync enabled, one million records.

| Operation | Configuration | Result |
|---|---|---:|
| Durable publish | batch 1000, 16 persistent clients, grouped fsync | 850,353 msg/s |
| Fetch + durable offset commits | fetch 4000, 8 clients | 273,453 msg/s |

These results exceeded the user-provided Kafka 4.3.1 run of 245,700 publish msg/s and 168,237 consume msg/s on that workload. They are not universal claims: hardware, filesystem, container limits, batching, partition count, Kafka configuration and durability semantics must remain equivalent.

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
