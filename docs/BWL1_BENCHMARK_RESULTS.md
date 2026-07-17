# BWL1 codec benchmark results

## Scope

These measurements isolate the new native BWL1 WAL record codec and then measure the complete durable append path. They compare the same 50-message workload against the previous JSON operation payload wrapped in an FWB frame.

Environment:

- Go 1.23.2
- Linux amd64
- AMD EPYC 9V74
- 256-byte payloads
- 50 messages per durable append batch
- one WAL file and one group-commit writer
- three benchmark repetitions

Absolute `fsync` throughput depends on the host filesystem and virtualization layer. Relative codec results are less sensitive to storage.

## Codec-only results

| Path | Median time/batch | Throughput equivalent | Bytes allocated | Allocations |
|---|---:|---:|---:|---:|
| Legacy JSON operation + FWB frame | 103.718 µs | 482,077 msg/s | 28,220 B | 207 |
| Native BWL1 record | 10.429 µs | 4,794,323 msg/s | 6,912 B | 1 |
| Improvement | **9.95× faster** | **9.95×** | **75.5% less** | **99.5% fewer** |

The acknowledgement-specific BWL1 benchmark encoded 256 acknowledgements in a median 13.224 µs with one allocation.

## Encoded size

For the representative 50-message append batch:

| Format | Record size |
|---|---:|
| Legacy JSON + FWB frame | 15,757 bytes |
| Native BWL1 | 1,892 bytes |
| Reduction | **88.0%** |

BWL1 achieves this by using operation-specific layouts, presence bitmaps, varints, compact enums, and shared batch fields instead of repeated JSON keys and repeated destination/message metadata.

## Complete durable append results

This benchmark includes message preparation, in-memory queue mutation, BWL1 encoding, group commit, buffered write, and `fsync`.

| Durable path | Median throughput | Median time/batch | Allocations/op |
|---|---:|---:|---:|
| Legacy JSON-in-frame WAL | 119,339 msg/s | 418.985 µs | 327 |
| Native BWL1 WAL | 223,392 msg/s | 223.830 µs | 104 |
| Improvement | **1.87×** | **46.6% lower** | **68.2% fewer** |

The remaining allocations include message IDs and in-memory store bookkeeping; they are not produced by the BWL1 encoder itself.

## Commands

```bash
go test -run '^$' \
  -bench 'Benchmark(WALRecordCodecAppendBatch50|LegacyJSONWALRecordAppendBatch50|WALRecordCodecAckBatch256)$' \
  -benchmem -count=3

go test -run '^$' \
  -bench '^BenchmarkBinaryWALPublishBatch50$' \
  -benchmem -count=3
```

## Interpretation

BWL1 removes JSON serialization from every new durable mutation. It does not by itself prove end-to-end Kafka superiority. A fair broker comparison must also align replication factor, acknowledgement policy, partition count, compression, retention, TLS, payload size, producer concurrency, consumer fetch behavior, and hardware.
