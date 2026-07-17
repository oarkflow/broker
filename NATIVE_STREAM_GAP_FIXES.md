# Native stream hot-path gap fixes

## Fixed

- Removed an O(total retained records) partition-index scan from every durable group commit. Only segments touched by the current commit are synced.
- Added BST1 raw fetch operation (`nativeOpFetchRaw`) using the on-disk `[u32 length][payload]` representation.
- Fetch now performs one contiguous `ReadAt` per segment rather than one syscall and allocation per record.
- Server response creation no longer creates `[][]byte` or copies every payload into an intermediate representation.
- Native client validates and visits raw records without per-record allocation or copying.
- Benchmark consumer uses the raw fetch path, matching Kafka consumer-perf-test's discard/count workload.
- Kafka benchmark parser validates exact record count and reports both full-lifecycle `nMsg.sec` and steady-state `fetch.nMsg.sec`.

## Local validation workload

- 1,000,000 records
- 256-byte payload
- 8 partitions
- producer batch 1,000
- producer concurrency 16
- fetch batch 4,000
- consumer concurrency 8
- grouped durable sync enabled

Observed in the validation container:

- Publish: 893,274 msg/s
- Consume: 2,990,542 msg/s
- Publish errors: 0
- Consume errors: 0

These numbers are environment-specific. Use the Docker comparison script on the target host for authoritative Kafka ratios.
