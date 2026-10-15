# Atomic stream processing

Stream transactions commit a consumer group's input position and its output
messages together. They use an expected offset to reject work based on an old
position. A competing consumer cannot commit outputs from the same expected
position after another transaction advances it.

## Embedded API

```go
previous, err := b.ConsumerOffset(ctx, broker.OffsetCommit{
    Stream: "orders", ConsumerGroup: "billing",
})
if err != nil { return err }
messages, err := b.ReadStream(ctx, broker.StreamReadRequest{
    Stream: "orders", ConsumerGroup: "billing", Limit: 100,
})
if err != nil { return err }
if len(messages) == 0 { return nil }

tx, err := b.Publisher().BeginStreamTx(ctx, broker.OffsetCommit{
    Stream: "orders", ConsumerGroup: "billing",
    Offset: messages[len(messages)-1].Offset,
}, previous)
if err != nil { return err }
defer tx.Rollback(ctx)
for _, message := range messages {
    if err := tx.Publish("billed", message.Payload,
        broker.WithStream("billing-results")); err != nil { return err }
}
_, err = tx.Commit(ctx)
return err
```

Fetch the previous committed position before reading the batch. Commit must
advance that position and cannot exceed the input stream's current end. An
offset-only transaction is allowed for a processor intentionally producing no
output. `PublishTx` is single-use and should be owned by one goroutine.

Run the complete [example](../examples/stream_transaction/main.go):

```sh
cd examples
go run ./stream_transaction -dir /tmp/broker-stream-transaction
```

Running it again reopens the same WAL and resumes from the committed position.

## HTTP and Go client

`POST /v1/streams/{input}/transactions` (also `/streams/{input}/transactions`)
requires both `stream:commit` and `message:publish` permission. The body uses
normal publish options and payload encoding for each output:

```json
{
  "consumer_group": "billing",
  "expected_offset": 0,
  "offset": 1,
  "messages": [
    {"topic": "billed", "stream": "billing-results", "payload": {"order_id": 42}}
  ]
}
```

A successful response is an array of publish results. A stale expected offset
returns HTTP 409 with code `offset_conflict`; the Go client preserves
`errors.Is(err, broker.ErrOffsetConflict)`. Binary outputs use the normal
`payload_encoding: "base64"` representation. Unknown top-level fields and
multiple JSON bodies are rejected.

The client method is:

```go
results, err := c.CommitStreamTransaction(ctx, "orders",
    broker.StreamTransactionHTTPPayload{
        ConsumerGroup: "billing", ExpectedOffset: previous,
        Offset: lastProcessedOffset, Messages: outputMessages,
    })
```

## Storage guarantees

| Store | Support | Commit boundary |
| --- | --- | --- |
| MemoryStore | Yes | One memory lock; volatile |
| FileWALStore | Yes | One synced JSON WAL record, then one memory installation |
| BinaryFileWALStore | Yes | One synced packed WAL record, then one memory installation |
| SegmentedWALStore | Yes | One synced packed record; checkpoints include both outputs and offsets |
| PartitionedBinaryWALStore | Within one shard | Unkeyed queue/topic outputs co-locate with the input; keyed outputs and named output streams must already map to that shard |
| SQLite MessageStore | Yes | One SQL snapshot transaction; failure restores the prior memory image |
| ReplicatedStore / encrypted SQLite / third-party stores | Not advertised by these implementations | Must explicitly implement `StreamTransactionStore`; no fallback to separate writes |

WAL stores hold their mutation lock through sync for this operation. Readers
continue to see the previous state until both outputs and offset are installed
under one memory lock. This favors a clear atomicity boundary; concurrent
transaction throughput has not been benchmarked.

Transactions reject output ID collisions and queue overflow even when the queue
normally drops messages. A failed transaction must not discard outputs or evict
other queued messages. Input and outputs share one tenant; omitted output tenants
inherit the input tenant. Tenant-scoped offset keys are persisted in the WAL.
Transaction output IDs must be globally unique, as with ordinary publishing.

## Retry and failure handling

- On `ErrOffsetConflict`, discard the prepared work, reread the committed offset
  and input batch, and recompute outputs. Do not simply change the expected offset
  on stale work.
- On cancellation before writing, validation failure, or a rejected cross-shard
  operation, neither the offset nor outputs are committed.
- An I/O failure or lost HTTP response can have an ambiguous outcome. The record
  may have synced before the failure became observable. Reconcile the consumer
  offset after recovery. Retrying the old expected offset cannot append outputs
  again after that position has advanced; it returns a conflict rather than
  replaying the original response.
- An unwritten/torn transaction record does not contain independent output and
  offset records to apply separately. A fully synced record recovers both,
  including a process exit between sync and memory installation.
- Publisher hooks, notifications, application databases, and external API calls
  are outside the atomic boundary. Use the [SQLite outbox](TRANSACTIONAL_OUTBOX.md)
  for application-database publish intent. This API does not provide distributed
  exactly-once side effects, consumer membership epochs, prepared transactions,
  or a cross-shard transaction coordinator.

Take backups before enabling this feature. Packed WALs use the new
`stream_transaction` operation code 19; codes 1–18 retain their existing meaning.
Older binaries do not understand the new record. Downgrade by restoring a backup
from before stream transactions were written, not by opening the new WAL with an
older binary.
