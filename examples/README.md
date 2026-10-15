# Runnable examples

Run commands in this directory, which is a separate Go module with local
replacements for the broker and SQLite contrib module.

## Examples for the recent enhancements

```sh
go run ./enhancements -scenario all
```

Each scenario checks the behavior it demonstrates and exits with an error if the
expected result is not observed. No external broker, database service, or
credentials are required. The HTTP scenario starts its own loopback fixture.
The runner uses a temporary directory and cleans it up automatically. Keep
artifacts in a new, unique run directory for inspection with:

```sh
go run ./enhancements -scenario all -dir /tmp/broker-example-runs
```

| Scenario | Changes demonstrated | Source |
| --- | --- | --- |
| `routing` | Exact and wildcard lookup order; destination deduplication; route removal and clearing; persistent round-robin and weighted strategy state; sticky load changes, failover and bounded FIFO eviction; tenant filtering without a matching route; task-router reload | [routing.go](enhancements/routing.go) |
| `producer` | Four-shard WAL producer pinning across changing keys; latest-sequence deduplication after restart; stale/gapped sequences and epoch fencing; atomic/minimal/owned batches; independent registry snapshots | [producer.go](enhancements/producer.go) |
| `request-reply` | A real responder; multiple responses and clean end-of-stream; active correlation rejection; cancellation and safe correlation reuse; timeout; broker shutdown cleanup | [request_reply.go](enhancements/request_reply.go) |
| `stream` | Atomic offset-plus-produce through memory, JSON WAL, binary WAL, segmented WAL, four-shard WAL and SQLite; stale commit rejection; tenant inheritance and offset isolation; mixed standalone/transactional offsets; durable-store restart | [stream.go](enhancements/stream.go) |
| `stream-http` | Go client transaction API; binary payload encoding; persisted confirmation; typed offset conflicts; tenant-scoped credentials; both publish and commit permissions, including trailing slashes; strict JSON fields | [stream_http.go](enhancements/stream_http.go) |
| `outbox` | Application update and intent in one SQLite transaction; rollback and reopen; ambiguous append outcome and idempotent retry; persisted attempt/status fields; volatile target rejection; three competing dispatch workers with independent DB handles | [outbox.go](enhancements/outbox.go) |
| `sqlite-rollback` | An intentionally failed database write leaves no output or offset advance and preserves the token/status of an unrelated leased message | [sqlite_rollback.go](enhancements/sqlite_rollback.go) |
| `legacy-outbox` | The compatibility worker remains in memory; enqueue owns a copy; duplicate IDs are rejected; lifecycle restart works; synchronous flush returns append failures | [legacy_outbox.go](enhancements/legacy_outbox.go) |
| `wal` | Verify/export/import of transaction operation 19; exclusive import; tenant offset recovery; migration to segmented WAL; tail repair with a backup; truthful memory-store confirmations | [wal.go](enhancements/wal.go) |

Select one scenario by name, for example:

```sh
go run ./enhancements -scenario producer
go run -race ./enhancements -scenario outbox
```

The failure fixtures operate only on the runner's disposable files. They are
examples of handling failures, not production store implementations. The legacy
worker has no restart durability; the SQL outbox has at-least-once dispatch.
Memory stream transactions are volatile; durable transactions are limited to
one store/WAL shard. See [stream transactions](../docs/STREAM_TRANSACTIONS.md),
[the SQL outbox contract](../docs/TRANSACTIONAL_OUTBOX.md), and
[migration notes](../docs/MIGRATION_PRE_V1.md) for the boundaries.

## Smaller introductory examples

```sh
# Process an input batch and commit its offset with output records.
go run ./stream_transaction -dir /tmp/broker-stream-example
# Run it again to reopen the same WAL and resume the committed group position.
go run ./stream_transaction -dir /tmp/broker-stream-example

# Insert an order and its event intent in one SQL transaction, then dispatch.
go run ./transactional_outbox -dir /tmp/broker-outbox-example
```

Sources: [stream transaction](stream_transaction/main.go) and
[transactional outbox](transactional_outbox/main.go).

## Internal correctness and performance examples

Some changes have no additional application API: synchronization of reply
delivery with cancellation, an end marker rejected by a full reply buffer,
fencing an expired outbox claim, atomic visibility while syncing, recovery after
a process exits immediately after sync, and recovery after compaction. The
executable regression scenarios show these boundaries directly. Run from the
repository root:

```sh
go test -race -run 'Test(ResponseDeliveryConcurrentCancellation|StreamEndCanBeRetriedAfterBufferFull|TaskRouterConcurrentReloadAndSelection|StreamTransactionVisibilityAndPersistenceFailure|StreamTransactionCrashRecovery|StreamTransactionSurvivesSegmentCompaction|StreamTransactionRejectsCrossShardOutputsBeforeWriting)' .
(cd contrib && go test -race -run '^TestOutboxConcurrentWorkersAndLeaseFencing$' ./sqlitestore)

# Routing-only measurement; this does not measure network or disk throughput.
go test -run '^$' -bench '^BenchmarkRouterExactSubjects$' -benchmem -count=5 .

# Bounded decoder fuzz smoke, including the new transaction record seed.
go test -run '^$' -fuzz '^FuzzWALRecordDecode$' -fuzztime=5s -parallel=2 .
```

Older binaries cannot read WAL operation 19. Use backups made before the feature
was enabled when downgrading; the examples do not attempt a downgrade or modify
existing data directories.

## Check the examples

From the repository root:

```sh
bash scripts/check_enhancement_examples.sh
```

This builds/tests and vets the examples module, runs the scenario runner with the
race detector, and runs both introductory examples, including a second stream
invocation against the same directory to demonstrate restart.

The existing [complete examples](complete/README.md) cover broader embedded,
durable and distributed workflows. [Pipeline consumer](pipeline_consumer/README.md)
is a separate nested module. The older `routing` program is an interactive,
long-running demo; the `enhancements` runner terminates automatically.
