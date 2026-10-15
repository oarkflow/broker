# Pre-v1 migration notes

## Atomic stream transactions

`Publisher.BeginStreamTx`, the HTTP stream transaction endpoint, and
`StreamTransactionStore` atomically commit outputs with a consumer offset using
an expected-offset comparison. Unsupported stores reject the operation rather
than falling back to separate writes. See [stream transactions](STREAM_TRANSACTIONS.md).

WAL operation code 19 (`stream_transaction`) is new; existing codes are unchanged.
Back up before enabling the feature. Older binaries cannot read the new operation;
restore a backup from before the feature was enabled when downgrading. Do not
open upgraded WAL directories with older binaries.

New standalone offset commits in the segmented WAL now persist the same
tenant-scoped group key used by binary WALs and stream transactions. This makes
mixing standalone and transactional commits recover consistently. Existing
segmented records whose tenant identity was omitted cannot reconstruct it
retroactively; reconcile those group positions when migrating affected data.

## Publish confirmations

`WithConfirmLevel` is now a required minimum, not a label copied into the
response. A publish fails with `ErrUnsupportedMode` when the active store cannot
provide the requested level. `PublishResult.Confirmed` always describes the
level actually achieved.

Memory stores achieve `enqueued`. Local WAL and transactional SQLite stores
achieve `persisted` with local durability. The legacy distributed layer does not
claim replicated or committed durability because it cannot prove follower fsync.

## Protocol naming

The existing JSON-over-TCP transport is a native RPC protocol, not gRPC. The
existing streaming HTTP transport is SSE, not WebSocket. Compatibility aliases
may remain during pre-v1 development but stable documentation and listener names
will use the accurate protocol names.

## Delivery semantics

Queues are at-least-once. Deduplication can provide effectively-once handling
inside its persisted window. Broker stream transactions do not make external
database or API side effects exactly once.

## Producer sequencing and sharded WALs

Sequenced producers are now pinned to one WAL shard independently of message
keys. Existing single-shard producer placement is recovered from WAL state.
Opening a legacy directory with the same producer's sequence history on multiple
shards fails with `ErrStorageUnavailable`: reconcile that producer history from
verified backups before reopening. Automatic merging cannot establish which
previously acknowledged duplicate or conflicting epoch should win.

A producer cannot span streams assigned to different WAL shards. Such appends
fail with `ErrUnsupportedMode` before writing messages; use a separate producer
ID per stream shard or a single WAL shard. Producer IDs are store-wide, so
applications sharing a store must namespace them across tenants and workloads.
Striped locks coordinate concurrent producers without serializing all producers.

The standalone `ProducerRegistry` now follows the store contract: retrying the
latest sequence returns its original message ID; older sequences are rejected
with `ErrOutOfOrderSequence` instead of returning the latest message's ID.
Registration returns a snapshot, and registering a newer epoch clears the old
message identity. This helper remains in memory; committed store appends carry
the durable sequencing state.

## Request/reply lifecycle

An already-active correlation ID is rejected with `ErrDuplicateMessage` instead
of replacing its waiter. Stream contexts and explicit timeouts release waiters
even if callers stop reading. Stream close only removes that stream's own waiter,
and broker shutdown releases all remaining waiters. An end marker rejected
because the response buffer is full leaves the stream open so delivery can be
retried.

## Application outbox

Use `sqlitestore.NewOutbox` and `EnqueueTx` for an application-database
transactional outbox. The root `DurableOutbox` worker is retained for source
compatibility and is explicitly documented as in memory. See
[Transactional outbox](TRANSACTIONAL_OUTBOX.md) before migration.

## Routing strategy state

Routes retain their strategy instance instead of recreating it per message.
Round-robin distributes across calls, weighted round-robin honors effective
weights (nonpositive weights count as one), and sticky sessions retain a binding
while that consumer remains available. Membership or weight changes reset the
weighted cycle. Sticky state defaults to 65,536 bindings with FIFO eviction;
set `max_sticky_bindings` on a route, strategy configuration or routing plan to
change that bound. Sessions evicted at the bound are assigned again.
