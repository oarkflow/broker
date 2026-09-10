# Pre-v1 migration notes

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
