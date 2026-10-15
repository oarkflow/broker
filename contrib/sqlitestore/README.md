# fastworker broker SQLite contrib stores

This directory is a separate Go submodule:

```bash
go test ./...
```

It provides two `database/sql` backed stores for the broker:

- `StateStore`: persistent control-plane state for broker/task/consumer/publisher records.
- `MessageStore`: persistent message data-plane for queues, topics, streams, leases, offsets, DLQ, and message movement.

The package registers the pure-Go `modernc.org/sqlite` driver under `sqlite`.
You can also register another compatible driver in your application:

```go
package main

import (
    "github.com/oarkflow/broker"
    "github.com/oarkflow/broker/contrib/sqlitestore"
)

func main() {
    msgStore, err := sqlitestore.OpenMessageStore("sqlite", "./broker-messages.sqlite")
    if err != nil { panic(err) }
    stateStore, err := sqlitestore.Open("sqlite", "./broker-state.sqlite")
    if err != nil { panic(err) }

    b := broker.MustNew(broker.Config{
        Mode:       broker.ModeStandalone,
        Store:      msgStore,
        StateStore: stateStore,
    })
    _ = b
}
```

## MessageStore durability model

`MessageStore` uses the broker `MemoryStore` as the hot execution index and persists an atomic JSON snapshot after every mutating operation. It is simple, reliable, and excellent for embedded/single-node deployments that prefer SQLite operational tooling.

For very high write throughput, prefer the broker binary WAL store. For enterprise SQL persistence with higher throughput, implement the same `broker.Store` interface with normalized append/event tables or use Postgres/MySQL as the backend.

## Root-module usage

The examples module uses a local replacement for the contrib module. An
application working from a checkout can use the same pattern:

```go
require github.com/oarkflow/broker/contrib v0.0.0
replace github.com/oarkflow/broker/contrib => ./contrib
```

## Transactional application outbox

`NewOutbox(db)` adds an outbox to the application's SQLite database.
`EnqueueTx` records a message using the application's `*sql.Tx`; `Dispatch`
delivers committed entries to a broker store with leased, fenced claims and
persistent retry state. See the [integration guide](../../docs/TRANSACTIONAL_OUTBOX.md)
for delivery guarantees and the runnable example.
