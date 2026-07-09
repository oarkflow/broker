# fastworker broker SQLite contrib stores

This directory is a separate Go submodule:

```bash
go test ./...
```

It provides two `database/sql` backed stores for the broker:

- `StateStore`: persistent control-plane state for broker/task/consumer/publisher records.
- `MessageStore`: persistent message data-plane for queues, topics, streams, leases, offsets, DLQ, and message movement.

The package intentionally does **not** import a SQLite driver directly. Register the driver in your application or command binary:

```go
package main

import (
    _ "modernc.org/sqlite" // or github.com/mattn/go-sqlite3

    "github.com/oarkflow/fastworker/broker"
    "github.com/oarkflow/fastworker/contrib/sqlitestore"
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

The root module has a `replace` entry so examples can import this contrib module locally. For your own application, either keep this submodule path or copy the same pattern:

```go
require github.com/oarkflow/fastworker/contrib/sqlitestore v0.0.0
replace github.com/oarkflow/fastworker/contrib/sqlitestore => ./contrib/sqlitestore
```
