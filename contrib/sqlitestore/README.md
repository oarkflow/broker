# fastworker broker SQLite contrib stores

This directory is a separate Go submodule:

```bash
go test ./...
```

It provides two `database/sql` backed stores for the broker:

- `StateStore`: persistent control-plane state for broker/task/consumer/publisher records, MQTT persistent-session subscriptions, inbound QoS 2 and outbound QoS 1/2 state, tenant-scoped retained messages with ordered paging for subscription replay, and durable outbox entries.
- `MessageStore`: persistent message data-plane for queues, topics, streams, leases, offsets, DLQ, and message movement.

The package intentionally does **not** import a SQLite driver directly. Register the driver in your application or command binary:

```go
package main

import (
    _ "modernc.org/sqlite" // or github.com/mattn/go-sqlite3

    "github.com/oarkflow/broker"
    "github.com/oarkflow/broker/contrib/sqlitestore"
)

func main() {
    msgStore, stateStore, err := sqlitestore.OpenStores("sqlite", "./broker.sqlite")
    if err != nil { panic(err) }

    b := broker.MustNew(broker.Config{
        Mode:       broker.ModeStandalone,
        Store:      msgStore,
        StateStore: stateStore,
    })
    _ = b
}
```

`OpenStores` shares one `*sql.DB` between message and control-plane state; the
message store owns and closes that connection pool. To keep them in separate
files, use `OpenMessageStore` and `Open` independently.

`NewDurableOutbox` uses the broker's `StateStore` automatically when it
implements `broker.OutboxStateStore`, as this SQLite store does. Pending entries
are restored after restart and dispatched at least once. If a crash happens
after the message append succeeds but before the outbox status update is saved,
the message may be appended again; consumers should be idempotent and use
the broker’s deduplication facilities where they fit the workload. The default
`broker.MemoryStateStore` only preserves entries while that process remains
alive. SQLite state stores use expiring claim leases so multiple dispatchers
can share the outbox. A worker whose claim expires can overlap with recovery if
it already appended the message, so delivery remains at-least-once.
Dispatch failures use exponential backoff with jitter. Once the retry limit is
reached, the outbox persists a dead-letter phase; a failed DLQ write retries
that phase without sending the original message again.
Published and dead-lettered outbox rows remain available to `GetEntry` until
`Prune(ctx, before, limit)` removes terminal records older than the cutoff.

### Commit business data and an outbox row together

Use the same `*sql.DB` for the application transaction and `StateStore`. The
broker can use a different message store; the transaction covers the
application row and outbox row, while broker dispatch starts after commit.

```go
payload, err := json.Marshal(map[string]string{"order_id": orderID}) // import encoding/json
if err != nil { return err }

tx, err := db.BeginTx(ctx, nil)
if err != nil { return err }
defer tx.Rollback()

if _, err := tx.ExecContext(ctx,
    `INSERT INTO orders(id, status) VALUES(?, ?)`, orderID, "created"); err != nil {
    return err
}

_, err = outbox.EnqueueTx(ctx, stateStore.OutboxTransaction(tx), broker.Message{
	Payload: payload,
}, []broker.Destination{{Kind: broker.DestinationQueue, Name: "orders.created"}})
if err != nil { return err }
return tx.Commit()
```

The transaction belongs to the caller. `EnqueueTx` only inserts the outbox row;
the normal outbox poller discovers it after commit. A rollback removes both the
business write and the outbox row. Once dispatch starts, delivery is still
at-least-once, so consumers must handle retries and duplicates.

Other `database/sql` backends can use the same transaction helper without
depending on this SQLite package. Implement `OutboxStateStore` for that
backend, create the compatible `fw_outbox_entries` table, and pass the
caller-owned `*sql.Tx` to `broker.NewSQLOutboxEntryTransaction`. Use
`broker.SQLBindDollarNumbered` for PostgreSQL and
`broker.SQLBindQuestionMark` for SQLite or MySQL. The helper inserts JSON text
and Unix-nanosecond timestamps; the transaction and `OutboxStateStore` must
use the same database. Choose a `data_json` type large enough for your maximum
serialized entry (for MySQL, use `LONGTEXT` or `JSON` rather than the 64 KiB
`TEXT` type).

For example, the PostgreSQL table may use `TEXT` for `data_json`:

```sql
CREATE TABLE fw_outbox_entries (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  claim_token TEXT NOT NULL DEFAULT '',
  lease_until BIGINT NOT NULL DEFAULT 0,
  next_attempt_at BIGINT NOT NULL DEFAULT 0,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  data_json TEXT NOT NULL
);
```

## MessageStore durability model

`MessageStore` uses the broker `MemoryStore` as the hot execution index and persists an atomic JSON snapshot after every mutating operation. It is simple, reliable, and excellent for embedded/single-node deployments that prefer SQLite operational tooling.

For very high write throughput, prefer the broker binary WAL store. For enterprise SQL persistence with higher throughput, implement the same `broker.Store` interface with normalized append/event tables or use Postgres/MySQL as the backend.

## Root-module usage

The root module has a `replace` entry so examples can import this contrib module locally. For your own application, either keep this submodule path or copy the same pattern:

```go
require github.com/oarkflow/broker/contrib/sqlitestore v0.0.0
replace github.com/oarkflow/broker/contrib/sqlitestore => ./contrib/sqlitestore
```
