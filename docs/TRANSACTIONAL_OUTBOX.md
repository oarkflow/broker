# Transactional SQLite outbox

`contrib/sqlitestore.NewOutbox(db)` records publish intent in the same SQLite
transaction as an application update. `EnqueueTx(ctx, tx, message, destinations)`
requires a nonempty, globally unique message ID and resolved destinations. Commit
the application's `*sql.Tx` to retain both changes; rollback retains neither.
Initialize the outbox schema before opening application transactions.

The [runnable example](../examples/transactional_outbox/main.go) creates an order,
enqueues its event in the same transaction, and dispatches pending events to a
segmented broker WAL:

```sh
cd examples
go run ./transactional_outbox -dir /tmp/broker-outbox-example
```

For a service, invoke `Dispatch(ctx, brokerStore, config)` periodically with
backoff after failures. Each call processes up to `BatchSize` entries (default
100, maximum 10,000). A failed entry retains its error and stops that pass;
resolve permanently invalid entries through application policy rather than
retrying in a tight loop. There is no automatic deletion, DLQ, or retry limit.

## Guarantees and boundaries

- Application state and publish intent commit together in one SQLite database.
  The supplied transaction must belong to the database used by `NewOutbox`.
  The application owns its database connection and durability configuration;
  use durable files and `synchronous=FULL` for power-loss durability.
- Claiming work is one SQL mutation. Independent workers receive distinct
  claims; expired claims become eligible for recovery. A unique claim token
  fences completion by an old worker after a new worker takes over.
- Dispatch defaults to a 30-second lease and 15-second append timeout. Configure
  the timeout shorter than the lease. The target store must honor context
  cancellation; leases do not provide exactly-once execution.
- The target must advertise persisted, replicated or committed confirmation.
  `AllowVolatileTarget` explicitly opts into completion after a volatile append,
  which can lose events when that target restarts.
- A crash after broker append but before SQL completion can publish again.
  Empty idempotency keys become `outbox:<message ID>`; existing explicit keys
  are preserved. Supporting broker stores can suppress the retry while their
  deduplication state remains available. Consumers still need idempotent
  application side effects. This is at-least-once dispatch, not a distributed
  exactly-once transaction.
- Dispatch uses `Store.Append` directly. Resolve destinations, tenant ownership,
  schema, payload encoding, and authorization in the application before enqueue.
  Broker publisher routing, hooks, notifications and quotas do not run here.
- `GetEntry` returns persisted status, attempts, error and completion time.
  Published rows remain for auditing; the application controls their retention.
  Failed rows remain pending until successfully dispatched or explicitly
  resolved by the application.

`broker.DurableOutbox`, `PublishRetryBuffer`, and its legacy
`TransactionalOutbox` alias retain entries in memory. Their existing names do
not make them durable or atomic with an application database. Migrate to the
SQLite outbox when those guarantees are needed; existing pending in-memory
entries are not transferred automatically.
