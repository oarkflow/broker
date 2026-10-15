# Broker enhancement plan

Scope: durable queues and streams, low-latency pub/sub and request/reply,
and compatibility with existing messaging clients. This plan complements
[Tasks.md](../Tasks.md), which remains the readiness tracker. Features in the
repository are starting points, not evidence of full NATS, Kafka or RabbitMQ
compatibility.

## First implementation: subject routing

Exact subjects now use an index shared by `Route` and `RouteWithStrategy`.
For E bindings to the requested exact subject and W wildcard/catch-all bindings,
lookup visits E + W bindings rather than the entire routing table. Exact and
wildcard bindings remain in registration order. Tenant, header, priority,
condition, disabled-route and destination-deduplication behavior is preserved.
Add updates the index; removal rebuilds it under the existing lock. Removal
therefore remains linear and incurs additional index allocation. Wildcard-heavy
tables still require a scan; a token trie should be evaluated separately.

Local routing-only benchmark, Linux amd64, Intel i9-13900K, Go 1.27.1,
200 ms per case, one run before and after:

| Exact bindings | Before ns/op | After ns/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: |
| 1 | 578.4 | 608.1 | 2 | 2 |
| 100 | 28,159 | 652.5 | 200 | 2 |
| 10,000 | 2,359,385 | 603.3 | 20,000 | 2 |

This measures one matching destination, no network or storage, and does not
establish end-to-end throughput or performance relative to another broker.
Reproduce with:

```sh
go test -run '^$' -bench '^BenchmarkRouterExactSubjects$' -benchmem -count=5 .
```

## Delivery stages

Implemented in the next pass:

- Retain route strategy instances so round-robin, weighted distribution and
  sticky bindings continue across messages. Weighted membership/weight changes
  reset their cycle; tenant filtering also runs when no route matches.
  Sticky bindings default to a FIFO limit of 65,536 sessions, configurable with
  `max_sticky_bindings`; evicted sessions are assigned again on their next message.
- Make shared strategy selection safe from candidate-slice mutation and protect
  fallback selection during task-router reload.
- Recover producer shard placement from committed WAL state, pin sequenced
  producers across key changes, and reject incompatible cross-shard streams.
  Competing retries, restart, batch variants and legacy placement are tested.
- Synchronize reply delivery with waiter cancellation; reject active correlation
  collisions, clean up canceled/timed-out streams and release waiters on shutdown.
- Add the [transactional SQLite outbox](TRANSACTIONAL_OUTBOX.md), with application
  transaction rollback, restart, retry and worker-fencing tests.

These changes advance stages 1, 3 and 4. They do not complete all stage acceptance
criteria. Atomic stream offset-plus-produce is now implemented within one store
or WAL shard; see [stream transactions](STREAM_TRANSACTIONS.md) for support,
concurrency and retry guarantees. Protocol interoperability, distributed daemon
wiring, cross-shard transaction coordination and release qualification remain
outstanding.

| Stage | Work | Acceptance evidence |
| --- | --- | --- |
| 1. Correctness baseline | Audit publish confirms, crash recovery, lease fencing, retry/DLQ, producer deduplication and stream offsets across supported stores. Reconcile tracker claims with tests. | Restart and injected-storage-failure tests prove acknowledged messages survive at the promised durability level; rejected mutations do not become visible. |
| 2. Performance baseline | Measure publish, consume and round trip separately for queues, streams and request/reply. Include slow consumers and overload. Profile locks, allocations, batching and disk sync. | Reproducible commands, hardware, payload size, concurrency, durability, p50/p95/p99 latency, throughput, RSS and backlog growth. Use bounded runs and report errors. |
| 3. Durable messaging | Close verified gaps in producer identity persistence, transactional consume-offset-plus-produce, retention, replay and bounded backlog. Define atomicity for each store/shard configuration. | Crash/retry tests prove the documented transaction and deduplication boundaries; retention never invalidates committed state silently. |
| 4. Low-latency delivery | Extend indexed routing where profiles justify it; verify request/reply cancellation, queue-group fairness, slow-consumer policy and bounded flow control. | Mixed-subject and fanout load tests; bounded memory and inflight work under overload; ordering and fairness regressions covered. |
| 5. Client interoperability | Maintain a method-by-method protocol matrix. Test AMQP and MQTT with named third-party clients. Treat NATS and Kafka wire protocols as separate projects requiring explicit versions and supported operations. | Automated client suites for connect/auth, publish, confirms, subscribe, acknowledgements, reconnect and unsupported-operation errors; no compatibility claim without passing evidence. |
| 6. Distributed operation | Wire the library cluster into a runnable daemon, then implement/verify membership, partition placement, leader routing, quorum durability, fencing and recovery. | Multi-process tests cover leader loss, network partitions, restart, catch-up and membership changes. Multiple standalone daemons do not count as a cluster. |
| 7. Release qualification | Exercise backup/restore, upgrades, security review and sustained load. | Complete the existing [release gates](RELEASE_GATES.md) with artifacts for the release commit. |

Stages cover all three requested workloads. Correctness and measurement precede
new guarantees; client interoperability and distributed replication each need
their own implementation and verification effort. Current protocol scope is
recorded in [PROTOCOL_COMPATIBILITY.md](PROTOCOL_COMPATIBILITY.md).
