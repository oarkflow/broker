# Complete runnable examples

These examples exercise complete workflows and return a non-zero exit code if
an expected broker guarantee is not observed. Run them from the `examples`
module:

```bash
cd examples
go run ./complete embedded
go run ./complete durable
go run ./complete distributed
```

## Embedded

The embedded flow demonstrates competing queue workers using round-robin
selection, manual acknowledgements, producer idempotency, atomic multi-message
publishing, batch consumption, exact and wildcard topic subscriptions, topic
fan-out, stream reads and committed offsets, and task progress/results.

```bash
go run ./complete embedded -timeout 15s
```

## Durable restart

The durable flow writes a queue and stream message to the file WAL, shuts down,
opens a new broker on the same directory, verifies recovery, and verifies that
the producer sequence is still deduplicated after restart.

```bash
data="$(mktemp -d)"
go run ./complete durable -data "$data"
```

When `-data` is omitted the example uses and removes a temporary directory.

## Three-node etcd/Raft cluster

The distributed flow starts three real TCP Raft transports, waits for leader
election, replicates a queue declaration and message, and waits until every
node has applied the message. It binds loopback without TLS solely to keep the
example self-contained; use TLS and fixed addresses for deployments.

```bash
go run ./complete distributed
go run ./complete distributed \
  -addresses 127.0.0.1:19101,127.0.0.1:19102,127.0.0.1:19103 \
  -data ./.broker-data/complete-cluster
```

The example enables follower-local reads only so it can assert convergence.
Production callers should leave `AllowStaleReads` disabled unless stale reads
are explicitly acceptable.

## Verify all examples

```bash
./complete/check.sh
```

## Separate broker, consumer, and publisher

The primary example uses three independent programs and the authenticated HTTP
data plane:

```bash
go run ./complete/broker -addr 127.0.0.1:18091 -data ./.broker-data/complete
go run ./complete/consumer -url http://127.0.0.1:18091
go run ./complete/publisher -url http://127.0.0.1:18091
```

The consumer covers queue, topic, request/response, and task handlers. The
publisher waits for consumer discovery, publishes queue/topic/stream messages,
performs request/response, waits for a task result, and commits its stream
offset. `check.sh` builds and orchestrates all three automatically.
