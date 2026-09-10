# Release gates

The repository is pre-v1 and experimental. A stable public release is blocked
until every mandatory gate below is green and a license has been selected by
the project owner.

## Mandatory correctness gates

- All four Go modules pass tests and vet on current stable and oldstable Go.
- The root broker, cluster, protocol, client, and pool packages pass `-race`.
- WAL recovery, acknowledgement, transactions, authorization, and replication
  state transitions each have at least 90% coverage; core overall has 80%.
- Twelve-hour nightly fuzzing and a 72-hour mixed-workload release soak pass.
- Fault tests prove no acknowledged loss inside the declared fault model, epoch
  fencing, monotonic offsets, atomic broker transactions, at-least-once queue
  delivery, and tenant isolation.

## Mandatory protocol gates

- HTTP has a versioned OpenAPI contract.
- Native protocols have versioned wire specifications and malformed-frame tests.
- AMQP 0-9-1 and MQTT 3.1.1 pass standard-client interoperability suites.
- gRPC uses protobuf over HTTP/2; native JSON/TCP is not called gRPC.
- WebSocket implements RFC 6455; SSE is documented as a separate transport.

## Mandatory security and supply-chain gates

- No unresolved critical or high-severity vulnerability or independent audit finding.
- Production mode refuses anonymous operation and unsafe plaintext credentials.
- Tenant authorization is enforced on every data and control-plane operation.
- Release artifacts are reproducible, signed, checksummed, scanned, and include an SBOM.

## Mandatory performance gate

Equivalent single-node and three-node workloads must be published against Kafka,
NATS JetStream, and RabbitMQ. At least one declared broker profile must lead each
selected workload/metric category. Results that lose must remain in the report.

## Publication blockers

- The owner has not selected a project license. No stable public release may be
  published until the chosen license file is committed.
- Machine-specific `replace` directives are forbidden in release commits.
