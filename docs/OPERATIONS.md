# Operations Runbook

## Deployment baseline

- Use the durable profile and segmented WAL on a dedicated, monitored volume.
- Require TLS or mTLS and named least-privilege credentials. Never enable
  `--insecure-dev` outside an isolated development machine.
- Set explicit message, request-body, queue-depth, concurrency, and retention
  limits. Alert before disk or file-descriptor exhaustion.
- Distributed nodes require distinct durable `WALDir` paths and an odd-sized
  quorum. Do not share a writable data directory between nodes.

## Backup and restore drill

1. Stop writes or take an application-consistent volume snapshot.
2. Copy the WAL directory while preserving permissions and fsync the target.
3. Verify BWL data with `broker-wal -path <wal-file>` before accepting backup.
4. Restore into a new empty directory; never overlay an active broker.
5. Start one isolated node, verify queue counts, stream offsets, producer
   fencing, and a publish/consume/ack cycle.
6. Record commit, format version, checksums, timestamps, and drill results.

For pre-v1 format conversion, export to NDJSON and import into a separate path.
Keep the original immutable until the restored broker passes validation.

## Rolling upgrade

1. Run `brokerctl cluster upgrade-plan <tag>` (or request the authenticated
   `GET /cluster/upgrade-plan?target_version=<tag>`) and resolve every reported
   blocker. Follow its recommended peer order only after the manual
   compatibility checks are complete; the plan never asserts that mixed
   versions or storage formats are compatible.
2. Query the authenticated `GET /cluster/nodes` endpoint and record each
   node's `metadata.broker_version`. If a node is unreachable, run
   `brokerd -version` inside its deployment to inspect the binary directly.
   This is build inventory; it does not establish protocol or storage
   compatibility.
3. Back up every node and verify the target version understands the current
   storage format.
4. Confirm quorum health and zero under-replicated partitions.
5. Drain and upgrade one follower. Wait for it to rejoin and catch up.
6. Repeat for remaining followers.
7. Use `brokerctl cluster leadership-transfer <peer-id>` to move leadership
   away from the final old-version leader, then upgrade it.
8. Run publish, consume, lease-expiry, and failover probes before completion.

Do not mix versions when a release changes Raft commands or storage encoding
without an explicitly documented compatibility window.

## Rollback

Stop the rollout if health fencing, recovery, conformance, or error-rate checks
fail. Roll back only when the older binary supports every storage/command format
already written. Otherwise restore the pre-upgrade backup into an isolated
cluster and redirect traffic after validation.

## Required alerts

- readiness/health false or Raft fatal error;
- no leader, leader churn, quorum loss, or replication lag;
- WAL fsync latency/errors, disk utilization and inode exhaustion;
- queue depth, oldest-message age, lease expiry/redelivery, and DLQ growth;
- publish/consume error rate and p95/p99 latency;
- authentication failures, rejected authorization, and rate-limit saturation.

## Capacity and reliability evidence

Run `scripts/benchmark_parity.sh` on named hardware for capacity planning. Run
`DURATION=72h scripts/soak.sh` for release reliability evidence. Preserve the
generated artifact directory with the release record; shortened runs are smoke
tests and do not satisfy the soak gate.
