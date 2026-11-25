# fastworker Broker (pre-v1)

An experimental message broker for Go with embedded, single-node, and distributed prototypes. Correctness, durability, protocol conformance, and security hardening are still in progress.

## What This Is

| Capability | Status |
|------------|--------|
| Embedded broker (in-process) | Pre-v1; functional, hardening in progress |
| Standalone broker (HTTP/native server) | Pre-v1; functional, hardening in progress |
| Distributed broker (multi-node) | Pre-v1 etcd/raft implementation; release gates remain |
| Encryption at rest (AES-GCM) | Optional, configurable |
| Encryption in transit (TLS/mTLS) | Listener-dependent; conformance in progress |

> **Release status**: No current mode has a stable production-support contract. See the [release gates](docs/RELEASE_GATES.md) and [pre-v1 migration notes](docs/MIGRATION_PRE_V1.md).

The [enhancement plan](docs/ENHANCEMENT_PLAN.md) covers durable messaging,
low-latency delivery, client interoperability, and the measurements required
to validate improvements.

For publishing atomically with an application database update, see the
[transactional SQLite outbox](docs/TRANSACTIONAL_OUTBOX.md) and its runnable example.

[Atomic stream transactions](docs/STREAM_TRANSACTIONS.md) commit a consumer offset
and output messages together through the embedded API, HTTP, or Go client.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Running the Broker](#running-the-broker)
- [Complete Examples](#complete-examples)
- [Installation](#installation)
- [Broker Modes](#broker-modes)
- [Features](#features)
- [Protocols](#protocols)
- [Performance](PERFORMANCE.md)
- [Encryption](#encryption)
- [Security](#security)
- [Configuration](#configuration)
- [CLI Usage](#cli-usage)
- [HTTP API](#http-api)
- [Publisher](#publisher)
- [Consumer](#consumer)
- [Routing](#routing)
- [Queues](#queues)
- [Streams](#streams)
- [Retries and DLQ](#retries-and-dlq)
- [Tasks](#tasks)
- [Multi-Tenancy](#multi-tenancy)
- [Federation and Shovel](#federation-and-shovel)
- [Partitioning](#partitioning)
- [Schema Evolution](#schema-evolution)
- [Observability](#observability)
- [Production Checklist](#production-checklist)
- [Package Layout](#package-layout)

---

## Quick Start

```bash
# Install
go get github.com/oarkflow/broker

# Run tests
go test ./...
go test -race ./broker
go vet ./...
```

### Embedded (in-process)

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/oarkflow/broker"
)

func main() {
    ctx := context.Background()
    b := broker.MustNew(broker.Config{
        DefaultQueue: "jobs",
        DefaultVisibility: 30 * time.Second,
    })
    b.Start(ctx)
    defer b.Stop(context.Background())

    b.Subscribe(ctx, broker.ConsumerConfig{
        Name: "worker", Queue: "email.send", AckMode: broker.AckAuto,
    }, func(ctx context.Context, d broker.Delivery) error {
        fmt.Println("received:", string(d.Message.Payload))
        return nil
    })

    b.Publisher().Publish(ctx, "email.created", map[string]string{
        "to": "user@example.com",
    }, broker.WithQueue("email.send"))
}
```

### Standalone (HTTP server)

```bash
# Start broker
export BROKER_TOKEN='your-secret-token'
go run ./cmd/brokerd -addr :8090 -wal ./.broker-data -token "$BROKER_TOKEN"

# Publish a message
curl -X POST -H "Authorization: Bearer $BROKER_TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/publish \
  -d '{"topic":"events","queue":"jobs","payload":{"task":"send-email"}}'

# Pull a message
curl -X POST -H "Authorization: Bearer $BROKER_TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/queues/jobs/pull \
  -d '{"consumer_id":"worker-1","limit":1}'
```

## Running the Broker

This is the complete operating guide: build, start, publish, consume, run tasks,
and inspect everything. Every command here has been run against a real broker.

### Build the binaries

All binaries live in the `cmd` module and build from its own `go.mod`:

```bash
cd cmd
go build -o /tmp/brokerd    ./brokerd        # broker + admin API + console
go build -o /tmp/brokerctl  ./brokerctl      # CLI / TUI operator client
go build -o /tmp/broker-worker ./broker-worker # remote worker process
cd ..
```

### Start the broker

Two modes. Pick one.

**Local development — plain HTTP, no authentication:**

```bash
/tmp/brokerd --insecure-dev -addr 127.0.0.1:8090 -wal /tmp/broker-data -store wal
```

`--insecure-dev` disables authentication entirely: every request is treated as an
admin with full permissions. Local use only. Anything reachable from a network
must not use it.

**Production-shaped — TLS and a bearer token:**

```bash
/tmp/brokerd \
  -addr 127.0.0.1:8090 \
  -token "$BROKER_TOKEN" \
  -tls-cert cert.pem -tls-key key.pem \
  -wal /var/lib/oarkflow -store wal
```

`brokerd` refuses to start unless one of these is true:

- `--insecure-dev` is set, **or**
- `-token` is set, **and**
- TLS is configured via `-tls-cert` and `-tls-key`

Both must be supplied together. Skipping TLS without `--insecure-dev` is a
deliberate startup failure, not a warning.

**Useful flags:**

| Flag | Default | Meaning |
| --- | --- | --- |
| `-addr` | `:8090` | admin API, data plane and console listen address |
| `-wal` | *(empty)* | WAL directory. Empty uses an in-memory store |
| `-store` | auto | `memory`, `wal`, `wal2` (segmented), or `sqlite` |
| `-wal-shards` | `1` | independent binary WAL shards |
| `-profile` | `durable` | `durable`, `latency`, `throughput`, `lightweight` |
| `-queue` | `default` | default queue name |
| `-token` | *(empty)* | admin bearer token |
| `-read-token` | *(empty)* | additional read-only token |
| `-worker-token` | *(empty)* | token for remote workers |
| `-publisher-token` | *(empty)* | publish-only token |
| `-quiet` | `false` | suppress live broker event logs |
| `-max-body` | `1MiB` | maximum HTTP request body |

Every flag also has an environment variable equivalent (`BROKER_ADDR`,
`BROKER_TOKEN`, `BROKER_WAL`, and so on).

### Experimental distributed mode

`brokerd -cluster` starts the existing etcd/raft distributed broker. It requires
at least three nodes (an odd-sized peer set), a durable message WAL, a separate
Raft WAL directory, and mutual TLS on peer connections. Give every node the same
complete peer map and a certificate signed by the configured peer CA, with the
node addresses covered by certificate SANs. The shared `-require-client-cert`
setting also requires client certificates on TLS-enabled client listeners. For
example, the node with ID `1`:

```bash
/tmp/brokerd \
  -cluster \
  -cluster-node-id 1 \
  -cluster-listen 10.0.0.11:7000 \
  -cluster-peers '1=10.0.0.11:7000,2=10.0.0.12:7000,3=10.0.0.13:7000' \
  -cluster-wal /var/lib/oarkflow/raft \
  -wal /var/lib/oarkflow/messages -store wal2 \
  -token "$BROKER_TOKEN" \
  -tls-cert node-1.pem -tls-key node-1-key.pem \
  -client-ca cluster-ca.pem -require-client-cert
```

Use unique message and Raft directories on each node; never share either
directory between nodes. Cluster mode is still pre-v1 and experimental. The
etcd/raft library path shares a log between broker data and controller metadata
and exposes `DistributedBroker.ControllerQuorum()`. `brokerd` exposes
authenticated controller placement and partition route APIs. Topic and stream
partitions can run as independent Raft groups. Before changing a replica set,
`GET /cluster/partitions/{name}/{partition}/reassignment-plan` and
`pkg/client.Client.PlanPartitionReassignment` provide a read-only preflight
showing target nodes, learner/catch-up steps, leader transfer, removals, and
blockers, including missing advertised client addresses.
`GET /cluster/nodes/{peer_id}/removal-plan` and
`pkg/client.Client.PlanRaftNodeRemoval` list every partition still assigned to
that controller peer's logical node and report controller leadership, pending
joint changes, and whether this broker can propose removal. The node must have
no assignments and be marked `left` before voter removal; `left` is terminal,
and its metadata record stays until the Raft peer is removed so another logical
node cannot claim the same address during the drain. The removal guard requires
leadership to move first. `POST /cluster/nodes/{peer_id}/drain` and
`pkg/client.Client.BeginRaftNodeDrain` mark the logical node `draining`, which
blocks new assignments while existing partitions are moved. Once assignments
are gone, `POST /cluster/nodes/{peer_id}/drain/complete` and
`pkg/client.Client.CompleteRaftNodeDrain` commit `left`; the controller state
machine checks assignments again when applying that transition. Operator-
requested reassignment remains an explicit step; the opt-in daemon sweep can
replace replicas after a node enters `draining`, `failed`, or `left`.
`GET /cluster/upgrade-plan?target_version=<tag>` and
`pkg/client.Client.PlanClusterUpgrade` report the broker-version inventory,
control-plane blockers, and a deterministic order with learners and follower
voters before the controller leader. Stable `vMAJOR.MINOR.PATCH` downgrade
targets are blocked; non-release version strings are left for manual ordering.
`state_preflight_ready` only means the state preflight passed, including fresh
live progress for every assigned partition replica; `compatibility_verified`
remains false, and the response lists the storage, protocol, health, and backup
checks operators must complete before rollout.
Library users that implement durable transaction participants can provide a
`cluster.TransactionParticipantResolver` through
`DistributedConfig.TransactionResolver`. The distributed broker then sweeps
recovery records on the controller leader every five seconds by default, and
joins the recovery loop before closing local partition stores during shutdown.
Partition state machines can durably stage atomic appends and stream offsets;
`NewPartitionAppendTransactionParticipant` and
`NewPartitionOffsetTransactionParticipant` create participants, and
`DistributedBroker.TransactionParticipantResolver` resolves their durable
decisions. Prepare requests carry a digest of their original payload; the
partition leader reuses the durable staged result on retry and rejects a
changed payload for the same transaction participant. The prepare endpoint
also verifies that the coordinator record is undecided, unexpired, and includes
that participant. After execution, append participants expose their prepared
output messages through `broker.PartitionTransactionResult`. Callers must still
perform normal broker authorization, schema, rate, and tenant-quota checks
before creating append participants. Proposals route to the assigned partition
leader's registered HTTPS `ClientAddress` using the broker's mTLS client
certificate. The receiving admin API maps that certificate to a principal with
`cluster:write`; configure the node certificate identity with
`-client-cert-identities` and grant it `-client-cert-permissions cluster:write`.
The built-in resolver participates in the distributed broker's leader-only
recovery loop. `Broker.CommitOffsetAndAppend` orchestrates this path for one
assigned destination per output entry; it rejects fanout,
archive/dead-letter destinations, unassigned destinations, and output to the
source checkpoint partition.
Read-only plans do not execute reassignment. The opt-in daemon sweep advances
the durable catch-up and metadata switch-over protocol for replicas on nodes
marked `draining`, `failed`, or `left`; it does not initiate node drains or
complete controller-peer removal. Full cluster failure certification,
distributed consumer-offset coverage, and release validation remain open. Do
not treat this as a production-ready high-availability cluster.
Partition reconciliation closes a stale local partition group after the local
peer has been removed from that group's Raft membership; while the peer remains
a voter or learner, reconciliation keeps the group and reports the pending
safe-removal requirement.
The library now provides `cluster.RaftMultiplexer` and `cluster.RaftGroupHost`
for independent etcd/raft controllers. In etcd/raft mode,
`DistributedBroker.AddRaftGroup` also registers application state machines on
the broker's authenticated shared listener, with one durable namespace per
group. Groups can be added or removed while the listener runs. The broker data
plane routes topic and stream appends to assigned partition groups. The library
rejects multi-node etcd/raft controllers or application groups unless mutual
TLS peer identity validation is enabled.
`DistributedBroker.ReconcilePartitionGroups` can provision missing local groups
from a quorum-confirmed, consistent topology snapshot when node registry
addresses match authenticated controller peers. Its state-machine factory must
choose whether each local copy bootstraps or joins existing durable history.
Set `DistributedConfig.PartitionStoreFactory` and pass a nil state-machine
factory to have reconciliation open, adapt, own, and close one isolated
snapshot-capable broker store per local partition. For durable local state,
`NewSegmentedWALPartitionStoreFactory(root, cfg)` supplies stable BWL2
directories keyed by a hash of the resource kind, name, and partition; keep
`root` separate from the broker-wide message store directory.
`brokerd -cluster` configures this factory automatically and retries local
partition reconciliation every five seconds by default. Set the separate
store root with `-cluster-partition-wal` and the retry interval with
`-cluster-reconcile-interval`; each broker reconciles after assignments exist.
Automatic replacement of replicas on nodes marked `draining`, `failed`, or
`left`, and healthy replica moves when one node has at least two more assigned
replicas than an eligible target, are opt-in through
`-cluster-auto-reassign-interval` (or
`BROKER_CLUSTER_AUTO_REASSIGN_INTERVAL`). Each broker advances only the
partition groups it currently leads, using the durable learner catch-up,
in-sync replica, leadership transfer, and cleanup steps. Healthy balancing
keeps the current partition leader in place and moves one non-leader replica at
a time. Fresh complete per-partition workload snapshots can also trigger one
move per sweep when the projected source/target workload gap improves by at
least 100 message/lease units and 20%; complete Raft progress snapshots prefer recent,
in-sync targets with lower lag. Missing, stale, or partial signals disable
telemetry-based moves for that sweep. The default interval is zero, which leaves
reassignment operator-driven. When every store reports its partition size,
disk balancing also moves a non-leader replica only if its source is at least
ten utilization points fuller, projected source/target utilization skew
improves, and the target retains a 20% reserve over the incoming partition
size.
It refuses to change an existing group's membership and reports local groups
that metadata has unassigned. Existing groups must have the assigned voter set
and leader, match the configured replication factor, and must not have a joint
membership change pending; reconciliation does not move existing messages.
With a managed partition store, topic and stream appends route through the
assigned Raft group, and atomic appends are accepted only when all entries
target that same partition. Set `WithPartition(n)` on `Publisher.Publish` or
`pkg/client.Client.Publish` to target a nonzero partition; the HTTP
`/v1/publish` payload also accepts `partition`. `ListTopic` and `ReadTopic` use each local partition group's
linearizable ReadIndex and merge messages in stable creation-time order.
`ReadTopic` scans with bounded pages, so a small result limit does not load all
retained topic data. `GET /v1/topics/{topic}/partitions/{partition}/messages`
reads one partition, and `pkg/client.Client.ReadPartitionTopic` resolves its
advertised leader endpoint and requests it. The client sends credentials only
when that endpoint's origin is in its configured broker URLs.
`pkg/client.Client.ListTopic` fans out a bounded first-page read across all
assigned partitions and merges the results. `pkg/client.Client.ReadTopicPage`
continues from an opaque per-partition cursor, rotates the round-robin starting
partition between pages, and advances only through messages returned on that
page. Stream assignments use `resource_kind: "stream"`; a same-named topic has
a separate group identity. `pkg/client.Client.ReadStreamPage` uses a separate
local offset per assigned partition and can read from advertised leader
endpoints via `GET /v1/streams/{stream}/partitions/{partition}/messages`. The
partition-scoped offset APIs use
`/v1/streams/{stream}/partitions/{partition}/commit` and `/offset`, with
`pkg/client.Client.CommitPartitionStreamOffset` and
`PartitionStreamOffset`. Atomic checkpoint plus append is available through
`POST /v1/streams/{stream}/partitions/{partition}/commit-and-append` and
`pkg/client.Client.CommitPartitionStreamOffsetAndAppend` when every output
targets that same assigned stream partition; pass a transaction ID to dedupe
retries. `Broker.CommitOffsetAndAppend` also coordinates a partitioned source
checkpoint with outputs across assigned topic, stream, and queue partitions
through the controller quorum. This path requires a transaction ID and exactly
one assigned topic, stream, or queue destination per output entry; archive and
dead-letter destinations are unsupported. It rejects output to the source
checkpoint partition or any mix of assigned and unassigned destinations. Its
source-partition outcome cache returns full messages while retained and compact
metadata after output cleanup. Prepared appends to bounded assigned queues
reserve capacity in the partition state machine. Normal appends account for
those reservations, abort releases them, commit consumes them atomically, and
partition snapshots rebuild reservations from staged transactions. A prepare
that cannot reserve every required slot is rejected and the coordinator aborts.
The coordinator deadline uses `DistributedConfig.TransactionTimeout` (30 seconds
by default).
Legacy scalar consumer-group offsets and stream lag/head-offset
reports remain unsupported for assigned streams. Per-partition lag reports are
available at `/v1/streams/{stream}/partitions/{partition}/lag`. The aggregate
`GET /v1/streams/{stream}/lag?consumer_group={group}` endpoint gathers reports
from assigned partition leaders and returns total lag plus each partition's
local head and committed offsets; it requires a globally scoped principal.
Partial results return HTTP `207 Multi-Status` with successful partition reports
and a failure map; `total_lag` sums the successful reports. The Go client's
`StreamLagAssigned` calls this endpoint, validates its partition reports and
aggregate total, and returns partial data alongside an error.
Queue assignments use
`resource_kind: "queue"`; enqueue, lease, ack, nack, and lease extension run in
the assigned queue Raft group. `pkg/client.Client.PollPartition` resolves the
leader and returns partition metadata that its Ack/Nack/Extend methods reuse.
Queue configuration is synchronized into each partition group before queue
operations. Partition-local listing, pause/resume, and purge use
`/queues/{queue}/partitions/{partition}/...` and the corresponding Go client
methods. `pkg/client.Client.PollAssignedQueue` discovers assignments, rotates
the initial per-partition lease budget, then redistributes unused capacity to
partitions that filled their share. Follow-up polls do not wait, aggregate
leases stay within the requested limit, and partial successful leases are
returned alongside errors from failed partitions. `ListAssignedQueue` fetches a bounded share from each partition and
sorts the selected results by creation time; it is not a cursor-based global
FIFO page. The server-side `GET /queues/{queue}/assigned/messages?limit={n}`
endpoint performs the same bounded fan-out over authenticated broker identities
and returns completed and failed partitions with the message sample; partial
results use HTTP `207 Multi-Status`;
`ListAssignedQueueDetailed` exposes that report through the Go client. The
endpoint rejects tenant-scoped credentials because it aggregates globally.
`POST /queues/{queue}/assigned/{pause|resume|purge}` coordinates all assigned
partitions through one durable transaction and returns its transaction ID,
completed and failed partitions, and exact per-partition purge counts. Every
partition prepares before the coordinator records commit; HTTP `207 Multi-Status`
reports a failed operation. After commit, partition effects can become visible
at slightly different times while participant resolution catches up. Purge
counts come from each partition leader's committed state-machine apply result.
`PauseAssignedQueue`, `ResumeAssignedQueue`, and
`PurgeAssignedQueue` use these server-side routes. Assigned, partition, and
scalar pause/resume/purge routes reject tenant-scoped credentials because they
change shared queue state. Scalar APIs return
`unsupported_mode` instead of reading or changing the broker-wide queue.
Replication factor defaults to three. Never share one partition store across groups:
restoring one group's snapshot replaces that store's state.

The `cluster.ControllerQuorum` library API, available from
`DistributedBroker.ControllerQuorum()`, can persist two-phase transaction
participant sets, prepare votes, commit/abort decisions, deadlines, and
participant resolution markers. `ExecuteTransaction` sequences participant
preparation, the durable decision, and recovery; participants must stage work
by transaction ID and make prepare/commit/abort idempotent. Participants prepare
in ID order and commit in reverse order; a failed commit stops later dependent
effects until recovery retries it. `ExecuteTransactionWithFingerprint` also
binds a request digest to the transaction ID so changed-input retries fail
before participant preparation. `RecoverTransactions`
can also be called after restart to abort expired undecided transactions and
retry durable decisions through the participants. The coordinator shares the
etcd/raft log and powers multi-partition `Broker.CommitOffsetAndAppend` for
assigned output partitions. That broker path requires a transaction ID, one
assigned destination per output, and outputs distinct from the source
checkpoint partition; mixed assigned/unassigned outputs remain unsupported.
The controller decision is durable and recovery retries participant resolution,
but each partition applies that decision independently, so readers can briefly
observe outputs from only some participants after commit while recovery is
finishing. The source checkpoint participant resolves last, preventing the
checkpoint from advancing before output participants have committed.

The authenticated controller API exposes coordinator records through
`GET /cluster/transactions?id={id}` and cursor-based pages through
`GET /cluster/transactions?after={cursor}&limit={n}`. The Go client provides
`GetClusterTransaction` and `ListClusterTransactionsPage`. To limit retained
history, `POST /cluster/transactions/prune` accepts an explicit `before`
timestamp and optional `limit`; it deletes only decided transactions whose
participants are all resolved. Use globally unique transaction IDs for at
least as long as records are retained: after a record is pruned, reusing its ID
starts a new coordinator transaction.

`GET /cluster/status` reports the current leader, term, commit/applied indexes,
joint voter groups, per-peer replication lag when served by the leader, and
bounded controller metadata and transaction recovery counts. It also reports
`local_partition_progress`: snapshots from partition groups led by the broker
serving the request, with per-replica match index, lag, recent activity, and
ISR eligibility. Query each broker to see its locally led partitions; followers
do not claim to know the leader's full peer progress. The authenticated daemon
admin API also exposes controller placement metadata:

- `GET /cluster/partitions` and `GET /cluster/partitions/{name}/{partition}`
  read assignments; pass `resource_kind=stream` for stream resources (topic is
  the default).
- `POST /cluster/partitions` assigns replicas and a leader using `name`,
  `resource_kind` (`topic`, `stream`, or `queue`), `partition`, `replicas`,
  and `leader`.
  Set `auto_assign: true` to choose active replicas and a leader. Set
  `metadata.failure_domain` when registering nodes to spread replicas across
  racks or zones. Distinct configured domains are preferred before reusing a
  domain; missing labels leave those nodes unconstrained. A node's failure
  domain must be valid UTF-8 without control characters and at most 256 bytes.
  Set optional `metadata.capacity_weight` to a decimal value from 1 through
  1000 (default 1) to describe relative node capacity; higher weights receive
  proportionally more partitions when balancing normalized replica/leader
  counts. This is an operator-supplied weight, not a live disk or workload
  measurement.
  It cannot change while the node hosts an assigned replica; reassign its
  partitions first. Assignments serialize at the controller quorum and retries
  return an existing assignment unchanged.
  Legacy `topic` assignments remain supported and default to kind `topic`.
- `GET /cluster/partitions/{name}/{partition}/placement-plan` previews the
  deterministic load-aware initial assignment. It considers registered active
  nodes authenticated as distinct Raft voters with advertised client addresses,
  prefers distinct `metadata.failure_domain` values, then balances replica
  counts relative to `metadata.capacity_weight`, prefers candidates with known,
  recent Raft progress and lower controller-log lag. When every active
  candidate is reachable through the configured broker mTLS client and each
  assigned partition has a complete leader progress report, it also prefers
  replicas with lower maximum observed partition lag and recent activity.
  Replicated partition-store message and lease counts provide additional
  tie-breakers when every candidate reports them. The default `brokerd`
  partition WAL root reports logical bytes used and filesystem capacity
  available; custom stores can implement `StorageUsageReporter` or set
  `DistributedConfig.PartitionStoragePath`. After replica health and lag, the
  planner prefers candidates with more available bytes. The plan reports
  whether live lag, workload, and disk capacity signals were used, plus
  per-node measurements with observation timestamps; workload and disk
  counters may be cached for up to one second, while Raft progress is fetched
  for each plan. Partial or unavailable
  telemetry is discarded. Placement does not
  move existing partitions. The internal signal request uses broker mTLS and
  the remote certificate principal needs `cluster:read`; if it cannot collect
  every active candidate's report, the plan identifies the omitted live signal
  and falls back to controller progress. Explicit distributed assignments
  also require client addresses on every replica so any replica can become a
  reachable leader.
- `PUT /cluster/partitions/{topic}/{partition}/isr` updates the `isr` list.
- `POST /cluster/partitions/{topic}/{partition}/epoch` advances the fencing
  epoch and optionally changes the `leader`; stream routes use the same paths
  with `resource_kind=stream`.
- `GET /cluster/metadata/nodes` lists the replicated logical node registry;
  `POST` registers or updates a node using `id`, `address`, optional `state`,
  optional `client_address` (an HTTP(S) base URL), and optional `metadata`.
  Omitting `metadata` on update preserves existing labels; send an empty object
  to clear them when policy permits. The endpoint
  `DELETE /cluster/metadata/nodes/{id}` removes a node that has no partition
  assignments and no Raft membership. The `brokerd -cluster` controller leader
  registers current controller Raft peers, including learners added after
  startup, as `joining`
  nodes, preserving existing lifecycle states and reusing an existing logical
  ID when its address is already registered.
  Operators must mark a node `active` before assigning replicas to it.
  Distributed partition assignment also requires a `client_address` on every
  replica; an assigned node cannot clear its client address.
  Partition route responses include the assigned leader's
  `leader_client_address` when registered; `pkg/client.Client.PartitionLeaderURL`
  validates and returns it. Valid states are `joining`, `active`,
  `draining`, `left`, and `failed`; omitted state defaults to `joining` for a
  new node and preserves the current state when updating one. These records do
  not add or remove Raft voters. Assignments cannot add registered draining or
  failed nodes, and only active registered Raft voters can receive a replica or
  leader assignment; learners and existing placements remain unchanged until
  an explicit safe reassignment protocol is available.

These routes replicate controller metadata. Partition writes and partition
read endpoints route to the assigned group leader; the Go client validates the
advertised client origin before forwarding credentials. Reads require
`cluster:read` or `admin:*`; writes require
`cluster:write` or `admin:*`. Tenant-scoped credentials cannot read or mutate
global cluster metadata. Partition reads use a quorum-confirmed Raft ReadIndex.
Followers forward the barrier to the known leader and wait for local
application; a node with no known leader returns a retryable leader hint rather
than serving stale state.
Successful etcd/raft-backed publishes report `confirmed: replicated` and
`durability: quorum_fsync`; local WAL-only publishes report `local_fsync`.

The daemon admin API can also propose a Raft membership addition:

```bash
curl -X POST "$BROKER/cluster/nodes" \
  -H "Authorization: Bearer $BROKER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"address":"10.0.0.14:7000"}'
```

The response includes the assigned `peer_id` and adds the node as a non-voting
learner. Start it with the complete peer map, its assigned ID, and
`-cluster-join-existing` (or `BROKER_CLUSTER_JOIN_EXISTING=true`) so it does not
bootstrap an independent cluster. When `/cluster/status` reports zero lag for
that peer, the leader automatically promotes it to a voter. Operators can also
request the same guarded, idempotent promotion explicitly:

```bash
curl -X POST "$BROKER/cluster/nodes/4/promote" \
  -H "Authorization: Bearer $BROKER_TOKEN"
```

The membership endpoints require `cluster:write` or `admin:*`; tenant-scoped
credentials cannot change cluster membership. Mutations must reach the current
leader; follower errors include a retryable leader hint. Promotion and voter
removal use etcd/raft's joint-consensus transition, and removed-peer transport
identities remain enabled until the automatic leave entry commits. A second
change is rejected while that transition is pending. Promotion is gated on
replication catch-up. Drain start and completion are available, while automatic
reassignment and cluster certification remain open. The Go client exposes
`AddRaftLearner`, `PromoteRaftLearner`, `TransferControllerLeadership`, and
`RemoveRaftNode` alongside the drain operations. Before
removing a leader, transfer leadership to a caught-up voter:

```bash
curl -X POST "$BROKER/cluster/leadership" \
  -H "Authorization: Bearer $BROKER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"peer_id":2}'
```

This waits until the target reports itself as leader; a lagging target is
rejected. Then remove the old leader with `DELETE /cluster/nodes/{peer_id}` and
the same permission. For library-created ops servers, health/readiness probes
stay available without credentials, while metrics, stats, and cluster state
require `OpsConfig.AuthorizeOpsRead`. Membership changes require the separate
`OpsConfig.AuthorizeClusterMutation` callback. These routes add or remove Raft
voters or transfer leadership. A controller voter cannot be removed while its
registered logical node remains assigned to any partition; move those
assignments first.

With `durable`, `latency` or `throughput` profiles, `brokerd` refuses to run on
the in-memory store. Use `-store=wal`, `-store=wal2`, `-store=sqlite`, or
`-profile=lightweight`.

For `SubscribeBatch`, profiles default to batch sizes of 32 (`durable`), 1
(`latency`), 128 (`throughput`), and 16 (`lightweight`). An explicit
`ConsumerConfig.BatchSize` overrides the selected profile. If `BatchWait` is
set, it remains the maximum collection delay while the initial wait and
adjustment speed follow the selected profile: latency starts at 50µs and
backs off quickly, durable starts at 200µs, throughput starts at 1ms and
adapts more conservatively to favor full batches, and lightweight starts at
100µs. Each profile also sets a lower wait floor. A zero `BatchWait` still
disables collection delay.

### Open the console

With the broker running, open:

```
http://127.0.0.1:8090/console/
```

- The bare host `http://127.0.0.1:8090` redirects there automatically.
- `/console` (no trailing slash) redirects to `/console/`.
- Paste the admin token and press **Connect**. The token is kept in
  `sessionStorage` for that tab only and sent as an `Authorization: Bearer`
  header — never in a URL.
- Under `--insecure-dev` there is no token; press Connect to proceed.

The console has no build step and needs no Node tooling. It is a static app
served from the binary and reads nothing server-side: every value it shows is
fetched by the browser from the same endpoints you would call with `curl`, using
your own credentials.

Views: Overview, Queues, Topics, Streams, Publish, Consumers, Tasks, Dead letters,
Routes, Cluster, Audit. It polls every 5 seconds while the **auto** box is ticked.

You can create and control from the UI: declare queues, topics and streams,
register consumers, add routes, publish messages, submit and cancel tasks, replay
dead letters, and pause or resume the broker and individual queues.

### Publish a message

```bash
curl -s -X POST http://127.0.0.1:8090/publish \
  -H 'Content-Type: application/json' \
  -d '{"queue":"orders","payload":{"id":"order-1","amount":100}}'
```

Target a topic, a stream, or a queue — or set several at once:

```bash
# Topic with retention
curl -s -X POST http://127.0.0.1:8090/publish \
  -d '{"topic":"orders.created","payload":{"id":"order-1"}}'

# Stream, for ordered replay
curl -s -X POST http://127.0.0.1:8090/publish \
  -d '{"stream":"audit","payload":{"action":"login"}}'

# Fan out to a topic and a queue in one call
curl -s -X POST http://127.0.0.1:8090/publish \
  -d '{"topic":"orders.created","queue":"orders.process","payload":{"id":"order-1"}}'
```

Batch publishing takes a bare JSON **array**, not an object with a `messages`
key. A `{"messages":[...]}` body is rejected with `400`:

```bash
curl -s -X POST http://127.0.0.1:8090/publish/batch \
  -d '[{"queue":"orders","payload":{"n":1}},
       {"queue":"orders","payload":{"n":2}}]'
```

With a token, add `-H "Authorization: Bearer $BROKER_TOKEN"`. Note that a
published `payload` is returned **base64-encoded**, because `Message.Payload` is
`[]byte`:

```bash
curl -s "$BROKER/v1/topics/events?limit=1" \
  | python3 -c 'import sys,json,base64;print(base64.b64decode(json.load(sys.stdin)[0]["payload"]).decode())'
```

### Create queues, topics, streams and consumers

Everything the console can create is also a plain HTTP call. These are the exact
bodies its dialogs send.

```bash
# Queue. Durations are nanoseconds.
curl -s -X POST $BROKER/queues -d '{
  "name": "orders",
  "max_inflight": 10,
  "max_depth": 500,
  "visibility_timeout": 30000000000,
  "ack_mode": "manual",
  "dlq": "orders.dlq"
}'

# Topic, with retention
curl -s -X POST $BROKER/topics -d '{"name":"orders.created","partitions":1,"retention":86400000000000}'

# Stream, optionally compacted
curl -s -X POST $BROKER/streams -d '{"name":"audit","partitions":1,"compact":true}'

# Consumer. The id is optional; the broker assigns one when it is blank.
curl -s -X POST $BROKER/v1/consumers/register -d '{
  "consumer_id": "worker-1",
  "group": "workers",
  "queues": ["orders","invoices"],
  "concurrency": 2,
  "max_inflight": 8,
  "ack_mode": "manual"
}'

# Route: forward matching topics to queues
curl -s -X POST $BROKER/routes -d '{
  "name": "orders-to-queue",
  "match": "orders.*",
  "mode": "publish",
  "to": [{"type":"queue","name":"orders"}],
  "enabled": true
}'
```

`GET /routes` lists routes. There is **no delete route over HTTP** — disable one by
re-adding it with `"enabled": false`. Route creation is refused on a
tenant-scoped broker, because routes are global.

The console exposes all of the above: **New queue**, **New topic**,
**New stream**, **Register consumer**, **Add route**, and a **Publish** view that
accepts any JSON payload and shows the resulting message id. The Dead letters view
has a **Replay all** action per queue.

Durations in the console accept `30s`, `5m`, `1h` and are converted to
nanoseconds before sending. The HTTP API itself takes nanoseconds.

### Read a topic

Topics keep a retained copy of what was published:

```bash
curl -s "$BROKER/v1/topics/orders.created?limit=20"

# Resume after a known message id, for scrolling
curl -s "$BROKER/v1/topics/orders.created?limit=20&after=msg-42-ab12cd34"
```

Pages never repeat or skip messages. An unknown topic returns `404` with
`topic_not_found`.

### Run a consumer

**In-process**, which is the usual embedded shape:

```go
b.Subscribe(ctx, broker.ConsumerConfig{
    Name: "email-worker", Queue: "email.send", AckMode: broker.AckAuto,
}, func(ctx context.Context, d broker.Delivery) error {
    log.Println("got", d.Message.ID, string(d.Message.Payload))
    return nil // non-nil error triggers retry, then the DLQ
})
```

**As a separate process** against a running broker:

```bash
/tmp/broker-worker \
  -broker http://127.0.0.1:8090 \
  -token "$BROKER_TOKEN" \
  -id worker-1 -group workers \
  -queue orders \
  -task invoice.send
```

`broker-worker` needs at least one of `-queue`, `-pattern`, `-task`, `-request`
or `-command`, and logs every message it handles. Once it connects you will see it
register:

```bash
curl -s $BROKER/v1/consumers \
  | python3 -c 'import sys,json;[print(c["consumer_id"],c["state"],c.get("queues")) for c in json.load(sys.stdin)]'
# worker-1 ready ['orders']
```

End-to-end check — publish three messages with no worker attached, then start one:

```bash
for i in 1 2 3; do curl -s -X POST $BROKER/publish -d '{"queue":"orders","payload":{"n":1}}' -o /dev/null; done
curl -s $BROKER/queues          # {"orders":3,...}
# start broker-worker, then:
curl -s $BROKER/queues          # {"orders":0,...}
```

Controlling a remote consumer:

```bash
CID=$(curl -s -X POST $BROKER/v1/consumers/register \
      -d '{"consumer_id":"c1","queues":["orders"],"group":"g1"}' \
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["consumer_id"])')

curl -s -X POST $BROKER/v1/consumers/$CID/pause
curl -s -X POST $BROKER/v1/consumers/$CID/drain
curl -s -X POST $BROKER/v1/consumers/$CID/resume
curl -s -X POST $BROKER/v1/consumers/$CID/ban  -d '{"reason":"misbehaving"}'
curl -s -X POST $BROKER/v1/consumers/$CID/unban
curl -s -X POST $BROKER/v1/consumers/$CID/unregister
```

The identifier is **generated by the broker**. A `consumer_id` you send is
honoured if you supply one; otherwise the server assigns its own and returns it.
Always use the returned value.

### Run a task

A task is a message with a registered handler, tracked by id, with status,
progress, result and logs. Submit one to a queue a task handler is watching:

```bash
TID=$(curl -s -X POST $BROKER/v1/tasks/submit \
      -d '{"name":"invoice.send","queue":"tasks.invoice.send","payload":{"amount":42}}' \
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')

curl -s $BROKER/v1/tasks/$TID
curl -s $BROKER/v1/tasks/$TID/result
curl -s $BROKER/v1/tasks/$TID/logs
curl -s "$BROKER/v1/tasks?limit=50"
```

A completed task looks like this:

```json
{
  "id": "task-4-1635d05f1d36",
  "name": "invoice.send",
  "status": "succeeded",
  "progress": { "percent": 50, "message": "working" },
  "result": "eyJtZXNzYWdlX2lkIjoi..."
}
```

`result` is base64, like any payload.

Task control:

```bash
curl -s -X POST $BROKER/v1/tasks/$TID/cancel   # stop a running task
curl -s -X POST $BROKER/v1/tasks/$TID/retry    # re-queue its message
```

`cancel` interrupts a running handler through its context, and the task stays
`cancelled` — a late completion cannot overwrite it. If the task has not started,
its message is acknowledged without invoking the handler. Remote workers receive
the same cancellation signal, including when it arrives just before their task
handler starts.

`retry` re-queues the underlying message with attempts and error reset. It does
so under the same task ID. Its status returns to `queued` (or `scheduled` for a
delayed task), then advances as a worker retries it; the same handle and `Wait`
call observe the eventual result. Cancelling an already-cancelled task is
idempotent. Cancelling a succeeded, failed, or dead-lettered task returns an
invalid-state error and leaves its terminal result unchanged.

Registering a task handler in Go:

```go
b.RegisterTask(ctx, "invoice.send", func(ctx context.Context, d broker.TaskDelivery) (any, error) {
    _ = d.Progress(ctx, broker.Progress{Percent: 50, Message: "charging"})
    return map[string]any{"ok": true}, nil // the returned value is the task result
}, broker.ConsumerConfig{Name: "billing", Queue: "tasks.invoice.send"})
```

### Retries and the dead-letter queue

A message is retried on nack, then dead-lettered once attempts are exhausted.
The default is 3 attempts. To watch it happen, run a worker that always fails:

```bash
/tmp/broker-worker -broker $BROKER -id w2 -group g2 -queue risky -fail
for i in 1 2 3 4 5; do curl -s -X POST $BROKER/publish -d '{"queue":"risky","payload":{"n":1}}' -o /dev/null; done
sleep 5

curl -s $BROKER/queues
# {"risky":0,"risky.dlq":5,...}

curl -s "$BROKER/dlq/risky.dlq/messages?limit=5"
# each message shows attempts=3 and last_error="forced failure"
```

Replay a dead letter back to its queue:

```bash
curl -s -X POST $BROKER/dlq/risky.dlq/replay-all
curl -s $BROKER/queues     # {"risky":5,"risky.dlq":0,...}
```

If the worker is still failing, messages will simply dead-letter again — stop the
worker before replaying if you want to see the queue refill.

### Pause, resume and queue control

```bash
# Broker-wide
curl -s $BROKER/v1/broker                                    # {"state":"running"}
curl -s -X POST $BROKER/v1/broker -d '{"action":"pause"}'
curl -s -X POST $BROKER/v1/broker -d '{"action":"resume"}'
```

Know what broker pause actually does:

- **Publishing is rejected** with `409` and `{"error":{"code":"paused"}}`.
- **In-process pool workers stop.**
- **Remote consumers are not stopped.** A `broker-worker` or HTTP poller will keep
  draining whatever is already queued.

For a hard stop, pause the queues themselves:

```bash
curl -s -X POST $BROKER/queues/orders/pause
curl -s -X POST $BROKER/queues/orders/resume
curl -s -X POST $BROKER/queues/orders/purge    # deletes every message; irreversible
curl -s "$BROKER/queues/orders/messages?limit=20"
```

### Inspect broker state

```bash
curl -s $BROKER/stats
```

Note the shape — everything is nested under `store`, and `queues`, `topics` and
`dlq` are **name → count** maps, not objects:

```json
{
  "broker_state": "running",
  "error": "",
  "store": {
    "queues": { "orders": 3, "orders.dlq": 0 },
    "topics": { "events": 5 },
    "streams": {},
    "dlq":    { "orders.dlq": 0 },
    "messages": 8, "leased": 0, "dead_letter": 0
  }
}
```

Extract a count with:

```bash
curl -s $BROKER/stats | python3 -c 'import sys,json;print(json.load(sys.stdin)["store"]["queues"]["orders"])'
```

Other endpoints:

```bash
curl -s $BROKER/healthz      # unauthenticated probe, always allowed
curl -s $BROKER/readyz       # unauthenticated probe; returns 503 when not ready
curl -s $BROKER/queues
curl -s $BROKER/topics
curl -s $BROKER/streams
curl -s $BROKER/dlq
curl -s $BROKER/cluster/status
curl -s "$BROKER/audit?limit=20"
curl -s "$BROKER/lag?stream=audit"   # the stream name is required; bare /lag returns 400
                                      # and an unknown stream returns 404
```

`/healthz` is a liveness probe. `/readyz` checks broker/store health and, in
cluster mode, requires a quorum-confirmed partition topology, all locally
assigned partition groups to be running with replica membership and leader
matching metadata, and no stale local partition groups.

There is no `/queues/{name}` detail route — queue counters come from `/stats`,
and queued messages from `/queues/{name}/messages`.

`/audit` returns an empty list unless the broker was given an audit logger;
`brokerd` does not set one.

### Use the CLI

`brokerctl` talks to a running broker. Global flags come **before** the
subcommand; subcommand flags come after.

```bash
export BROKER_ADDR=http://127.0.0.1:8090
export BROKER_TOKEN=your-secret-token     # omit under --insecure-dev

/tmp/brokerctl stats
/tmp/brokerctl queues
/tmp/brokerctl topics
/tmp/brokerctl consumers

/tmp/brokerctl publish -queue orders -payload '{"id":"order-1"}'
/tmp/brokerctl publish -topic orders.created -payload '{"id":"order-1"}'

/tmp/brokerctl pull email.send -consumer cli -group cli -limit 1
/tmp/brokerctl ack <message-id> -queue email.send -lease <lease-token> -consumer cli

/tmp/brokerctl pause  orders
/tmp/brokerctl resume orders

/tmp/brokerctl dlq                       # dead-letter summary
/tmp/brokerctl dlq-dashboard
/tmp/brokerctl dlq-replay -queue orders.dlq

/tmp/brokerctl cluster
/tmp/brokerctl cluster nodes
/tmp/brokerctl cluster upgrade-plan v1.2.3
/tmp/brokerctl cluster removal-plan 3
/tmp/brokerctl cluster leadership-transfer 3
/tmp/brokerctl tui                       # interactive terminal UI
```

Point it at a different broker with `-addr` and `-token`, again before the
subcommand:

```bash
/tmp/brokerctl -addr https://broker.internal:8090 -token "$BROKER_TOKEN" stats
```

`brokerctl` has no flag to skip TLS verification or to supply a private CA, so it
cannot connect to a broker using a certificate your machine does not already
trust. Trust the CA at the OS level, or use plain HTTP for local work.

### Verify an installation

A script that starts a broker and exercises every management surface:

```bash
bash scripts/manual_verify.sh          # 70 checks over HTTP
bash scripts/manual_verify.sh --tls    # 75 checks, also proves auth is enforced
```

It builds `brokerd`, runs it against a temporary WAL, and asserts the responses,
including that the console enforces CSP, that topic cursor paging does not repeat
messages, and that unauthenticated requests get `401`.

### Load and benchmark

```bash
cd cmd
go build -o /tmp/loadgen ./loadgen
go build -o /tmp/native-bench ./native-bench
```

`loadgen` drives publishers and consumers against a running broker;
`native-bench` measures the native stream protocol. See `PERFORMANCE.md` for
published numbers and the method behind them.

### End-to-end smoke test

Copy-paste this to confirm a working install in under a minute:

```bash
BROKER=http://127.0.0.1:8090

# 1. broker up?
curl -s $BROKER/healthz

# 2. declare a queue and publish
curl -s -X POST $BROKER/queues -d '{"name":"orders"}'
for i in 1 2 3; do curl -s -X POST $BROKER/publish -d '{"queue":"orders","payload":{"n":1}}' -o /dev/null; done
curl -s $BROKER/queues

# 3. topic retention
for i in 1 2 3; do curl -s -X POST $BROKER/publish -d '{"topic":"events","payload":{"n":1}}' -o /dev/null; done
curl -s "$BROKER/v1/topics/events?limit=3"

# 4. task with no worker attached: submit, then cancel
TID=$(curl -s -X POST $BROKER/v1/tasks/submit -d '{"name":"demo","queue":"orders"}' \
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
curl -s $BROKER/v1/tasks/$TID
curl -s -X POST $BROKER/v1/tasks/$TID/cancel
curl -s $BROKER/v1/tasks/$TID     # status: cancelled

# 5. console
open $BROKER/console/
```

To see a consumer and a task actually run, start a worker first:

```bash
/tmp/broker-worker -broker $BROKER -id w1 -group g1 -queue orders -task invoice.send &

curl -s $BROKER/v1/consumers
for i in 1 2 3; do curl -s -X POST $BROKER/publish -d '{"queue":"orders","payload":{"n":1}}' -o /dev/null; done
curl -s $BROKER/queues            # orders drains to 0

TID=$(curl -s -X POST $BROKER/v1/tasks/submit \
      -d '{"name":"invoice.send","queue":"tasks.invoice.send"}' \
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
sleep 2 && curl -s $BROKER/v1/tasks/$TID   # status: succeeded
```


---

## Installation

```bash
go get github.com/oarkflow/broker
```

### Build binaries

```bash
go build -o brokerd ./cmd/brokerd/
go build -o brokerctl ./cmd/brokerctl/
go build -o broker-worker ./cmd/broker-worker/
```

---

## Broker Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Embedded** | Runs in your Go process as a library | High-performance in-process messaging |
| **Standalone** | HTTP server with auth/TLS/ACL | Microservices, API gateway |
| **Distributed** | Durable etcd/raft replication, snapshots, membership, and leader-gated reads | Pre-v1 clustered evaluation |

## Complete Examples

The [example index](examples/README.md) maps every recent enhancement to a runnable
scenario. Run all nine, including failure handling and durable-store restart:

```bash
cd examples
go run ./enhancements -scenario all
```

Runnable, fail-fast examples for embedded workflows, durable WAL restart, and
a real three-node etcd/Raft cluster are in
[`examples/complete`](examples/complete/README.md). From the `examples` module:

```bash
go run ./complete embedded
go run ./complete durable
go run ./complete distributed
./complete/check.sh
```

---

## Features

### Messaging

| Feature | Description |
|---------|-------------|
| Queues | Point-to-point with FIFO/priority leasing |
| Topics | Pub-sub with wildcard routing (`*`, `#`) |
| Streams | Append-only logs with consumer offsets |
| Request/Response | Sync, async futures, streaming |
| Commands | RPC-style request/response |
| Tasks | Managed submission with progress tracking |
| Saga | Multi-step workflow orchestration |

### Reliability

| Feature | Description |
|---------|-------------|
| WAL durability | JSONL or native packed BWL1 records with CRC32C |
| Crash recovery | Replay WAL, in-flight requeue |
| Retry policy | Exponential backoff with jitter |
| DLQ | Dead-letter queue with replay |
| Idempotency | Publisher and consumer dedup |
| Outbox dispatch | At-least-once dispatch; restart recovery with a persistent `OutboxStateStore` |
| Delivery semantics | At-least-once queues with bounded idempotency helpers |
| Circuit breaker | Automatic failure detection and recovery |
| Rate limiting | Per-queue and per-topic token bucket |
| Backpressure | Block, reject, drop-oldest, drop-newest, DLQ |

### Operations

| Feature | Description |
|---------|-------------|
| Prometheus metrics | `/metrics` endpoint with histograms |
| Distributed tracing | OpenTelemetry-compatible spans |
| Audit logging | Structured audit trail with sinks |
| Webhooks | Event callbacks with retry |
| SSE streaming | Real-time event stream |
| Schema evolution | Versioned schema registry |
| Stream lag monitoring | Consumer lag per stream |
| WAL compaction | Experimental detached JSON-WAL utility; not active-store compaction |
| Consumer priority | Weighted routing based on consumer capacity |
| Consumer group rebalancing | Weighted, graceful drain |
| Partitioning | Hash, round-robin, key-based routing, least-depth balancing for unkeyed publishes |
| Multi-tenancy | Tenant-scoped core data paths; full control-plane isolation is pending |
| Federation | Inter-broker message forwarding |
| Shovel | Queue-to-queue message movement |
| Graceful shutdown | HTTP connection draining |

The outbox persists entries through the broker `StateStore` when that store
implements `OutboxStateStore`; the contrib SQLite state store does. Failed
dispatches retry with exponential backoff and jitter. If writing a failed entry
to the DLQ also fails, the recorded dead-letter phase retries that write
without republishing the original message. The default
memory state store is process-local. A crash after the message append succeeds
but before the outbox status update is saved can cause a duplicate append, so
consumers should remain idempotent. This does not make an application database
transaction atomic with broker publishing.
SQLite and memory state stores assign each entry to one claim token until its
lease expires. An expired claim can be recovered after worker failure; if the
old worker already appended before expiry, recovery can still produce a
duplicate. A stale worker cannot update the entry after another worker replaces
its claim token.

Completed outbox records stay available to `GetEntry` until explicitly pruned.
Use `Prune(ctx, before, limit)` to remove published or dead-lettered records
older than the chosen cutoff and bound storage growth. Pruning ends duplicate-ID
protection for those records.

For application data stored in the same SQL database as the SQLite state
store, use `DurableOutbox.EnqueueTx` with the caller's open `*sql.Tx` via
`StateStore.OutboxTransaction(tx)`. The business row and outbox row then commit
or roll back together; broker dispatch begins after that transaction commits.

---

## Protocols

| Protocol | Transport | Port | Status |
|----------|-----------|------|--------|
| HTTP/JSON | TCP | 8090 | Pre-v1; daemon-integrated |
| Native stream | TCP/TLS | configurable | Pre-v1; authenticated (token/mTLS) handshake, see below |
| AMQP 0-9-1 | TCP/TLS | configurable | Experimental; conformance not established |
| MQTT 3.1.1 | TCP/TLS | configurable | Experimental; persistent subscriptions, inbound and outbound QoS 1/2 session recovery, Last Will, and retained-message replay are supported |
| Native RPC | TCP/TLS | configurable | Experimental JSON/TCP; not gRPC |
| SSE | HTTP(S) | configurable | Experimental event stream, separate from WebSocket |
| WebSocket | RFC 6455 | configurable | Pre-v1; protobuf-independent JSON data plane |
| gRPC | protobuf/HTTP2 | configurable | Pre-v1 versioned `broker.v1` API |

For MQTT, persistent-session subscription metadata is saved through the
broker's optional `MQTTSessionStore` state-store extension. The contrib SQLite
state store implements it, and `brokerd -store sqlite` opens message and
control-plane state on the same database. The default memory state store is
process-local.
Accepted CONNECT Wills are persisted through the optional `MQTTWillStore`
extension; the built-in memory and SQLite state stores implement it. Pending
Wills are replayed before the MQTT listener accepts clients, and graceful
DISCONNECT removes the pending Will. Transient publish and record-delete
failures retry with bounded exponential backoff; stable publish idempotency
keys prevent duplicates when the message store retains idempotency records.
SQLite preserves pending Wills across process restart; memory state only
survives an MQTT server restart while its `MemoryStateStore` remains alive.
`MQTTServer.ListenAndServe` requires TLS and an authentication mechanism in
production mode. Plaintext or unauthenticated development listeners require
`MQTTConfig.AllowInsecure`; `brokerd` enables it only with `--insecure-dev`.
Clients must complete CONNECT within `MQTTConfig.ConnectTimeout` (5 seconds by
default). Nonzero client keep-alive values are capped at `MQTTConfig.KeepAlive`
(60 seconds by default); a client keep-alive of zero disables inactivity
monitoring. MQTT packet writes time out after `MQTTConfig.WriteTimeout` (10
seconds by default).
Tenant-scoped MQTT credentials are restricted to one tenant per connection;
MQTT 3.1.1 has no field for selecting among multiple tenants.
When ACL is enabled, `message:publish` gates client publishes and Last Wills,
and `message:read` gates subscriptions.
Retained publications are keyed by tenant and topic. The built-in memory and
SQLite state stores retain them across MQTT reconnects; a custom state store
without `MQTTRetainedStore` gets process-local retained state. Empty retained
publishes remove the retained value while still being delivered live.
Built-in retained stores page subscription replay in batches so each replay
does not clone the full retained payload set at once.
Outbound QoS 1 messages remain in the MQTT session until `PUBACK`; they resume
with the same packet identifier after reconnect when the state store supports
`MQTTSessionStore`. Outbound QoS 2 stores its PUBLISH/PUBREL phase and resumes
the appropriate packet with the same packet identifier until `PUBCOMP`.
The retained-state write and live message append use separate stores and are
not one atomic transaction; a crash can leave the live append without its
retained update.
Queued offline deliveries survive only when the message store is durable.
Inbound QoS 2 state is saved before `PUBREC`; replay uses a stable idempotency
key and clears the session record before `PUBCOMP`. Duplicate suppression
depends on the message store retaining that idempotency record until the
handshake finishes, and this does not establish full MQTT conformance.

`brokerd` supervises every enabled listener. Protocol listeners are opt-in by
address; production mode requires TLS and authentication on every listener,
including MQTT, Native RPC, and the native BST1 stream. These listeners enforce
their authentication requirements unless `--insecure-dev` is set. The RFC 6455
handler also fails closed when its production ACL is missing.

---

## Encryption

### At Rest

| Component | Encryption | How to Enable |
|-----------|-----------|---------------|
| JSONL WAL | AES-256-GCM | `-frame-encryption-key <hex-key>` |
| Binary WAL (BWL1) | AES-256-GCM + optional HMAC-SHA256 | `-frame-encryption-key`, `-frame-signing-key` |
| FWB1/FWB2 network codecs | AES-GCM + HMAC-SHA256 | `WithFrameEncryptionKey()`, `WithFrameSigningKey()` |
| SQLite | AES-256-GCM | `NewEncryptedMessageStore(db, key)` |

### In Transit

| Transport | TLS | mTLS | How to Enable |
|-----------|-----|------|---------------|
| HTTP Admin | ✅ | ✅ | `-tls-cert`, `-tls-key`, `-client-ca`, `-require-client-cert`; map identities with `-client-cert-identities` |
| MQTT | ✅ | ✅ | `MQTTConfig.TLSCertFile`, etc. |
| Native JSON/TCP RPC | ✅ | ✅ | `NativeRPCConfig.TLSCertFile`, etc.; this is not gRPC |
| AMQP | ✅ | ✅ | `ListenTLS()`, `EXTERNAL` auth |
| gRPC v1 | ✅ | ✅ | `GRPCV1Config.TLSConfig` |
| RFC 6455 WebSocket | ✅ | via HTTPS client identity | `RFC6455Config` behind a TLS HTTP server |
| Client SDK | ✅ | ✅ | `Config.TLSCertFile`, etc. |

At-rest encryption remains opt-in. `brokerd` rejects plaintext production mode;
plaintext listeners require the explicit `--insecure-dev` flag.
The library MQTT and Native RPC listeners also enforce TLS and authentication
unless their explicit `AllowInsecure` option is enabled.

---

## Security

### Authentication

| Method | Protocol | How |
|--------|----------|-----|
| Bearer token | HTTP | `Authorization: Bearer <token>` |
| ACL tokens | HTTP | Named tokens with 28+ permissions |
| mTLS certificate | HTTP, Native RPC, gRPC | Verified certificate identity mapped with `-client-cert-identities`; permissions set with `-client-cert-permissions` |
| MQTT password | MQTT | `BrokerToken` in `MQTTConfig` |
| AMQP PLAIN | AMQP | Username/password |
| AMQP EXTERNAL | AMQP | Client certificate (mTLS) |

AMQP PLAIN can also use the shared ACL token set. When ACL authentication is
configured, an invalid ACL token is rejected rather than accepted as a fallback.
Mapped AMQP principals are checked against message, queue, and topic permissions;
their tenant scope also applies to broker store operations.

### Authorization (HTTP)

| Permission | Description |
|------------|-------------|
| `admin:*` | All admin/API actions |
| `broker:read` | broker summary/read operations |
| `stats:read` | detailed store statistics |
| `metrics:read` | global Prometheus metrics |
| `cluster:read` | cluster topology and controller metadata |
| `cluster:write` | cluster membership and controller metadata changes |
| `message:publish` | publish messages |
| `message:ack/nack` | ack/nack messages |
| `message:move` | move/copy/replay |
| `queue:read/write/control/purge` | queue management |
| `topic:read/write` | topic management |
| `stream:read/write/commit` | stream management |
| `dlq:read/replay` | DLQ management |
| `route:read/write` | route management |
| `consumer:register/read/control/poll` | consumer management |
| `task:read/write` | task management |
| `notification:read` | notification/SSE access |

GET and HEAD requests to `/healthz` and `/readyz` are unauthenticated probes;
readiness responses do not include internal failure details.

ACL principals can set `RequestsPerSecond`, `Burst`, and `MaxConcurrent`. The
rate bucket is shared across listeners and charges each request on long-lived
protocol connections; the concurrency limit counts their open connections
across MQTT, AMQP, gRPC, WebSocket, and BST1.

### TLS/mTLS

```bash
# TLS
brokerd -addr :8443 -tls-cert server.crt -tls-key server.key

# mTLS
brokerd -addr :8443 -tls-cert server.crt -tls-key server.key \
  -client-ca ca.crt -require-client-cert \
  -client-cert-identities 'cn:operator' \
  -client-cert-permissions 'admin:*'
```

Certificate identities use `cn:`, `dns:`, `uri:`, `email:`, or `ip:` prefixes.
Mapped certificates default to `broker:read`; grant broader permissions only
when the client requires them. Set `BROKER_CLIENT_CERT_IDENTITIES` and
`BROKER_CLIENT_CERT_PERMISSIONS` to configure the same mapping through the
environment.

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BROKER_ADDR` | `:8090` | Listen address |
| `BROKER_WAL` | - | WAL directory (empty = memory) |
| `BROKER_WAL_FORMAT` | `binary` | `binary` or `json` |
| `BROKER_TOKEN` | - | Admin token; production may instead require mTLS and map client certificate identities |
| `BROKER_READ_TOKEN` | - | Read-only token |
| `BROKER_WORKER_TOKEN` | - | Worker token |
| `BROKER_PUBLISHER_TOKEN` | - | Publisher token |
| `BROKER_TLS_CERT` | - | TLS certificate |
| `BROKER_TLS_KEY` | - | TLS key |
| `BROKER_CLIENT_CA` | - | Client CA for mTLS |
| `BROKER_REQUIRE_CLIENT_CERT` | `false` | Require mTLS |
| `BROKER_CLIENT_CERT_IDENTITIES` | - | Comma-separated verified certificate identities mapped to an ACL principal |
| `BROKER_CLIENT_CERT_PERMISSIONS` | `broker:read` | Comma-separated ACL permissions for mapped certificates |
| `BROKER_MAX_BODY` | `1048576` | Max request body (bytes) |
| `BROKER_FRAME_SIGNING_KEY` | - | HMAC signing key |
| `BROKER_FRAME_ENCRYPTION_KEY` | - | AES encryption key (hex) |
| `BROKER_INSECURE_DEV` | `false` | Disable auth (dev only) |

### CLI Flags

```bash
brokerd \
  -addr :8090 \
  -wal ./.broker-data \
  -wal-format binary \
  -token "your-secret-token" \
  -tls-cert server.crt \
  -tls-key server.key \
  -client-ca ca.crt \
  -require-client-cert \
  -max-body 1048576 \
  -frame-signing-key "hex-signing-key" \
  -frame-encryption-key "hex-encryption-key" \
  -frame-compress-threshold 1024
```

---

## CLI Usage

```bash
export BROKER_TOKEN='your-secret-token'
export BROKER_ADDR='http://127.0.0.1:8090'

# Stats
brokerctl stats

# List queues
brokerctl queues

# Publish
brokerctl publish -topic orders.created -queue orders.process \
  -type order -payload '{"id":"order-1","amount":100}'

# Pull
brokerctl pull email.send -consumer cli -group cli -limit 1

# Ack
brokerctl ack <message-id> -queue email.send -lease <lease-token> -consumer cli

# Move (DLQ replay)
brokerctl move -from-type queue -from email.send.dlq \
  -to-type queue -to email.send -mode replay
```

---

## HTTP API

### Health and Stats

```bash
curl http://localhost:8090/healthz           # Health check
curl http://localhost:8090/readyz            # Readiness check
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/stats                # Broker stats
curl http://localhost:8090/metrics           # Prometheus metrics
curl http://localhost:8090/lag?stream=audit  # Stream lag
```

### Publish

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/publish \
  -d '{
    "topic": "orders.created",
    "queue": "orders.process",
    "type": "order",
    "payload": {"id": "order-1", "amount": 100}
  }'
```

### Queue Operations

```bash
# Create queue
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/queues \
  -d '{"name":"orders","visibility_timeout":30000000000,"max_depth":100000}'

# Pull messages
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/queues/orders/pull \
  -d '{"consumer_id":"worker-1","consumer_group":"workers","limit":1}'

# Pause/Resume/Purge
curl -X POST -H "Authorization: Bearer $TOKEN" http://localhost:8090/queues/orders/pause
curl -X POST -H "Authorization: Bearer $TOKEN" http://localhost:8090/queues/orders/resume
curl -X POST -H "Authorization: Bearer $TOKEN" http://localhost:8090/queues/orders/purge
```

### Message Operations

```bash
# Ack
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/messages/<id>/ack \
  -d '{"queue":"orders","consumer_id":"worker-1","lease_token":"<token>"}'

# Nack with retry
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/messages/<id>/nack \
  -d '{"queue":"orders","consumer_id":"worker-1","lease_token":"<token>","action":"retry","delay":5000000000}'
```

### Movement

```bash
# Replay DLQ
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/move \
  -d '{
    "source_type":"queue","source":"orders.dlq",
    "destination_type":"queue","destination":"orders",
    "mode":"replay","reset_attempts":true,"reset_error":true
  }'
```

### SSE Events

The standalone SSE listener is secure by default: configure its ACL (or call
`SetToken` for the pre-v1 compatibility path), or explicitly set
`InsecureDev` for isolated development. Tenant-scoped connections receive only
tenant-tagged broadcasts for an allowed tenant; unowned global events are not
forwarded.

```bash
curl -N -H "Authorization: Bearer $TOKEN" http://localhost:8090/events
```

---

## Publisher

```go
pub := b.Publisher()

// Fire-and-forget
pub.Publish(ctx, "orders.created", payload, broker.WithQueue("orders"))

// Confirmed publish
pub.PublishConfirm(ctx, "orders.created", payload, broker.WithQueue("orders"))

// Async publish
future := pub.PublishAsync(ctx, "events.raw", payload, broker.WithQueue("events"))
res, _ := future.Wait(ctx)

// Batch publish
pub.PublishBatch(ctx, "events.raw", []any{e1, e2, e3}, broker.WithQueue("events"))

// Transactional publish
tx, _ := pub.BeginTx(ctx)
tx.Publish("order.created", order, broker.WithQueue("orders"))
tx.Publish("audit.event", audit, broker.WithStream("audit"))
tx.Commit(ctx)

// Delayed publish
pub.PublishAfter(ctx, "email.reminder", payload, 30*time.Minute, broker.WithQueue("email"))

// Scheduled publish
pub.PublishAt(ctx, "billing.renew", payload, renewAt, broker.WithQueue("billing"))
```

---

## Consumer

```go
// Auto-ack
b.Subscribe(ctx, broker.ConsumerConfig{
    Name: "worker", Queue: "jobs", AckMode: broker.AckAuto, Concurrency: 32,
}, func(ctx context.Context, d broker.Delivery) error {
    // process
    return nil
})

// Manual ack
b.Subscribe(ctx, broker.ConsumerConfig{
    Name: "worker", Queue: "jobs", AckMode: broker.AckManual,
}, func(ctx context.Context, d broker.Delivery) error {
    if err := process(d); err != nil {
        return d.Nack(ctx, broker.WithNackAction(broker.NackRetry), broker.WithNackDelay(5*time.Second))
    }
    return d.Ack(ctx)
})

// Batch consumer
b.SubscribeBatch(ctx, broker.ConsumerConfig{
    Name: "batcher", Queue: "events", BatchSize: 500, BatchWait: 50*time.Millisecond,
}, func(ctx context.Context, ds []broker.Delivery) error {
    // process batch
    return nil
})

// Idempotent consumer (deduplicates messages)
b.Subscribe(ctx, broker.ConsumerConfig{
    Name: "idempotent-worker", Queue: "orders",
    Idempotent: true, DeduplicationWindow: 5*time.Minute,
}, func(ctx context.Context, d broker.Delivery) error {
    // automatically deduplicates by idempotency_key
    return nil
})
```

`BatchWait` is the maximum collection delay. When positive, the subscription
starts with a short wait, reduces it after full batches, and increases it after
underfilled timeouts below the ceiling. `/metrics` reports the mean current
wait across active batch consumers.

The store scopes deduplication IDs, idempotency keys, producer sequence state,
and stream consumer offsets by `TenantID`. Persisted snapshots retain these
indexes. Older snapshots migrate on restore, and legacy WAL offset records are
decoded during replay.

---

## Routing

```go
// Wildcard routes
b.AddRoute(broker.Route{
    Name: "tenant-invoices", Match: "tenant.*.invoice.created",
    To: []broker.Destination{{Kind: broker.DestinationQueue, Name: "invoice.process"}},
})

// Fanout
b.AddRoute(broker.Route{
    Name: "all-events", Match: "events.*", Mode: broker.RouteFanout,
    To: []broker.Destination{
        {Kind: broker.DestinationQueue, Name: "analytics"},
        {Kind: broker.DestinationQueue, Name: "audit"},
    },
})

// Add route via HTTP
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8090/routes \
  -d '{"name":"my-route","match":"orders.*","to":[{"kind":"queue","name":"orders.process"}]}'
```

---

## Queues

```go
b.DeclareQueue(ctx, broker.QueueConfig{
    Name: "orders", VisibilityTimeout: 30*time.Second, MaxDepth: 100000,
    RetryPolicy: broker.DefaultRetryPolicy(), DLQ: "orders.dlq",
})
```

Features: FIFO/priority leasing, delayed availability, TTL, manual/auto ack, visibility timeout, lease extension, redelivery, pause/resume/purge, max-depth backpressure.

---

## Streams

```go
b.DeclareStream(ctx, broker.StreamConfig{Name: "audit.events"})

// Publish to stream
pub.Publish(ctx, "audit.login", event, broker.WithStream("audit.events"))

// Read from stream
msgs, _ := b.ReadStream(ctx, broker.StreamReadRequest{Stream: "audit.events", ConsumerGroup: "analytics", Limit: 100})

// Commit offset
b.CommitOffset(ctx, broker.OffsetCommit{Stream: "audit.events", ConsumerGroup: "analytics", Offset: msgs[len(msgs)-1].Offset})

// Atomically commit the input offset with produced output on stores that
// support OffsetAppendStore (memory, SQLite, local WAL, and ReplicatedStore).
_, err := b.CommitOffsetAndAppend(ctx,
    broker.OffsetCommit{Stream: "audit.events", ConsumerGroup: "analytics", Offset: msgs[len(msgs)-1].Offset, TransactionID: "analytics-batch-0001"},
    []broker.AppendEntry{{
        Message: broker.Message{ID: "derived-1", Topic: "audit.enriched", Payload: []byte("...")},
        Destinations: []broker.Destination{{Kind: broker.DestinationTopic, Name: "audit.enriched"}},
    }},
)

// Check lag
lag, _ := b.StreamLag(ctx, "audit.events", "analytics")
```

Set `TransactionID` to a stable value when the caller may retry after a timeout.
The store returns the prior result for the same input and reports
`ErrTransactionConflict` if that ID is reused with different input. Stores
retain the most recent 4096 transaction outcomes. Retry results resolve full
messages while retained; after message cleanup, the transaction record returns
the output metadata it kept.

---

## Retries and DLQ

```go
// Retry on error
return fmt.Errorf("temporary failure")

// Permanent error (skip retry, go to DLQ)
return broker.Permanent(fmt.Errorf("invalid payload"))

// Manual retry
return d.Nack(ctx, broker.WithNackAction(broker.NackRetry), broker.WithNackDelay(10*time.Second))

// Reject to DLQ
return d.Reject(ctx, "invalid customer id")

// Replay DLQ
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/dlq/orders.dlq/replay?queue=orders
```

---

## Tasks

```go
// Register task handler
b.RegisterTask(ctx, "image.resize", func(ctx context.Context, d broker.TaskDelivery) (any, error) {
    d.Progress(ctx, broker.Progress{Current: 1, Total: 2, Percent: 50})
    return resize(d.Message.Payload)
})

// Submit task
h, _ := pub.SubmitTask(ctx, "image.resize", req, broker.WithTaskQueue("image.jobs"))
result, _ := h.Wait(ctx)
```

---

## Multi-Tenancy

```go
b := broker.MustNew(broker.Config{
    Tenancy: &broker.TenancyConfig{
        Enabled: true, IsolationLevel: broker.IsolationQueuePrefix, QueuePrefix: "t.",
    },
})

tenantMgr := b.TenantManager()
tenantMgr.EnsureTenant("tenant-a")

// Tenant-isolated publish
tenantMgr.TenantAwarePublish(ctx, "tenant-a", "orders.created", payload)

// Tenant-isolated subscribe
tenantMgr.TenantAwareSubscribe(ctx, "tenant-a", broker.ConsumerConfig{Queue: "orders"}, handler)
```

---

## Federation and Shovel

```go
// Shovel: move messages from queue A to queue B
b.ShovelManager().StartShovel(broker.ShovelConfig{
    Source: "source-queue", Destination: "dest-queue", BatchSize: 100,
})

// Federation: link to upstream broker
b.FederationManager().AddLink(broker.FederationConfig{
    Upstream: "http://upstream:8090", Queues: []string{"orders"},
})
```

---

## Partitioning

```go
b := broker.MustNew(broker.Config{
    Partition: &broker.PartitionConfig{Partitions: 4, Strategy: broker.PartitionLeastDepth},
})

// Unkeyed messages go to a shallow partition; keyed messages use stable hashing.
_, err := b.PublishPartitioned(ctx, "events", payload)
_, err = b.PublishPartitioned(ctx, "events", payload, broker.WithKey("user-123"))
stats, err := b.TopicPartitionStats(ctx, "events", 4) // includes hot partitions and depth skew
```

`PartitionReassignment` rules move queued messages between queues in a single
store operation. Leased and scheduled messages are left in the source queue;
`PreserveOffset` controls whether a message's existing offset is retained.
Moves skip messages when a destination queue is at `MaxDepth`, leaving them
available for a later reassignment pass. The move checks source membership
under the store lock, so concurrent reassignment passes cannot move the same
message twice.

---

## Schema Evolution

```go
registry := broker.NewMemorySchemaRegistry(
    broker.SchemaDefinition{Name: "invoice", Version: "v1", Required: []string{"id", "amount"}},
    broker.SchemaDefinition{Name: "invoice", Version: "v2", Required: []string{"id", "amount", "currency"}, CompatibleWith: "v1"},
)
b := broker.MustNew(broker.Config{SchemaRegistry: registry})

pub.Publish(ctx, "invoice.created", payload, broker.WithSchema("invoice", "v2"))
```

When `NewDistributed` uses etcd/raft and no custom `SchemaRegistry` is
configured, schema definitions are stored in the controller quorum and
publish-time validation reads committed versions linearly. Register, list, read,
and deprecate definitions through the authenticated
`/cluster/metadata/schemas` endpoints. A custom `SchemaRegistry` remains in
control when supplied in `broker.Config`.

---

## Observability

### Prometheus Metrics

```bash
curl http://localhost:8090/metrics
```

Metrics: `broker_messages_published_total`, `broker_messages_delivered_total`, `broker_messages_acked_total`, `broker_publish_rate_limited_total`, `broker_publish_backpressured_total`, durable outbox dispatch attempts, failures, published entries, and dead-lettered entries (`broker_outbox_*`), automatic partition reassignment starts, completions, failures, and pending observations (`broker_partition_reassignment_*`), consumer batch/message counts, collection time, and current adaptive wait (`broker_consumer_*`), `broker_queue_depth{queue}`, `broker_topic_published_total{topic}`, latency histograms, and WAL group-commit batches, request counts, maximum batch size, cumulative coalescing time, current adaptive coalescing wait, and errors (`broker_wal_group_commit_*`). The OTLP exporter includes matching rate-limit, queue-depth backpressure, outbox, and partition-reassignment counters.

### Tracing

```go
b := broker.MustNew(broker.Config{
    EnableTracing: true,
    TracerConfig: &broker.TracerConfig{ServiceName: "my-service"},
})
```

### Audit Logging

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8090/audit
```

---

## Production Checklist

- [ ] Set `BROKER_TOKEN` or `-token`; never use `--insecure-dev`
- [ ] Enable TLS; use mTLS for sensitive environments
- [ ] Use named ACL tokens with least-privilege permissions
- [ ] Set `-max-body` to a sane value
- [ ] Put WAL on reliable storage and back it up
- [ ] Enable encryption at rest if storing sensitive data
- [ ] Run behind a private network or authenticated ingress
- [ ] Monitor `/stats`, `/metrics`, `/lag`, queue depth, DLQ count
- [ ] Alert on DLQ growth and callback failure
- [ ] Use idempotency keys for unsafe operations
- [ ] Use manual ack for operations requiring confirmation
- [ ] Keep visibility timeout longer than handler time or extend leases
- [ ] Run `scripts/release_gate.sh` and archive its artifacts for the exact commit
- [ ] Complete `DURATION=72h scripts/soak.sh` on production-like hardware
- [ ] Perform the backup, restore, upgrade, rollback, and alert drills in [Operations](docs/OPERATIONS.md)
- [ ] Review the [threat model](docs/THREAT_MODEL.md) and close deployment-specific risks
- [ ] Verify required third-party clients against the [protocol compatibility scope](docs/PROTOCOL_COMPATIBILITY.md)
- [ ] Obtain independent security review and explicit release sign-off

---

## Package Layout

```
broker/
  broker.go              Core broker runtime
  publisher.go           Publisher/request/task APIs
  consumer.go            Consumers, delivery, ack/nack
  router.go              Routes and wildcard matching
  store.go               Store interface, memory store, WAL stores
  task.go                Task state/result/wait/progress
  saga.go                Saga orchestration
  notification.go        Notification bus and webhooks
  security.go            Auth, ACL, audit
  hooks.go               Lifecycle hooks
  types.go               Types and config
  health.go              Health checks, metrics
  admin.go               HTTP admin/API server
  dataplane.go           /v1/* data-plane endpoints
  remote.go              Remote consumer registry
  distributed.go         Distributed broker and replicated store adapter
  rebalance.go           Consumer group rebalancing
  strategy.go            14 routing strategies
  schema.go              Schema registry
  errors.go              Error types
  mqtt.go                MQTT 3.1.1 server
  grpc_transport.go      Experimental native JSON/TCP RPC transport
  websocket.go           Server-Sent Events (SSE) transport
  federation.go          Federation and shovel
  tenancy.go             Multi-tenancy
  exactly_once.go        Legacy in-memory publish retry buffer and replay helpers
  partition_ordering.go  Partition routing
  wal_compaction.go      WAL segment management
  metrics.go             Prometheus metrics
  tracing.go             Distributed tracing
  ratelimit.go           Rate limiting
  circuit_breaker.go     Circuit breaker
  audit.go               Audit logging
  frame_codec.go         FWB1 network message codec
  compact_codec.go       FWB2 compact network codec
  wal_record_codec.go    BWL1 packed mutation codec
  binary_wal_store.go    Native BWL1 WAL store

  amqp/                  AMQP 0-9-1 protocol server
  cluster/               Durable etcd/raft consensus and transports
  contrib/sqlitestore/   SQLite state + message stores
  cmd/brokerd/           Standalone broker server
  cmd/brokerctl/         CLI client
  cmd/broker-worker/     Remote worker process
  pkg/client/            Client SDK
  pkg/pool/              Worker pool (execution engine)
  examples/              BCL-driven examples
```

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for the full text.

## High-throughput BST1 stream mode

For Kafka-equivalent append/fetch workloads use the native stream listener rather than queue leasing:

The command below uses `--insecure-dev` and is for isolated local development
only. Shared deployments must configure TLS plus a bearer token or mapped mTLS
identity; production startup rejects the listener without them.

```bash
brokerd -wal .broker-data/queue -native-addr :8091 \
  -native-dir .broker-data/native-stream -native-partitions 8 \
  -native-sync=true -insecure-dev
```

The native stream listener supports an authenticated handshake (bearer token or
mTLS, enforced via the same ACL used by the HTTP admin API). Outside
`--insecure-dev`, `brokerd` requires `-token` or `-require-client-cert` to be
set before it will start the listener. `--insecure-dev` disables that
enforcement (anonymous/auto-admin access) and must only be used for local
development.

The `native-bench` command uses persistent TCP clients, compact record batches, partition offsets and restart-safe segmented logs. See `NATIVE_STREAM_PERFORMANCE.md` and `scripts/benchmark_native_vs_kafka.sh`.

New Go clients negotiate the highest common BST1 frame version through the
BST2 handshake envelope; this release supports frame version 1. The server
still accepts legacy BST1 clients. A new client connecting to an older
BST1-only server fails explicitly; set `NativeStreamDialOptions.LegacyHandshake`
for a controlled migration. This does not yet provide rolling-upgrade
or interoperability certification.
