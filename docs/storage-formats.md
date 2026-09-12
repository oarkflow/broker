# Storage format status

## BWL1

BWL1 is the active packed mutation WAL. Every record has a bounded length,
magic/version header, operation code, CRC32C, and optional HMAC-SHA256 and
AES-GCM protection. A successful durable-store write waits for the active group
commit's local sync before returning `persisted/local_fsync`.

BWL1 recovery accepts the legacy FWB-wrapped JSON records read-only. A corrupt
middle BWL1 record currently stops recovery; an incomplete tail is truncated.
BWL1 does not yet provide segment manifests, sparse indexes, atomic snapshots,
or integrated compaction and is not the target production format.

`broker-wal` provides read-only verification, backup-preserving torn-tail
repair, decoded newline-delimited JSON export, and import into a new path. An
import never overwrites an existing WAL and verifies the resulting native WAL
before reporting success. Middle-record corruption is reported and is never
silently discarded.

Examples:

```sh
cd cmd
go run ./broker-wal -path ../data/broker.wal.fwb
go run ./broker-wal -path ../data/broker.wal.fwb -repair-tail
go run ./broker-wal -path ../data/broker.wal.fwb -export-json ../backup.jsonl
go run ./broker-wal -path ../restored/broker.wal.fwb -import-json ../backup.jsonl
```

## BWL2

BWL2 is the active segmented WAL implementation. It provides manifests,
checksummed bounded records, sparse indexes, clean-shutdown trailers, rotation,
compaction, torn-tail handling, and tested BWL1/JSON migration. Migration is
fail-closed: the original is renamed to `.migrated` only after every record is
verified and the BWL2 destination is synced. A failed migration removes partial
BWL2 output and leaves the source untouched.

## BST2

BST2 remains reserved for a future authenticated, version-negotiated native
stream protocol. No implementation may advertise or emit BST2 until its format
and conformance tests are committed. BST1 remains the implemented native stream
format described in `native-protocols.md`.
