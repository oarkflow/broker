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

BWL2 is the segmented WAL implementation. Select it with `-store wal2` (also
accepted: `segmented`, `segmented-wal`). It provides manifests, checksummed
bounded records, sparse indexes, rotation, checkpoint-based compaction,
torn-tail handling, and tested BWL1/JSON migration. Migration is fail-closed:
the original is renamed to `.migrated` only after every record is verified and
the BWL2 destination is synced. A failed migration removes partial BWL2 output
and leaves the source untouched.

Layout on disk, in a directory given by `-wal`:

```text
manifest.json              # segment list, active segment, next seq, checkpoint pointer
00000000000000000001.bwl2  # a segment: 22-byte header + length-prefixed records
00000000000000000001.idx   # that segment's sparse index (seq -> byte offset)
checkpoint.bwl2            # full MemoryStore snapshot as of the last compaction
```

Rotation, retention, and compaction:

- A segment rotates to a new file once it reaches `-wal-segment-size` bytes
  (`BROKER_WAL_SEGMENT_SIZE`).
- A background compactor (interval: `-wal-compaction-interval`, default 5m)
  writes a fresh checkpoint of the live in-memory state, then prunes segments
  that exceed `-wal-max-segments` (`BROKER_WAL_MAX_SEGMENTS`) or are older
  than `-wal-retention` (`BROKER_WAL_RETENTION_SEC`; 0 disables age-based
  pruning). The active segment is never pruned.
- Compaction always persists the updated manifest *before* deleting the
  segment files it just pruned. If the process is killed in between, the
  pruned files become harmless, manifest-unreferenced orphans; the next open
  of the store removes them automatically.
- A segment referenced by the manifest with no file on disk is treated as a
  hard failure on open (`ErrStorageUnavailable`), not silently skipped — by
  construction this should only happen from external deletion or filesystem
  corruption, never from normal compaction.
- Mid-segment (non-tail) corruption in a closed segment also fails the store
  open closed, with a pointer to `broker-wal -dir`, rather than silently
  skipping or guessing at the damaged record. Only a torn write at the very
  end of the *active* segment is auto-truncated and repaired on open.

`broker-wal` provides read-only, non-destructive verification for a BWL2
directory:

```sh
cd cmd
go run ./broker-wal -dir ../data
```

This reports per-segment record/byte counts and any problems (missing files,
invalid magic, corrupt records) without modifying anything. Unlike BWL1, a
BWL2 store self-heals a torn tail in its own active segment automatically the
next time it is opened, so there is no separate `-repair-tail` for BWL2.

Not yet implemented: encryption-key rotation (BWL2 inherits the WAL codec's
single configured key, same as BWL1), and an operator tool to quarantine a
single corrupted closed segment and resume from a known-good checkpoint
instead of failing the whole store closed.

## BST2

BST2 remains reserved for a future authenticated, version-negotiated native
stream protocol. No implementation may advertise or emit BST2 until its format
and conformance tests are committed. BST1 remains the implemented native stream
format described in `native-protocols.md`.
