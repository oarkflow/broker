# Storage format status

Atomic stream transactions use packed operation code 22, containing prepared
output entries, a tenant-scoped offset commit and the expected previous offset
in one record. Older operation codes retain their meaning. See
[stream transactions](STREAM_TRANSACTIONS.md) for the commit boundary and the
backup requirement before downgrade.

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
- Applications can set `SegmentedWALConfig.SegmentArchiver` to stream each
  pruned segment to a cold-storage implementation before the manifest is
  changed. If archival fails, compaction leaves the old manifest and segment
  files intact. A crash after archive success but before manifest update can
  cause the same segment to be archived again, so archive writes should be
  idempotent by `SegmentMeta.Filename`. Segment bytes are archived as stored
  (including WAL encryption when configured); the sparse index is regenerated
  as needed and is not archived.
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
invalid magic, corrupt records, and the referenced checkpoint's encryption,
JSON, and required snapshot structure) without modifying anything. Unlike BWL1, a
BWL2 store self-heals a torn tail in its own active segment automatically the
next time it is opened, so there is no separate `-repair-tail` for BWL2.

When a WAL encryption key is configured, compaction checkpoints are also
AES-GCM encrypted and authenticated. Legacy plaintext checkpoints are accepted
for recovery and atomically rewritten with encryption during store open.
Missing, unreadable, undecryptable, or malformed checkpoints referenced by the
manifest fail store open closed rather than silently dropping the checkpoint
and replaying only the retained tail. Sparse indexes and the manifest contain
offset and topology metadata, not message payloads.

For BWL1 key rotation, stop `brokerd`, then run `broker-wal -path <wal-file>
-rotate-key -encryption-key <new-hex-key> -decryption-keys <old-hex-key>[,...]`.
For a plaintext BWL1 file, omit `-decryption-keys` to enable encryption. The
tool verifies the source, rewrites each record with the new key, verifies the
replacement, writes a synced `.key-rotation-*.bak`, then atomically replaces
the source file. It also converts legacy framed records to native BWL1 records.
The backup remains encrypted or plaintext as the original was, so retain its
old keys if you may need to restore it.

For BWL2 key rotation, stop `brokerd`, then run `broker-wal -dir <store-dir>
-rotate-key -encryption-key <new-hex-key> -decryption-keys <old-hex-key>[,...]`.
The new key is the primary encryption key; the bounded decryption keyring (up
to 16 previous keys) lets recovery read existing records and checkpoints. The
tool opens and replays the store, writes an encrypted checkpoint and a fresh
active segment with the new primary key, persists that manifest, then removes
older segments. If the command succeeds, the old keys are no longer needed for
that store. In a clustered deployment, stop every broker and rotate each BWL2
directory separately, including the broker WAL and every local partition WAL
directory under `-cluster-partition-wal`. For the partition-WAL root, add
`-recursive` to discover and rotate each child directory containing a
`manifest.json`; rotate the broker WAL separately. If a recursive run stops
partway, keep both keys configured and rerun it to finish the remaining stores.
Each rotated directory is verified again with only the new key before it is
reported complete.
Remove old keys from configuration only after every store that may contain data
encrypted with them has been rotated. Keep the old keys until the last rotation
completes and retain backups before rotating.
`brokerd` accepts the same previous-key list through
`BROKER_WAL_DECRYPTION_KEYS` (new writes use `BROKER_FRAME_ENCRYPTION_KEY`).

An operator tool to quarantine a single corrupted closed segment and resume
from a known-good checkpoint is still not implemented; recovery fails the
whole store closed in that case.

## BST2

BST2 is currently used only as the native stream handshake envelope for
version negotiation. The selected data-frame format remains BST1 version 1;
there is no BST2 frame codec or conformance certification yet. See
`native-protocols.md` for the negotiated handshake and legacy-client behavior.
