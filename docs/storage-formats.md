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

## BWL2 and BST2

BWL2/BST2 are reserved for the segmented production formats. No implementation
may emit either magic/version until manifests, checksummed segment headers,
indexes, checkpoints, clean-shutdown markers, and migration tests are complete.
Existing BWL1 data must only be migrated by an explicit backup-preserving tool;
it must never be silently rewritten.
