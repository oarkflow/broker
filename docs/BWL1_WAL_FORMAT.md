# BWL1 Native Write-Ahead-Log Format

BWL1 is the broker's dedicated durable mutation format. It replaces the former design that encoded each mutation as JSON and then wrapped that JSON inside an FWB message frame.

BWL1 is used only for WAL persistence. FWB1/FWB2 remain the network message codecs.

## Goals

- One linear encode/decode pass
- No reflection and no JSON on the write hot path
- One allocation for ordinary uncompressed records
- Compact batches with repeated fields encoded once
- Versioned operation schema
- CRC32C corruption detection
- Optional HMAC-SHA256 authentication
- Optional AES-GCM encryption
- Bounded parsing and decompression
- Recovery of legacy FWB1/FWB2 JSON WAL files

## File envelope

Each WAL file contains consecutive length-prefixed records:

```text
record_length  unsigned LEB128/uvarint
record         record_length bytes
```

A torn or incomplete final length/record is truncated during recovery.

## BWL1 record layout

```text
+---------------+-----------------------------------------------+
| Field         | Encoding                                      |
+---------------+-----------------------------------------------+
| magic         | 4 bytes: ASCII "BWL1"                         |
| version       | uint8, currently 1                            |
| operation     | uint8 operation code                          |
| flags         | uint8                                         |
| reserved      | uint8, must be zero                           |
| payload_len   | unsigned LEB128/uvarint                       |
| nonce_len     | uint8                                         |
| nonce         | nonce_len bytes                               |
| payload       | packed operation-specific fields             |
| crc32c        | uint32 little-endian                          |
| hmac          | 32 bytes when the signed flag is present      |
+---------------+-----------------------------------------------+
```

CRC32C uses the Castagnoli polynomial and covers `magic` through the stored payload. HMAC-SHA256 covers the same bytes plus the CRC field.

## Flags

```text
bit 0  compressed
bit 1  encrypted
bit 2  signed
```

Unknown flags and non-zero reserved bits are rejected.

Encoding order is:

```text
packed payload -> optional gzip -> optional AES-GCM -> CRC32C -> optional HMAC
```

Decoding validates lengths, CRC, and HMAC before decrypting or allocating decoded collections. Gzip output is bounded by the configured maximum record size.

## Operation codes

```text
 1 declare_queue       10 ack_batch
 2 declare_topic       11 nack
 3 declare_stream      12 move
 4 append              13 pause_queue
 5 append_atomic       14 resume_queue
 6 lease               15 purge_queue
 7 lease_batch         16 update
 8 update_batch        17 commit_offset
 9 ack
```

Each operation has a fixed, versioned field order. Unknown operation codes are rejected.

## Packed messages

Messages use a 64-bit presence bitmap followed by only the present fields in fixed order. Empty strings, zero integers, zero times, empty payloads, and empty maps require no field bytes.

Known message statuses and destination kinds use one-byte numeric codes. Unknown future/custom enum values use an escape code followed by their string value.

### Batch-common fields

For `append_atomic`, `lease_batch`, and `update_batch`, BWL1 detects repeated message fields and writes them once per batch. Current common candidates include:

```text
topic, queue, stream, type, content_type, schema, schema_version,
tenant_id, producer_id, consumer_id, consumer_group, original destinations,
status
```

When all append entries use the same destination list, that list is also encoded once. Ack batches similarly encode common queue and consumer IDs once.

## Security configuration

The existing built-in FWB codec options are transferred to BWL1 automatically:

```go
store, err := broker.NewBinaryFileWALStore(
    "./data",
    broker.WithFrameMaxSize(128<<20),
    broker.WithFrameSigningKey([]byte("separate-signing-key")),
    broker.WithFrameEncryptionKey([]byte("32-byte-encryption-key-material")),
)
```

Applications using a custom network frame codec can configure BWL1 explicitly:

```go
walCodec, err := broker.NewWALRecordCodec(broker.WALCodecConfig{
    MaxRecordSize: 128 << 20,
    SigningKey:    signingKey,
    EncryptionKey: encryptionKey,
})
store, err := broker.NewBinaryFileWALStoreWithWALCodec(
    "./data",
    customLegacyFrameCodec,
    walCodec,
)
```

Compression is disabled by default. For latency-sensitive durable workloads, leave it disabled unless disk bandwidth is the measured bottleneck.

## Compatibility

New writes always use BWL1. On recovery, records without the `BWL1` magic are decoded through the configured legacy FWB1/FWB2 codec and its JSON payload decoder. This permits in-place restart and migration without rewriting an existing WAL.

Once a legacy file is reopened, new BWL1 records can follow old records in the same file; recovery handles both record types.
