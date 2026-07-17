# FastWorker Broker Codec Format: FWB1

FWB1 is the native binary network message frame used for high-performance publish/poll endpoints and replication transport. Durable WAL mutations now use the dedicated packed **BWL1** format rather than JSON inside an FWB frame; see `BWL1_WAL_FORMAT.md`. Legacy FWB-wrapped JSON WAL records remain recoverable. Message payloads stay opaque so applications can use JSON, raw bytes, Protobuf, MessagePack, or another payload codec.

## Design goals

- Lightweight frame overhead
- Fast linear parsing
- Versioned and forward-compatible
- Cross-language readable
- Safe for WAL/storage and network transport
- Corruption detection with CRC32C
- Optional HMAC-SHA256 frame authentication
- Optional AES-GCM payload encryption
- Optional gzip compression
- Max frame-size protection
- Header-only peek without decoding payload
- TLV metadata for future fields

## Binary layout

All integer fields are little-endian except varints, which use unsigned LEB128/uvarint.

```text
+-------------------+-----------------------------------------------+
| field             | encoding                                      |
+-------------------+-----------------------------------------------+
| magic             | 4 bytes: ASCII "FWB1"                         |
| version           | uint8, currently 1                            |
| frame_type        | uint8                                         |
| flags             | uint16 little-endian                          |
| header_len        | uvarint                                       |
| payload_len       | uvarint                                       |
| crc32c            | uint32 little-endian                          |
| header            | TLV records, header_len bytes                 |
| payload           | payload_len bytes                             |
| hmac_len          | uvarint, 0 when unsigned                      |
| hmac              | HMAC-SHA256 over bytes magic..payload         |
+-------------------+-----------------------------------------------+
```

`crc32c` covers `header || payload` using the Castagnoli table. HMAC, when enabled, covers all frame bytes from `magic` through `payload`, including the CRC field.

## Frame types

```go
FrameMessageAppend = 1
FrameMessageAck    = 2
FrameMessageNack   = 3
FrameMessageLease  = 4
FrameMessageExtend = 5
FrameMessageMove   = 6
FrameMessageCopy   = 7
FrameMessageDLQ    = 8
FrameOffsetCommit  = 9
FrameTaskProgress  = 10
FrameTaskResult    = 11
FrameConsumerEvent = 12
FrameHeartbeat     = 13
FrameMessage       = 100
```

The data-plane `/v1/publish/frame` and `/v1/poll/frame` use `FrameMessage`.

## Flags

```go
FrameFlagCompressed = 1 << 0
FrameFlagEncrypted  = 1 << 1
FrameFlagSigned     = 1 << 2
FrameFlagHasHeaders = 1 << 3
FrameFlagHasMetadata= 1 << 4
FrameFlagTask       = 1 << 5
FrameFlagRequest    = 1 << 6
FrameFlagResponse   = 1 << 7
FrameFlagStreamEnd  = 1 << 8
```

## TLV header format

Each header record is:

```text
key_len   uvarint
value_len uvarint
key       key_len bytes UTF-8
value     value_len bytes
```

Known keys include:

```text
id, parent_id, topic, queue, stream, partition, offset, key, routing_key,
type, content_type, schema, schema_version, trace_id, span_id,
correlation_id, causation_id, request_id, reply_to, reply_group,
idempotency_key, deduplication_id, checksum, priority, created_at,
available_at, expires_at, attempts, max_attempts, redeliveries,
tenant_id, producer_id, consumer_id, consumer_group, lease_token,
lease_until, original_topic, original_queue, original_stream,
original_offset, status, last_error, dead_letter_reason, task_id, task_name
```

Headers are encoded as `h:<name>`. Metadata is encoded as `m:<name>`.

Unknown keys must be skipped by decoders. This is what makes the format forward-compatible.

## Payload handling order

Encoding:

```text
1. Start with raw message payload bytes.
2. Compress with gzip when threshold is configured and compressed size is smaller.
3. Encrypt compressed/plain payload with AES-GCM when encryption key is configured.
4. Encode TLV header and payload.
5. Add CRC32C.
6. Add HMAC-SHA256 when signing key is configured.
```

Decoding:

```text
1. Validate magic/version/lengths/max frame size.
2. Validate CRC32C.
3. Validate HMAC when frame is signed.
4. Decode TLV header.
5. Decrypt payload when encrypted.
6. Decompress payload when compressed.
7. Return broker.Message with opaque Payload bytes.
```

## Security configuration

Recommended production settings:

```bcl
codec "fwb1" {
  max_frame_size "128MB"
  compress_threshold "4KB"
  signing_key env("BROKER_FRAME_SIGNING_KEY", "")
  encryption_key env("BROKER_FRAME_ENCRYPTION_KEY", "")
}
```

Use different keys for signing and encryption. Keep them outside source control. For AES-GCM, 16, 24, and 32-byte keys are used directly. Other key lengths are normalized with SHA-256.

## HTTP endpoints

Publish one frame:

```text
POST /v1/publish/frame
Content-Type: application/vnd.pool.broker.frame
Body: raw FWB1 frame bytes
```

Poll frames:

```text
POST /v1/poll/frame
Content-Type: application/json
Body: {"queue":"codec.frames","consumer_id":"codec-client","consumer_group":"codec-demo","limit":10}
Response: length-prefixed FWB1 frame batch
```

Each response frame is prefixed by a uvarint frame length.


## BCL payload syntax

Example BCL files should represent payloads as native BCL object blocks, not escaped strings. The example config loader converts the block to compact JSON bytes before publishing/encoding.

```bcl
codec_example "complete-codec" {
  topic "codec.demo"
  queue "codec.frames"

  payload {
    hello "world"
    mode "fwb1"
    note "BCL object block converted to JSON payload bytes"
  }
}

operation "publish-batch" {
  payloads {
    item { to "batch1@example.com" }
    item { to "batch2@example.com" }
  }
}
```

The broker still receives opaque bytes. In these examples the bytes are JSON generated from the BCL block, keeping configuration readable without escaped payload strings. Raw backtick strings remain supported only for compatibility and special cases such as scripts or SQL.

## Go usage

```go
codec := broker.NewBinaryMessageCodec(
    broker.WithFrameMaxSize(128<<20),
    broker.WithFrameCompressionThreshold(1024),
    broker.WithFrameSigningKey([]byte("signing-secret")),
    broker.WithFrameEncryptionKey([]byte("encryption-secret")),
)

msg := broker.Message{
    ID: "msg-1",
    Topic: "codec.demo",
    Queue: "codec.frames",
    ContentType: "application/json",
    Payload: []byte(`{"hello":"world"}`),
    Headers: map[string]string{"x-example":"true"},
}

frame, err := codec.EncodeMessage(nil, msg)

var decoded broker.Message
err = codec.DecodeMessage(frame, &decoded)
```

## Failure detection

FWB1 rejects:

- invalid magic
- unsupported version
- oversized frames
- invalid TLV lengths
- CRC mismatch
- HMAC mismatch
- encrypted frames without encryption key
- signed frames without signing key
- trailing bytes
- truncated frames


## BCL payload syntax

Example BCL files should represent payloads as native BCL object blocks, not escaped strings. The example config loader converts the block to compact JSON bytes before publishing/encoding.

```bcl
codec_example "complete-codec" {
  topic "codec.demo"
  queue "codec.frames"

  payload {
    hello "world"
    mode "fwb1"
    note "BCL object block converted to JSON payload bytes"
  }
}

operation "publish-batch" {
  payloads {
    item { to "batch1@example.com" }
    item { to "batch2@example.com" }
  }
}
```

The broker still receives opaque bytes. In these examples the bytes are JSON generated from the BCL block, keeping configuration readable without escaped payload strings. Raw backtick strings remain supported only for compatibility and special cases such as scripts or SQL.
