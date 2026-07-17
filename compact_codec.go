package broker

import (
	"bytes"
	"compress/gzip"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

// FWB2 is a lightweight binary message frame format optimized for minimal size.
// It uses numeric tags instead of string keys, packed integers, and compact encoding.
//
// Frame layout:
//   Magic (4) | Version (1) | Flags (1) | HeaderLen (varint) | PayloadLen (varint)
//   CRC32C (4) | Header (TLV) | Payload
//
// TLV layout per field:
//   Tag (1 byte) | Length (varint) | Value (N bytes)
//
// Tag assignments (1-byte, 0-255):
//   0x01: id            0x0b: type           0x15: correlation_id
//   0x02: parent_id     0x0c: content_type   0x16: causation_id
//   0x03: topic         0x0d: schema         0x17: request_id
//   0x04: queue         0x0e: schema_version 0x18: reply_to
//   0x05: stream        0x0f: trace_id       0x19: reply_group
//   0x06: partition     0x10: span_id        0x1a: idempotency_key
//   0x07: offset        0x11: key            0x1b: deduplication_id
//   0x08: routing_key   0x12: priority       0x1c: tenant_id
//   0x09: status        0x13: created_at     0x1d: lease_token
//   0x0a: attempts      0x14: available_at   0x1e: last_error

const (
	FWB2Magic        = "FWB2"
	FWB2Version byte = 2

	// Compact TLV tags (1 byte each)
	tagID               byte = 0x01
	tagParentID         byte = 0x02
	tagTopic            byte = 0x03
	tagQueue            byte = 0x04
	tagStream           byte = 0x05
	tagPartition        byte = 0x06
	tagOffset           byte = 0x07
	tagRoutingKey       byte = 0x08
	tagStatus           byte = 0x09
	tagAttempts         byte = 0x0A
	tagType             byte = 0x0B
	tagContentType      byte = 0x0C
	tagSchema           byte = 0x0D
	tagSchemaVersion    byte = 0x0E
	tagTraceID          byte = 0x0F
	tagSpanID           byte = 0x10
	tagKey              byte = 0x11
	tagPriority         byte = 0x12
	tagCreatedAt        byte = 0x13
	tagAvailableAt      byte = 0x14
	tagCorrelationID    byte = 0x15
	tagCausationID      byte = 0x16
	tagRequestID        byte = 0x17
	tagReplyTo          byte = 0x18
	tagReplyGroup       byte = 0x19
	tagIdempotencyKey   byte = 0x1A
	tagDeduplicationID  byte = 0x1B
	tagTenantID         byte = 0x1C
	tagLeaseToken       byte = 0x1D
	tagLastError        byte = 0x1E
	tagMaxAttempts      byte = 0x1F
	tagRedeliveries     byte = 0x20
	tagExpiresAt        byte = 0x21
	tagLeaseUntil       byte = 0x22
	tagProducerID       byte = 0x23
	tagConsumerID       byte = 0x24
	tagConsumerGroup    byte = 0x25
	tagTaskID           byte = 0x26
	tagTaskName         byte = 0x27
	tagDeadLetterReason byte = 0x28
	tagOriginalTopic    byte = 0x29
	tagOriginalQueue    byte = 0x2A
	tagOriginalStream   byte = 0x2B
	tagOriginalOffset   byte = 0x2C
	tagHeaders          byte = 0x30
	tagMetadata         byte = 0x31
	tagPayloadChecksum  byte = 0x3F

	// Frame flags (compact)
	fwb2FlagCompressed uint8 = 1 << 0
	fwb2FlagEncrypted  uint8 = 1 << 1
	fwb2FlagSigned     uint8 = 1 << 2
	fwb2FlagHasHeaders uint8 = 1 << 3
	fwb2FlagHasMeta    uint8 = 1 << 4
)

// CompactMessageCodec is a lightweight binary codec optimized for minimal frame size.
// Uses 1-byte numeric tags instead of string keys, packed integers, and delta timestamps.
type CompactMessageCodec struct {
	CompressThreshold int
	MaxFrameSize      int
	EncryptionKey     []byte
	SigningKey        []byte
}

type CompactCodecOption func(*CompactMessageCodec)

func WithCompactCompressThreshold(n int) CompactCodecOption {
	return func(c *CompactMessageCodec) { c.CompressThreshold = n }
}
func WithCompactMaxFrameSize(n int) CompactCodecOption {
	return func(c *CompactMessageCodec) { c.MaxFrameSize = n }
}
func WithCompactEncryptionKey(key []byte) CompactCodecOption {
	return func(c *CompactMessageCodec) { c.EncryptionKey = append([]byte(nil), key...) }
}
func WithCompactSigningKey(key []byte) CompactCodecOption {
	return func(c *CompactMessageCodec) { c.SigningKey = append([]byte(nil), key...) }
}

func NewCompactMessageCodec(opts ...CompactCodecOption) *CompactMessageCodec {
	c := &CompactMessageCodec{MaxFrameSize: 1 << 20} // 1MB default
	for _, fn := range opts {
		if fn != nil {
			fn(c)
		}
	}
	return c
}

// EncodeMessage encodes a Message into a compact FWB2 frame.
func (c *CompactMessageCodec) EncodeMessage(dst []byte, msg Message) ([]byte, error) {
	if c == nil {
		c = NewCompactMessageCodec()
	}
	max := c.MaxFrameSize
	if max <= 0 {
		max = 1 << 20
	}
	payload := msg.Payload

	// Optional compression
	flags := uint8(0)
	if c.CompressThreshold > 0 && len(payload) >= c.CompressThreshold {
		compressed, err := gzipBytes(payload)
		if err != nil {
			return nil, err
		}
		if len(compressed) < len(payload) {
			payload = compressed
			flags |= fwb2FlagCompressed
		}
	}

	// Optional encryption
	meta := msg.Metadata
	if len(c.EncryptionKey) > 0 {
		meta = cloneMap(meta)
		sealed, nonce, err := sealPayload(c.EncryptionKey, payload, msg.ID)
		if err != nil {
			return nil, err
		}
		payload = sealed
		flags |= fwb2FlagEncrypted
		if meta == nil {
			meta = map[string]string{}
		}
		meta["_nonce"] = base64.RawStdEncoding.EncodeToString(nonce)
	}

	// Encode header using compact TLV
	header := encodeCompactTLV(nil, msg, meta, &flags)
	if len(header)+len(payload) > max {
		return nil, ErrMessageTooLarge
	}

	start := len(dst)
	// Frame: Magic(4) + Version(1) + Flags(1) + HeaderLen(varint) + PayloadLen(varint) + CRC32(4)
	dst = append(dst, FWB2Magic...)
	dst = append(dst, FWB2Version, flags)
	dst = binary.AppendUvarint(dst, uint64(len(header)))
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	crcPos := len(dst)
	dst = append(dst, 0, 0, 0, 0) // CRC placeholder
	dst = append(dst, header...)
	dst = append(dst, payload...)

	// CRC32C over everything after CRC field
	crc := crc32.Checksum(dst[crcPos+4:], castagnoliTable)
	binary.LittleEndian.PutUint32(dst[crcPos:], crc)

	// Optional HMAC signing
	if len(c.SigningKey) > 0 {
		mac := hmac.New(sha256.New, c.SigningKey)
		mac.Write(dst[start:])
		sig := mac.Sum(nil)
		dst = append(dst, sig...)
	}

	return dst[start:], nil
}

// DecodeMessage decodes a FWB2 frame into a Message.
func (c *CompactMessageCodec) DecodeMessage(frame []byte, msg *Message) error {
	if c == nil {
		c = NewCompactMessageCodec()
	}
	if len(frame) < 12 {
		return errors.New("fwb2: frame too short")
	}
	if string(frame[:4]) != FWB2Magic {
		return errors.New("fwb2: invalid magic")
	}
	version := frame[4]
	if version != FWB2Version {
		return fmt.Errorf("fwb2: unsupported version %d", version)
	}
	flags := frame[5]
	off := 6

	headerLen, n := binary.Uvarint(frame[off:])
	if n <= 0 {
		return errors.New("fwb2: invalid header length")
	}
	off += n

	payloadLen, n := binary.Uvarint(frame[off:])
	if n <= 0 {
		return errors.New("fwb2: invalid payload length")
	}
	off += n

	crcPos := off
	off += 4 // skip CRC

	// Verify CRC
	crc := crc32.Checksum(frame[off:off+int(headerLen)+int(payloadLen)], castagnoliTable)
	if crc != binary.LittleEndian.Uint32(frame[crcPos:]) {
		return errors.New("fwb2: CRC mismatch")
	}

	header := frame[off : off+int(headerLen)]
	off += int(headerLen)
	payload := frame[off : off+int(payloadLen)]

	// Optional HMAC verification
	if len(c.SigningKey) > 0 {
		off += int(payloadLen)
		if off > len(frame) {
			return errors.New("fwb2: frame truncated before signature")
		}
		sig := frame[off:]
		mac := hmac.New(sha256.New, c.SigningKey)
		mac.Write(frame[:off-len(sig)])
		if !hmac.Equal(sig, mac.Sum(nil)) {
			return errors.New("fwb2: HMAC mismatch")
		}
	}

	// Decompress if needed
	if flags&fwb2FlagCompressed != 0 {
		zr, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("fwb2: decompress: %w", err)
		}
		decompressed, err := io.ReadAll(zr)
		if err != nil {
			return fmt.Errorf("fwb2: decompress read: %w", err)
		}
		payload = decompressed
	}

	// Decrypt if needed
	if flags&fwb2FlagEncrypted != 0 {
		meta := make(map[string]string)
		decodeCompactTLV(header, nil, meta)
		nonceB64, ok := meta["_nonce"]
		if !ok {
			return errors.New("fwb2: missing nonce for decryption")
		}
		nonce, err := base64.RawStdEncoding.DecodeString(nonceB64)
		if err != nil {
			return fmt.Errorf("fwb2: decode nonce: %w", err)
		}
		decrypted, err := openPayload(c.EncryptionKey, payload, nonce, "")
		if err != nil {
			return fmt.Errorf("fwb2: decrypt: %w", err)
		}
		payload = decrypted
	}

	msg.Payload = payload
	decodeCompactTLV(header, msg, nil)
	return nil
}

// encodeCompactTLV encodes a Message using 1-byte numeric tags.
func encodeCompactTLV(dst []byte, m Message, metadata map[string]string, flags *uint8) []byte {
	put := func(tag byte, v string) {
		if v != "" {
			dst = appendCompactTLVString(dst, tag, v)
		}
	}
	putInt := func(tag byte, v int) {
		if v != 0 {
			dst = appendCompactTLVInt(dst, tag, int64(v))
		}
	}
	putUint64 := func(tag byte, v uint64) {
		if v != 0 {
			dst = appendCompactTLVUint64(dst, tag, v)
		}
	}
	putTime := func(tag byte, v time.Time) {
		if !v.IsZero() {
			dst = appendCompactTLVTime(dst, tag, v)
		}
	}

	put(tagID, m.ID)
	put(tagParentID, m.ParentID)
	put(tagTopic, m.Topic)
	put(tagQueue, m.Queue)
	put(tagStream, m.Stream)
	putInt(tagPartition, m.Partition)
	putUint64(tagOffset, m.Offset)
	put(tagRoutingKey, m.RoutingKey)
	put(tagType, m.Type)
	put(tagContentType, m.ContentType)
	put(tagSchema, m.Schema)
	put(tagSchemaVersion, m.SchemaVersion)
	put(tagTraceID, m.TraceID)
	put(tagSpanID, m.SpanID)
	put(tagKey, m.Key)
	put(tagStatus, string(m.Status))
	put(tagAttempts, fmt.Sprintf("%d", m.Attempts))
	put(tagMaxAttempts, fmt.Sprintf("%d", m.MaxAttempts))
	put(tagRedeliveries, fmt.Sprintf("%d", m.Redeliveries))
	putInt(tagPriority, m.Priority)
	putTime(tagCreatedAt, m.CreatedAt)
	putTime(tagAvailableAt, m.AvailableAt)
	putTime(tagExpiresAt, m.ExpiresAt)
	put(tagCorrelationID, m.CorrelationID)
	put(tagCausationID, m.CausationID)
	put(tagRequestID, m.RequestID)
	put(tagReplyTo, m.ReplyTo)
	put(tagReplyGroup, m.ReplyGroup)
	put(tagIdempotencyKey, m.IdempotencyKey)
	put(tagDeduplicationID, m.DeduplicationID)
	put(tagTenantID, m.TenantID)
	put(tagLeaseToken, m.LeaseToken)
	putTime(tagLeaseUntil, m.LeaseUntil)
	put(tagLastError, m.LastError)
	put(tagProducerID, m.ProducerID)
	put(tagConsumerID, m.ConsumerID)
	put(tagConsumerGroup, m.ConsumerGroup)
	put(tagTaskID, m.TaskID)
	put(tagTaskName, m.TaskName)
	put(tagDeadLetterReason, m.DeadLetterReason)
	put(tagOriginalTopic, m.OriginalTopic)
	put(tagOriginalQueue, m.OriginalQueue)
	put(tagOriginalStream, m.OriginalStream)
	putUint64(tagOriginalOffset, m.OriginalOffset)

	// Headers as single packed TLV
	if len(m.Headers) > 0 {
		*flags |= fwb2FlagHasHeaders
		dst = appendCompactHeaderMap(dst, tagHeaders, m.Headers)
	}
	// Metadata as single packed TLV
	if len(metadata) > 0 {
		*flags |= fwb2FlagHasMeta
		dst = appendCompactHeaderMap(dst, tagMetadata, metadata)
	}

	return dst
}

// decodeCompactTLV decodes a compact TLV header into a Message.
func decodeCompactTLV(src []byte, msg *Message, meta map[string]string) {
	if msg != nil {
		msg.Headers = map[string]string{}
	}
	if meta == nil {
		meta = map[string]string{}
	}
	off := 0
	for off < len(src) {
		if off >= len(src) {
			break
		}
		tag := src[off]
		off++

		// Handle packed header/metadata maps
		if tag == tagHeaders || tag == tagMetadata {
			count, n := binary.Uvarint(src[off:])
			if n <= 0 {
				break
			}
			off += n
			for i := uint64(0); i < count; i++ {
				kLen, n := binary.Uvarint(src[off:])
				if n <= 0 {
					break
				}
				off += n
				k := string(src[off : off+int(kLen)])
				off += int(kLen)
				vLen, n := binary.Uvarint(src[off:])
				if n <= 0 {
					break
				}
				off += n
				v := string(src[off : off+int(vLen)])
				off += int(vLen)
				if tag == tagHeaders && msg != nil {
					msg.Headers[k] = v
				} else {
					meta[k] = v
				}
			}
			continue
		}

		val, n := readCompactTLVValue(src, off)
		if n <= 0 {
			break
		}
		off += n
		s := string(val)

		if msg == nil {
			continue
		}
		switch tag {
		case tagID:
			msg.ID = s
		case tagParentID:
			msg.ParentID = s
		case tagTopic:
			msg.Topic = s
		case tagQueue:
			msg.Queue = s
		case tagStream:
			msg.Stream = s
		case tagPartition:
			msg.Partition = compactAtoi(s)
		case tagOffset:
			msg.Offset = compactAtou64(s)
		case tagRoutingKey:
			msg.RoutingKey = s
		case tagType:
			msg.Type = s
		case tagContentType:
			msg.ContentType = s
		case tagSchema:
			msg.Schema = s
		case tagSchemaVersion:
			msg.SchemaVersion = s
		case tagTraceID:
			msg.TraceID = s
		case tagSpanID:
			msg.SpanID = s
		case tagKey:
			msg.Key = s
		case tagStatus:
			msg.Status = MessageStatus(s)
		case tagAttempts:
			msg.Attempts = compactAtoi(s)
		case tagMaxAttempts:
			msg.MaxAttempts = compactAtoi(s)
		case tagRedeliveries:
			msg.Redeliveries = compactAtoi(s)
		case tagPriority:
			msg.Priority = compactAtoi(s)
		case tagCreatedAt:
			msg.CreatedAt = compactParseTime(s)
		case tagAvailableAt:
			msg.AvailableAt = compactParseTime(s)
		case tagExpiresAt:
			msg.ExpiresAt = compactParseTime(s)
		case tagCorrelationID:
			msg.CorrelationID = s
		case tagCausationID:
			msg.CausationID = s
		case tagRequestID:
			msg.RequestID = s
		case tagReplyTo:
			msg.ReplyTo = s
		case tagReplyGroup:
			msg.ReplyGroup = s
		case tagIdempotencyKey:
			msg.IdempotencyKey = s
		case tagDeduplicationID:
			msg.DeduplicationID = s
		case tagTenantID:
			msg.TenantID = s
		case tagLeaseToken:
			msg.LeaseToken = s
		case tagLeaseUntil:
			msg.LeaseUntil = compactParseTime(s)
		case tagLastError:
			msg.LastError = s
		case tagProducerID:
			msg.ProducerID = s
		case tagConsumerID:
			msg.ConsumerID = s
		case tagConsumerGroup:
			msg.ConsumerGroup = s
		case tagTaskID:
			msg.TaskID = s
		case tagTaskName:
			msg.TaskName = s
		case tagDeadLetterReason:
			msg.DeadLetterReason = s
		case tagOriginalTopic:
			msg.OriginalTopic = s
		case tagOriginalQueue:
			msg.OriginalQueue = s
		case tagOriginalStream:
			msg.OriginalStream = s
		case tagOriginalOffset:
			msg.OriginalOffset = compactAtou64(s)
		}
	}
}

// --- Compact TLV helpers ---

func appendCompactTLVString(dst []byte, tag byte, v string) []byte {
	b := []byte(v)
	dst = append(dst, tag)
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	dst = append(dst, b...)
	return dst
}

func appendCompactTLVInt(dst []byte, tag byte, v int64) []byte {
	s := fmt.Sprintf("%d", v)
	return appendCompactTLVString(dst, tag, s)
}

func appendCompactTLVUint64(dst []byte, tag byte, v uint64) []byte {
	s := fmt.Sprintf("%d", v)
	return appendCompactTLVString(dst, tag, s)
}

func appendCompactTLVTime(dst []byte, tag byte, t time.Time) []byte {
	s := t.Format(time.RFC3339Nano)
	return appendCompactTLVString(dst, tag, s)
}

func appendCompactHeaderMap(dst []byte, tag byte, m map[string]string) []byte {
	dst = append(dst, tag)
	dst = binary.AppendUvarint(dst, uint64(len(m)))
	for k, v := range m {
		kb := []byte(k)
		vb := []byte(v)
		dst = binary.AppendUvarint(dst, uint64(len(kb)))
		dst = append(dst, kb...)
		dst = binary.AppendUvarint(dst, uint64(len(vb)))
		dst = append(dst, vb...)
	}
	return dst
}

func readCompactTLVValue(src []byte, off int) ([]byte, int) {
	if off >= len(src) {
		return nil, 0
	}
	length, n := binary.Uvarint(src[off:])
	if n <= 0 || off+n+int(length) > len(src) {
		return nil, 0
	}
	off += n
	return src[off : off+int(length)], n + int(length)
}

func compactAtoi(s string) int {
	n := 0
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		} else if c == '-' && n == 0 {
			// negative - skip for now
		}
	}
	return n
}

func compactAtou64(s string) uint64 {
	n := uint64(0)
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + uint64(c-'0')
		}
	}
	return n
}

func compactParseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

// Size estimate: compare FWB1 vs FWB2 for the same message.
// FWB1 uses string keys like "idempotency_key" (15 bytes per field key).
// FWB2 uses 1-byte numeric tags.
//
// Example: A message with id="msg-1", topic="orders.created", queue="orders"
//
// FWB1 header overhead per field: 1 byte tag length + N bytes key + 1 byte value length + value
//   "id" = 1+2+1+6 = 10 bytes
//   "topic" = 1+5+1+14 = 21 bytes
//   "queue" = 1+5+1+6 = 13 bytes
//   Total: ~44 bytes just for field keys
//
// FWB2 header overhead per field: 1 byte tag + 1 byte value length + value
//   0x01 = 1+1+6 = 8 bytes
//   0x03 = 1+1+14 = 16 bytes
//   0x04 = 1+1+6 = 8 bytes
//   Total: ~32 bytes for field keys
//
// Savings: ~27% reduction in header size per field.
// For 30 fields, savings = ~360 bytes per message.
