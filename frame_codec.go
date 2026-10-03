package broker

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"time"
)

const (
	FrameMagic        = "FWB1"
	FrameVersion byte = 1
)

type FrameType byte

const (
	FrameMessageAppend FrameType = 1
	FrameMessageAck    FrameType = 2
	FrameMessageNack   FrameType = 3
	FrameMessageLease  FrameType = 4
	FrameMessageExtend FrameType = 5
	FrameMessageMove   FrameType = 6
	FrameMessageCopy   FrameType = 7
	FrameMessageDLQ    FrameType = 8
	FrameOffsetCommit  FrameType = 9
	FrameTaskProgress  FrameType = 10
	FrameTaskResult    FrameType = 11
	FrameConsumerEvent FrameType = 12
	FrameHeartbeat     FrameType = 13
	FrameMessage       FrameType = 100
)

type FrameFlags uint16

const (
	FrameFlagCompressed FrameFlags = 1 << iota
	FrameFlagEncrypted
	FrameFlagSigned
	FrameFlagHasHeaders
	FrameFlagHasMetadata
	FrameFlagTask
	FrameFlagRequest
	FrameFlagResponse
	FrameFlagStreamEnd
)

const (
	framePreludeLen = 4 + 1 + 1 + 2
	defaultMaxFrame = 128 << 20
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type BinaryMessageCodec struct {
	CompressThreshold int
	MaxFrameSize      int
	EncryptionKey     []byte
	SigningKey        []byte
}

type FrameHeader struct {
	Version    byte
	Type       FrameType
	Flags      FrameFlags
	HeaderLen  uint64
	PayloadLen uint64
	CRC32C     uint32
	HMACLen    uint64
}

func NewBinaryMessageCodec(opts ...BinaryMessageCodecOption) *BinaryMessageCodec {
	c := &BinaryMessageCodec{MaxFrameSize: defaultMaxFrame, CompressThreshold: 0}
	for _, fn := range opts {
		if fn != nil {
			fn(c)
		}
	}
	return c
}

type BinaryMessageCodecOption func(*BinaryMessageCodec)

func WithFrameCompressionThreshold(n int) BinaryMessageCodecOption {
	return func(c *BinaryMessageCodec) { c.CompressThreshold = n }
}
func WithFrameMaxSize(n int) BinaryMessageCodecOption {
	return func(c *BinaryMessageCodec) { c.MaxFrameSize = n }
}
func WithFrameEncryptionKey(key []byte) BinaryMessageCodecOption {
	return func(c *BinaryMessageCodec) { c.EncryptionKey = append([]byte(nil), key...) }
}
func WithFrameSigningKey(key []byte) BinaryMessageCodecOption {
	return func(c *BinaryMessageCodec) { c.SigningKey = append([]byte(nil), key...) }
}

func (c *BinaryMessageCodec) EncodeMessage(dst []byte, msg Message) ([]byte, error) {
	return c.EncodeFrame(dst, FrameMessage, msg)
}

func (c *BinaryMessageCodec) DecodeMessage(frame []byte, msg *Message) error {
	return c.DecodeFrame(frame, msg)
}

func (c *BinaryMessageCodec) EncodeMessageBase64(msg Message) (string, error) {
	b, err := c.EncodeMessage(nil, msg)
	if err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(b), nil
}

func (c *BinaryMessageCodec) DecodeMessageBase64(s string, msg *Message) error {
	b, err := base64.RawStdEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	return c.DecodeMessage(b, msg)
}

func (c *BinaryMessageCodec) EncodeFrame(dst []byte, typ FrameType, msg Message) ([]byte, error) {
	if c == nil {
		c = NewBinaryMessageCodec()
	}
	max := c.MaxFrameSize
	if max <= 0 {
		max = defaultMaxFrame
	}
	payload := msg.Payload
	flags := FrameFlags(0)
	if len(msg.Headers) > 0 {
		flags |= FrameFlagHasHeaders
	}
	if len(msg.Metadata) > 0 {
		flags |= FrameFlagHasMetadata
	}
	if msg.TaskID != "" || msg.TaskName != "" {
		flags |= FrameFlagTask
	}
	if msg.ReplyTo != "" || msg.RequestID != "" {
		flags |= FrameFlagRequest
	}
	if msg.CorrelationID != "" && msg.ReplyTo == "" && msg.RequestID == "" {
		flags |= FrameFlagResponse
	}
	meta := msg.Metadata
	if c.CompressThreshold > 0 && len(payload) >= c.CompressThreshold {
		compressed, err := gzipBytes(payload)
		if err != nil {
			return nil, err
		}
		if len(compressed) < len(payload) {
			payload = compressed
			flags |= FrameFlagCompressed
		}
	}
	if len(c.EncryptionKey) > 0 {
		meta = cloneMap(meta)
		sealed, nonce, err := sealPayload(c.EncryptionKey, payload, msg.ID)
		if err != nil {
			return nil, err
		}
		payload = sealed
		flags |= FrameFlagEncrypted
		if meta == nil {
			meta = map[string]string{}
		}
		meta["_fwb_nonce"] = base64.RawStdEncoding.EncodeToString(nonce)
	}
	if len(c.SigningKey) > 0 {
		flags |= FrameFlagSigned
	}
	header := encodeMessageTLV(nil, msg, meta)
	if len(header)+len(payload) > max {
		return nil, ErrMessageTooLarge
	}
	start := len(dst)
	dst = append(dst, FrameMagic...)
	dst = append(dst, FrameVersion, byte(typ))
	var two [2]byte
	binary.LittleEndian.PutUint16(two[:], uint16(flags))
	dst = append(dst, two[:]...)
	dst = binary.AppendUvarint(dst, uint64(len(header)))
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	crcPos := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	dst = append(dst, header...)
	dst = append(dst, payload...)
	crc := crc32.Checksum(dst[crcPos+4:], castagnoliTable)
	binary.LittleEndian.PutUint32(dst[crcPos:crcPos+4], crc)
	if len(c.SigningKey) > 0 {
		mac := hmac.New(sha256.New, c.SigningKey)
		_, _ = mac.Write(dst[start:])
		sig := mac.Sum(nil)
		dst = binary.AppendUvarint(dst, uint64(len(sig)))
		dst = append(dst, sig...)
	} else {
		dst = binary.AppendUvarint(dst, 0)
	}
	if len(dst)-start > max {
		return nil, ErrMessageTooLarge
	}
	return dst, nil
}

func (c *BinaryMessageCodec) DecodeFrame(frame []byte, msg *Message) error {
	h, header, payload, signedPart, hmacBytes, err := c.split(frame)
	if err != nil {
		return err
	}
	if h.Flags&FrameFlagSigned != 0 {
		if len(c.SigningKey) == 0 {
			return errf(ErrInvalidMessage, "frame is signed but no signing key configured")
		}
		mac := hmac.New(sha256.New, c.SigningKey)
		_, _ = mac.Write(signedPart)
		if !hmac.Equal(mac.Sum(nil), hmacBytes) {
			return errf(ErrInvalidMessage, "frame hmac mismatch")
		}
	}
	m, err := decodeMessageTLV(header)
	if err != nil {
		return err
	}
	payload = payload[:len(payload):len(payload)]
	if h.Flags&FrameFlagEncrypted != 0 {
		if len(c.EncryptionKey) == 0 {
			return errf(ErrInvalidMessage, "frame is encrypted but no encryption key configured")
		}
		nonceText := m.Metadata["_fwb_nonce"]
		nonce, err := base64.RawStdEncoding.DecodeString(nonceText)
		if err != nil {
			return errf(ErrInvalidMessage, "invalid frame nonce")
		}
		opened, err := openPayload(c.EncryptionKey, payload, nonce, m.ID)
		if err != nil {
			return err
		}
		payload = opened
		delete(m.Metadata, "_fwb_nonce")
	}
	if h.Flags&FrameFlagCompressed != 0 {
		plain, err := gunzipBytes(payload)
		if err != nil {
			return err
		}
		payload = plain
	}
	m.Payload = append(m.Payload[:0], payload...)
	*msg = m
	return nil
}

func (c *BinaryMessageCodec) PeekHeader(frame []byte, msg *Message) error {
	_, header, _, _, _, err := c.split(frame)
	if err != nil {
		return err
	}
	m, err := decodeMessageTLV(header)
	if err != nil {
		return err
	}
	m.Payload = nil
	*msg = m
	return nil
}

func (c *BinaryMessageCodec) split(frame []byte) (FrameHeader, []byte, []byte, []byte, []byte, error) {
	if c == nil {
		c = NewBinaryMessageCodec()
	}
	max := c.MaxFrameSize
	if max <= 0 {
		max = defaultMaxFrame
	}
	if len(frame) > max {
		return FrameHeader{}, nil, nil, nil, nil, ErrMessageTooLarge
	}
	if len(frame) < framePreludeLen+1 || string(frame[:4]) != FrameMagic {
		return FrameHeader{}, nil, nil, nil, nil, errf(ErrInvalidMessage, "invalid frame magic")
	}
	h := FrameHeader{Version: frame[4], Type: FrameType(frame[5]), Flags: FrameFlags(binary.LittleEndian.Uint16(frame[6:8]))}
	if h.Version != FrameVersion {
		return FrameHeader{}, nil, nil, nil, nil, errf(ErrInvalidMessage, "unsupported frame version %d", h.Version)
	}
	idx := 8
	v, n := binary.Uvarint(frame[idx:])
	if n <= 0 {
		return FrameHeader{}, nil, nil, nil, nil, errf(ErrInvalidMessage, "invalid header length")
	}
	h.HeaderLen = v
	idx += n
	v, n = binary.Uvarint(frame[idx:])
	if n <= 0 {
		return FrameHeader{}, nil, nil, nil, nil, errf(ErrInvalidMessage, "invalid payload length")
	}
	h.PayloadLen = v
	idx += n
	if len(frame[idx:]) < 4 {
		return FrameHeader{}, nil, nil, nil, nil, io.ErrUnexpectedEOF
	}
	h.CRC32C = binary.LittleEndian.Uint32(frame[idx : idx+4])
	crcStart := idx + 4
	idx += 4
	endHeader := idx + int(h.HeaderLen)
	endPayload := endHeader + int(h.PayloadLen)
	if endHeader < idx || endPayload < endHeader || endPayload > len(frame) {
		return FrameHeader{}, nil, nil, nil, nil, io.ErrUnexpectedEOF
	}
	gotCRC := crc32.Checksum(frame[crcStart:endPayload], castagnoliTable)
	if gotCRC != h.CRC32C {
		return FrameHeader{}, nil, nil, nil, nil, errf(ErrInvalidMessage, "frame crc32c mismatch")
	}
	macLen, n := binary.Uvarint(frame[endPayload:])
	if n <= 0 {
		return FrameHeader{}, nil, nil, nil, nil, errf(ErrInvalidMessage, "invalid hmac length")
	}
	h.HMACLen = macLen
	macStart := endPayload + n
	macEnd := macStart + int(macLen)
	if macEnd < macStart || macEnd > len(frame) {
		return FrameHeader{}, nil, nil, nil, nil, io.ErrUnexpectedEOF
	}
	if macEnd != len(frame) {
		return FrameHeader{}, nil, nil, nil, nil, errf(ErrInvalidMessage, "trailing frame bytes")
	}
	return h, frame[idx:endHeader], frame[endHeader:endPayload], frame[:endPayload], frame[macStart:macEnd], nil
}

// encodeMessageTLV writes only the fields that carry information. Absent keys
// decode to zero values, so this is wire-compatible with every decoder while
// shrinking a typical message from ~40 key/value pairs to the handful in use.
func encodeMessageTLV(dst []byte, m Message, metadata map[string]string) []byte {
	str := func(k, v string) {
		if v != "" {
			dst = appendTLVString(dst, k, v)
		}
	}
	num := func(k string, v int64) {
		if v != 0 {
			var tmp [20]byte
			dst = appendTLVBytes(dst, k, strconv.AppendInt(tmp[:0], v, 10))
		}
	}
	unum := func(k string, v uint64) {
		if v != 0 {
			var tmp [20]byte
			dst = appendTLVBytes(dst, k, strconv.AppendUint(tmp[:0], v, 10))
		}
	}
	ts := func(k string, t time.Time) {
		if !t.IsZero() {
			num(k, t.UnixNano())
		}
	}
	str("id", m.ID)
	str("parent_id", m.ParentID)
	str("topic", m.Topic)
	str("queue", m.Queue)
	str("stream", m.Stream)
	num("partition", int64(m.Partition))
	unum("offset", m.Offset)
	str("key", m.Key)
	str("routing_key", m.RoutingKey)
	str("type", m.Type)
	str("content_type", m.ContentType)
	str("schema", m.Schema)
	str("schema_version", m.SchemaVersion)
	str("trace_id", m.TraceID)
	str("span_id", m.SpanID)
	str("correlation_id", m.CorrelationID)
	str("causation_id", m.CausationID)
	str("request_id", m.RequestID)
	str("reply_to", m.ReplyTo)
	str("reply_group", m.ReplyGroup)
	str("idempotency_key", m.IdempotencyKey)
	str("deduplication_id", m.DeduplicationID)
	str("checksum", m.Checksum)
	num("priority", int64(m.Priority))
	ts("created_at", m.CreatedAt)
	ts("available_at", m.AvailableAt)
	ts("expires_at", m.ExpiresAt)
	num("attempts", int64(m.Attempts))
	num("max_attempts", int64(m.MaxAttempts))
	num("redeliveries", int64(m.Redeliveries))
	str("tenant_id", m.TenantID)
	str("producer_id", m.ProducerID)
	str("consumer_id", m.ConsumerID)
	str("consumer_group", m.ConsumerGroup)
	str("lease_token", m.LeaseToken)
	ts("lease_until", m.LeaseUntil)
	str("original_topic", m.OriginalTopic)
	str("original_queue", m.OriginalQueue)
	str("original_stream", m.OriginalStream)
	unum("original_offset", m.OriginalOffset)
	str("status", string(m.Status))
	str("last_error", m.LastError)
	str("dead_letter_reason", m.DeadLetterReason)
	str("task_id", m.TaskID)
	str("task_name", m.TaskName)
	for k, v := range m.Headers {
		dst = appendTLVString2(dst, "h:", k, v)
	}
	for k, v := range metadata {
		dst = appendTLVString2(dst, "m:", k, v)
	}
	if len(m.Payload) > 0 {
		dst = appendTLVString(dst, "payload_checksum", payloadChecksum(m.Payload))
	}
	return dst
}

// decodeMessageTLV decodes without allocating for keys or numeric values; only
// string fields that are present allocate, and header/metadata maps are created
// on first use.
func decodeMessageTLV(src []byte) (Message, error) {
	var m Message
	err := walkTLVBytes(src, func(k, v []byte) error {
		switch string(k) {
		case "id":
			m.ID = string(v)
		case "parent_id":
			m.ParentID = string(v)
		case "topic":
			m.Topic = string(v)
		case "queue":
			m.Queue = string(v)
		case "stream":
			m.Stream = string(v)
		case "partition":
			m.Partition = int(parseInt64Bytes(v))
		case "offset":
			m.Offset = parseUint64Bytes(v)
		case "key":
			m.Key = string(v)
		case "routing_key":
			m.RoutingKey = string(v)
		case "type":
			m.Type = string(v)
		case "content_type":
			m.ContentType = string(v)
		case "schema":
			m.Schema = string(v)
		case "schema_version":
			m.SchemaVersion = string(v)
		case "trace_id":
			m.TraceID = string(v)
		case "span_id":
			m.SpanID = string(v)
		case "correlation_id":
			m.CorrelationID = string(v)
		case "causation_id":
			m.CausationID = string(v)
		case "request_id":
			m.RequestID = string(v)
		case "reply_to":
			m.ReplyTo = string(v)
		case "reply_group":
			m.ReplyGroup = string(v)
		case "idempotency_key":
			m.IdempotencyKey = string(v)
		case "deduplication_id":
			m.DeduplicationID = string(v)
		case "checksum":
			m.Checksum = string(v)
		case "priority":
			m.Priority = int(parseInt64Bytes(v))
		case "created_at":
			m.CreatedAt = bytesToTime(v)
		case "available_at":
			m.AvailableAt = bytesToTime(v)
		case "expires_at":
			m.ExpiresAt = bytesToTime(v)
		case "attempts":
			m.Attempts = int(parseInt64Bytes(v))
		case "max_attempts":
			m.MaxAttempts = int(parseInt64Bytes(v))
		case "redeliveries":
			m.Redeliveries = int(parseInt64Bytes(v))
		case "tenant_id":
			m.TenantID = string(v)
		case "producer_id":
			m.ProducerID = string(v)
		case "consumer_id":
			m.ConsumerID = string(v)
		case "consumer_group":
			m.ConsumerGroup = string(v)
		case "lease_token":
			m.LeaseToken = string(v)
		case "lease_until":
			m.LeaseUntil = bytesToTime(v)
		case "original_topic":
			m.OriginalTopic = string(v)
		case "original_queue":
			m.OriginalQueue = string(v)
		case "original_stream":
			m.OriginalStream = string(v)
		case "original_offset":
			m.OriginalOffset = parseUint64Bytes(v)
		case "status":
			m.Status = MessageStatus(v)
		case "last_error":
			m.LastError = string(v)
		case "dead_letter_reason":
			m.DeadLetterReason = string(v)
		case "task_id":
			m.TaskID = string(v)
		case "task_name":
			m.TaskName = string(v)
		default:
			if len(k) > 2 && k[0] == 'h' && k[1] == ':' {
				if m.Headers == nil {
					m.Headers = make(map[string]string, 4)
				}
				m.Headers[string(k[2:])] = string(v)
			} else if len(k) > 2 && k[0] == 'm' && k[1] == ':' {
				if m.Metadata == nil {
					m.Metadata = make(map[string]string, 4)
				}
				m.Metadata[string(k[2:])] = string(v)
			}
		}
		return nil
	})
	if err != nil {
		return Message{}, err
	}
	return m, nil
}

func appendTLV(dst []byte, key string, val []byte) []byte {
	if key == "" && len(val) == 0 {
		return dst
	}
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	dst = binary.AppendUvarint(dst, uint64(len(val)))
	dst = append(dst, key...)
	dst = append(dst, val...)
	return dst
}

func appendTLVString(dst []byte, key, val string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	dst = binary.AppendUvarint(dst, uint64(len(val)))
	dst = append(dst, key...)
	return append(dst, val...)
}

func appendTLVString2(dst []byte, prefix, key, val string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(prefix)+len(key)))
	dst = binary.AppendUvarint(dst, uint64(len(val)))
	dst = append(dst, prefix...)
	dst = append(dst, key...)
	return append(dst, val...)
}

func appendTLVBytes(dst []byte, key string, val []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	dst = binary.AppendUvarint(dst, uint64(len(val)))
	dst = append(dst, key...)
	return append(dst, val...)
}

// walkTLVBytes is walkTLV without the per-field key string allocation.
func walkTLVBytes(src []byte, fn func(key, val []byte) error) error {
	for len(src) > 0 {
		kl, n := binary.Uvarint(src)
		if n <= 0 {
			return errf(ErrInvalidMessage, "invalid tlv key length")
		}
		src = src[n:]
		vl, n := binary.Uvarint(src)
		if n <= 0 {
			return errf(ErrInvalidMessage, "invalid tlv value length")
		}
		src = src[n:]
		if kl > uint64(len(src)) {
			return io.ErrUnexpectedEOF
		}
		key := src[:kl]
		src = src[kl:]
		if vl > uint64(len(src)) {
			return io.ErrUnexpectedEOF
		}
		val := src[:vl]
		src = src[vl:]
		if err := fn(key, val); err != nil {
			return err
		}
	}
	return nil
}

// parseInt64Bytes parses a decimal integer, returning 0 on malformed input
// exactly like the previous strconv-with-ignored-error behavior.
func parseInt64Bytes(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	neg := false
	if b[0] == '-' || b[0] == '+' {
		neg = b[0] == '-'
		b = b[1:]
		if len(b) == 0 {
			return 0
		}
	}
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' || n > (1<<63)/10 {
			return 0
		}
		n = n*10 + uint64(c-'0')
	}
	if neg {
		if n > 1<<63 {
			return 0
		}
		return -int64(n)
	}
	if n > 1<<63-1 {
		return 0
	}
	return int64(n)
}

func parseUint64Bytes(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' || n > (1<<64-1)/10 {
			return 0
		}
		d := uint64(c - '0')
		if n*10 > 1<<64-1-d {
			return 0
		}
		n = n*10 + d
	}
	return n
}

func bytesToTime(b []byte) time.Time {
	n := parseInt64Bytes(b)
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func walkTLV(src []byte, fn func(string, []byte) error) error {
	for len(src) > 0 {
		kl, n := binary.Uvarint(src)
		if n <= 0 {
			return errf(ErrInvalidMessage, "invalid tlv key length")
		}
		src = src[n:]
		vl, n := binary.Uvarint(src)
		if n <= 0 {
			return errf(ErrInvalidMessage, "invalid tlv value length")
		}
		src = src[n:]
		if kl > uint64(len(src)) {
			return io.ErrUnexpectedEOF
		}
		key := string(src[:kl])
		src = src[kl:]
		if vl > uint64(len(src)) {
			return io.ErrUnexpectedEOF
		}
		val := src[:vl]
		src = src[vl:]
		if err := fn(key, val); err != nil {
			return err
		}
	}
	return nil
}

func gzipBytes(in []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(in); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gunzipBytes(in []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

func sealPayload(key, payload []byte, aad string) ([]byte, []byte, error) {
	block, err := aes.NewCipher(normalizeAESKey(key))
	if err != nil {
		return nil, nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, err
	}
	return gcm.Seal(nil, nonce, payload, []byte(aad)), nonce, nil
}

func openPayload(key, payload, nonce []byte, aad string) ([]byte, error) {
	block, err := aes.NewCipher(normalizeAESKey(key))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return gcm.Open(nil, nonce, payload, []byte(aad))
}

func normalizeAESKey(key []byte) []byte {
	switch len(key) {
	case 16, 24, 32:
		return append([]byte(nil), key...)
	default:
		sum := sha256.Sum256(key)
		return sum[:]
	}
}

func payloadChecksum(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	sum := crc32.Checksum(payload, castagnoliTable)
	return strconv.FormatUint(uint64(sum), 16)
}

func timeToString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}
func stringToTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}
func atoi(s string) int      { i, _ := strconv.Atoi(s); return i }
func atou64(s string) uint64 { i, _ := strconv.ParseUint(s, 10, 64); return i }

func IsBinaryFrame(b []byte) bool { return len(b) >= 4 && string(b[:4]) == FrameMagic }

func FrameFingerprint(b []byte) string {
	s := sha256.Sum256(b)
	return base64.RawStdEncoding.EncodeToString(s[:])
}

var ErrFrameEncrypted = errors.New("broker: frame encrypted")

func (h FrameHeader) String() string {
	return fmt.Sprintf("FWB%d type=%d flags=%d header=%d payload=%d", h.Version, h.Type, h.Flags, h.HeaderLen, h.PayloadLen)
}
