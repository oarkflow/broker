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
	payload := append([]byte(nil), msg.Payload...)
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
	meta := cloneMap(msg.Metadata)
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

func encodeMessageTLV(dst []byte, m Message, metadata map[string]string) []byte {
	put := func(k, v string) { dst = appendTLV(dst, k, []byte(v)) }
	putb := func(k string, b []byte) {
		if len(b) > 0 {
			dst = appendTLV(dst, k, b)
		}
	}
	put("id", m.ID)
	put("parent_id", m.ParentID)
	put("topic", m.Topic)
	put("queue", m.Queue)
	put("stream", m.Stream)
	put("partition", strconv.Itoa(m.Partition))
	put("offset", strconv.FormatUint(m.Offset, 10))
	put("key", m.Key)
	put("routing_key", m.RoutingKey)
	put("type", m.Type)
	put("content_type", m.ContentType)
	put("schema", m.Schema)
	put("schema_version", m.SchemaVersion)
	put("trace_id", m.TraceID)
	put("span_id", m.SpanID)
	put("correlation_id", m.CorrelationID)
	put("causation_id", m.CausationID)
	put("request_id", m.RequestID)
	put("reply_to", m.ReplyTo)
	put("reply_group", m.ReplyGroup)
	put("idempotency_key", m.IdempotencyKey)
	put("deduplication_id", m.DeduplicationID)
	put("checksum", m.Checksum)
	put("priority", strconv.Itoa(m.Priority))
	put("created_at", timeToString(m.CreatedAt))
	put("available_at", timeToString(m.AvailableAt))
	put("expires_at", timeToString(m.ExpiresAt))
	put("attempts", strconv.Itoa(m.Attempts))
	put("max_attempts", strconv.Itoa(m.MaxAttempts))
	put("redeliveries", strconv.Itoa(m.Redeliveries))
	put("tenant_id", m.TenantID)
	put("producer_id", m.ProducerID)
	put("consumer_id", m.ConsumerID)
	put("consumer_group", m.ConsumerGroup)
	put("lease_token", m.LeaseToken)
	put("lease_until", timeToString(m.LeaseUntil))
	put("original_topic", m.OriginalTopic)
	put("original_queue", m.OriginalQueue)
	put("original_stream", m.OriginalStream)
	put("original_offset", strconv.FormatUint(m.OriginalOffset, 10))
	put("status", string(m.Status))
	put("last_error", m.LastError)
	put("dead_letter_reason", m.DeadLetterReason)
	put("task_id", m.TaskID)
	put("task_name", m.TaskName)
	for k, v := range m.Headers {
		put("h:"+k, v)
	}
	for k, v := range metadata {
		put("m:"+k, v)
	}
	putb("payload_checksum", []byte(payloadChecksum(m.Payload)))
	return dst
}

func decodeMessageTLV(src []byte) (Message, error) {
	var m Message
	m.Headers = map[string]string{}
	m.Metadata = map[string]string{}
	err := walkTLV(src, func(k string, v []byte) error {
		s := string(v)
		switch k {
		case "id":
			m.ID = s
		case "parent_id":
			m.ParentID = s
		case "topic":
			m.Topic = s
		case "queue":
			m.Queue = s
		case "stream":
			m.Stream = s
		case "partition":
			m.Partition = atoi(s)
		case "offset":
			m.Offset = atou64(s)
		case "key":
			m.Key = s
		case "routing_key":
			m.RoutingKey = s
		case "type":
			m.Type = s
		case "content_type":
			m.ContentType = s
		case "schema":
			m.Schema = s
		case "schema_version":
			m.SchemaVersion = s
		case "trace_id":
			m.TraceID = s
		case "span_id":
			m.SpanID = s
		case "correlation_id":
			m.CorrelationID = s
		case "causation_id":
			m.CausationID = s
		case "request_id":
			m.RequestID = s
		case "reply_to":
			m.ReplyTo = s
		case "reply_group":
			m.ReplyGroup = s
		case "idempotency_key":
			m.IdempotencyKey = s
		case "deduplication_id":
			m.DeduplicationID = s
		case "checksum":
			m.Checksum = s
		case "priority":
			m.Priority = atoi(s)
		case "created_at":
			m.CreatedAt = stringToTime(s)
		case "available_at":
			m.AvailableAt = stringToTime(s)
		case "expires_at":
			m.ExpiresAt = stringToTime(s)
		case "attempts":
			m.Attempts = atoi(s)
		case "max_attempts":
			m.MaxAttempts = atoi(s)
		case "redeliveries":
			m.Redeliveries = atoi(s)
		case "tenant_id":
			m.TenantID = s
		case "producer_id":
			m.ProducerID = s
		case "consumer_id":
			m.ConsumerID = s
		case "consumer_group":
			m.ConsumerGroup = s
		case "lease_token":
			m.LeaseToken = s
		case "lease_until":
			m.LeaseUntil = stringToTime(s)
		case "original_topic":
			m.OriginalTopic = s
		case "original_queue":
			m.OriginalQueue = s
		case "original_stream":
			m.OriginalStream = s
		case "original_offset":
			m.OriginalOffset = atou64(s)
		case "status":
			m.Status = MessageStatus(s)
		case "last_error":
			m.LastError = s
		case "dead_letter_reason":
			m.DeadLetterReason = s
		case "task_id":
			m.TaskID = s
		case "task_name":
			m.TaskName = s
		default:
			if len(k) > 2 && k[:2] == "h:" {
				m.Headers[k[2:]] = s
			}
			if len(k) > 2 && k[:2] == "m:" {
				m.Metadata[k[2:]] = s
			}
		}
		return nil
	})
	if err != nil {
		return Message{}, err
	}
	if len(m.Headers) == 0 {
		m.Headers = nil
	}
	if len(m.Metadata) == 0 {
		m.Metadata = nil
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
