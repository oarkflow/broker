package broker

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"time"
)

// BWL1 is the broker's native write-ahead-log record format. Unlike the
// legacy binary WAL, BWL1 does not serialize a walRecord through JSON and does
// not wrap the mutation in a generic Message frame. It encodes the operation
// and its fields directly, with batch-common fields written once.
//
// Record layout (the file still prefixes every record with a uvarint length):
//
//	magic[4] | version[1] | op[1] | flags[1] | reserved[1]
//	payload_len[uvarint] | nonce_len[1] | nonce | payload | crc32c[4] | hmac[32]?
//
// The payload is a versioned, operation-specific packed binary structure.
// CRC32C covers magic through payload. HMAC-SHA256, when enabled, covers the
// same bytes plus CRC32C. Compression happens before encryption.
const (
	binaryWALMagic       = "BWL1"
	binaryWALVersion     = byte(1)
	binaryWALFixedHeader = 8
	binaryWALDefaultMax  = 128 << 20

	binaryWALFlagCompressed byte = 1 << iota
	binaryWALFlagEncrypted
	binaryWALFlagSigned
	binaryWALKnownFlags = binaryWALFlagCompressed | binaryWALFlagEncrypted | binaryWALFlagSigned
)

const (
	walOpDeclareQueue byte = iota + 1
	walOpDeclareTopic
	walOpDeclareStream
	walOpAppend
	walOpAppendAtomic
	walOpLease
	walOpLeaseBatch
	walOpUpdateBatch
	walOpAck
	walOpAckBatch
	walOpNack
	walOpMove
	walOpPauseQueue
	walOpResumeQueue
	walOpPurgeQueue
	walOpUpdate
	walOpCommitOffset
	// walOpSweepLeases is appended after the released codes so on-disk records
	// written by earlier builds keep their operation bytes.
	walOpSweepLeases
	walOpStreamTransaction
)

// WALCodecConfig configures the native BWL1 WAL record codec.
type WALCodecConfig struct {
	CompressThreshold int
	MaxRecordSize     int
	EncryptionKey     []byte
	SigningKey        []byte
}

// WALCodecConfigProvider lets a frame codec expose equivalent protection
// settings to the dedicated WAL codec. The built-in FWB1 and FWB2 codecs
// implement this interface.
type WALCodecConfigProvider interface {
	WALCodecConfig() WALCodecConfig
}

// WALRecordCodec encodes and decodes native BWL1 records. It is immutable and
// safe for concurrent use.
type WALRecordCodec struct {
	compressThreshold int
	maxRecordSize     int
	aead              cipher.AEAD
	signingKey        []byte
}

func NewWALRecordCodec(cfg WALCodecConfig) (*WALRecordCodec, error) {
	max := cfg.MaxRecordSize
	if max <= 0 {
		max = binaryWALDefaultMax
	}
	c := &WALRecordCodec{
		compressThreshold: cfg.CompressThreshold,
		maxRecordSize:     max,
		signingKey:        append([]byte(nil), cfg.SigningKey...),
	}
	if len(cfg.EncryptionKey) > 0 {
		block, err := newWALAESBlock(cfg.EncryptionKey)
		if err != nil {
			return nil, err
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("wal codec: initialize AES-GCM: %w", err)
		}
		c.aead = aead
	}
	return c, nil
}

func newWALRecordCodecFromFrameCodec(codec FrameCodec) (*WALRecordCodec, error) {
	cfg := WALCodecConfig{MaxRecordSize: binaryWALDefaultMax}
	if provider, ok := codec.(WALCodecConfigProvider); ok {
		cfg = provider.WALCodecConfig()
	}
	return NewWALRecordCodec(cfg)
}

func newWALAESBlock(key []byte) (cipher.Block, error) {
	normalized := normalizeAESKey(key)
	block, err := aes.NewCipher(normalized)
	if err != nil {
		return nil, fmt.Errorf("wal codec: initialize AES: %w", err)
	}
	return block, nil
}

// encode serializes one mutation into a complete BWL1 record (without the
// outer file-length prefix).
func (c *WALRecordCodec) encode(dst []byte, record walRecord) ([]byte, error) {
	if c == nil {
		var err error
		c, err = NewWALRecordCodec(WALCodecConfig{})
		if err != nil {
			return nil, err
		}
	}
	op, ok := walOperationCode(record.Op)
	if !ok {
		return nil, errf(ErrInvalidMessage, "unsupported WAL operation %q", record.Op)
	}
	if c.compressThreshold <= 0 && c.aead == nil {
		return c.encodePlain(dst, op, record)
	}
	return c.encodeProtected(dst, op, record)
}

// encodePlain is the default high-throughput path. The operation payload is
// written directly into the final frame allocation; no intermediate payload or
// generic message frame is created.
func (c *WALRecordCodec) encodePlain(dst []byte, op byte, record walRecord) ([]byte, error) {
	const headerReserve = binaryWALFixedHeader + binary.MaxVarintLen64 + 1
	start := len(dst)
	signatureLen := 0
	flags := byte(0)
	if len(c.signingKey) > 0 {
		flags |= binaryWALFlagSigned
		signatureLen = sha256.Size
	}
	estimated := estimateWALRecordSize(record)
	needed := headerReserve + estimated + 4 + signatureLen
	if needed > c.maxRecordSize {
		needed = c.maxRecordSize
	}
	if needed < headerReserve+4+signatureLen {
		needed = headerReserve + 4 + signatureLen
	}
	if cap(dst)-len(dst) < needed {
		grown := make([]byte, len(dst), len(dst)+needed)
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:start+headerReserve]
	w := walBinaryWriter{buf: dst}
	w.time(record.At)
	encodeWALOperation(&w, op, record)
	dst = w.buf
	payloadStart := start + headerReserve
	payloadLen := len(dst) - payloadStart
	if payloadLen < 0 || payloadLen > c.maxRecordSize {
		return nil, ErrMessageTooLarge
	}
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(payloadLen))
	headerEnd := start + binaryWALFixedHeader + n + 1
	copy(dst[headerEnd:headerEnd+payloadLen], dst[payloadStart:payloadStart+payloadLen])
	dst = dst[:headerEnd+payloadLen]
	copy(dst[start:start+4], binaryWALMagic)
	dst[start+4] = binaryWALVersion
	dst[start+5] = op
	dst[start+6] = flags
	dst[start+7] = 0
	copy(dst[start+binaryWALFixedHeader:headerEnd-1], length[:n])
	dst[headerEnd-1] = 0 // nonce length
	crc := crc32.Checksum(dst[start:], castagnoliTable)
	dst = binary.LittleEndian.AppendUint32(dst, crc)
	if flags&binaryWALFlagSigned != 0 {
		mac := hmac.New(sha256.New, c.signingKey)
		_, _ = mac.Write(dst[start:])
		dst = mac.Sum(dst)
	}
	if len(dst)-start > c.maxRecordSize {
		return nil, ErrMessageTooLarge
	}
	return dst, nil
}

func (c *WALRecordCodec) encodeProtected(dst []byte, op byte, record walRecord) ([]byte, error) {
	body := make([]byte, 0, estimateWALRecordSize(record))
	w := walBinaryWriter{buf: body}
	w.time(record.At)
	encodeWALOperation(&w, op, record)
	body = w.buf
	if len(body) > c.maxRecordSize {
		return nil, ErrMessageTooLarge
	}

	flags := byte(0)
	payload := body
	if c.compressThreshold > 0 && len(payload) >= c.compressThreshold {
		compressed, err := gzipBytes(payload)
		if err != nil {
			return nil, err
		}
		if len(compressed) < len(payload) {
			payload = compressed
			flags |= binaryWALFlagCompressed
		}
	}
	if c.aead != nil {
		flags |= binaryWALFlagEncrypted
	}
	if len(c.signingKey) > 0 {
		flags |= binaryWALFlagSigned
	}

	start := len(dst)
	dst = append(dst, binaryWALMagic...)
	dst = append(dst, binaryWALVersion, op, flags, 0)

	var nonce []byte
	if c.aead != nil {
		nonce = make([]byte, c.aead.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return nil, fmt.Errorf("wal codec: generate nonce: %w", err)
		}
		payload = c.aead.Seal(nil, nonce, payload, dst[start:start+binaryWALFixedHeader])
	}
	if len(payload) > c.maxRecordSize {
		return nil, ErrMessageTooLarge
	}

	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	if len(nonce) > 255 {
		return nil, errf(ErrInvalidMessage, "WAL nonce too large")
	}
	dst = append(dst, byte(len(nonce)))
	dst = append(dst, nonce...)
	dst = append(dst, payload...)
	crc := crc32.Checksum(dst[start:], castagnoliTable)
	dst = binary.LittleEndian.AppendUint32(dst, crc)
	if flags&binaryWALFlagSigned != 0 {
		mac := hmac.New(sha256.New, c.signingKey)
		_, _ = mac.Write(dst[start:])
		dst = mac.Sum(dst)
	}
	if len(dst)-start > c.maxRecordSize {
		return nil, ErrMessageTooLarge
	}
	return dst, nil
}

func (c *WALRecordCodec) decode(frame []byte, record *walRecord) error {
	if record == nil {
		return errf(ErrInvalidMessage, "nil WAL record destination")
	}
	if c == nil {
		var err error
		c, err = NewWALRecordCodec(WALCodecConfig{})
		if err != nil {
			return err
		}
	}
	if len(frame) > c.maxRecordSize {
		return ErrMessageTooLarge
	}
	if len(frame) < binaryWALFixedHeader+1+4 || string(frame[:4]) != binaryWALMagic {
		return errf(ErrInvalidMessage, "invalid BWL1 magic")
	}
	if frame[4] != binaryWALVersion {
		return errf(ErrInvalidMessage, "unsupported BWL version %d", frame[4])
	}
	op := frame[5]
	operation, ok := walOperationName(op)
	if !ok {
		return errf(ErrInvalidMessage, "unsupported WAL operation code %d", op)
	}
	flags := frame[6]
	if flags&^binaryWALKnownFlags != 0 || frame[7] != 0 {
		return errf(ErrInvalidMessage, "unsupported BWL flags/reserved bits")
	}
	idx := binaryWALFixedHeader
	payloadLen, n := binary.Uvarint(frame[idx:])
	if n <= 0 {
		return errf(ErrInvalidMessage, "invalid BWL payload length")
	}
	idx += n
	if idx >= len(frame) {
		return io.ErrUnexpectedEOF
	}
	nonceLen := int(frame[idx])
	idx++
	if nonceLen < 0 || idx+nonceLen < idx || idx+nonceLen > len(frame) {
		return io.ErrUnexpectedEOF
	}
	nonce := frame[idx : idx+nonceLen]
	idx += nonceLen
	if payloadLen > uint64(c.maxRecordSize) || payloadLen > uint64(len(frame)-idx) {
		return io.ErrUnexpectedEOF
	}
	payloadEnd := idx + int(payloadLen)
	crcEnd := payloadEnd + 4
	signatureLen := 0
	if flags&binaryWALFlagSigned != 0 {
		signatureLen = sha256.Size
	}
	if crcEnd < payloadEnd || crcEnd+signatureLen != len(frame) {
		return errf(ErrInvalidMessage, "invalid BWL record length")
	}
	wantCRC := binary.LittleEndian.Uint32(frame[payloadEnd:crcEnd])
	gotCRC := crc32.Checksum(frame[:payloadEnd], castagnoliTable)
	if wantCRC != gotCRC {
		return errf(ErrInvalidMessage, "BWL CRC32C mismatch")
	}
	if flags&binaryWALFlagSigned != 0 {
		if len(c.signingKey) == 0 {
			return errf(ErrInvalidMessage, "BWL record is signed but no signing key is configured")
		}
		mac := hmac.New(sha256.New, c.signingKey)
		_, _ = mac.Write(frame[:crcEnd])
		if !hmac.Equal(mac.Sum(nil), frame[crcEnd:]) {
			return errf(ErrInvalidMessage, "BWL HMAC mismatch")
		}
	}

	payload := frame[idx:payloadEnd]
	if flags&binaryWALFlagEncrypted != 0 {
		if c.aead == nil {
			return errf(ErrInvalidMessage, "BWL record is encrypted but no encryption key is configured")
		}
		if len(nonce) != c.aead.NonceSize() {
			return errf(ErrInvalidMessage, "invalid BWL nonce size")
		}
		opened, err := c.aead.Open(nil, nonce, payload, frame[:binaryWALFixedHeader])
		if err != nil {
			return errf(ErrInvalidMessage, "BWL decrypt failed: %v", err)
		}
		payload = opened
	} else if nonceLen != 0 {
		return errf(ErrInvalidMessage, "unexpected BWL nonce")
	}
	if flags&binaryWALFlagCompressed != 0 {
		plain, err := gunzipWALBytes(payload, c.maxRecordSize)
		if err != nil {
			return errf(ErrInvalidMessage, "BWL decompress failed: %v", err)
		}
		payload = plain
	}
	if len(payload) > c.maxRecordSize {
		return ErrMessageTooLarge
	}

	r := walBinaryReader{buf: payload, max: c.maxRecordSize}
	decoded := walRecord{Op: operation, At: r.time()}
	decodeWALOperation(&r, op, &decoded)
	if r.err != nil {
		return r.err
	}
	if r.off != len(r.buf) {
		return errf(ErrInvalidMessage, "BWL record has %d trailing bytes", len(r.buf)-r.off)
	}
	*record = decoded
	return nil
}

func gunzipWALBytes(payload []byte, max int) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	limited := io.LimitReader(zr, int64(max)+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(out) > max {
		return nil, ErrMessageTooLarge
	}
	return out, nil
}

func isNativeBinaryWALRecord(frame []byte) bool {
	return len(frame) >= 4 && string(frame[:4]) == binaryWALMagic
}

func walOperationCode(op string) (byte, bool) {
	switch op {
	case "declare_queue":
		return walOpDeclareQueue, true
	case "declare_topic":
		return walOpDeclareTopic, true
	case "declare_stream":
		return walOpDeclareStream, true
	case "append":
		return walOpAppend, true
	case "append_atomic":
		return walOpAppendAtomic, true
	case "lease":
		return walOpLease, true
	case "lease_batch":
		return walOpLeaseBatch, true
	case "update_batch":
		return walOpUpdateBatch, true
	case "ack":
		return walOpAck, true
	case "ack_batch":
		return walOpAckBatch, true
	case "nack":
		return walOpNack, true
	case "move":
		return walOpMove, true
	case "pause_queue":
		return walOpPauseQueue, true
	case "resume_queue":
		return walOpResumeQueue, true
	case "purge_queue":
		return walOpPurgeQueue, true
	case "update":
		return walOpUpdate, true
	case "commit_offset":
		return walOpCommitOffset, true
	case "sweep_leases":
		return walOpSweepLeases, true
	case "stream_transaction":
		return walOpStreamTransaction, true
	default:
		return 0, false
	}
}

func walOperationName(op byte) (string, bool) {
	switch op {
	case walOpDeclareQueue:
		return "declare_queue", true
	case walOpDeclareTopic:
		return "declare_topic", true
	case walOpDeclareStream:
		return "declare_stream", true
	case walOpAppend:
		return "append", true
	case walOpAppendAtomic:
		return "append_atomic", true
	case walOpLease:
		return "lease", true
	case walOpLeaseBatch:
		return "lease_batch", true
	case walOpUpdateBatch:
		return "update_batch", true
	case walOpAck:
		return "ack", true
	case walOpAckBatch:
		return "ack_batch", true
	case walOpNack:
		return "nack", true
	case walOpMove:
		return "move", true
	case walOpPauseQueue:
		return "pause_queue", true
	case walOpResumeQueue:
		return "resume_queue", true
	case walOpPurgeQueue:
		return "purge_queue", true
	case walOpUpdate:
		return "update", true
	case walOpCommitOffset:
		return "commit_offset", true
	case walOpSweepLeases:
		return "sweep_leases", true
	case walOpStreamTransaction:
		return "stream_transaction", true
	default:
		return "", false
	}
}

func encodeWALOperation(w *walBinaryWriter, op byte, r walRecord) {
	switch op {
	case walOpDeclareQueue:
		w.queueConfig(r.Queue)
	case walOpDeclareTopic:
		w.topicConfig(r.Topic)
	case walOpDeclareStream:
		w.streamConfig(r.Stream)
	case walOpAppend:
		w.message(r.Message, 0)
		w.destinations(r.Dests)
	case walOpAppendAtomic:
		w.appendEntries(r.Entries)
	case walOpStreamTransaction:
		w.appendEntries(r.Entries)
		w.offsetCommit(r.Offset)
		w.uvarint(r.ExpectedOffset)
	case walOpLease:
		w.message(r.Message, 0)
		w.leaseRequest(r.Lease)
	case walOpLeaseBatch, walOpUpdateBatch:
		w.messages(r.Messages)
		if op == walOpLeaseBatch {
			w.leaseRequest(r.Lease)
		}
	case walOpAck:
		w.ackRequest(r.Ack, 0)
	case walOpAckBatch:
		w.ackRequests(r.Acks)
	case walOpNack:
		w.nackRequest(r.Nack)
	case walOpMove:
		w.moveRequest(r.Move)
	case walOpPauseQueue, walOpResumeQueue, walOpPurgeQueue:
		w.string(r.QueueName)
	case walOpUpdate:
		w.message(r.Message, 0)
	case walOpCommitOffset:
		w.offsetCommit(r.Offset)
	case walOpSweepLeases:
		w.messages(r.Messages)
	}
}

func decodeWALOperation(r *walBinaryReader, op byte, out *walRecord) {
	switch op {
	case walOpDeclareQueue:
		out.Queue = r.queueConfig()
	case walOpDeclareTopic:
		out.Topic = r.topicConfig()
	case walOpDeclareStream:
		out.Stream = r.streamConfig()
	case walOpAppend:
		out.Message = r.message(Message{})
		out.Dests = r.destinations()
	case walOpAppendAtomic:
		out.Entries = r.appendEntries()
	case walOpStreamTransaction:
		out.Entries = r.appendEntries()
		out.Offset = r.offsetCommit()
		out.ExpectedOffset = r.uvarint()
	case walOpLease:
		out.Message = r.message(Message{})
		out.Lease = r.leaseRequest()
	case walOpLeaseBatch, walOpUpdateBatch:
		out.Messages = r.messages()
		if op == walOpLeaseBatch {
			out.Lease = r.leaseRequest()
		}
	case walOpAck:
		out.Ack = r.ackRequest(AckRequest{})
	case walOpAckBatch:
		out.Acks = r.ackRequests()
	case walOpNack:
		out.Nack = r.nackRequest()
	case walOpMove:
		out.Move = r.moveRequest()
	case walOpPauseQueue, walOpResumeQueue, walOpPurgeQueue:
		out.QueueName = r.string()
	case walOpUpdate:
		out.Message = r.message(Message{})
	case walOpCommitOffset:
		out.Offset = r.offsetCommit()
	case walOpSweepLeases:
		out.Messages = r.messages()
	}
}

func estimateWALRecordSize(r walRecord) int {
	size := 128 + len(r.Message.Payload) + len(r.Entries)*128 + len(r.Messages)*128 + len(r.Acks)*48
	for i := range r.Entries {
		size += len(r.Entries[i].Message.Payload)
	}
	for i := range r.Messages {
		size += len(r.Messages[i].Payload)
	}
	if size > binaryWALDefaultMax {
		return binaryWALDefaultMax
	}
	return size
}

// -----------------------------------------------------------------------------
// Packed operation payload codec
// -----------------------------------------------------------------------------

type walBinaryWriter struct {
	buf []byte
}

func (w *walBinaryWriter) byte(v byte) { w.buf = append(w.buf, v) }
func (w *walBinaryWriter) bool(v bool) {
	if v {
		w.byte(1)
	} else {
		w.byte(0)
	}
}
func (w *walBinaryWriter) uvarint(v uint64)         { w.buf = binary.AppendUvarint(w.buf, v) }
func (w *walBinaryWriter) varint(v int64)           { w.buf = binary.AppendVarint(w.buf, v) }
func (w *walBinaryWriter) int(v int)                { w.varint(int64(v)) }
func (w *walBinaryWriter) duration(v time.Duration) { w.varint(int64(v)) }
func (w *walBinaryWriter) float64(v float64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, math.Float64bits(v))
}
func (w *walBinaryWriter) string(v string) { w.uvarint(uint64(len(v))); w.buf = append(w.buf, v...) }
func (w *walBinaryWriter) bytes(v []byte)  { w.uvarint(uint64(len(v))); w.buf = append(w.buf, v...) }
func (w *walBinaryWriter) time(v time.Time) {
	if v.IsZero() {
		w.byte(0)
		return
	}
	w.byte(1)
	w.varint(v.UnixNano())
}
func (w *walBinaryWriter) stringMap(v map[string]string) {
	w.uvarint(uint64(len(v)))
	for k, value := range v {
		w.string(k)
		w.string(value)
	}
}
func (w *walBinaryWriter) stringSlice(v []string) {
	w.uvarint(uint64(len(v)))
	for _, item := range v {
		w.string(item)
	}
}

type walBinaryReader struct {
	buf []byte
	off int
	max int
	err error
}

func (r *walBinaryReader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = errf(ErrInvalidMessage, format, args...)
	}
}
func (r *walBinaryReader) byte() byte {
	if r.err != nil || r.off >= len(r.buf) {
		if r.err == nil {
			r.err = io.ErrUnexpectedEOF
		}
		return 0
	}
	v := r.buf[r.off]
	r.off++
	return v
}
func (r *walBinaryReader) bool() bool {
	v := r.byte()
	if v > 1 {
		r.fail("invalid boolean value %d", v)
	}
	return v == 1
}
func (r *walBinaryReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.buf[r.off:])
	if n == 0 {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	if n < 0 {
		r.fail("uvarint overflow")
		return 0
	}
	r.off += n
	return v
}
func (r *walBinaryReader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.buf[r.off:])
	if n == 0 {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	if n < 0 {
		r.fail("varint overflow")
		return 0
	}
	r.off += n
	return v
}
func (r *walBinaryReader) int() int {
	v := r.varint()
	maxInt := int64(^uint(0) >> 1)
	minIntValue := -maxInt - 1
	if v > maxInt || v < minIntValue {
		r.fail("integer overflow")
		return 0
	}
	return int(v)
}
func (r *walBinaryReader) duration() time.Duration { return time.Duration(r.varint()) }
func (r *walBinaryReader) float64() float64 {
	if r.err != nil || len(r.buf)-r.off < 8 {
		if r.err == nil {
			r.err = io.ErrUnexpectedEOF
		}
		return 0
	}
	v := math.Float64frombits(binary.LittleEndian.Uint64(r.buf[r.off : r.off+8]))
	r.off += 8
	return v
}
func (r *walBinaryReader) length(limit int) int {
	v := r.uvarint()
	if r.err != nil {
		return 0
	}
	if v > uint64(limit) || v > uint64(len(r.buf)-r.off) {
		r.fail("length %d exceeds limit", v)
		return 0
	}
	return int(v)
}
func (r *walBinaryReader) string() string {
	n := r.length(r.max)
	if r.err != nil {
		return ""
	}
	v := string(r.buf[r.off : r.off+n])
	r.off += n
	return v
}
func (r *walBinaryReader) bytes() []byte {
	n := r.length(r.max)
	if r.err != nil {
		return nil
	}
	v := append([]byte(nil), r.buf[r.off:r.off+n]...)
	r.off += n
	return v
}
func (r *walBinaryReader) time() time.Time {
	present := r.byte()
	if present == 0 || r.err != nil {
		return time.Time{}
	}
	if present != 1 {
		r.fail("invalid time marker %d", present)
		return time.Time{}
	}
	return time.Unix(0, r.varint()).UTC()
}
func (r *walBinaryReader) collectionLength() int {
	const maxItems = 1 << 16
	v := r.uvarint()
	if v > maxItems || v > uint64(len(r.buf)-r.off) {
		r.fail("collection size %d exceeds limit", v)
		return 0
	}
	return int(v)
}
func (r *walBinaryReader) stringMap() map[string]string {
	n := r.collectionLength()
	if n == 0 || r.err != nil {
		return nil
	}
	out := make(map[string]string, n)
	for i := 0; i < n && r.err == nil; i++ {
		out[r.string()] = r.string()
	}
	return out
}
func (r *walBinaryReader) stringSlice() []string {
	n := r.collectionLength()
	if n == 0 || r.err != nil {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = r.string()
	}
	return out
}

// Message field bitset. Fixed field order avoids per-field tags and lets batch
// records omit common values such as topic, queue, content type, and status.
const (
	msgFieldID uint64 = 1 << iota
	msgFieldParentID
	msgFieldTopic
	msgFieldQueue
	msgFieldStream
	msgFieldPartition
	msgFieldOffset
	msgFieldKey
	msgFieldRoutingKey
	msgFieldType
	msgFieldContentType
	msgFieldSchema
	msgFieldSchemaVersion
	msgFieldPayload
	msgFieldHeaders
	msgFieldMetadata
	msgFieldTraceID
	msgFieldSpanID
	msgFieldCorrelationID
	msgFieldCausationID
	msgFieldRequestID
	msgFieldReplyTo
	msgFieldReplyGroup
	msgFieldIdempotencyKey
	msgFieldDeduplicationID
	msgFieldChecksum
	msgFieldPriority
	msgFieldCreatedAt
	msgFieldAvailableAt
	msgFieldExpiresAt
	msgFieldAttempts
	msgFieldMaxAttempts
	msgFieldRedeliveries
	msgFieldTenantID
	msgFieldProducerID
	msgFieldConsumerID
	msgFieldConsumerGroup
	msgFieldLeaseToken
	msgFieldLeaseUntil
	msgFieldOriginalTopic
	msgFieldOriginalQueue
	msgFieldOriginalStream
	msgFieldOriginalOffset
	msgFieldStatus
	msgFieldLastError
	msgFieldDeadLetterReason
	msgFieldTaskID
	msgFieldTaskName
	msgFieldProducerEpoch
	msgFieldProducerSequence
)

const msgBatchCommonCandidates = msgFieldTopic | msgFieldQueue | msgFieldStream |
	msgFieldType | msgFieldContentType | msgFieldSchema | msgFieldSchemaVersion |
	msgFieldTenantID | msgFieldProducerID | msgFieldConsumerID | msgFieldConsumerGroup |
	msgFieldOriginalTopic | msgFieldOriginalQueue | msgFieldOriginalStream | msgFieldStatus

const msgKnownFields uint64 = (msgFieldProducerSequence << 1) - 1

func messageFieldMask(m Message) uint64 {
	var mask uint64
	if m.ID != "" {
		mask |= msgFieldID
	}
	if m.ParentID != "" {
		mask |= msgFieldParentID
	}
	if m.Topic != "" {
		mask |= msgFieldTopic
	}
	if m.Queue != "" {
		mask |= msgFieldQueue
	}
	if m.Stream != "" {
		mask |= msgFieldStream
	}
	if m.Partition != 0 {
		mask |= msgFieldPartition
	}
	if m.Offset != 0 {
		mask |= msgFieldOffset
	}
	if m.Key != "" {
		mask |= msgFieldKey
	}
	if m.RoutingKey != "" {
		mask |= msgFieldRoutingKey
	}
	if m.Type != "" {
		mask |= msgFieldType
	}
	if m.ContentType != "" {
		mask |= msgFieldContentType
	}
	if m.Schema != "" {
		mask |= msgFieldSchema
	}
	if m.SchemaVersion != "" {
		mask |= msgFieldSchemaVersion
	}
	if len(m.Payload) > 0 {
		mask |= msgFieldPayload
	}
	if len(m.Headers) > 0 {
		mask |= msgFieldHeaders
	}
	if len(m.Metadata) > 0 {
		mask |= msgFieldMetadata
	}
	if m.TraceID != "" {
		mask |= msgFieldTraceID
	}
	if m.SpanID != "" {
		mask |= msgFieldSpanID
	}
	if m.CorrelationID != "" {
		mask |= msgFieldCorrelationID
	}
	if m.CausationID != "" {
		mask |= msgFieldCausationID
	}
	if m.RequestID != "" {
		mask |= msgFieldRequestID
	}
	if m.ReplyTo != "" {
		mask |= msgFieldReplyTo
	}
	if m.ReplyGroup != "" {
		mask |= msgFieldReplyGroup
	}
	if m.IdempotencyKey != "" {
		mask |= msgFieldIdempotencyKey
	}
	if m.DeduplicationID != "" {
		mask |= msgFieldDeduplicationID
	}
	if m.Checksum != "" {
		mask |= msgFieldChecksum
	}
	if m.Priority != 0 {
		mask |= msgFieldPriority
	}
	if !m.CreatedAt.IsZero() {
		mask |= msgFieldCreatedAt
	}
	if !m.AvailableAt.IsZero() {
		mask |= msgFieldAvailableAt
	}
	if !m.ExpiresAt.IsZero() {
		mask |= msgFieldExpiresAt
	}
	if m.Attempts != 0 {
		mask |= msgFieldAttempts
	}
	if m.MaxAttempts != 0 {
		mask |= msgFieldMaxAttempts
	}
	if m.Redeliveries != 0 {
		mask |= msgFieldRedeliveries
	}
	if m.TenantID != "" {
		mask |= msgFieldTenantID
	}
	if m.ProducerID != "" {
		mask |= msgFieldProducerID
	}
	if m.ProducerSequence != nil {
		mask |= msgFieldProducerEpoch | msgFieldProducerSequence
	}
	if m.ConsumerID != "" {
		mask |= msgFieldConsumerID
	}
	if m.ConsumerGroup != "" {
		mask |= msgFieldConsumerGroup
	}
	if m.LeaseToken != "" {
		mask |= msgFieldLeaseToken
	}
	if !m.LeaseUntil.IsZero() {
		mask |= msgFieldLeaseUntil
	}
	if m.OriginalTopic != "" {
		mask |= msgFieldOriginalTopic
	}
	if m.OriginalQueue != "" {
		mask |= msgFieldOriginalQueue
	}
	if m.OriginalStream != "" {
		mask |= msgFieldOriginalStream
	}
	if m.OriginalOffset != 0 {
		mask |= msgFieldOriginalOffset
	}
	if m.Status != "" {
		mask |= msgFieldStatus
	}
	if m.LastError != "" {
		mask |= msgFieldLastError
	}
	if m.DeadLetterReason != "" {
		mask |= msgFieldDeadLetterReason
	}
	if m.TaskID != "" {
		mask |= msgFieldTaskID
	}
	if m.TaskName != "" {
		mask |= msgFieldTaskName
	}
	return mask
}

func (w *walBinaryWriter) message(m Message, omit uint64) {
	mask := messageFieldMask(m) &^ omit
	w.uvarint(mask)
	w.messageFields(m, mask)
}

func (w *walBinaryWriter) messageFields(m Message, mask uint64) {
	if mask&msgFieldID != 0 {
		w.string(m.ID)
	}
	if mask&msgFieldParentID != 0 {
		w.string(m.ParentID)
	}
	if mask&msgFieldTopic != 0 {
		w.string(m.Topic)
	}
	if mask&msgFieldQueue != 0 {
		w.string(m.Queue)
	}
	if mask&msgFieldStream != 0 {
		w.string(m.Stream)
	}
	if mask&msgFieldPartition != 0 {
		w.int(m.Partition)
	}
	if mask&msgFieldOffset != 0 {
		w.uvarint(m.Offset)
	}
	if mask&msgFieldKey != 0 {
		w.string(m.Key)
	}
	if mask&msgFieldRoutingKey != 0 {
		w.string(m.RoutingKey)
	}
	if mask&msgFieldType != 0 {
		w.string(m.Type)
	}
	if mask&msgFieldContentType != 0 {
		w.string(m.ContentType)
	}
	if mask&msgFieldSchema != 0 {
		w.string(m.Schema)
	}
	if mask&msgFieldSchemaVersion != 0 {
		w.string(m.SchemaVersion)
	}
	if mask&msgFieldPayload != 0 {
		w.bytes(m.Payload)
	}
	if mask&msgFieldHeaders != 0 {
		w.stringMap(m.Headers)
	}
	if mask&msgFieldMetadata != 0 {
		w.stringMap(m.Metadata)
	}
	if mask&msgFieldTraceID != 0 {
		w.string(m.TraceID)
	}
	if mask&msgFieldSpanID != 0 {
		w.string(m.SpanID)
	}
	if mask&msgFieldCorrelationID != 0 {
		w.string(m.CorrelationID)
	}
	if mask&msgFieldCausationID != 0 {
		w.string(m.CausationID)
	}
	if mask&msgFieldRequestID != 0 {
		w.string(m.RequestID)
	}
	if mask&msgFieldReplyTo != 0 {
		w.string(m.ReplyTo)
	}
	if mask&msgFieldReplyGroup != 0 {
		w.string(m.ReplyGroup)
	}
	if mask&msgFieldIdempotencyKey != 0 {
		w.string(m.IdempotencyKey)
	}
	if mask&msgFieldDeduplicationID != 0 {
		w.string(m.DeduplicationID)
	}
	if mask&msgFieldChecksum != 0 {
		w.string(m.Checksum)
	}
	if mask&msgFieldPriority != 0 {
		w.int(m.Priority)
	}
	if mask&msgFieldCreatedAt != 0 {
		w.time(m.CreatedAt)
	}
	if mask&msgFieldAvailableAt != 0 {
		w.time(m.AvailableAt)
	}
	if mask&msgFieldExpiresAt != 0 {
		w.time(m.ExpiresAt)
	}
	if mask&msgFieldAttempts != 0 {
		w.int(m.Attempts)
	}
	if mask&msgFieldMaxAttempts != 0 {
		w.int(m.MaxAttempts)
	}
	if mask&msgFieldRedeliveries != 0 {
		w.int(m.Redeliveries)
	}
	if mask&msgFieldTenantID != 0 {
		w.string(m.TenantID)
	}
	if mask&msgFieldProducerID != 0 {
		w.string(m.ProducerID)
	}
	if mask&msgFieldConsumerID != 0 {
		w.string(m.ConsumerID)
	}
	if mask&msgFieldConsumerGroup != 0 {
		w.string(m.ConsumerGroup)
	}
	if mask&msgFieldLeaseToken != 0 {
		w.string(m.LeaseToken)
	}
	if mask&msgFieldLeaseUntil != 0 {
		w.time(m.LeaseUntil)
	}
	if mask&msgFieldOriginalTopic != 0 {
		w.string(m.OriginalTopic)
	}
	if mask&msgFieldOriginalQueue != 0 {
		w.string(m.OriginalQueue)
	}
	if mask&msgFieldOriginalStream != 0 {
		w.string(m.OriginalStream)
	}
	if mask&msgFieldOriginalOffset != 0 {
		w.uvarint(m.OriginalOffset)
	}
	if mask&msgFieldStatus != 0 {
		w.messageStatus(m.Status)
	}
	if mask&msgFieldLastError != 0 {
		w.string(m.LastError)
	}
	if mask&msgFieldDeadLetterReason != 0 {
		w.string(m.DeadLetterReason)
	}
	if mask&msgFieldTaskID != 0 {
		w.string(m.TaskID)
	}
	if mask&msgFieldTaskName != 0 {
		w.string(m.TaskName)
	}
	if mask&msgFieldProducerEpoch != 0 {
		w.int(int(m.ProducerEpoch))
	}
	if mask&msgFieldProducerSequence != 0 {
		w.uvarint(*m.ProducerSequence)
	}
}

func (r *walBinaryReader) message(base Message) Message {
	mask := r.uvarint()
	if mask&^msgKnownFields != 0 {
		r.fail("unknown message field mask 0x%x", mask&^msgKnownFields)
		return base
	}
	return r.messageFields(base, mask)
}

func (r *walBinaryReader) messageFields(m Message, mask uint64) Message {
	if mask&msgFieldID != 0 {
		m.ID = r.string()
	}
	if mask&msgFieldParentID != 0 {
		m.ParentID = r.string()
	}
	if mask&msgFieldTopic != 0 {
		m.Topic = r.string()
	}
	if mask&msgFieldQueue != 0 {
		m.Queue = r.string()
	}
	if mask&msgFieldStream != 0 {
		m.Stream = r.string()
	}
	if mask&msgFieldPartition != 0 {
		m.Partition = r.int()
	}
	if mask&msgFieldOffset != 0 {
		m.Offset = r.uvarint()
	}
	if mask&msgFieldKey != 0 {
		m.Key = r.string()
	}
	if mask&msgFieldRoutingKey != 0 {
		m.RoutingKey = r.string()
	}
	if mask&msgFieldType != 0 {
		m.Type = r.string()
	}
	if mask&msgFieldContentType != 0 {
		m.ContentType = r.string()
	}
	if mask&msgFieldSchema != 0 {
		m.Schema = r.string()
	}
	if mask&msgFieldSchemaVersion != 0 {
		m.SchemaVersion = r.string()
	}
	if mask&msgFieldPayload != 0 {
		m.Payload = r.bytes()
	}
	if mask&msgFieldHeaders != 0 {
		m.Headers = r.stringMap()
	}
	if mask&msgFieldMetadata != 0 {
		m.Metadata = r.stringMap()
	}
	if mask&msgFieldTraceID != 0 {
		m.TraceID = r.string()
	}
	if mask&msgFieldSpanID != 0 {
		m.SpanID = r.string()
	}
	if mask&msgFieldCorrelationID != 0 {
		m.CorrelationID = r.string()
	}
	if mask&msgFieldCausationID != 0 {
		m.CausationID = r.string()
	}
	if mask&msgFieldRequestID != 0 {
		m.RequestID = r.string()
	}
	if mask&msgFieldReplyTo != 0 {
		m.ReplyTo = r.string()
	}
	if mask&msgFieldReplyGroup != 0 {
		m.ReplyGroup = r.string()
	}
	if mask&msgFieldIdempotencyKey != 0 {
		m.IdempotencyKey = r.string()
	}
	if mask&msgFieldDeduplicationID != 0 {
		m.DeduplicationID = r.string()
	}
	if mask&msgFieldChecksum != 0 {
		m.Checksum = r.string()
	}
	if mask&msgFieldPriority != 0 {
		m.Priority = r.int()
	}
	if mask&msgFieldCreatedAt != 0 {
		m.CreatedAt = r.time()
	}
	if mask&msgFieldAvailableAt != 0 {
		m.AvailableAt = r.time()
	}
	if mask&msgFieldExpiresAt != 0 {
		m.ExpiresAt = r.time()
	}
	if mask&msgFieldAttempts != 0 {
		m.Attempts = r.int()
	}
	if mask&msgFieldMaxAttempts != 0 {
		m.MaxAttempts = r.int()
	}
	if mask&msgFieldRedeliveries != 0 {
		m.Redeliveries = r.int()
	}
	if mask&msgFieldTenantID != 0 {
		m.TenantID = r.string()
	}
	if mask&msgFieldProducerID != 0 {
		m.ProducerID = r.string()
	}
	if mask&msgFieldConsumerID != 0 {
		m.ConsumerID = r.string()
	}
	if mask&msgFieldConsumerGroup != 0 {
		m.ConsumerGroup = r.string()
	}
	if mask&msgFieldLeaseToken != 0 {
		m.LeaseToken = r.string()
	}
	if mask&msgFieldLeaseUntil != 0 {
		m.LeaseUntil = r.time()
	}
	if mask&msgFieldOriginalTopic != 0 {
		m.OriginalTopic = r.string()
	}
	if mask&msgFieldOriginalQueue != 0 {
		m.OriginalQueue = r.string()
	}
	if mask&msgFieldOriginalStream != 0 {
		m.OriginalStream = r.string()
	}
	if mask&msgFieldOriginalOffset != 0 {
		m.OriginalOffset = r.uvarint()
	}
	if mask&msgFieldStatus != 0 {
		m.Status = r.messageStatus()
	}
	if mask&msgFieldLastError != 0 {
		m.LastError = r.string()
	}
	if mask&msgFieldDeadLetterReason != 0 {
		m.DeadLetterReason = r.string()
	}
	if mask&msgFieldTaskID != 0 {
		m.TaskID = r.string()
	}
	if mask&msgFieldTaskName != 0 {
		m.TaskName = r.string()
	}
	if mask&msgFieldProducerEpoch != 0 {
		m.ProducerEpoch = int32(r.int())
	}
	if mask&msgFieldProducerSequence != 0 {
		sequence := r.uvarint()
		m.ProducerSequence = &sequence
	}
	return m
}

func commonMessageMaskMessages(messages []Message) uint64 {
	if len(messages) == 0 {
		return 0
	}
	first := messages[0]
	mask := messageFieldMask(first) & msgBatchCommonCandidates
	for i := 1; i < len(messages) && mask != 0; i++ {
		mask = retainCommonMessageFields(mask, first, messages[i])
	}
	return mask
}

func commonMessageMaskEntries(entries []AppendEntry) uint64 {
	if len(entries) == 0 {
		return 0
	}
	first := entries[0].Message
	mask := messageFieldMask(first) & msgBatchCommonCandidates
	for i := 1; i < len(entries) && mask != 0; i++ {
		mask = retainCommonMessageFields(mask, first, entries[i].Message)
	}
	return mask
}

func retainCommonMessageFields(mask uint64, first, current Message) uint64 {
	if mask&msgFieldTopic != 0 && current.Topic != first.Topic {
		mask &^= msgFieldTopic
	}
	if mask&msgFieldQueue != 0 && current.Queue != first.Queue {
		mask &^= msgFieldQueue
	}
	if mask&msgFieldStream != 0 && current.Stream != first.Stream {
		mask &^= msgFieldStream
	}
	if mask&msgFieldType != 0 && current.Type != first.Type {
		mask &^= msgFieldType
	}
	if mask&msgFieldContentType != 0 && current.ContentType != first.ContentType {
		mask &^= msgFieldContentType
	}
	if mask&msgFieldSchema != 0 && current.Schema != first.Schema {
		mask &^= msgFieldSchema
	}
	if mask&msgFieldSchemaVersion != 0 && current.SchemaVersion != first.SchemaVersion {
		mask &^= msgFieldSchemaVersion
	}
	if mask&msgFieldTenantID != 0 && current.TenantID != first.TenantID {
		mask &^= msgFieldTenantID
	}
	if mask&msgFieldProducerID != 0 && current.ProducerID != first.ProducerID {
		mask &^= msgFieldProducerID
	}
	if mask&msgFieldConsumerID != 0 && current.ConsumerID != first.ConsumerID {
		mask &^= msgFieldConsumerID
	}
	if mask&msgFieldConsumerGroup != 0 && current.ConsumerGroup != first.ConsumerGroup {
		mask &^= msgFieldConsumerGroup
	}
	if mask&msgFieldOriginalTopic != 0 && current.OriginalTopic != first.OriginalTopic {
		mask &^= msgFieldOriginalTopic
	}
	if mask&msgFieldOriginalQueue != 0 && current.OriginalQueue != first.OriginalQueue {
		mask &^= msgFieldOriginalQueue
	}
	if mask&msgFieldOriginalStream != 0 && current.OriginalStream != first.OriginalStream {
		mask &^= msgFieldOriginalStream
	}
	if mask&msgFieldStatus != 0 && current.Status != first.Status {
		mask &^= msgFieldStatus
	}
	return mask
}

func (w *walBinaryWriter) messages(messages []Message) {
	w.uvarint(uint64(len(messages)))
	if len(messages) == 0 {
		return
	}
	common := commonMessageMaskMessages(messages)
	w.uvarint(common)
	w.messageFields(messages[0], common)
	for i := range messages {
		w.message(messages[i], common)
	}
}

func (r *walBinaryReader) messages() []Message {
	n := r.collectionLength()
	if n == 0 || r.err != nil {
		return nil
	}
	commonMask := r.uvarint()
	if commonMask&^msgBatchCommonCandidates != 0 {
		r.fail("invalid common message field mask 0x%x", commonMask)
		return nil
	}
	common := r.messageFields(Message{}, commonMask)
	out := make([]Message, n)
	for i := range out {
		out[i] = r.message(common)
	}
	return out
}

func equalDestinations(a, b []Destination) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (w *walBinaryWriter) appendEntries(entries []AppendEntry) {
	w.uvarint(uint64(len(entries)))
	if len(entries) == 0 {
		return
	}
	commonDests := true
	for i := 1; i < len(entries); i++ {
		if !equalDestinations(entries[0].Destinations, entries[i].Destinations) {
			commonDests = false
			break
		}
	}
	w.bool(commonDests)
	if commonDests {
		w.destinations(entries[0].Destinations)
	}
	commonMessage := commonMessageMaskEntries(entries)
	w.uvarint(commonMessage)
	w.messageFields(entries[0].Message, commonMessage)
	for i := range entries {
		w.message(entries[i].Message, commonMessage)
		if !commonDests {
			w.destinations(entries[i].Destinations)
		}
	}
}

func (r *walBinaryReader) appendEntries() []AppendEntry {
	n := r.collectionLength()
	if n == 0 || r.err != nil {
		return nil
	}
	commonDestsEnabled := r.bool()
	var commonDests []Destination
	if commonDestsEnabled {
		commonDests = r.destinations()
	}
	commonMask := r.uvarint()
	if commonMask&^msgBatchCommonCandidates != 0 {
		r.fail("invalid common append field mask 0x%x", commonMask)
		return nil
	}
	commonMessage := r.messageFields(Message{}, commonMask)
	out := make([]AppendEntry, n)
	for i := range out {
		out[i].Message = r.message(commonMessage)
		if commonDestsEnabled {
			out[i].Destinations = append([]Destination(nil), commonDests...)
		} else {
			out[i].Destinations = r.destinations()
		}
	}
	return out
}

func (w *walBinaryWriter) destination(v Destination) {
	w.destinationKind(v.Kind)
	w.string(v.Name)
}
func (r *walBinaryReader) destination() Destination {
	return Destination{Kind: r.destinationKind(), Name: r.string()}
}
func (w *walBinaryWriter) destinations(v []Destination) {
	w.uvarint(uint64(len(v)))
	for _, item := range v {
		w.destination(item)
	}
}
func (r *walBinaryReader) destinations() []Destination {
	n := r.collectionLength()
	if n == 0 || r.err != nil {
		return nil
	}
	out := make([]Destination, n)
	for i := range out {
		out[i] = r.destination()
	}
	return out
}

func (w *walBinaryWriter) retryPolicy(v RetryPolicy) {
	w.int(v.MaxAttempts)
	w.duration(v.Initial)
	w.duration(v.Max)
	w.float64(v.Factor)
	w.bool(v.Jitter)
}
func (r *walBinaryReader) retryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: r.int(), Initial: r.duration(), Max: r.duration(), Factor: r.float64(), Jitter: r.bool()}
}
func (w *walBinaryWriter) queueConfig(v QueueConfig) {
	w.string(v.Name)
	w.int(v.Workers)
	w.int(v.MaxInFlight)
	w.int(v.MaxDepth)
	w.duration(v.VisibilityTimeout)
	w.string(string(v.AckMode))
	w.retryPolicy(v.RetryPolicy)
	w.string(v.DLQ)
	w.string(string(v.Ordering))
	w.string(string(v.Backpressure))
	w.string(string(v.Strategy))
}
func (r *walBinaryReader) queueConfig() QueueConfig {
	return QueueConfig{
		Name: r.string(), Workers: r.int(), MaxInFlight: r.int(), MaxDepth: r.int(),
		VisibilityTimeout: r.duration(), AckMode: AckMode(r.string()), RetryPolicy: r.retryPolicy(),
		DLQ: r.string(), Ordering: OrderMode(r.string()), Backpressure: BackpressurePolicy(r.string()), Strategy: StrategyType(r.string()),
	}
}
func (w *walBinaryWriter) topicConfig(v TopicConfig) {
	w.string(v.Name)
	w.int(v.Partitions)
	w.duration(v.Retention)
	w.bool(v.Paused)
}
func (r *walBinaryReader) topicConfig() TopicConfig {
	return TopicConfig{Name: r.string(), Partitions: r.int(), Retention: r.duration(), Paused: r.bool()}
}
func (w *walBinaryWriter) streamConfig(v StreamConfig) {
	w.string(v.Name)
	w.int(v.Partitions)
	w.duration(v.Retention)
	w.bool(v.Compact)
}
func (r *walBinaryReader) streamConfig() StreamConfig {
	return StreamConfig{Name: r.string(), Partitions: r.int(), Retention: r.duration(), Compact: r.bool()}
}
func (w *walBinaryWriter) leaseRequest(v LeaseRequest) {
	w.string(v.Queue)
	w.string(v.ConsumerID)
	w.string(v.ConsumerGroup)
	w.int(v.Limit)
	w.duration(v.VisibilityTimeout)
	w.string(v.LeaseToken)
	w.string(v.MessageID)
}
func (r *walBinaryReader) leaseRequest() LeaseRequest {
	return LeaseRequest{Queue: r.string(), ConsumerID: r.string(), ConsumerGroup: r.string(), Limit: r.int(), VisibilityTimeout: r.duration(), LeaseToken: r.string(), MessageID: r.string()}
}

const (
	ackFieldMessageID byte = 1 << iota
	ackFieldQueue
	ackFieldLeaseToken
	ackFieldConsumerID
)

func ackMask(v AckRequest) byte {
	var mask byte
	if v.MessageID != "" {
		mask |= ackFieldMessageID
	}
	if v.Queue != "" {
		mask |= ackFieldQueue
	}
	if v.LeaseToken != "" {
		mask |= ackFieldLeaseToken
	}
	if v.ConsumerID != "" {
		mask |= ackFieldConsumerID
	}
	return mask
}
func (w *walBinaryWriter) ackRequest(v AckRequest, omit byte) {
	mask := ackMask(v) &^ omit
	w.byte(mask)
	if mask&ackFieldMessageID != 0 {
		w.string(v.MessageID)
	}
	if mask&ackFieldQueue != 0 {
		w.string(v.Queue)
	}
	if mask&ackFieldLeaseToken != 0 {
		w.string(v.LeaseToken)
	}
	if mask&ackFieldConsumerID != 0 {
		w.string(v.ConsumerID)
	}
}
func (r *walBinaryReader) ackRequest(v AckRequest) AckRequest {
	mask := r.byte()
	if mask&ackFieldMessageID != 0 {
		v.MessageID = r.string()
	}
	if mask&ackFieldQueue != 0 {
		v.Queue = r.string()
	}
	if mask&ackFieldLeaseToken != 0 {
		v.LeaseToken = r.string()
	}
	if mask&ackFieldConsumerID != 0 {
		v.ConsumerID = r.string()
	}
	if mask&^(ackFieldMessageID|ackFieldQueue|ackFieldLeaseToken|ackFieldConsumerID) != 0 {
		r.fail("unknown ack fields")
	}
	return v
}
func (w *walBinaryWriter) ackRequests(v []AckRequest) {
	w.uvarint(uint64(len(v)))
	if len(v) == 0 {
		return
	}
	common := byte(0)
	if v[0].Queue != "" {
		common |= ackFieldQueue
	}
	if v[0].ConsumerID != "" {
		common |= ackFieldConsumerID
	}
	for i := 1; i < len(v); i++ {
		if common&ackFieldQueue != 0 && v[i].Queue != v[0].Queue {
			common &^= ackFieldQueue
		}
		if common&ackFieldConsumerID != 0 && v[i].ConsumerID != v[0].ConsumerID {
			common &^= ackFieldConsumerID
		}
	}
	w.byte(common)
	if common&ackFieldQueue != 0 {
		w.string(v[0].Queue)
	}
	if common&ackFieldConsumerID != 0 {
		w.string(v[0].ConsumerID)
	}
	for i := range v {
		w.ackRequest(v[i], common)
	}
}
func (r *walBinaryReader) ackRequests() []AckRequest {
	n := r.collectionLength()
	if n == 0 || r.err != nil {
		return nil
	}
	commonMask := r.byte()
	base := AckRequest{}
	if commonMask&ackFieldQueue != 0 {
		base.Queue = r.string()
	}
	if commonMask&ackFieldConsumerID != 0 {
		base.ConsumerID = r.string()
	}
	if commonMask&^(ackFieldQueue|ackFieldConsumerID) != 0 {
		r.fail("unknown common ack fields")
	}
	out := make([]AckRequest, n)
	for i := range out {
		out[i] = r.ackRequest(base)
	}
	return out
}

func (w *walBinaryWriter) nackRequest(v NackRequest) {
	w.string(v.MessageID)
	w.string(v.Queue)
	w.string(v.LeaseToken)
	w.string(v.ConsumerID)
	w.nackAction(v.Action)
	w.string(v.Reason)
	w.string(v.Error)
	w.duration(v.Delay)
	w.string(v.DLQ)
	var flags byte
	if v.ResetLease {
		flags |= 1
	}
	if v.IncrementAttempt {
		flags |= 2
	}
	w.byte(flags)
}
func (r *walBinaryReader) nackRequest() NackRequest {
	out := NackRequest{MessageID: r.string(), Queue: r.string(), LeaseToken: r.string(), ConsumerID: r.string(), Action: r.nackAction(), Reason: r.string(), Error: r.string(), Delay: r.duration(), DLQ: r.string()}
	flags := r.byte()
	if flags&^byte(3) != 0 {
		r.fail("unknown nack flags")
	}
	out.ResetLease = flags&1 != 0
	out.IncrementAttempt = flags&2 != 0
	return out
}
func (w *walBinaryWriter) messageFilter(v MessageFilter) {
	w.messageStatus(v.Status)
	w.string(v.Type)
	w.string(v.TenantID)
	w.stringMap(v.Headers)
}
func (r *walBinaryReader) messageFilter() MessageFilter {
	return MessageFilter{Status: r.messageStatus(), Type: r.string(), TenantID: r.string(), Headers: r.stringMap()}
}
func (w *walBinaryWriter) moveRequest(v MoveRequest) {
	w.destinationKind(v.SourceType)
	w.string(v.Source)
	w.destinationKind(v.DestinationType)
	w.string(v.Destination)
	w.stringSlice(v.MessageIDs)
	w.messageFilter(v.Filter)
	w.moveMode(v.Mode)
	var flags byte
	if v.PreserveID {
		flags |= 1 << 0
	}
	if v.PreserveOffset {
		flags |= 1 << 1
	}
	if v.ResetAttempts {
		flags |= 1 << 2
	}
	if v.ResetLease {
		flags |= 1 << 3
	}
	if v.ResetError {
		flags |= 1 << 4
	}
	if v.DryRun {
		flags |= 1 << 5
	}
	w.byte(flags)
	w.string(v.Reason)
	w.string(v.Actor)
}
func (r *walBinaryReader) moveRequest() MoveRequest {
	out := MoveRequest{SourceType: r.destinationKind(), Source: r.string(), DestinationType: r.destinationKind(), Destination: r.string(), MessageIDs: r.stringSlice(), Filter: r.messageFilter(), Mode: r.moveMode()}
	flags := r.byte()
	if flags&^byte(0x3f) != 0 {
		r.fail("unknown move flags")
	}
	out.PreserveID = flags&(1<<0) != 0
	out.PreserveOffset = flags&(1<<1) != 0
	out.ResetAttempts = flags&(1<<2) != 0
	out.ResetLease = flags&(1<<3) != 0
	out.ResetError = flags&(1<<4) != 0
	out.DryRun = flags&(1<<5) != 0
	out.Reason = r.string()
	out.Actor = r.string()
	return out
}
func (w *walBinaryWriter) offsetCommit(v OffsetCommit) {
	w.string(v.Stream)
	w.string(v.ConsumerGroup)
	w.uvarint(v.Offset)
}
func (r *walBinaryReader) offsetCommit() OffsetCommit {
	return OffsetCommit{Stream: r.string(), ConsumerGroup: r.string(), Offset: r.uvarint()}
}

func (w *walBinaryWriter) destinationKind(v DestinationKind) {
	switch v {
	case "":
		w.byte(0)
	case DestinationTopic:
		w.byte(1)
	case DestinationQueue:
		w.byte(2)
	case DestinationStream:
		w.byte(3)
	case DestinationDLQ:
		w.byte(4)
	case DestinationArchive:
		w.byte(5)
	default:
		w.byte(255)
		w.string(string(v))
	}
}
func (r *walBinaryReader) destinationKind() DestinationKind {
	switch r.byte() {
	case 0:
		return ""
	case 1:
		return DestinationTopic
	case 2:
		return DestinationQueue
	case 3:
		return DestinationStream
	case 4:
		return DestinationDLQ
	case 5:
		return DestinationArchive
	case 255:
		return DestinationKind(r.string())
	default:
		r.fail("unknown destination kind")
		return ""
	}
}
func (w *walBinaryWriter) nackAction(v NackAction) {
	switch v {
	case "":
		w.byte(0)
	case NackRequeue:
		w.byte(1)
	case NackRetry:
		w.byte(2)
	case NackDLQ:
		w.byte(3)
	case NackReject:
		w.byte(4)
	default:
		w.byte(255)
		w.string(string(v))
	}
}
func (r *walBinaryReader) nackAction() NackAction {
	switch r.byte() {
	case 0:
		return ""
	case 1:
		return NackRequeue
	case 2:
		return NackRetry
	case 3:
		return NackDLQ
	case 4:
		return NackReject
	case 255:
		return NackAction(r.string())
	default:
		r.fail("unknown nack action")
		return ""
	}
}
func (w *walBinaryWriter) moveMode(v MoveMode) {
	switch v {
	case "":
		w.byte(0)
	case MoveRemoveSource:
		w.byte(1)
	case MoveCopy:
		w.byte(2)
	case MoveReplay:
		w.byte(3)
	case MoveReroute:
		w.byte(4)
	case MoveQuarantine:
		w.byte(5)
	case MovePark:
		w.byte(6)
	case MoveArchive:
		w.byte(7)
	default:
		w.byte(255)
		w.string(string(v))
	}
}
func (r *walBinaryReader) moveMode() MoveMode {
	switch r.byte() {
	case 0:
		return ""
	case 1:
		return MoveRemoveSource
	case 2:
		return MoveCopy
	case 3:
		return MoveReplay
	case 4:
		return MoveReroute
	case 5:
		return MoveQuarantine
	case 6:
		return MovePark
	case 7:
		return MoveArchive
	case 255:
		return MoveMode(r.string())
	default:
		r.fail("unknown move mode")
		return ""
	}
}

func (w *walBinaryWriter) messageStatus(v MessageStatus) {
	code := messageStatusCode(v)
	w.byte(code)
	if code == 255 {
		w.string(string(v))
	}
}
func (r *walBinaryReader) messageStatus() MessageStatus {
	code := r.byte()
	if code == 255 {
		return MessageStatus(r.string())
	}
	status, ok := messageStatusFromCode(code)
	if !ok {
		r.fail("unknown message status code %d", code)
	}
	return status
}
func messageStatusCode(v MessageStatus) byte {
	switch v {
	case "":
		return 0
	case MessageCreated:
		return 1
	case MessageValidated:
		return 2
	case MessageAccepted:
		return 3
	case MessageRouted:
		return 4
	case MessagePersisted:
		return 5
	case MessageScheduled:
		return 6
	case MessageAvailable:
		return 7
	case MessageQueued:
		return 8
	case MessageLeased:
		return 9
	case MessageDelivered:
		return 10
	case MessageProcessing:
		return 11
	case MessageProgressing:
		return 12
	case MessageAcked:
		return 13
	case MessageCompleted:
		return 14
	case MessageFailed:
		return 15
	case MessageRetryScheduled:
		return 16
	case MessageDeadLettered:
		return 17
	case MessageCancelled:
		return 18
	case MessageExpired:
		return 19
	case MessageMoved:
		return 20
	case MessageCopied:
		return 21
	case MessageReplayed:
		return 22
	case MessageArchived:
		return 23
	case MessagePurged:
		return 24
	case MessageQuarantined:
		return 25
	case MessageParked:
		return 26
	default:
		return 255
	}
}
func messageStatusFromCode(code byte) (MessageStatus, bool) {
	switch code {
	case 0:
		return "", true
	case 1:
		return MessageCreated, true
	case 2:
		return MessageValidated, true
	case 3:
		return MessageAccepted, true
	case 4:
		return MessageRouted, true
	case 5:
		return MessagePersisted, true
	case 6:
		return MessageScheduled, true
	case 7:
		return MessageAvailable, true
	case 8:
		return MessageQueued, true
	case 9:
		return MessageLeased, true
	case 10:
		return MessageDelivered, true
	case 11:
		return MessageProcessing, true
	case 12:
		return MessageProgressing, true
	case 13:
		return MessageAcked, true
	case 14:
		return MessageCompleted, true
	case 15:
		return MessageFailed, true
	case 16:
		return MessageRetryScheduled, true
	case 17:
		return MessageDeadLettered, true
	case 18:
		return MessageCancelled, true
	case 19:
		return MessageExpired, true
	case 20:
		return MessageMoved, true
	case 21:
		return MessageCopied, true
	case 22:
		return MessageReplayed, true
	case 23:
		return MessageArchived, true
	case 24:
		return MessagePurged, true
	case 25:
		return MessageQuarantined, true
	case 26:
		return MessageParked, true
	default:
		return "", false
	}
}
