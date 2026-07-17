package broker

// FrameCodec is the unified interface for encoding/decoding messages to binary frames.
// Both BinaryMessageCodec (FWB1) and CompactMessageCodec (FWB2) implement this interface.
type FrameCodec interface {
	EncodeMessage(dst []byte, msg Message) ([]byte, error)
	DecodeMessage(frame []byte, msg *Message) error
}

// Ensure both codecs implement the interface.
var _ FrameCodec = (*BinaryMessageCodec)(nil)
var _ FrameCodec = (*CompactMessageCodec)(nil)

// CodecType identifies the binary frame format.
type CodecType string

const (
	CodecFWB1 CodecType = "fwb1" // Original format with string keys
	CodecFWB2 CodecType = "fwb2" // Compact format with numeric tags
)

// FrameCodecConfig holds configuration for creating a FrameCodec.
type FrameCodecConfig struct {
	Type              CodecType
	CompressThreshold int
	MaxFrameSize      int
	SigningKey        []byte
	EncryptionKey     []byte
}

// NewFrameCodec creates a FrameCodec based on the config.
func NewFrameCodec(cfg FrameCodecConfig) FrameCodec {
	switch cfg.Type {
	case CodecFWB2:
		return NewCompactMessageCodec(
			WithCompactCompressThreshold(cfg.CompressThreshold),
			WithCompactMaxFrameSize(cfg.MaxFrameSize),
			WithCompactSigningKey(cfg.SigningKey),
			WithCompactEncryptionKey(cfg.EncryptionKey),
		)
	default: // CodecFWB1 or empty
		return NewBinaryMessageCodec(
			WithFrameCompressionThreshold(cfg.CompressThreshold),
			WithFrameMaxSize(cfg.MaxFrameSize),
			WithFrameSigningKey(cfg.SigningKey),
			WithFrameEncryptionKey(cfg.EncryptionKey),
		)
	}
}

// WALCodecConfig exposes equivalent durability protection settings to the
// dedicated BWL1 write-ahead-log codec.
func (c *BinaryMessageCodec) WALCodecConfig() WALCodecConfig {
	if c == nil {
		return WALCodecConfig{MaxRecordSize: binaryWALDefaultMax}
	}
	return WALCodecConfig{
		CompressThreshold: c.CompressThreshold,
		MaxRecordSize:     c.MaxFrameSize,
		EncryptionKey:     append([]byte(nil), c.EncryptionKey...),
		SigningKey:        append([]byte(nil), c.SigningKey...),
	}
}

// WALCodecConfig exposes equivalent durability protection settings to the
// dedicated BWL1 write-ahead-log codec.
func (c *CompactMessageCodec) WALCodecConfig() WALCodecConfig {
	if c == nil {
		return WALCodecConfig{MaxRecordSize: binaryWALDefaultMax}
	}
	return WALCodecConfig{
		CompressThreshold: c.CompressThreshold,
		MaxRecordSize:     c.MaxFrameSize,
		EncryptionKey:     append([]byte(nil), c.EncryptionKey...),
		SigningKey:        append([]byte(nil), c.SigningKey...),
	}
}
