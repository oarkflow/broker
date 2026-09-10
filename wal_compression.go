package broker

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// CompressionCodec represents the algorithm used to compress record batches or stream segments.
type CompressionCodec byte

const (
	CompressionNone CompressionCodec = 0
	CompressionLZ4  CompressionCodec = 1
	CompressionZstd CompressionCodec = 2
)

func (c CompressionCodec) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionLZ4:
		return "lz4"
	case CompressionZstd:
		return "zstd"
	default:
		return "unknown"
	}
}

// ParseCompressionCodec parses a string representation into a CompressionCodec.
func ParseCompressionCodec(s string) (CompressionCodec, error) {
	switch s {
	case "", "none", "NONE":
		return CompressionNone, nil
	case "lz4", "LZ4":
		return CompressionLZ4, nil
	case "zstd", "ZSTD":
		return CompressionZstd, nil
	default:
		return CompressionNone, fmt.Errorf("unknown compression codec: %s", s)
	}
}

var (
	zstdEncoderPool = sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
			if err != nil {
				panic(err)
			}
			return enc
		},
	}

	zstdDecoderPool = sync.Pool{
		New: func() any {
			dec, err := zstd.NewReader(nil)
			if err != nil {
				panic(err)
			}
			return dec
		},
	}
)

// CompressPayload compresses the provided byte slice using the requested codec.
// If CompressionNone is requested, data is returned unchanged.
func CompressPayload(codec CompressionCodec, data []byte) ([]byte, error) {
	switch codec {
	case CompressionNone:
		return data, nil

	case CompressionLZ4:
		var buf bytes.Buffer
		w := lz4.NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			return nil, fmt.Errorf("lz4 write: %w", err)
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("lz4 close: %w", err)
		}
		return buf.Bytes(), nil

	case CompressionZstd:
		enc := zstdEncoderPool.Get().(*zstd.Encoder)
		defer zstdEncoderPool.Put(enc)
		return enc.EncodeAll(data, make([]byte, 0, len(data))), nil

	default:
		return nil, fmt.Errorf("unsupported compression codec: %d", codec)
	}
}

// DecompressPayload decompresses the provided byte slice according to the specified codec.
func DecompressPayload(codec CompressionCodec, data []byte) ([]byte, error) {
	switch codec {
	case CompressionNone:
		return data, nil

	case CompressionLZ4:
		r := lz4.NewReader(bytes.NewReader(data))
		out, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("lz4 decompress: %w", err)
		}
		return out, nil

	case CompressionZstd:
		dec := zstdDecoderPool.Get().(*zstd.Decoder)
		defer zstdDecoderPool.Put(dec)
		return dec.DecodeAll(data, nil)

	default:
		return nil, fmt.Errorf("unsupported compression codec: %d", codec)
	}
}
