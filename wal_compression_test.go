package broker

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestWALCompressionRoundTrip(t *testing.T) {
	testPayloads := [][]byte{
		[]byte("hello world, fastworker broker compression test!"),
		bytes.Repeat([]byte("repetitive-block-of-log-structured-data-"), 100),
		make([]byte, 8192),
	}
	_, _ = rand.Read(testPayloads[2])

	codecs := []CompressionCodec{CompressionNone, CompressionLZ4, CompressionZstd}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			for i, orig := range testPayloads {
				compressed, err := CompressPayload(codec, orig)
				if err != nil {
					t.Fatalf("compress failed for payload %d: %v", i, err)
				}

				if codec != CompressionNone && len(orig) > 100 && i == 1 {
					if len(compressed) >= len(orig) {
						t.Fatalf("expected compression to reduce size: orig=%d compressed=%d", len(orig), len(compressed))
					}
				}

				decompressed, err := DecompressPayload(codec, compressed)
				if err != nil {
					t.Fatalf("decompress failed for payload %d: %v", i, err)
				}

				if !bytes.Equal(orig, decompressed) {
					t.Fatalf("roundtrip mismatch for payload %d under %s", i, codec.String())
				}
			}
		})
	}
}

func TestParseCompressionCodec(t *testing.T) {
	cases := []struct {
		input    string
		expected CompressionCodec
		hasErr   bool
	}{
		{"", CompressionNone, false},
		{"none", CompressionNone, false},
		{"NONE", CompressionNone, false},
		{"lz4", CompressionLZ4, false},
		{"LZ4", CompressionLZ4, false},
		{"zstd", CompressionZstd, false},
		{"ZSTD", CompressionZstd, false},
		{"gzip", CompressionNone, true},
		{"snappy", CompressionNone, true},
	}

	for _, c := range cases {
		got, err := ParseCompressionCodec(c.input)
		if c.hasErr && err == nil {
			t.Errorf("expected error for input %q", c.input)
		}
		if !c.hasErr && (err != nil || got != c.expected) {
			t.Errorf("input %q: got %v, want %v (err: %v)", c.input, got, c.expected, err)
		}
	}
}
