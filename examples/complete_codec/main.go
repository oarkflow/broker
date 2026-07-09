package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/fastworker/broker"
	"github.com/oarkflow/fastworker/examples/internal/exconfig"
)

func main() {
	configPath := flag.String("config", "examples/complete_codec/codec.bcl", "BCL codec example configuration")
	flag.Parse()
	root, err := exconfig.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	cc := exconfig.FirstBlock(root, "codec_example")
	if cc == nil {
		log.Fatal("missing codec_example block")
	}
	codec := broker.NewBinaryMessageCodec(
		broker.WithFrameMaxSize(int(exconfig.ParseBytes(cc.String("max_frame_size", "16MB"), 16<<20))),
		broker.WithFrameCompressionThreshold(int(exconfig.ParseBytes(cc.String("compress_threshold", "0"), 0))),
		broker.WithFrameSigningKey([]byte(cc.String("signing_key", ""))),
		broker.WithFrameEncryptionKey([]byte(cc.String("encryption_key", ""))),
	)
	msg := broker.Message{
		ID:          "codec-example-1",
		Topic:       cc.String("topic", "codec.demo"),
		Queue:       cc.String("queue", "codec.frames"),
		Type:        "codec.example",
		ContentType: "application/json",
		Payload:     []byte(exconfig.MustJSONPayload(cc.String("payload", `{}`))),
		Headers:     map[string]string{"x-codec": "fwb1", "x-example": "complete_codec"},
		Metadata:    map[string]string{"source": "examples/complete_codec"},
		CreatedAt:   time.Now(),
		Status:      broker.MessageQueued,
	}
	frame, err := codec.EncodeMessage(nil, msg)
	if err != nil {
		log.Fatal(err)
	}
	var header broker.Message
	if err := codec.PeekHeader(frame, &header); err != nil {
		log.Fatal(err)
	}
	var decoded broker.Message
	if err := codec.DecodeMessage(frame, &decoded); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("FWB1 frame bytes=%d fingerprint=%s\n", len(frame), broker.FrameFingerprint(frame))
	fmt.Printf("peek id=%s topic=%s queue=%s type=%s payload_len=%d\n", header.ID, header.Topic, header.Queue, header.Type, len(header.Payload))
	fmt.Printf("decode id=%s topic=%s queue=%s headers=%v metadata=%v payload=%s\n", decoded.ID, decoded.Topic, decoded.Queue, decoded.Headers, decoded.Metadata, string(decoded.Payload))
	fmt.Printf("first_32_bytes=%s\n", hex.EncodeToString(frame[:min(32, len(frame))]))
	// Demonstrate tamper detection.
	tampered := append([]byte(nil), frame...)
	if len(tampered) > 16 {
		tampered[len(tampered)-1] ^= 0x01
	}
	if err := codec.DecodeMessage(tampered, &decoded); err != nil {
		fmt.Printf("tamper_detected=%v\n", err)
	} else {
		log.Fatal("tamper was not detected")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
