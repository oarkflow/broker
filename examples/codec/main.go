package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/examples/internal/exconfig"
	"github.com/oarkflow/broker/pkg/logger"
	"github.com/oarkflow/zlog"
)

func main() {
	configPath := flag.String("config", "codec/codec.bcl", "BCL codec example configuration")
	flag.Parse()
	root, err := exconfig.Load(*configPath)
	if err != nil {
		logger.Log.Fatal("load config", zlog.Err(err))
	}
	cc := exconfig.FirstBlock(root, "codec_example")
	if cc == nil {
		logger.Log.Fatal("missing codec_example block")
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
		Payload:     []byte(exconfig.PayloadJSON(cc, `{}`)),
		Headers:     map[string]string{"x-codec": "fwb1", "x-example": "codec"},
		Metadata:    map[string]string{"source": "examples/codec"},
		CreatedAt:   time.Now(),
		Status:      broker.MessageQueued,
	}
	frame, err := codec.EncodeMessage(nil, msg)
	if err != nil {
		logger.Log.Fatal("encode failed", zlog.Err(err))
	}
	var header broker.Message
	if err := codec.PeekHeader(frame, &header); err != nil {
		logger.Log.Fatal("peek failed", zlog.Err(err))
	}
	var decoded broker.Message
	if err := codec.DecodeMessage(frame, &decoded); err != nil {
		logger.Log.Fatal("decode failed", zlog.Err(err))
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
		logger.Log.Info("tamper detected", zlog.Err(err))
	} else {
		logger.Log.Fatal("tamper was not detected")
	}
}
