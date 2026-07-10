package amqp

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

func TestCodecReadWriteFrame(t *testing.T) {
	var buf bytes.Buffer
	codec := NewCodec(&buf)

	// Write a method frame
	payload := []byte{0x00, 0x0A, 0x00, 0x0B, 0x01, 0x02} // class=10, method=11
	err := codec.WriteFrame(&Frame{
		Type:      FrameMethod,
		ChannelID: 1,
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	// Read the frame back
	frame, err := codec.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if frame.Type != FrameMethod {
		t.Errorf("expected type %d, got %d", FrameMethod, frame.Type)
	}
	if frame.ChannelID != 1 {
		t.Errorf("expected channel 1, got %d", frame.ChannelID)
	}
	if !bytes.Equal(frame.Payload, payload) {
		t.Errorf("payload mismatch")
	}
}

func TestEncodeMethodArgs(t *testing.T) {
	args := EncodeMethodArgs(
		uint16(100),
		string("hello"),
		bool(true),
		uint8(42),
	)

	if len(args) < 4 {
		t.Fatalf("expected at least 4 bytes, got %d", len(args))
	}

	// Check uint16
	v16 := binary.BigEndian.Uint16(args[0:2])
	if v16 != 100 {
		t.Errorf("expected uint16 100, got %d", v16)
	}

	// Check string length
	strLen := binary.BigEndian.Uint16(args[2:4])
	if strLen != 5 {
		t.Errorf("expected string length 5, got %d", strLen)
	}

	// Check string content
	str := string(args[4 : 4+strLen])
	if str != "hello" {
		t.Errorf("expected string 'hello', got '%s'", str)
	}
}

func TestDecodeTable(t *testing.T) {
	// Create a simple table
	original := table{
		"key1": "value1",
		"key2": int(42),
	}

	// Encode it
	encoded := encodeTableToBytes(original)

	// Decode it
	decoded, err := DecodeTable(encoded)
	if err != nil {
		t.Fatalf("DecodeTable failed: %v", err)
	}

	if decoded["key1"] != "value1" {
		t.Errorf("expected key1=value1, got %v", decoded["key1"])
	}
}

func TestServerStartStop(t *testing.T) {
	b, err := broker.New(broker.Config{
		Name:              "test-broker",
		Mode:              broker.ModeEmbedded,
		DefaultQueue:      "test",
		DefaultVisibility: time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	ctx := t.Context()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer b.Stop(ctx)

	server := NewServer(ServerConfig{
		ListenAddr: ":0",
		Broker:     b,
	})

	if err := server.Listen(); err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	// Verify we can connect
	addr := server.Addr().String()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	conn.Close()

	// Stop server
	stopCtx := t.Context()
	if err := server.Stop(stopCtx); err != nil {
		t.Errorf("Failed to stop server: %v", err)
	}
}

func TestConnectionHandshake(t *testing.T) {
	b, err := broker.New(broker.Config{
		Name:              "test-broker",
		Mode:              broker.ModeEmbedded,
		DefaultQueue:      "test",
		DefaultVisibility: time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	ctx := t.Context()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer b.Stop(ctx)

	server := NewServer(ServerConfig{
		ListenAddr: ":0",
		Broker:     b,
	})

	if err := server.Listen(); err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	go server.Serve()
	defer server.Stop(t.Context())

	// Connect as AMQP client
	addr := server.Addr().String()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Send AMQP header
	header := "AMQP\x00\x00\x09\x01"
	_, err = conn.Write([]byte(header))
	if err != nil {
		t.Fatalf("Failed to write AMQP header: %v", err)
	}

	// Read response (Connection.Start)
	conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read: %v", err)
	}

	if n < 7 {
		t.Fatalf("Expected at least 7 bytes, got %d", n)
	}

	// Verify frame type is method (1)
	if buf[0] != FrameMethod {
		t.Errorf("Expected frame type %d, got %d", FrameMethod, buf[0])
	}
}

func TestParseMethodFrame(t *testing.T) {
	payload := []byte{0x00, 0x0A, 0x00, 0x0B} // class=10, method=11
	method, err := ParseMethodFrame(payload)
	if err != nil {
		t.Fatalf("ParseMethodFrame failed: %v", err)
	}

	if method.ClassID != 10 {
		t.Errorf("expected class 10, got %d", method.ClassID)
	}
	if method.MethodID != 11 {
		t.Errorf("expected method 11, got %d", method.MethodID)
	}
}

func TestParseHeaderFrame(t *testing.T) {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint16(payload[0:2], 60)  // class ID
	binary.BigEndian.PutUint64(payload[2:10], 100) // body size
	binary.BigEndian.PutUint16(payload[10:12], 0)  // property flags

	hdr, err := ParseHeaderFrame(payload)
	if err != nil {
		t.Fatalf("ParseHeaderFrame failed: %v", err)
	}

	if hdr.ClassID != 60 {
		t.Errorf("expected class 60, got %d", hdr.ClassID)
	}
	if hdr.BodySize != 100 {
		t.Errorf("expected body size 100, got %d", hdr.BodySize)
	}
}

func BenchmarkEncodeMethodArgs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeMethodArgs(
			uint16(100),
			string("hello"),
			bool(true),
			uint8(42),
			uint32(12345),
		)
	}
}

func BenchmarkEncodeTable(b *testing.B) {
	t := table{
		"key1": "value1",
		"key2": int(42),
		"key3": bool(true),
	}
	for i := 0; i < b.N; i++ {
		encodeTableToBytes(t)
	}
}
