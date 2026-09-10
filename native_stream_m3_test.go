package broker

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeStreamChannelMultiplexing(t *testing.T) {
	dir := t.TempDir()
	cfg := NativeStreamConfig{
		Dir:              dir,
		Partitions:       2,
		SegmentBytes:     1 << 20,
		GroupCommitDelay: 5 * time.Millisecond,
		GroupCommitBytes: 64 * 1024,
		MaxFrameBytes:    4 << 20,
		InsecureDev:      true,
	}
	s, err := NewNativeStreamServer(cfg)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer s.Close()

	addr, err := netListenLocal(s)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	client, err := DialNativeStream(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	// Channel 1: Produce and fetch on logical channel 1
	ch1 := client.Channel(1)
	base1, err := ch1.Produce("events", 0, [][]byte{[]byte("ch1-payload-A"), []byte("ch1-payload-B")})
	if err != nil {
		t.Fatalf("ch1 produce: %v", err)
	}
	if base1 != 0 {
		t.Fatalf("expected base 0, got %d", base1)
	}

	// Channel 2: Produce and fetch on logical channel 2 (interleaved over same TCP connection)
	ch2 := client.Channel(2)
	base2, err := ch2.Produce("events", 1, [][]byte{[]byte("ch2-payload-1"), []byte("ch2-payload-2")})
	if err != nil {
		t.Fatalf("ch2 produce: %v", err)
	}
	if base2 != 0 {
		t.Fatalf("expected base 0, got %d", base2)
	}

	// Fetch via ch1
	recs1, next1, err := ch1.Fetch("events", 0, 0, 10, 1<<20)
	if err != nil {
		t.Fatalf("ch1 fetch: %v", err)
	}
	if len(recs1) != 2 || string(recs1[0]) != "ch1-payload-A" || next1 != 2 {
		t.Fatalf("ch1 unexpected records: %v (next=%d)", recs1, next1)
	}

	// Fetch via ch2
	recs2, next2, err := ch2.Fetch("events", 1, 0, 10, 1<<20)
	if err != nil {
		t.Fatalf("ch2 fetch: %v", err)
	}
	if len(recs2) != 2 || string(recs2[1]) != "ch2-payload-2" || next2 != 2 {
		t.Fatalf("ch2 unexpected records: %v (next=%d)", recs2, next2)
	}
}

func TestNativeStreamZeroCopyFetch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "zerocopy")
	cfg := NativeStreamConfig{
		Dir:              dir,
		Partitions:       1,
		SegmentBytes:     1 << 20,
		GroupCommitDelay: 5 * time.Millisecond,
		GroupCommitBytes: 64 * 1024,
		MaxFrameBytes:    4 << 20,
		InsecureDev:      true,
	}
	s, err := NewNativeStreamServer(cfg)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer s.Close()

	part, err := s.partition("zerostream", 0)
	if err != nil {
		t.Fatalf("partition: %v", err)
	}

	payloads := [][]byte{
		[]byte("zerocopy-message-1"),
		bytes.Repeat([]byte("binary-payload-data-"), 50),
	}
	base, err := part.append(context.Background(), payloads)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if base != 0 {
		t.Fatalf("base: %d", base)
	}

	// Create in-memory net.Pipe to simulate socket zero-copy streaming
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	errCh := make(chan error, 1)
	go func() {
		next, count, err := part.WriteZeroCopyFetch(serverConn, 42, 0, 10, 1<<20)
		if err != nil {
			errCh <- err
			return
		}
		if next != 2 || count != 2 {
			errCh <- fmt.Errorf("unexpected next=%d, count=%d", next, count)
			return
		}
		errCh <- nil
	}()

	// Read frame on client side
	op, corr, body, err := readNativeFrame(clientConn, 4<<20)
	if err != nil {
		t.Fatalf("read native frame: %v", err)
	}
	if op != (nativeOpFetchRaw | nativeResponseFlag) {
		t.Fatalf("unexpected op: %02x", op)
	}
	if corr != 42 {
		t.Fatalf("unexpected corr: %d", corr)
	}
	if len(body) == 0 || body[0] != 0 {
		t.Fatalf("unexpected body status: %v", body)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("server streaming error: %v", err)
	}
}
