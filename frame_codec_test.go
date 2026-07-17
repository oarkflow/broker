package broker

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// findNonEmptyWALFile locates the shard file a test's single queue actually
// wrote to (the WAL is partitioned across DefaultWALShards files by queue
// hash, so most shard files exist but stay empty in single-queue tests).
func findNonEmptyWALFile(t *testing.T, dir, pattern string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range matches {
		if info, err := os.Stat(m); err == nil && info.Size() > 0 {
			return m
		}
	}
	t.Fatalf("no non-empty WAL file matching %s in %s", pattern, dir)
	return ""
}

func TestBinaryMessageCodecRoundTripSecure(t *testing.T) {
	codec := NewBinaryMessageCodec(WithFrameCompressionThreshold(1), WithFrameEncryptionKey([]byte("test encryption secret")), WithFrameSigningKey([]byte("test signing secret")))
	in := Message{ID: "m1", Topic: "tenant.1.invoice.created", Queue: "invoice", Stream: "audit", Offset: 42, Key: "tenant-1", RoutingKey: "tenant.1.invoice.created", Type: "invoice", ContentType: "application/json", Payload: bytes.Repeat([]byte(`{"ok":true}`), 64), Headers: map[string]string{"h1": "v1"}, Metadata: map[string]string{"m1": "v1"}, TraceID: "tr", CorrelationID: "corr", Priority: 7, CreatedAt: time.Now(), AvailableAt: time.Now().Add(time.Second), TenantID: "tenant-1", Status: MessageQueued, TaskID: "task-1", TaskName: "pdf.generate"}
	frame, err := codec.EncodeMessage(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	if !IsBinaryFrame(frame) {
		t.Fatal("expected FWB1 binary frame")
	}
	var peek Message
	if err := codec.PeekHeader(frame, &peek); err != nil {
		t.Fatal(err)
	}
	if peek.ID != in.ID || peek.Topic != in.Topic || len(peek.Payload) != 0 {
		t.Fatalf("bad peek: %#v", peek)
	}
	var out Message
	if err := codec.DecodeMessage(frame, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != in.ID || out.Topic != in.Topic || out.Headers["h1"] != "v1" || out.Metadata["m1"] != "v1" || !bytes.Equal(out.Payload, in.Payload) {
		t.Fatalf("roundtrip mismatch: %#v", out)
	}
	frame[len(frame)-2] ^= 0xff
	if err := codec.DecodeMessage(frame, &out); err == nil {
		t.Fatal("expected tamper detection failure")
	}
}

func TestBinaryFileWALStoreRecovery(t *testing.T) {
	dir := t.TempDir()
	st, err := NewBinaryFileWALStore(dir, WithFrameSigningKey([]byte("wal-signing-key")))
	if err != nil {
		t.Fatal(err)
	}
	b := MustNew(Config{Store: st, DefaultQueue: "q"})
	ctx := context.Background()
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q", VisibilityTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	res, err := b.Publisher().Publish(ctx, "", []byte("hello"), WithQueue("q"))
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := st.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", Limit: 1, VisibilityTimeout: time.Second})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("lease %d %v", len(msgs), err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := NewBinaryFileWALStore(dir, WithFrameSigningKey([]byte("wal-signing-key")))
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	got, ok, err := st2.Get(ctx, res.MessageID)
	if err != nil || !ok {
		t.Fatalf("missing recovered message: %v", err)
	}
	if got.Status != MessageQueued || got.LeaseToken != "" {
		t.Fatalf("expected recovered queued message, got status=%s lease=%q", got.Status, got.LeaseToken)
	}
	info, err := os.Stat(findNonEmptyWALFile(t, dir, "broker.wal.*-of-*.fwb"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("wal perm=%v", info.Mode().Perm())
	}
}
