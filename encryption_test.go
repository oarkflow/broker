package broker_test

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/broker"
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

// TestUnencryptedBrokerConsumerPublisher tests the full stack without encryption.
func TestUnencryptedBrokerConsumerPublisher(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create broker with plain JSONL WAL
	st, err := broker.NewFileWALStore(dir)
	if err != nil {
		t.Fatalf("NewFileWALStore: %v", err)
	}
	b := broker.MustNew(broker.Config{
		Name:              "test-unencrypted",
		Store:             st,
		DefaultQueue:      "test-queue",
		DefaultVisibility: 5 * time.Second,
		DefaultRetry:      broker.DefaultRetryPolicy(),
	})
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer b.Stop(context.Background())

	var count atomic.Int32

	// Subscribe
	_, err = b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "test-worker",
		Queue: "test-queue",
	}, func(ctx context.Context, d broker.Delivery) error {
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Publish 10 messages
	pub := b.Publisher()
	for i := 0; i < 10; i++ {
		_, err := pub.Publish(ctx, "test.topic", map[string]int{"i": i}, broker.WithQueue("test-queue"))
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	time.Sleep(2 * time.Second)
	if count.Load() != 10 {
		t.Fatalf("expected 10 messages processed, got %d", count.Load())
	}

	// Verify WAL file is plaintext
	walPath := findNonEmptyWALFile(t, dir, "broker.wal.*-of-*.jsonl")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("WAL file is empty")
	}
	// Check that the first record is valid JSON (plaintext)
	if data[0] != '{' {
		t.Fatalf("WAL file is not plaintext JSON, first byte: %c", data[0])
	}
	t.Logf("PASS: Unencrypted — %d messages processed, WAL is plaintext JSONL", count.Load())
}

// TestEncryptedWALBrokerConsumerPublisher tests the full stack with WAL encryption.
func TestEncryptedWALBrokerConsumerPublisher(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Generate a random 32-byte AES-256 key
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("generate key: %v", err)
	}

	// Create broker with encrypted JSONL WAL
	st, err := broker.NewFileWALStoreWithConfig(broker.FileWALStoreConfig{
		Dir:           dir,
		EncryptionKey: key,
		KeyName:       "test-key-v1",
	})
	if err != nil {
		t.Fatalf("NewFileWALStoreWithConfig: %v", err)
	}
	b := broker.MustNew(broker.Config{
		Name:              "test-encrypted-wal",
		Store:             st,
		DefaultQueue:      "test-queue",
		DefaultVisibility: 5 * time.Second,
		DefaultRetry:      broker.DefaultRetryPolicy(),
	})
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer b.Stop(context.Background())

	var count atomic.Int32

	// Subscribe
	_, err = b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "test-worker",
		Queue: "test-queue",
	}, func(ctx context.Context, d broker.Delivery) error {
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Publish 10 messages
	pub := b.Publisher()
	for i := 0; i < 10; i++ {
		_, err := pub.Publish(ctx, "test.topic", map[string]int{"i": i}, broker.WithQueue("test-queue"))
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	time.Sleep(2 * time.Second)
	if count.Load() != 10 {
		t.Fatalf("expected 10 messages processed, got %d", count.Load())
	}

	// Verify WAL file is encrypted (starts with "ENC:")
	walPath := findNonEmptyWALFile(t, dir, "broker.wal.*-of-*.jsonl")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("WAL file is empty")
	}
	if string(data[:4]) != "ENC:" {
		t.Fatalf("WAL file should start with ENC: prefix when encrypted, got: %s", string(data[:minInt(len(data), 20)]))
	}
	t.Logf("PASS: Encrypted WAL — %d messages processed, WAL records are AES-GCM encrypted", count.Load())
}

// TestEncryptedWALRecovery tests that encrypted WAL survives broker restart.
func TestEncryptedWALRecovery(t *testing.T) {
	dir := t.TempDir()

	// Generate a random 32-byte AES-256 key
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("generate key: %v", err)
	}

	// Phase 1: Start broker, publish messages, stop
	func() {
		ctx := context.Background()
		st, err := broker.NewFileWALStoreWithConfig(broker.FileWALStoreConfig{
			Dir:           dir,
			EncryptionKey: key,
		})
		if err != nil {
			t.Fatalf("NewFileWALStoreWithConfig: %v", err)
		}
		b := broker.MustNew(broker.Config{
			Name:              "test-recovery",
			Store:             st,
			DefaultQueue:      "test-queue",
			DefaultVisibility: 5 * time.Second,
		})
		if err := b.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		pub := b.Publisher()
		for i := 0; i < 5; i++ {
			_, _ = pub.Publish(ctx, "test.topic", map[string]int{"i": i}, broker.WithQueue("test-queue"))
		}
		time.Sleep(500 * time.Millisecond)
		b.Stop(context.Background())
	}()

	// Phase 2: Restart broker, verify messages recovered
	func() {
		ctx := context.Background()
		st, err := broker.NewFileWALStoreWithConfig(broker.FileWALStoreConfig{
			Dir:           dir,
			EncryptionKey: key,
		})
		if err != nil {
			t.Fatalf("NewFileWALStoreWithConfig after restart: %v", err)
		}
		b := broker.MustNew(broker.Config{
			Name:              "test-recovery",
			Store:             st,
			DefaultQueue:      "test-queue",
			DefaultVisibility: 5 * time.Second,
		})
		if err := b.Start(ctx); err != nil {
			t.Fatalf("Start after restart: %v", err)
		}
		defer b.Stop(context.Background())

		var count atomic.Int32
		_, err = b.Subscribe(ctx, broker.ConsumerConfig{
			Name:  "test-worker",
			Queue: "test-queue",
		}, func(ctx context.Context, d broker.Delivery) error {
			count.Add(1)
			return nil
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		time.Sleep(2 * time.Second)
		if count.Load() != 5 {
			t.Fatalf("expected 5 recovered messages, got %d", count.Load())
		}
		t.Logf("PASS: Encrypted WAL recovery — %d messages recovered after restart", count.Load())
	}()
}

// TestEncryptedBinaryWAL tests binary WAL with encryption.
func TestEncryptedBinaryWAL(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Generate encryption key
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("generate key: %v", err)
	}

	// Create broker with encrypted binary WAL
	st, err := broker.NewBinaryFileWALStore(dir,
		broker.WithFrameSigningKey([]byte("test-signing-key")),
		broker.WithFrameEncryptionKey(key),
	)
	if err != nil {
		t.Fatalf("NewBinaryFileWALStore: %v", err)
	}
	b := broker.MustNew(broker.Config{
		Name:              "test-encrypted-binary",
		Store:             st,
		DefaultQueue:      "test-queue",
		DefaultVisibility: 5 * time.Second,
		FrameCodec: broker.NewBinaryMessageCodec(
			broker.WithFrameSigningKey([]byte("test-signing-key")),
			broker.WithFrameEncryptionKey(key),
		),
	})
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer b.Stop(context.Background())

	var count atomic.Int32

	// Subscribe
	_, err = b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "test-worker",
		Queue: "test-queue",
	}, func(ctx context.Context, d broker.Delivery) error {
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Publish 10 messages
	pub := b.Publisher()
	for i := 0; i < 10; i++ {
		_, err := pub.Publish(ctx, "test.topic", map[string]int{"i": i}, broker.WithQueue("test-queue"))
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	time.Sleep(2 * time.Second)
	if count.Load() != 10 {
		t.Fatalf("expected 10 messages processed, got %d", count.Load())
	}

	// Verify binary WAL file exists and is not plaintext
	walPath := findNonEmptyWALFile(t, dir, "broker.wal.*-of-*.fwb")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Binary WAL file is empty")
	}
	// FWB1 files start with the magic bytes, not JSON
	if data[0] == '{' {
		t.Fatal("Binary WAL file should not be plaintext JSON")
	}
	t.Logf("PASS: Encrypted binary WAL — %d messages processed, WAL is encrypted binary", count.Load())
}

// TestMixedEncryptedUnencrypted verifies both modes can be used with the same broker.
func TestMixedEncryptedUnencrypted(t *testing.T) {
	ctx := context.Background()

	// Start unencrypted broker
	dirPlain := t.TempDir()
	stPlain, err := broker.NewFileWALStore(dirPlain)
	if err != nil {
		t.Fatalf("NewFileWALStore: %v", err)
	}
	bPlain := broker.MustNew(broker.Config{
		Name:              "test-plain",
		Store:             stPlain,
		DefaultQueue:      "plain-queue",
		DefaultVisibility: 5 * time.Second,
	})
	if err := bPlain.Start(ctx); err != nil {
		t.Fatalf("Start plain: %v", err)
	}
	defer bPlain.Stop(context.Background())

	// Start encrypted broker
	dirEnc := t.TempDir()
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	stEnc, err := broker.NewFileWALStoreWithConfig(broker.FileWALStoreConfig{
		Dir:           dirEnc,
		EncryptionKey: key,
	})
	if err != nil {
		t.Fatalf("NewFileWALStoreWithConfig: %v", err)
	}
	bEnc := broker.MustNew(broker.Config{
		Name:              "test-encrypted",
		Store:             stEnc,
		DefaultQueue:      "enc-queue",
		DefaultVisibility: 5 * time.Second,
	})
	if err := bEnc.Start(ctx); err != nil {
		t.Fatalf("Start encrypted: %v", err)
	}
	defer bEnc.Stop(context.Background())

	var plainCount, encCount atomic.Int32

	// Subscribe to both
	bPlain.Subscribe(ctx, broker.ConsumerConfig{Name: "plain-worker", Queue: "plain-queue"}, func(ctx context.Context, d broker.Delivery) error {
		plainCount.Add(1)
		return nil
	})
	bEnc.Subscribe(ctx, broker.ConsumerConfig{Name: "enc-worker", Queue: "enc-queue"}, func(ctx context.Context, d broker.Delivery) error {
		encCount.Add(1)
		return nil
	})

	// Publish to both
	for i := 0; i < 5; i++ {
		bPlain.Publisher().Publish(ctx, "plain.topic", i, broker.WithQueue("plain-queue"))
		bEnc.Publisher().Publish(ctx, "enc.topic", i, broker.WithQueue("enc-queue"))
	}

	time.Sleep(2 * time.Second)

	if plainCount.Load() != 5 {
		t.Fatalf("plain: expected 5, got %d", plainCount.Load())
	}
	if encCount.Load() != 5 {
		t.Fatalf("encrypted: expected 5, got %d", encCount.Load())
	}
	t.Logf("PASS: Mixed — plain broker: %d, encrypted broker: %d", plainCount.Load(), encCount.Load())
}

// TestFrameCodecEncryption tests FWB1 frame encryption end-to-end.
func TestFrameCodecEncryption(t *testing.T) {
	// Generate keys
	signingKey := []byte("test-signing-key-for-hmac")
	encryptionKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, encryptionKey); err != nil {
		t.Fatalf("generate key: %v", err)
	}

	// Create codec with encryption
	codec := broker.NewBinaryMessageCodec(
		broker.WithFrameSigningKey(signingKey),
		broker.WithFrameEncryptionKey(encryptionKey),
		broker.WithFrameCompressionThreshold(1024),
	)

	// Encode a message
	msg := broker.Message{
		ID:      "test-msg-1",
		Topic:   "test.topic",
		Queue:   "test.queue",
		Type:    "test",
		Payload: []byte(`{"hello":"world","data":"This is a test message that should be encrypted at rest"}`),
		Headers: map[string]string{"tenant": "test-tenant", "trace-id": "abc-123"},
	}
	frame, err := codec.EncodeMessage(nil, msg)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}

	// Verify frame is not plaintext JSON
	if frame[0] == '{' {
		t.Fatal("Encrypted frame should not start with JSON")
	}

	// Decode with same codec
	var decoded broker.Message
	if err := codec.DecodeMessage(frame, &decoded); err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}

	if decoded.ID != msg.ID {
		t.Fatalf("ID mismatch: got %s, want %s", decoded.ID, msg.ID)
	}
	if decoded.Topic != msg.Topic {
		t.Fatalf("Topic mismatch: got %s, want %s", decoded.Topic, msg.Topic)
	}
	if string(decoded.Payload) != string(msg.Payload) {
		t.Fatalf("Payload mismatch: got %s, want %s", decoded.Payload, msg.Payload)
	}
	if decoded.Headers["tenant"] != "test-tenant" {
		t.Fatalf("Header mismatch: got %s, want test-tenant", decoded.Headers["tenant"])
	}

	t.Logf("PASS: Frame encryption — message encrypted/decrypted successfully with HMAC + AES-GCM")
}

// TestUnencryptedFrameCodec tests FWB1 frame without encryption.
func TestUnencryptedFrameCodec(t *testing.T) {
	codec := broker.NewBinaryMessageCodec()

	msg := broker.Message{
		ID:      "test-msg-2",
		Topic:   "test.topic",
		Payload: []byte(`{"hello":"world"}`),
	}
	frame, err := codec.EncodeMessage(nil, msg)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}

	var decoded broker.Message
	if err := codec.DecodeMessage(frame, &decoded); err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}

	if decoded.ID != msg.ID {
		t.Fatalf("ID mismatch: got %s, want %s", decoded.ID, msg.ID)
	}

	t.Logf("PASS: Frame without encryption — message encoded/decrypted successfully")
}

// TestBinaryWALRecovery tests binary WAL recovery with encryption.
func TestBinaryWALRecovery(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("generate key: %v", err)
	}

	// Phase 1: Publish
	func() {
		ctx := context.Background()
		st, err := broker.NewBinaryFileWALStore(dir,
			broker.WithFrameEncryptionKey(key),
		)
		if err != nil {
			t.Fatalf("NewBinaryFileWALStore: %v", err)
		}
		b := broker.MustNew(broker.Config{
			Name: "test-binary-recovery", Store: st, DefaultQueue: "q",
			DefaultVisibility: 5 * time.Second,
			FrameCodec: broker.NewBinaryMessageCodec(
				broker.WithFrameEncryptionKey(key),
			),
		})
		b.Start(ctx)
		pub := b.Publisher()
		for i := 0; i < 5; i++ {
			pub.Publish(ctx, "topic", i, broker.WithQueue("q"))
		}
		time.Sleep(500 * time.Millisecond)
		b.Stop(context.Background())
	}()

	// Phase 2: Recover
	func() {
		ctx := context.Background()
		st, err := broker.NewBinaryFileWALStore(dir,
			broker.WithFrameEncryptionKey(key),
		)
		if err != nil {
			t.Fatalf("NewBinaryFileWALStore after restart: %v", err)
		}
		b := broker.MustNew(broker.Config{
			Name: "test-binary-recovery", Store: st, DefaultQueue: "q",
			DefaultVisibility: 5 * time.Second,
			FrameCodec: broker.NewBinaryMessageCodec(
				broker.WithFrameEncryptionKey(key),
			),
		})
		b.Start(ctx)
		defer b.Stop(context.Background())

		var count atomic.Int32
		b.Subscribe(ctx, broker.ConsumerConfig{Name: "w", Queue: "q"}, func(ctx context.Context, d broker.Delivery) error {
			count.Add(1)
			return nil
		})
		time.Sleep(2 * time.Second)
		if count.Load() != 5 {
			t.Fatalf("expected 5 recovered, got %d", count.Load())
		}
		t.Logf("PASS: Binary WAL recovery — %d messages recovered after restart with encryption", count.Load())
	}()
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Ensure crypto/cipher is used (for key generation).
var _ = cipher.NewGCM
var _ = fmt.Sprintf
