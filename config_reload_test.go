package broker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigHotReloadAtomicSwapAndRollback(t *testing.T) {
	b := MustNew(Config{
		Store: NewMemoryStore(),
		ACL:   NewACL(),
	})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	ctx := context.Background()
	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "v1-queue"})
	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "v2-queue"})

	watcher := NewConfigWatcher(b, "", 0)

	// 1. Apply valid config: adds route
	validCfg := DynamicConfig{
		Routes: []Route{
			{
				Name:    "payments-route",
				Mode:    RouteDirect,
				Match:   "payments",
				To:      []Destination{{Kind: DestinationQueue, Name: "v1-queue"}},
				Enabled: true,
			},
		},
		ACLTokens: map[string][]string{
			"sec-token-123": {"publish", "consume"},
		},
	}

	if err := watcher.ApplyConfig(ctx, validCfg); err != nil {
		t.Fatalf("expected valid config to apply, got %v", err)
	}

	// Verify route is active
	_, err := b.Publisher().Publish(ctx, "payments", "pay-1")
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := b.Store().ListQueue(ctx, "v1-queue", 10)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("expected 1 message in v1-queue, got %d", len(msgs))
	}

	// Verify ACL token was registered
	princ, ok := b.cfg.ACL.Authenticate("sec-token-123")
	if !ok || !princ.HasPermission("publish") {
		t.Fatalf("expected ACL token to have publish permission")
	}

	// 2. Apply invalid config: route has no destinations -> validation fails with ErrInvalidConfig
	invalidCfg := DynamicConfig{
		Routes: []Route{
			{
				Name:    "broken-route",
				Mode:    RouteDirect,
				Match:   "broken",
				To:      nil, // Invalid!
				Enabled: true,
			},
		},
	}

	err = watcher.ApplyConfig(ctx, invalidCfg)
	if err == nil {
		t.Fatal("expected error on invalid config, got nil")
	}

	// Existing routes should remain unaffected
	_, err = b.Publisher().Publish(ctx, "payments", "pay-2")
	if err != nil {
		t.Fatal(err)
	}
	msgs, err = b.Store().ListQueue(ctx, "v1-queue", 10)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("expected 2 messages in v1-queue, got %d", len(msgs))
	}
}

func TestConfigWatcherFileHotReload(t *testing.T) {
	b := MustNew(Config{
		Store: NewMemoryStore(),
	})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "events-queue"})

	tmpDir, err := os.MkdirTemp("", "config-watch-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cfgPath := filepath.Join(tmpDir, "broker-dynamic.json")

	// Write initial config
	cfg1 := DynamicConfig{
		Routes: []Route{
			{
				Name:    "events-v1",
				Mode:    RouteDirect,
				Match:   "events",
				To:      []Destination{{Kind: DestinationQueue, Name: "events-queue"}},
				Enabled: true,
			},
		},
	}
	data, _ := json.Marshal(cfg1)
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	watcher := NewConfigWatcher(b, cfgPath, 20*time.Millisecond)
	watcher.Start(ctx)
	defer watcher.Stop()

	// Initial reload
	if err := watcher.Reload(ctx); err != nil {
		t.Fatal(err)
	}

	// Update file
	time.Sleep(30 * time.Millisecond)
	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "events-v2-queue"})
	cfg2 := DynamicConfig{
		Routes: []Route{
			{
				Name:    "events-v2",
				Mode:    RouteDirect,
				Match:   "events.v2",
				To:      []Destination{{Kind: DestinationQueue, Name: "events-v2-queue"}},
				Enabled: true,
			},
		},
	}
	data2, _ := json.Marshal(cfg2)
	if err := os.WriteFile(cfgPath, data2, 0644); err != nil {
		t.Fatal(err)
	}

	// Wait for watcher to trigger reload
	time.Sleep(80 * time.Millisecond)

	_, err = b.Publisher().Publish(ctx, "events.v2", "v2-msg")
	if err != nil {
		t.Fatal(err)
	}

	msgs, err := b.Store().ListQueue(ctx, "events-v2-queue", 10)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("expected 1 message in events-v2-queue, got %d (err=%v)", len(msgs), err)
	}
}
