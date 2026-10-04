package broker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// PublishOwned transfers ownership of the payload, headers and metadata to the
// store, skipping the deep copy Publish has to do. These tests pin the stored
// result and the durability behaviour.

func TestPublishOwnedStoresMessage(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}

	msg := Message{
		ID:       "owned-1",
		Queue:    "q",
		Payload:  []byte("payload"),
		Headers:  map[string]string{"h": "1"},
		Metadata: map[string]string{"m": "2"},
	}
	res, err := b.Publisher().PublishOwned(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if res.MessageID != "owned-1" {
		t.Fatalf("MessageID = %q, want owned-1", res.MessageID)
	}
	if len(res.Destinations) != 1 || res.Destinations[0].Kind != DestinationQueue || res.Destinations[0].Name != "q" {
		t.Fatalf("Destinations = %+v", res.Destinations)
	}

	got, ok, err := b.Store().Get(ctx, "owned-1")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if string(got.Payload) != "payload" {
		t.Errorf("Payload = %q", got.Payload)
	}
	if got.Headers["h"] != "1" || got.Metadata["m"] != "2" {
		t.Errorf("headers/metadata = %v / %v", got.Headers, got.Metadata)
	}
	if got.ID == "" || got.CreatedAt.IsZero() {
		t.Errorf("store did not fill ID/CreatedAt: %+v", got)
	}
}

// PublishOwned applies options so it is a drop-in for Publish at the call site.
func TestPublishOwnedAppliesOptions(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}

	res, err := b.Publisher().PublishOwned(ctx,
		Message{ID: "owned-opts", Payload: []byte("p")},
		WithQueue("q"),
		WithContentType("application/x-owned"),
		WithHeaders(map[string]string{"oh": "1"}),
		WithMetadata(map[string]string{"om": "2"}),
		WithTraceID("trace-1"),
		WithCausationID("cause-1"),
		WithTTL(time.Hour),
		WithMaxAttempts(9),
		WithStrategy(StrategyRoundRobin),
	)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := b.Store().Get(ctx, res.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ContentType != "application/x-owned" {
		t.Errorf("ContentType = %q", got.ContentType)
	}
	if got.Headers["oh"] != "1" || got.Metadata["om"] != "2" {
		t.Errorf("headers/metadata = %v / %v", got.Headers, got.Metadata)
	}
	if got.Headers["x-routing-strategy"] != string(StrategyRoundRobin) {
		t.Errorf("strategy header = %q", got.Headers["x-routing-strategy"])
	}
	if got.TraceID != "trace-1" || got.CausationID != "cause-1" {
		t.Errorf("trace/causation = %q / %q", got.TraceID, got.CausationID)
	}
	if got.ExpiresAt.IsZero() {
		t.Error("TTL was dropped")
	}
	if got.MaxAttempts != 9 {
		t.Errorf("MaxAttempts = %d, want 9", got.MaxAttempts)
	}
	if got.Queue != "q" {
		t.Errorf("Queue = %q, want q", got.Queue)
	}
}

// The ownership fast path only exists where the store implements
// OwnedAtomicStore. Durable stores embed MemoryStore and must take the fast
// path, and the message must survive a reopen.
func TestPublishOwnedOnDurableStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewBinaryFileWALStore(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	res, err := b.Publisher().PublishOwned(ctx, Message{ID: "durable-1", Payload: []byte("payload")}, WithQueue("q"))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewBinaryFileWALStore(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, ok, err := reopened.Get(ctx, res.MessageID)
	if err != nil || !ok {
		t.Fatalf("after reopen: ok=%v err=%v", ok, err)
	}
	if string(got.Payload) != "payload" {
		t.Fatalf("payload after reopen = %q", got.Payload)
	}
}

// nonOwningStore embeds the Store interface rather than *MemoryStore. The
// interface has no AppendAtomicOwned method, so method promotion stops there and
// this type genuinely cannot take ownership, which is what forces the publisher
// down its fallback path. Embedding *MemoryStore would promote the method and
// make the test pass for the wrong reason.
type nonOwningStore struct {
	Store
}

func TestPublishOwnedFallsBackWhenStoreCannotTakeOwnership(t *testing.T) {
	ctx := context.Background()
	mem := NewMemoryStore()
	store := &nonOwningStore{Store: mem}
	b, err := New(Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	res, err := b.Publisher().PublishOwned(ctx, Message{ID: "fallback-1", Payload: []byte("payload")}, WithQueue("q"))
	if err != nil {
		t.Fatal(err)
	}
	if res.MessageID != "fallback-1" {
		t.Fatalf("MessageID = %q", res.MessageID)
	}
	got, ok, err := store.Get(ctx, "fallback-1")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if string(got.Payload) != "payload" {
		t.Fatalf("payload = %q", got.Payload)
	}
}

// Guards the premise of the fallback test: a Store-interface wrapper must not
// accidentally satisfy OwnedAtomicStore, otherwise the fallback path would never
// be reached and the test above would pass for the wrong reason.
func TestNonOwningStoreDoesNotSatisfyOwnedAtomicStore(t *testing.T) {
	mem := NewMemoryStore()
	defer mem.Close()
	store := &nonOwningStore{Store: mem}
	if _, ok := interface{}(store).(OwnedAtomicStore); ok {
		t.Fatal("nonOwningStore must not satisfy OwnedAtomicStore")
	}
	// The embedded MemoryStore does satisfy it, which is what makes the durable
	// store case take the fast path.
	if _, ok := interface{}(mem).(OwnedAtomicStore); !ok {
		t.Fatal("MemoryStore should satisfy OwnedAtomicStore")
	}
}

// PublishOwned must agree with Publish about state. Queue pause gates
// consumption, not publishing, so both accept a publish to a paused queue; a
// paused broker rejects both. Asserting parity rather than an absolute keeps the
// two publish paths from drifting.
func TestPublishOwnedRespectsStateLikePublish(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if err := b.PauseQueue(ctx, "q"); err != nil {
		t.Fatal(err)
	}

	_, ownedErr := b.Publisher().PublishOwned(ctx, Message{ID: "owned-paused-q", Payload: []byte("p")}, WithQueue("q"))
	_, copyErr := b.Publisher().Publish(ctx, "", []byte("p"), WithQueue("q"), WithMessageID("copy-paused-q"))
	if (ownedErr == nil) != (copyErr == nil) {
		t.Fatalf("paused queue: PublishOwned err=%v but Publish err=%v", ownedErr, copyErr)
	}

	if err := b.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().PublishOwned(ctx, Message{ID: "broker-paused", Payload: []byte("p")}, WithQueue("q")); !errors.Is(err, ErrPaused) {
		t.Fatalf("publish on a paused broker returned %v, want ErrPaused", err)
	}
	if _, err := b.Publisher().Publish(ctx, "", []byte("p"), WithQueue("q")); !errors.Is(err, ErrPaused) {
		t.Fatalf("Publish on a paused broker returned %v, want ErrPaused", err)
	}
}

// A leased message carries lease state that must not be republished verbatim.
func TestPublishOwnedGeneratesIDWhenAbsent(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	res, err := b.Publisher().PublishOwned(ctx, Message{Payload: []byte("p")}, WithQueue("q"))
	if err != nil {
		t.Fatal(err)
	}
	if res.MessageID == "" {
		t.Fatal("expected a generated message id")
	}
	if _, ok, _ := b.Store().Get(ctx, res.MessageID); !ok {
		t.Fatalf("generated id %q not found in the store", res.MessageID)
	}
}
