package broker

import (
	"context"
	"errors"
	"testing"
)

func TestPublishReportsAchievedConfirmation(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	got, err := b.Publisher().Publish(context.Background(), "events", []byte("x"), WithQueue("q"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Confirmed != ConfirmEnqueued || got.Durability != DurabilityMemory {
		t.Fatalf("confirmation=%q durability=%q", got.Confirmed, got.Durability)
	}
}

func TestPublishRejectsUnachievableConfirmation(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	_, err := b.Publisher().Publish(context.Background(), "events", []byte("x"), WithQueue("q"), WithConfirmLevel(ConfirmReplicated))
	if !errors.Is(err, ErrUnsupportedMode) {
		t.Fatalf("expected ErrUnsupportedMode, got %v", err)
	}
	if _, ok, getErr := b.Store().Get(context.Background(), ""); getErr != nil || ok {
		t.Fatalf("rejected publish mutated store: ok=%t err=%v", ok, getErr)
	}
}

func TestWALPublishReportsLocalFsync(t *testing.T) {
	store, err := NewBinaryFileWALStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := MustNew(Config{Store: store})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	got, err := b.Publisher().PublishConfirm(context.Background(), "events", []byte("x"), WithQueue("q"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Confirmed != ConfirmPersisted || got.Durability != DurabilityLocalFsync || got.Replicas != 1 {
		t.Fatalf("unexpected result: %+v", got)
	}
}
