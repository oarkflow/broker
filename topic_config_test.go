package broker

import (
	"context"
	"errors"
	"testing"
)

func TestTopicConfigurationIsEnforced(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.DeclareTopic(ctx, TopicConfig{Name: "events", Partitions: 4, Paused: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, Message{ID: "paused", Partition: 0}, []Destination{{Kind: DestinationTopic, Name: "events"}}); !errors.Is(err, ErrPaused) {
		t.Fatalf("paused topic append error=%v", err)
	}
	if err := store.DeclareTopic(ctx, TopicConfig{Name: "events", Partitions: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, Message{ID: "bad-partition", Partition: 4}, []Destination{{Kind: DestinationTopic, Name: "events"}}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("out-of-range partition error=%v", err)
	}
	if _, err := store.Append(ctx, Message{ID: "valid", Partition: 3}, []Destination{{Kind: DestinationTopic, Name: "events"}}); err != nil {
		t.Fatalf("valid partition append: %v", err)
	}
	if _, err := store.Append(ctx, Message{ID: "unknown-kind"}, []Destination{{Kind: DestinationKind("unknown"), Name: "events"}}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("unsupported destination error=%v", err)
	}
}

func TestAtomicAppendRejectsPausedTopicWithoutPartialMutation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "jobs"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeclareTopic(ctx, TopicConfig{Name: "paused", Paused: true}); err != nil {
		t.Fatal(err)
	}
	_, err := store.AppendAtomic(ctx, []AppendEntry{{Message: Message{ID: "atomic"}, Destinations: []Destination{
		{Kind: DestinationQueue, Name: "jobs"},
		{Kind: DestinationTopic, Name: "paused"},
	}}})
	if !errors.Is(err, ErrPaused) {
		t.Fatalf("atomic append error=%v", err)
	}
	if _, ok, getErr := store.Get(ctx, "atomic"); getErr != nil || ok {
		t.Fatalf("paused atomic append partially mutated store: ok=%v err=%v", ok, getErr)
	}
}
