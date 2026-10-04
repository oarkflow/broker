package broker

import (
	"context"
	"path/filepath"
	"testing"
)

// Lease and Append hand back a Message that shares Payload, Headers and Metadata
// with the stored record. That is the whole point: deep-copying two
// map[string]string values was over half of all allocations on the consume path.
// These tests pin the resulting contract from both sides. The shared fields must
// be safe to read, and the struct fields must stay independent, and the
// explicit fetch APIs must still hand back real copies.

// mustGet returns the stored message through Get, which deep copies.
func mustGet(t *testing.T, b *Broker, id string) Message {
	t.Helper()
	m, ok, err := b.Store().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if !ok {
		t.Fatalf("message %s not found", id)
	}
	return m
}

func leaseOnce(t *testing.T, b *Broker, queue string, limit int) []Message {
	t.Helper()
	msgs, err := b.Store().Lease(context.Background(), LeaseRequest{
		Queue: queue, ConsumerID: "c1", ConsumerGroup: "g1", Limit: limit, VisibilityTimeout: 30e9,
	})
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	return msgs
}

// A leased message must carry the full stored payload, headers and metadata.
func TestLeasedMessageCarriesHeadersAndMetadata(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "", []byte("body"),
		WithQueue("q"), WithHeaders(map[string]string{"h": "1"}), WithMetadata(map[string]string{"m": "2"})); err != nil {
		t.Fatal(err)
	}
	msgs := leaseOnce(t, b, "q", 1)
	if len(msgs) != 1 {
		t.Fatalf("leased %d, want 1", len(msgs))
	}
	if string(msgs[0].Payload) != "body" {
		t.Errorf("payload = %q", msgs[0].Payload)
	}
	if msgs[0].Headers["h"] != "1" || msgs[0].Metadata["m"] != "2" {
		t.Errorf("headers/metadata = %v / %v", msgs[0].Headers, msgs[0].Metadata)
	}
}

// Rewriting the struct fields of a leased message must not disturb the store.
// This is the case that makes sharing safe: everything the broker mutates
// in place is a scalar or a time value held in the struct.
func TestLeasedMessageStructFieldsAreIndependent(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	res, err := b.Publisher().Publish(ctx, "", []byte("body"),
		WithQueue("q"), WithHeaders(map[string]string{"h": "1"}))
	if err != nil {
		t.Fatal(err)
	}

	msgs := leaseOnce(t, b, "q", 1)
	if len(msgs) != 1 {
		t.Fatalf("leased %d, want 1", len(msgs))
	}
	// Scribble on every field the broker owns.
	msgs[0].Status = MessageDeadLettered
	msgs[0].Attempts = 99
	msgs[0].Redeliveries = 42
	msgs[0].LastError = "clobbered"
	msgs[0].ConsumerID = "someone-else"
	msgs[0].LeaseToken = "forged"
	msgs[0].LeaseUntil = msgs[0].LeaseUntil.Add(9999)
	msgs[0].Priority = -1
	msgs[0].Topic = "clobbered"
	msgs[0].Queue = "clobbered"

	stored := mustGet(t, b, res.MessageID)
	if stored.Status == MessageDeadLettered {
		t.Error("Status leaked from the leased copy into the store")
	}
	if stored.Attempts == 99 || stored.Redeliveries == 42 {
		t.Errorf("Attempts/Redeliveries leaked: %d/%d", stored.Attempts, stored.Redeliveries)
	}
	if stored.LastError == "clobbered" {
		t.Error("LastError leaked")
	}
	if stored.ConsumerID == "someone-else" {
		t.Error("ConsumerID leaked")
	}
	if stored.LeaseToken == "forged" {
		t.Error("LeaseToken leaked")
	}
	if stored.Priority == -1 {
		t.Error("Priority leaked")
	}
	if stored.Topic == "clobbered" || stored.Queue == "clobbered" {
		t.Errorf("routing leaked: %s/%s", stored.Topic, stored.Queue)
	}
	// The lease itself must still be recorded.
	if stored.Status != MessageLeased || stored.LeaseToken == "" {
		t.Errorf("store lost the lease: status=%s token=%q", stored.Status, stored.LeaseToken)
	}
	// Assigning fresh maps on the copy is fine and must not reach the store.
	msgs[0].Headers = map[string]string{"h": "replaced"}
	msgs[0].Payload = []byte("replaced")
	if got := mustGet(t, b, res.MessageID); got.Headers["h"] != "1" || string(got.Payload) != "body" {
		t.Errorf("reassigning the copy's maps reached the store: %v / %q", got.Headers, got.Payload)
	}
}

// Get is the documented way to get a message you intend to modify, so it must
// still be a real deep copy.
func TestGetReturnsIndependentCopy(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	res, err := b.Publisher().Publish(ctx, "", []byte("body"),
		WithQueue("q"), WithHeaders(map[string]string{"h": "1"}), WithMetadata(map[string]string{"m": "2"}))
	if err != nil {
		t.Fatal(err)
	}

	fetched := mustGet(t, b, res.MessageID)
	fetched.Headers["h"] = "mutated"
	fetched.Metadata["m"] = "mutated"
	fetched.Payload[0] = 'X'
	fetched.Status = MessageDeadLettered

	stored := mustGet(t, b, res.MessageID)
	if stored.Headers["h"] != "1" || stored.Metadata["m"] != "2" {
		t.Errorf("Get shared its maps: %v / %v", stored.Headers, stored.Metadata)
	}
	if string(stored.Payload) != "body" {
		t.Errorf("Get shared its payload: %q", stored.Payload)
	}
	if stored.Status == MessageDeadLettered {
		t.Error("Get shared its struct")
	}
}

// Update must store an independent copy even when handed a message that came
// from Lease, otherwise a later append could alias it.
func TestUpdateStoresIndependentCopy(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "", []byte("body"),
		WithQueue("q"), WithHeaders(map[string]string{"h": "1"})); err != nil {
		t.Fatal(err)
	}
	msgs := leaseOnce(t, b, "q", 1)
	if len(msgs) != 1 {
		t.Fatalf("leased %d, want 1", len(msgs))
	}
	leased := msgs[0]
	if err := b.Store().Update(ctx, leased); err != nil {
		t.Fatal(err)
	}
	leased.Headers["h"] = "mutated-after-update"
	if got := mustGet(t, b, leased.ID); got.Headers["h"] != "1" {
		t.Errorf("Update stored the caller's maps: %v", got.Headers)
	}
}

// The same sharing applies to the durable store, which leases through
// prepareLease rather than Lease. It must return the message content intact.
func TestDurableLeaseCarriesHeadersAndMetadata(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(t *testing.T, dir string) Store
	}{
		{"BinaryFileWALStore", func(t *testing.T, dir string) Store {
			s, err := NewBinaryFileWALStore(filepath.Join(dir, "wal"))
			if err != nil {
				t.Fatal(err)
			}
			return s
		}},
		{"FileWALStore", func(t *testing.T, dir string) Store {
			s, err := NewFileWALStore(filepath.Join(dir, "wal.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			return s
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.build(t, t.TempDir())
			defer store.Close()
			if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(ctx, Message{
				ID: "d1", Queue: "q", Payload: []byte("body"),
				Headers: map[string]string{"h": "1"}, Metadata: map[string]string{"m": "2"},
			}, []Destination{{Kind: DestinationQueue, Name: "q"}}); err != nil {
				t.Fatal(err)
			}
			msgs, err := store.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c1", ConsumerGroup: "g1", Limit: 1, VisibilityTimeout: 30e9})
			if err != nil {
				t.Fatalf("lease: %v", err)
			}
			if len(msgs) != 1 {
				t.Fatalf("leased %d, want 1", len(msgs))
			}
			if string(msgs[0].Payload) != "body" {
				t.Errorf("payload = %q", msgs[0].Payload)
			}
			if msgs[0].Headers["h"] != "1" || msgs[0].Metadata["m"] != "2" {
				t.Errorf("headers/metadata = %v / %v", msgs[0].Headers, msgs[0].Metadata)
			}
			if msgs[0].LeaseToken == "" || msgs[0].Status != MessageLeased {
				t.Errorf("lease fields not applied: status=%s token=%q", msgs[0].Status, msgs[0].LeaseToken)
			}
			if err := store.Ack(ctx, AckRequest{MessageID: msgs[0].ID, Queue: "q", ConsumerID: "c1", LeaseToken: msgs[0].LeaseToken}); err != nil {
				t.Errorf("ack: %v", err)
			}
		})
	}
}

// Append returns one entry per destination sharing the stored maps. Content must
// be intact for every destination.
func TestAppendResultCarriesHeadersPerDestination(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeclareTopic(ctx, TopicConfig{Name: "t"}); err != nil {
		t.Fatal(err)
	}
	out, err := store.Append(ctx, Message{
		ID: "m1", Payload: []byte("body"),
		Headers: map[string]string{"h": "1"}, Metadata: map[string]string{"m": "2"},
	}, []Destination{{Kind: DestinationQueue, Name: "q"}, {Kind: DestinationTopic, Name: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("append returned %d entries, want 2", len(out))
	}
	for i, m := range out {
		if m.Headers["h"] != "1" || m.Metadata["m"] != "2" {
			t.Errorf("entry %d lost headers/metadata: %v / %v", i, m.Headers, m.Metadata)
		}
		if string(m.Payload) != "body" {
			t.Errorf("entry %d payload = %q", i, m.Payload)
		}
	}
}
