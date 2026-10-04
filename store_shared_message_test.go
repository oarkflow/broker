package broker

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
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

// ReadStream used to scan a stream's ids from index 0 on every call, so a
// consumer reading from the tail of a long stream paid for every message ever
// written. The read hint should make sequential reads O(limit) while returning
// exactly the same messages.
func TestReadStreamReturnsSameMessagesWithHint(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		removed int
		setup   func(t *testing.T, store *MemoryStore)
	}{
		{"dense", 0, func(t *testing.T, store *MemoryStore) {}},
		{"with holes", 7, func(t *testing.T, store *MemoryStore) {
			// Remove entries from the middle so indices no longer line up with
			// offsets, which is what the backward correction has to handle.
			if err := store.DeclareQueue(ctx, QueueConfig{Name: "sink"}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 20; i += 3 {
				if _, err := store.Move(ctx, MoveRequest{
					SourceType: DestinationStream, Source: "s",
					DestinationType: DestinationQueue, Destination: "sink",
					MessageIDs: []string{streamMsgID(i)},
					Mode:       MoveRemoveSource,
				}); err != nil {
					t.Fatal(err)
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryStore()
			defer store.Close()
			if err := store.DeclareStream(ctx, StreamConfig{Name: "s"}); err != nil {
				t.Fatal(err)
			}
			const total = 300
			for i := 0; i < total; i++ {
				if _, err := store.Append(ctx, Message{
					ID: streamMsgID(i), Stream: "s", Payload: []byte("p"),
				}, []Destination{{Kind: DestinationStream, Name: "s"}}); err != nil {
					t.Fatal(err)
				}
			}
			tc.setup(t, store)

			// Sequential tail reads, which is what the hint optimises.
			var after uint64
			seen := 0
			for round := 0; round < 1000; round++ {
				msgs, err := store.ReadStream(ctx, StreamReadRequest{Stream: "s", AfterOffset: after, Limit: 25})
				if err != nil {
					t.Fatal(err)
				}
				if len(msgs) == 0 {
					break
				}
				for _, m := range msgs {
					if m.Offset <= after {
						t.Fatalf("round %d returned offset %d <= after %d", round, m.Offset, after)
					}
					if m.Offset != after+1 && seen > 0 {
						// Offsets may skip removed entries, but must not repeat or
						// go backwards, which they cannot given monotonic assignment.
						if m.Offset < after+1 {
							t.Fatalf("round %d went backwards: %d after %d", round, m.Offset, after)
						}
					}
					after = m.Offset
					seen++
				}
			}
			if want := total - tc.removed; seen != want {
				t.Fatalf("sequential reads saw %d messages, want %d", seen, want)
			}

			// Out-of-order reads must still be correct: the hint is only a hint.
			for _, probe := range []uint64{0, 1, 150, 299, 300, 5000} {
				msgs, err := store.ReadStream(ctx, StreamReadRequest{Stream: "s", AfterOffset: probe, Limit: 10})
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range msgs {
					if m.Offset <= probe {
						t.Fatalf("after=%d returned offset %d", probe, m.Offset)
					}
				}
				if probe == 0 && len(msgs) != 10 {
					t.Fatalf("after=0 returned %d messages, want 10", len(msgs))
				}
				if probe >= total && len(msgs) != 0 {
					t.Fatalf("after=%d (past the end) returned %d messages", probe, len(msgs))
				}
			}
		})
	}
}

func streamMsgID(i int) string { return fmt.Sprintf("s-%04d", i) }

// moveOps is keyed by a caller-supplied operation id, so nothing in the store
// refers to those keys. Only the replicated-move path populates it, and it never
// reclaimed them, so a replicated workload that moves messages in a loop grew the
// map without bound. Cleanup now evicts them, and a restored snapshot rebuilds the
// index so restored entries stay prunable.
func TestCleanupPrunesMoveResults(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "src"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "dst"}); err != nil {
		t.Fatal(err)
	}

	record := func(opID string) {
		t.Helper()
		planned := map[string]string{}
		if _, err := store.applyMovePlan(ctx, opID, MoveRequest{
			SourceType: DestinationQueue, Source: "src",
			DestinationType: DestinationQueue, Destination: "dst",
			Mode: MoveRemoveSource,
		}, planned); err != nil {
			t.Fatal(err)
		}
	}

	const ops = moveOpsMax + 500
	for i := 0; i < ops; i++ {
		record(fmt.Sprintf("op-%d", i))
	}

	store.mu.RLock()
	before := len(store.moveOps)
	store.mu.RUnlock()
	if before != ops {
		t.Fatalf("seeded %d move results, store holds %d", ops, before)
	}

	store.Cleanup(0)

	store.mu.RLock()
	after := len(store.moveOps)
	order := len(store.moveOrder)
	store.mu.RUnlock()
	if after >= before {
		t.Fatalf("Cleanup did not prune move results: %d -> %d", before, after)
	}
	if after > moveOpsMax {
		t.Fatalf("move results still above the cap after Cleanup: %d > %d", after, moveOpsMax)
	}
	if order != after {
		t.Fatalf("insertion index has %d entries but the map holds %d; they must stay in step", order, after)
	}

	// Repeating an operation id is idempotent and must not double-index it.
	newest := fmt.Sprintf("op-%d", ops-1)
	store.mu.RLock()
	_, kept := store.moveOps[newest]
	store.mu.RUnlock()
	if !kept {
		t.Fatalf("the most recent move result %q was evicted", newest)
	}
	record(newest)
	store.mu.RLock()
	orderAfter := len(store.moveOrder)
	store.mu.RUnlock()
	if orderAfter != order {
		t.Fatalf("re-recording an existing operation id grew the index from %d to %d", order, orderAfter)
	}
}

// A restored snapshot must carry a usable insertion index, otherwise its move
// results could never be pruned.
func TestRestoredMoveResultsStayPrunable(t *testing.T) {
	source := NewMemoryStore()
	defer source.Close()
	source.mu.Lock()
	for i := 0; i < 50; i++ {
		source.moveOps[fmt.Sprintf("op-%d", i)] = MoveResult{Moved: 1}
	}
	source.mu.Unlock()
	snap := source.Snapshot()

	target := NewMemoryStore()
	defer target.Close()
	target.Restore(snap)

	target.mu.RLock()
	got := len(target.moveOps)
	indexed := len(target.moveOrder)
	target.mu.RUnlock()
	if got != 50 {
		t.Fatalf("restored %d move results, want 50", got)
	}
	if indexed != got {
		t.Fatalf("restored %d move results but indexed %d; they must stay in step", got, indexed)
	}

	// Age them past the retention window and confirm they are reclaimed.
	target.mu.Lock()
	for i := range target.moveOrderAt {
		target.moveOrderAt[i] = time.Now().Add(-2 * moveOpsTTL)
	}
	target.mu.Unlock()
	target.Cleanup(0)

	target.mu.RLock()
	remaining := len(target.moveOps)
	target.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("%d restored move results survived pruning", remaining)
	}
}
