package broker

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Stats is the gate on every observability endpoint: /stats, /health/ready, the
// DLQ dashboard, lag alerts, circuit breaking and the tenant quota check all read
// it. It is currently O(total messages) while holding the store read lock, so a
// probe stalls every publisher for the duration of the scan.
//
// The obvious fix is to maintain counters, which means updating them at every
// insert, status change, expiry, move and purge. A single missed site would make
// Stats quietly wrong, and nothing else in the system would notice: the numbers
// are only ever read, never asserted against the store.
//
// This file is the gate such a refactor has to pass. statsByScan recomputes every
// figure the slow way, straight from the indexes, and the randomized workload
// interleaves every operation that can change a count. Any counter-based Stats
// must agree with statsByScan after every step, for every tenant scope.
func statsByScan(ctx context.Context, s *MemoryStore) StoreStats {
	st := StoreStats{
		Queues:  map[string]int{},
		Topics:  map[string]int{},
		Streams: map[string]int{},
		Offsets: map[string]map[string]uint64{},
		DLQ:     map[string]int{},
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.messages {
		if !tenantAllowed(ctx, m.TenantID) {
			continue
		}
		st.Messages++
		if m.Status == MessageLeased {
			st.Leased++
		}
		if m.Status == MessageDeadLettered {
			st.DeadLetter++
		}
	}
	for _, slot := range s.cold {
		if tenantAllowed(ctx, slot.tenant) {
			st.Messages++
		}
	}
	for name, q := range s.queues {
		count := 0
		for _, id := range q.ids[q.head:] {
			if v := s.scanViewLocked(id); v.exists && tenantAllowed(ctx, v.tenant) {
				count++
			}
		}
		if count > 0 || !hasRestrictedTenantScope(ctx) {
			st.Queues[name] = count
			if isDLQName(name) {
				st.DLQ[name] = count
			}
		}
	}
	for name, t := range s.topics {
		count := s.retainedTopicCountLocked(ctx, t.ids)
		if count > 0 || !hasRestrictedTenantScope(ctx) {
			st.Topics[name] = count
		}
	}
	for name, sm := range s.streams {
		count := s.visibleMessageCountLocked(ctx, sm.ids, 0, "")
		if count > 0 || !hasRestrictedTenantScope(ctx) {
			st.Streams[name] = count
		}
	}
	for stream, groups := range s.offsets {
		for group, offset := range groups {
			visible, ok := visibleOffsetGroup(ctx, group)
			if !ok {
				continue
			}
			if st.Offsets[stream] == nil {
				st.Offsets[stream] = map[string]uint64{}
			}
			st.Offsets[stream][visible] = offset
		}
	}
	return st
}

func compareStats(t *testing.T, label string, got, want StoreStats) {
	t.Helper()
	if got.Messages != want.Messages || got.Leased != want.Leased || got.DeadLetter != want.DeadLetter {
		t.Fatalf("%s: totals diverged: got messages=%d leased=%d dead=%d, want messages=%d leased=%d dead=%d",
			label, got.Messages, got.Leased, got.DeadLetter, want.Messages, want.Leased, want.DeadLetter)
	}
	for name, count := range want.Queues {
		if got.Queues[name] != count {
			t.Fatalf("%s: queue %q count %d, want %d (queues got=%v want=%v)",
				label, name, got.Queues[name], count, got.Queues, want.Queues)
		}
	}
	for name, count := range want.Topics {
		if got.Topics[name] != count {
			t.Fatalf("%s: topic %q count %d, want %d", label, name, got.Topics[name], count)
		}
	}
	for name, count := range want.Streams {
		if got.Streams[name] != count {
			t.Fatalf("%s: stream %q count %d, want %d", label, name, got.Streams[name], count)
		}
	}
	for name, count := range want.DLQ {
		if got.DLQ[name] != count {
			t.Fatalf("%s: dlq %q count %d, want %d", label, name, got.DLQ[name], count)
		}
	}
}

// statsScopes returns the tenant scopes every consistency check runs under.
func statsScopes() map[string]context.Context {
	return map[string]context.Context{
		"unscoped":     context.Background(),
		"tenant-a":     WithAdminPrincipal(context.Background(), AdminPrincipal{Name: "a", Tenants: []string{"tenant-a"}}),
		"tenant-b":     WithAdminPrincipal(context.Background(), AdminPrincipal{Name: "b", Tenants: []string{"tenant-b"}}),
		"multi-tenant": WithAdminPrincipal(context.Background(), AdminPrincipal{Name: "m", Tenants: []string{"tenant-a", "tenant-b"}}),
	}
}

// TestStatsAgreesWithFullScan drives a randomized workload that touches every
// operation able to change a count, and after each step requires Stats to equal a
// full independent recount.
func TestStatsAgreesWithFullScan(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	store.SetHistoryLimit(0)
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "q.dlq"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "sink"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeclareTopic(ctx, TopicConfig{Name: "t"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeclareStream(ctx, StreamConfig{Name: "s"}); err != nil {
		t.Fatal(err)
	}

	scopes := statsScopes()
	// Coverage flags: a consistency gate is only as good as the states it
	// actually reaches. These assert the workload produced the tricky
	// combinations, so a future edit that quietly stops covering them fails here
	// rather than leaving a weaker gate behind.
	var sawRetriedLease, sawDeadLettered, sawMultipleTenants bool
	check := func(step int) {
		t.Helper()
		store.mu.RLock()
		tenants := map[string]struct{}{}
		for _, m := range store.messages {
			if m.Status == MessageLeased && m.Attempts > 0 {
				sawRetriedLease = true
			}
			if m.Status == MessageDeadLettered {
				sawDeadLettered = true
			}
			tenants[m.TenantID] = struct{}{}
		}
		store.mu.RUnlock()
		if len(tenants) > 1 {
			sawMultipleTenants = true
		}
		for scope, sctx := range scopes {
			got, err := store.Stats(sctx)
			if err != nil {
				t.Fatalf("step %d scope %s: Stats: %v", step, scope, err)
			}
			compareStats(t, fmt.Sprintf("step %d scope %s", step, scope), got, statsByScan(sctx, store))
		}
	}

	rng := rand.New(rand.NewSource(20260804))
	tenants := []string{"", "tenant-a", "tenant-b"}
	published := make([]string, 0, 512)

	for step := 0; step < 600; step++ {
		switch rng.Intn(9) {
		case 0, 1, 2, 3: // publish to a mix of destinations and tenants
			tenant := tenants[rng.Intn(len(tenants))]
			id := fmt.Sprintf("m-%d", step)
			msg := Message{ID: id, Payload: []byte("p"), TenantID: tenant}
			var dests []Destination
			switch rng.Intn(4) {
			case 0:
				dests = []Destination{{Kind: DestinationQueue, Name: "q"}}
			case 1:
				dests = []Destination{{Kind: DestinationTopic, Name: "t"}}
			case 2:
				dests = []Destination{{Kind: DestinationStream, Name: "s"}}
			default:
				dests = []Destination{{Kind: DestinationQueue, Name: "q"}, {Kind: DestinationTopic, Name: "t"}}
			}
			if _, err := store.Append(ctx, msg, dests); err == nil {
				published = append(published, id)
			}
		case 4: // lease
			msgs, err := store.Lease(ctx, LeaseRequest{
				Queue: "q", ConsumerID: "c1", ConsumerGroup: "g1", Limit: 1 + rng.Intn(3),
				VisibilityTimeout: time.Millisecond,
			})
			if err == nil {
				for _, m := range msgs {
					if rng.Intn(2) == 0 {
						_ = store.Ack(ctx, AckRequest{MessageID: m.ID, Queue: "q", ConsumerID: "c1", LeaseToken: m.LeaseToken})
					}
				}
			}
		case 5: // nack, then immediately re-lease
			// Re-leasing matters: it produces a message that is both Leased and
			// carries Attempts > 0. A count that conflated the two, or that only
			// tracked fresh leases, passes a workload that never does this.
			msgs, err := store.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c2", ConsumerGroup: "g2", Limit: 2})
			if err == nil {
				for _, m := range msgs {
					action := []NackAction{NackRetry, NackDLQ, NackReject}[rng.Intn(3)]
					// The lease token is mandatory: nackLocked rejects a missing
					// token with ErrLeaseLost, which would silently make every
					// nack a no-op and leave attempts and dead-lettering untested.
					if err := store.Nack(ctx, NackRequest{
						MessageID: m.ID, Queue: "q", ConsumerID: "c2",
						LeaseToken:       m.LeaseToken,
						Action:           action,
						IncrementAttempt: true,
					}); err != nil {
						t.Fatalf("step %d: nack: %v", step, err)
					}
					if action == NackRetry {
						// Put the queue straight back in flight so later checks see
						// retried messages in the leased state.
						_, _ = store.Lease(ctx, LeaseRequest{
							Queue: "q", ConsumerID: "c3", ConsumerGroup: "g3", Limit: 2,
						})
					}
				}
			}
		case 6: // lease expiry sweep
			_, _ = store.SweepLeases(ctx, time.Now().Add(time.Hour))
		case 7: // move or purge
			if rng.Intn(2) == 0 && len(published) > 0 {
				id := published[rng.Intn(len(published))]
				_, _ = store.Move(ctx, MoveRequest{
					SourceType: DestinationQueue, Source: "q",
					DestinationType: DestinationQueue, Destination: "sink",
					MessageIDs: []string{id}, Mode: MoveRemoveSource,
				})
			} else {
				_, _ = store.PurgeQueue(ctx, "sink")
			}
		case 8: // stream offset commit and read
			msgs, err := store.ReadStream(ctx, StreamReadRequest{Stream: "s", Limit: 5})
			if err == nil && len(msgs) > 0 {
				_ = store.CommitOffset(ctx, OffsetCommit{Stream: "s", ConsumerGroup: "g1", Offset: msgs[len(msgs)-1].Offset})
			}
		}
		check(step)
	}

	if !sawRetriedLease {
		t.Error("workload never produced a leased message with prior attempts; retried-lease accounting is uncovered")
	}
	if !sawDeadLettered {
		t.Error("workload never produced a dead-lettered message; dead-letter accounting is uncovered")
	}
	if !sawMultipleTenants {
		t.Error("workload never held more than one tenant; per-tenant scoping is uncovered")
	}
}

// TestStatsCountsTrackStoreSize documents the remaining cost: Stats is linear in
// the number of retained messages, which is why a probe on a large store blocks
// publishers for as long as the scan takes.
func TestStatsCountsTrackStoreSize(t *testing.T) {
	ctx := context.Background()
	for _, total := range []int{1000, 8000} {
		store := NewMemoryStore()
		if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < total; i++ {
			if _, err := store.Append(ctx, Message{
				ID: fmt.Sprintf("m-%d", i), Queue: "q", Payload: []byte("p"),
			}, []Destination{{Kind: DestinationQueue, Name: "q"}}); err != nil {
				t.Fatal(err)
			}
		}
		st, err := store.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if st.Messages != total {
			t.Fatalf("Messages = %d, want %d", st.Messages, total)
		}
		if st.Queues["q"] != total {
			t.Fatalf("Queues[q] = %d, want %d", st.Queues["q"], total)
		}
		store.Close()
	}
}

// BenchmarkStatsCostByStoreSize quantifies the remaining gap. Stats holds the
// store read lock for the whole scan and is linear in retained messages, so a
// probe on a large store blocks every publisher for the duration.
//
// This is the measurement a counter refactor has to move. The harness above is
// the correctness gate for that change; this is the reason to do it.
func BenchmarkStatsCostByStoreSize(b *testing.B) {
	ctx := context.Background()
	for _, total := range []int{1000, 10000, 100000} {
		store := NewMemoryStore()
		if err := store.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
			b.Fatal(err)
		}
		if err := store.DeclareTopic(ctx, TopicConfig{Name: "t"}); err != nil {
			b.Fatal(err)
		}
		if err := store.DeclareStream(ctx, StreamConfig{Name: "s"}); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < total; i++ {
			switch i % 3 {
			case 0:
				if _, err := store.Append(ctx, Message{
					ID: fmt.Sprintf("q-%d", i), Queue: "q", Payload: []byte("p"),
				}, []Destination{{Kind: DestinationQueue, Name: "q"}}); err != nil {
					b.Fatal(err)
				}
			case 1:
				if _, err := store.Append(ctx, Message{
					ID: fmt.Sprintf("t-%d", i), Payload: []byte("p"),
				}, []Destination{{Kind: DestinationTopic, Name: "t"}}); err != nil {
					b.Fatal(err)
				}
			default:
				if _, err := store.Append(ctx, Message{
					ID: fmt.Sprintf("s-%d", i), Payload: []byte("p"),
				}, []Destination{{Kind: DestinationStream, Name: "s"}}); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.Run(fmt.Sprintf("messages=%d", total), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := store.Stats(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.StopTimer()
		store.Close()
	}
}
