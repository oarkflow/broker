package broker

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestLeaseSweepUsesIndexAndPrunes(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := s.Append(ctx, Message{ID: "m" + strconv.Itoa(i)}, []Destination{{Kind: DestinationQueue, Name: "q"}}); err != nil {
			t.Fatal(err)
		}
	}
	leased, err := s.Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", Limit: 10, VisibilityTimeout: time.Millisecond})
	if err != nil || len(leased) != 10 {
		t.Fatalf("lease: %d %v", len(leased), err)
	}
	// Ack three; they must not be re-queued by the sweep.
	for _, m := range leased[:3] {
		if err := s.Ack(ctx, AckRequest{MessageID: m.ID, Queue: "q", LeaseToken: m.LeaseToken, ConsumerID: "c"}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(5 * time.Millisecond)
	n, err := s.SweepLeases(ctx, time.Now())
	if err != nil || n != 7 {
		t.Fatalf("sweep=%d err=%v, want 7", n, err)
	}
	if got := len(s.leased); got != 0 {
		t.Fatalf("lease index not pruned: %d entries", got)
	}
	if n, _ := s.SweepLeases(ctx, time.Now()); n != 0 {
		t.Fatalf("second sweep re-expired %d messages", n)
	}
}

func TestLeaseSweepFullScanCatchesUnindexedLease(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, Message{ID: "x"}, []Destination{{Kind: DestinationQueue, Name: "q"}}); err != nil {
		t.Fatal(err)
	}
	// Simulate a status transition that bypassed the index.
	s.mu.Lock()
	m := s.msgLocked("x")
	m.Status = MessageLeased
	m.LeaseUntil = time.Now().Add(-time.Second)
	s.mu.Unlock()
	total := 0
	for i := 0; i < fullSweepEvery; i++ {
		n, _ := s.SweepLeases(ctx, time.Now())
		total += n
	}
	if total != 1 {
		t.Fatalf("full-scan reconciliation expired %d messages, want 1", total)
	}
}
