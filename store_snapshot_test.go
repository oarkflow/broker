package broker

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreSnapshotRestoreRedeliversLeased(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	if err := st.DeclareQueue(ctx, QueueConfig{Name: "jobs", VisibilityTimeout: time.Minute, RetryPolicy: DefaultRetryPolicy()}); err != nil {
		t.Fatal(err)
	}
	_, err := st.Append(ctx, Message{ID: "m1", Payload: []byte("hello")}, []Destination{{Kind: DestinationQueue, Name: "jobs"}})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := st.Lease(ctx, LeaseRequest{Queue: "jobs", ConsumerID: "c1", VisibilityTimeout: time.Hour})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease got=%d err=%v", len(leased), err)
	}
	snap := st.Snapshot()
	recovered := NewMemoryStore()
	recovered.Restore(snap)
	msg, ok, err := recovered.Get(ctx, "m1")
	if err != nil || !ok {
		t.Fatalf("get recovered ok=%v err=%v", ok, err)
	}
	if msg.Status != MessageQueued {
		t.Fatalf("status after restore=%s, want queued", msg.Status)
	}
	if msg.LeaseToken != "" || !msg.LeaseUntil.IsZero() || msg.ConsumerID != "" {
		t.Fatalf("restore left stale lease token=%q until=%v consumer=%q", msg.LeaseToken, msg.LeaseUntil, msg.ConsumerID)
	}
	again, err := recovered.Lease(ctx, LeaseRequest{Queue: "jobs", ConsumerID: "c2", VisibilityTimeout: time.Minute})
	if err != nil || len(again) != 1 || again[0].ID != "m1" {
		t.Fatalf("redelivery got=%v err=%v", again, err)
	}
}

func TestMemoryStoreExactSnapshotPreservesQueueAndLeaseState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "jobs"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, Message{ID: "high", Priority: 10}, []Destination{{Kind: DestinationQueue, Name: "jobs"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, Message{ID: "next"}, []Destination{{Kind: DestinationQueue, Name: "jobs"}}); err != nil {
		t.Fatal(err)
	}
	leased, err := store.Lease(ctx, LeaseRequest{Queue: "jobs", ConsumerID: "worker", Limit: 1, VisibilityTimeout: time.Hour})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: messages=%+v err=%v", leased, err)
	}
	restored := NewMemoryStore()
	restored.RestoreExact(store.Snapshot())
	message, ok, err := restored.Get(ctx, leased[0].ID)
	if err != nil || !ok || message.Status != MessageLeased || message.LeaseToken != leased[0].LeaseToken {
		t.Fatalf("exact restore changed lease: ok=%v message=%+v err=%v", ok, message, err)
	}
	listed, err := restored.ListQueue(ctx, "jobs", 10)
	if err != nil || len(listed) != 2 {
		t.Fatalf("restored queue metadata lost messages: count=%d err=%v", len(listed), err)
	}
}
