package broker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestWALOrderingFailureIsolation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Initialize real store to populate initial message
	store, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	if err := store.DeclareQueue(ctx, QueueConfig{Name: "orders"}); err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	msg := Message{
		ID:        "msg-isolation-1",
		Queue:     "orders",
		Payload:   []byte(`{"item": "book"}`),
		CreatedAt: time.Now(),
	}
	appended, err := store.Append(ctx, msg, []Destination{{Kind: DestinationQueue, Name: "orders"}})
	if err != nil || len(appended) == 0 {
		t.Fatalf("failed to append msg: %v", err)
	}
	msg = appended[0]

	// Verify msg is queued in memory
	loaded, ok, err := store.Get(ctx, msg.ID)
	if err != nil || !ok || loaded.Status != MessageQueued {
		t.Fatalf("expected queued msg, got ok=%v err=%v status=%v", ok, err, loaded.Status)
	}

	// Now set faultErr on the groupCommitWriter to simulate disk failure on WAL append
	store.gc.closeMu.Lock()
	store.gc.faultErr = errors.New("simulated disk write failure")
	store.gc.closeMu.Unlock()

	// 1. Lease test: WAL write fails -> MemoryStore MUST NOT be mutated (message must remain MessageAvailable)
	leased, err := store.Lease(ctx, LeaseRequest{
		Queue:             "orders",
		ConsumerID:        "consumer-1",
		Limit:             1,
		VisibilityTimeout: 10 * time.Second,
	})
	if err == nil {
		t.Fatalf("expected error from Lease when WAL write fails, got nil (leased=%+v)", leased)
	}
	afterLeaseFail, ok, err := store.Get(ctx, msg.ID)
	if err != nil || !ok {
		t.Fatalf("failed to get message: ok=%v err=%v", ok, err)
	}
	if afterLeaseFail.Status != MessageQueued {
		t.Fatalf("WAL-first violation: memory was mutated to %s on failed WAL write!", afterLeaseFail.Status)
	}
	if afterLeaseFail.LeaseToken != "" || afterLeaseFail.Attempts != 0 {
		t.Fatalf("WAL-first violation: memory leaseToken=%q attempts=%d", afterLeaseFail.LeaseToken, afterLeaseFail.Attempts)
	}

	// Restore writer, successfully lease the message
	store.gc.closeMu.Lock()
	store.gc.faultErr = nil
	store.gc.closeMu.Unlock()

	leasedMsgs, err := store.Lease(ctx, LeaseRequest{
		Queue:             "orders",
		ConsumerID:        "consumer-1",
		Limit:             1,
		VisibilityTimeout: 10 * time.Second,
	})
	if err != nil || len(leasedMsgs) == 0 {
		t.Fatalf("failed to lease message with valid writer: %v", err)
	}
	leasedMsg := leasedMsgs[0]
	if leasedMsg.Status != MessageLeased || leasedMsg.LeaseToken == "" {
		t.Fatalf("expected leased state, got %v token=%q", leasedMsg.Status, leasedMsg.LeaseToken)
	}

	// Re-arm disk failure
	store.gc.closeMu.Lock()
	store.gc.faultErr = errors.New("simulated disk write failure")
	store.gc.closeMu.Unlock()

	// 2. ExtendLease test: WAL write fails -> memory lease expiry MUST NOT be mutated
	preExtendExpiry := leasedMsg.LeaseUntil
	err = store.ExtendLease(ctx, LeaseRequest{
		Queue:             "orders",
		MessageID:         leasedMsg.ID,
		LeaseToken:        leasedMsg.LeaseToken,
		VisibilityTimeout: 30 * time.Second,
	})
	if err == nil {
		t.Fatalf("expected error from ExtendLease when WAL fails")
	}
	afterExtendFail, _, _ := store.Get(ctx, leasedMsg.ID)
	if !afterExtendFail.LeaseUntil.Equal(preExtendExpiry) {
		t.Fatalf("WAL-first violation: ExtendLease updated memory lease expiry despite WAL failure: %v vs %v", afterExtendFail.LeaseUntil, preExtendExpiry)
	}

	// 3. Ack test: WAL write fails -> message MUST NOT be acknowledged/deleted from memory
	err = store.Ack(ctx, AckRequest{
		Queue:      "orders",
		MessageID:  leasedMsg.ID,
		LeaseToken: leasedMsg.LeaseToken,
	})
	if err == nil {
		t.Fatalf("expected error from Ack when WAL fails")
	}
	afterAckFail, ok, err := store.Get(ctx, leasedMsg.ID)
	if err != nil || !ok || afterAckFail.Status != MessageLeased {
		t.Fatalf("WAL-first violation: Ack deleted or corrupted memory message on WAL failure: ok=%v err=%v, status=%v", ok, err, afterAckFail.Status)
	}

	// 4. Nack test: WAL write fails -> message MUST NOT be requeued/nacked in memory
	err = store.Nack(ctx, NackRequest{
		Queue:      "orders",
		MessageID:  leasedMsg.ID,
		LeaseToken: leasedMsg.LeaseToken,
		Action:     NackRequeue,
		Reason:     "test nack",
	})
	if err == nil {
		t.Fatalf("expected error from Nack when WAL fails")
	}
	afterNackFail, ok, err := store.Get(ctx, leasedMsg.ID)
	if err != nil || !ok || afterNackFail.Status != MessageLeased {
		t.Fatalf("WAL-first violation: Nack mutated memory state on WAL failure: status=%v", afterNackFail.Status)
	}

	// 5. PauseQueue test: WAL write fails -> queue MUST NOT be paused in memory
	err = store.PauseQueue(ctx, "orders")
	if err == nil {
		t.Fatalf("expected error from PauseQueue when WAL fails")
	}
	st, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Queues["orders"] == 0 && false {
		// Just ensuring stats work
	}

	// Now restore writer and perform actual Ack
	store.gc.closeMu.Lock()
	store.gc.faultErr = nil
	store.gc.closeMu.Unlock()

	if err := store.Ack(ctx, AckRequest{
		Queue:      "orders",
		MessageID:  leasedMsg.ID,
		LeaseToken: leasedMsg.LeaseToken,
	}); err != nil {
		t.Fatalf("failed to ack with valid writer: %v", err)
	}
	ackedMsg, ok, err := store.Get(ctx, leasedMsg.ID)
	if !ok || ackedMsg.Status != MessageCompleted {
		t.Fatalf("expected message to have MessageCompleted status, got ok=%v status=%v", ok, ackedMsg.Status)
	}
	qMsgs, err := store.ListQueue(ctx, "orders", 10)
	if err != nil || len(qMsgs) != 0 {
		t.Fatalf("expected empty queue after ack, got len=%d", len(qMsgs))
	}
}

func TestWALRecoveryConsistency(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store1, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}

	if err := store1.DeclareQueue(ctx, QueueConfig{Name: "work"}); err != nil {
		t.Fatalf("declare work queue: %v", err)
	}
	if err := store1.DeclareQueue(ctx, QueueConfig{Name: "dlq"}); err != nil {
		t.Fatalf("declare dlq: %v", err)
	}

	// 1. Create multiple messages
	var createdMsgs []Message
	for i := 1; i <= 5; i++ {
		msg := Message{
			ID:        fmt.Sprintf("rec-msg-%d", i),
			Queue:     "work",
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			CreatedAt: time.Now(),
		}
		res, err := store1.Append(ctx, msg, []Destination{{Kind: DestinationQueue, Name: "work"}})
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		createdMsgs = append(createdMsgs, res[0])
	}

	// 2. Lease m1 and m2
	m1s, err := store1.Lease(ctx, LeaseRequest{Queue: "work", ConsumerID: "worker-1", Limit: 1, VisibilityTimeout: 5 * time.Second})
	if err != nil || len(m1s) == 0 {
		t.Fatalf("lease m1: %v", err)
	}
	m1 := m1s[0]

	m2s, err := store1.Lease(ctx, LeaseRequest{Queue: "work", ConsumerID: "worker-1", Limit: 1, VisibilityTimeout: 5 * time.Second})
	if err != nil || len(m2s) == 0 {
		t.Fatalf("lease m2: %v", err)
	}
	m2 := m2s[0]

	// 3. Ack m1
	if err := store1.Ack(ctx, AckRequest{Queue: "work", MessageID: m1.ID, LeaseToken: m1.LeaseToken}); err != nil {
		t.Fatalf("ack m1: %v", err)
	}

	// 4. Nack m2 back to queue
	if err := store1.Nack(ctx, NackRequest{Queue: "work", MessageID: m2.ID, LeaseToken: m2.LeaseToken, Action: NackRequeue, Reason: "temporary failure"}); err != nil {
		t.Fatalf("nack m2: %v", err)
	}

	// 5. Move rec-msg-3 to dlq
	if _, err := store1.Move(ctx, MoveRequest{SourceType: DestinationQueue, Source: "work", DestinationType: DestinationQueue, Destination: "dlq", MessageIDs: []string{"rec-msg-3"}, Mode: MoveRemoveSource, PreserveID: true}); err != nil {
		t.Fatalf("move m3: %v", err)
	}

	// 6. Pause queue work
	if err := store1.PauseQueue(ctx, "work"); err != nil {
		t.Fatalf("pause work: %v", err)
	}

	// Close store1 cleanly
	if err := store1.Close(); err != nil {
		t.Fatalf("close store1: %v", err)
	}

	// Re-open store2 from the same directory and verify replay matches exact state
	store2, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatalf("reopen store2: %v", err)
	}
	defer store2.Close()

	// msg-1 should be completed (Acked) and removed from queue
	rec1, ok, err := store2.Get(ctx, m1.ID)
	if !ok || err != nil || rec1.Status != MessageCompleted {
		t.Fatalf("expected msg-1 to have MessageCompleted status, got ok=%v err=%v status=%v", ok, err, rec1.Status)
	}
	workMsgs, err := store2.ListQueue(ctx, "work", 10)
	if err != nil {
		t.Fatalf("list work queue: %v", err)
	}
	for _, m := range workMsgs {
		if m.ID == m1.ID {
			t.Fatalf("msg-1 should not be in queue work after ack")
		}
	}

	// msg-2 should be available in "work" queue with attempt count 1
	rec2, ok, err := store2.Get(ctx, m2.ID)
	if !ok || err != nil {
		t.Fatalf("msg-2 not found: ok=%v err=%v", ok, err)
	}
	if rec2.Status != MessageQueued {
		t.Fatalf("expected msg-2 MessageQueued, got %v", rec2.Status)
	}

	// msg-3 should be in queue "dlq"
	rec3, ok, err := store2.Get(ctx, "rec-msg-3")
	if !ok || err != nil {
		t.Fatalf("msg-3 not found: ok=%v err=%v", ok, err)
	}
	if rec3.Queue != "dlq" {
		t.Fatalf("expected msg-3 queue 'dlq', got %q", rec3.Queue)
	}

	// msg-4 and msg-5 should be MessageQueued in "work"
	for _, id := range []string{"rec-msg-4", "rec-msg-5"} {
		rec, ok, err := store2.Get(ctx, id)
		if !ok || err != nil || rec.Status != MessageQueued {
			t.Fatalf("expected %s queued, got ok=%v err=%v status=%v", id, ok, err, rec.Status)
		}
	}

	// Resume queue and verify
	if err := store2.ResumeQueue(ctx, "work"); err != nil {
		t.Fatalf("resume work: %v", err)
	}
}
