package broker

import (
	"context"
	"errors"
	"testing"
	"time"
)

type storeWithoutDistributedCapabilities struct{ Store }

func TestDistributedRejectsNonDeterministicStore(t *testing.T) {
	_, err := NewDistributed(Config{Store: &storeWithoutDistributedCapabilities{Store: NewMemoryStore()}}, DistributedConfig{UseEtcdRaft: true, WALDir: t.TempDir()})
	if !errors.Is(err, ErrUnsupportedMode) {
		t.Fatalf("expected unsupported store error, got %v", err)
	}
}

func TestDistributedBrokerWithEtcdRaft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := Config{
		NodeID: "node-test-1",
		Store:  NewMemoryStore(),
	}

	distCfg := DistributedConfig{
		UseEtcdRaft:       true,
		WALDir:            t.TempDir(),
		RaftNodeID:        1,
		ReplicationFactor: 1,
		SubmitTimeout:     2 * time.Second,
	}

	db, err := NewDistributed(cfg, distCfg)
	if err != nil {
		t.Fatalf("failed to create distributed broker: %v", err)
	}

	if err := db.Start(ctx); err != nil {
		t.Fatalf("failed to start distributed broker: %v", err)
	}
	defer db.Stop(ctx)

	// Wait for single node to become leader
	deadline := time.Now().Add(3 * time.Second)
	for !db.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("node did not become leader; leader=%s, state=%s", db.Leader(), db.RaftState())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 1. Declare queue replicated via etcd/raft
	err = db.Store().DeclareQueue(ctx, QueueConfig{
		Name:    "orders",
		AckMode: AckManual,
	})
	if err != nil {
		t.Fatalf("declare queue failed: %v", err)
	}

	// 2. Publish message with ConfirmReplicated
	msg := Message{
		ID:      "order-101",
		Queue:   "orders",
		Payload: []byte("replicated payload"),
	}

	msgs, err := db.Store().Append(ctx, msg, []Destination{{Kind: DestinationQueue, Name: "orders"}})
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if len(msgs) == 0 || msgs[0].ID != "order-101" {
		t.Fatalf("unexpected append result: %+v", msgs)
	}

	// Verify message in local store
	got, ok, err := db.Store().Get(ctx, "order-101")
	if err != nil || !ok {
		t.Fatalf("get message failed: ok=%t, err=%v", ok, err)
	}
	if string(got.Payload) != "replicated payload" {
		t.Fatalf("payload mismatch: %s", string(got.Payload))
	}

	// 3. Ack message replicated via etcd/raft
	err = db.Store().Ack(ctx, AckRequest{
		MessageID: "order-101",
		Queue:     "orders",
	})
	if err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	// 4. Check cluster stats
	stats := db.ClusterStats()
	if stats["consensus_engine"] != "etcd/raft" {
		t.Fatalf("expected consensus_engine etcd/raft, got %v", stats["consensus_engine"])
	}
	if stats["is_leader"] != true {
		t.Fatalf("expected is_leader true in stats")
	}
}

func TestDistributedAtomicPublishAndLeaseExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := NewDistributed(Config{NodeID: "node-atomic", Store: NewMemoryStore()}, DistributedConfig{
		UseEtcdRaft:   true,
		WALDir:        t.TempDir(),
		RaftNodeID:    1,
		SubmitTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer db.Stop(context.Background())
	deadline := time.Now().Add(3 * time.Second)
	for !db.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !db.IsLeader() {
		t.Fatal("single-node raft did not elect a leader")
	}
	for _, queue := range []string{"atomic-a", "atomic-b"} {
		if err := db.Store().DeclareQueue(ctx, QueueConfig{Name: queue}); err != nil {
			t.Fatal(err)
		}
	}
	sequence := uint64(1)
	stored, err := db.Store().(AtomicStore).AppendAtomic(ctx, []AppendEntry{{
		Message:      Message{ID: "atomic-1", ProducerID: "producer-1", ProducerEpoch: 1, ProducerSequence: &sequence},
		Destinations: []Destination{{Kind: DestinationQueue, Name: "atomic-a"}, {Kind: DestinationQueue, Name: "atomic-b"}},
	}})
	if err != nil {
		t.Fatalf("atomic append: %v", err)
	}
	if len(stored) != 2 || stored[0].ID != "atomic-1" || stored[1].ID != "atomic-1:copy:1" {
		t.Fatalf("unexpected stored messages: %+v", stored)
	}
	leased, err := db.Store().Lease(ctx, LeaseRequest{Queue: "atomic-a", ConsumerID: "worker-1", Limit: 1, VisibilityTimeout: time.Millisecond})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: messages=%+v err=%v", leased, err)
	}
	time.Sleep(2 * time.Millisecond)
	n, err := db.Store().SweepLeases(ctx, time.Now())
	if err != nil || n != 1 {
		t.Fatalf("sweep: count=%d err=%v", n, err)
	}
	message, ok, err := db.Store().Get(ctx, "atomic-1")
	if err != nil || !ok || message.Status != MessageQueued || message.Redeliveries != 1 {
		t.Fatalf("unexpected post-sweep message: ok=%v message=%+v err=%v", ok, message, err)
	}
}

func TestDistributedBrokerRebuildsStateFromDurableRaftLog(t *testing.T) {
	dir := t.TempDir()
	newBroker := func() *DistributedBroker {
		db, err := NewDistributed(Config{NodeID: "restart-node", Store: NewMemoryStore()}, DistributedConfig{
			UseEtcdRaft: true, WALDir: dir, RaftNodeID: 1, SubmitTimeout: 2 * time.Second,
			SnapshotEntries: 1, SnapshotCatchUpEntries: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first := newBroker()
	if err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !first.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := first.Store().DeclareQueue(ctx, QueueConfig{Name: "restart-q"}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Store().Append(ctx, Message{ID: "restart-message", Payload: []byte("durable")}, []Destination{{Kind: DestinationQueue, Name: "restart-q"}}); err != nil {
		t.Fatal(err)
	}
	if err := first.Store().DeclareQueue(ctx, QueueConfig{Name: "restart-copy"}); err != nil {
		t.Fatal(err)
	}
	moveResult, err := first.Store().Move(ctx, MoveRequest{
		SourceType: DestinationQueue, Source: "restart-q",
		DestinationType: DestinationQueue, Destination: "restart-copy",
		MessageIDs: []string{"restart-message"}, Mode: MoveCopy,
	})
	if err != nil || moveResult.Copied != 1 {
		t.Fatalf("replicated move: result=%+v err=%v", moveResult, err)
	}
	firstCopies, err := first.Store().ListQueue(ctx, "restart-copy", 10)
	if err != nil || len(firstCopies) != 1 {
		t.Fatalf("list copied messages: count=%d err=%v", len(firstCopies), err)
	}
	copyID := firstCopies[0].ID
	if err := first.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	second := newBroker()
	if err := second.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer second.Stop(context.Background())
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		message, ok, err := second.Store().Get(ctx, "restart-message")
		if err == nil && ok {
			if string(message.Payload) != "durable" || message.Queue != "restart-q" {
				t.Fatalf("unexpected recovered message: %+v", message)
			}
			copies, listErr := second.Store().ListQueue(ctx, "restart-copy", 10)
			if listErr != nil || len(copies) != 1 || copies[0].ID != copyID {
				t.Fatalf("unexpected recovered move: copies=%+v err=%v", copies, listErr)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("committed message was not rebuilt from durable raft log")
}
