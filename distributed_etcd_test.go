package broker

import (
	"context"
	"testing"
	"time"
)

func TestDistributedBrokerWithEtcdRaft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := Config{
		NodeID: "node-test-1",
		Store:  NewMemoryStore(),
	}

	distCfg := DistributedConfig{
		UseEtcdRaft:       true,
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
