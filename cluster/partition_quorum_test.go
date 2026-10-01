package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPartitionGroupLifecycleAndISR(t *testing.T) {
	sm := &testStateMachine{}
	ctrl, err := NewRaftController(RaftControllerConfig{
		ID:            1,
		NodeID:        "node-1",
		Peers:         map[uint64]string{1: "node-1"},
		ElectionTick:  10,
		HeartbeatTick: 1,
		TickInterval:  10 * time.Millisecond,
		StateMachine:  sm,
	})
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := ctrl.Start(); err != nil {
		t.Fatalf("start controller: %v", err)
	}
	defer ctrl.Stop()

	_ = ctrl.Campaign()
	deadline := time.Now().Add(2 * time.Second)
	for !ctrl.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	pg, err := NewPartitionGroup(PartitionGroupConfig{
		ID: PartitionGroupID{
			Topic:     "orders",
			Partition: 0,
		},
		Controller:        ctrl,
		ReplicationFactor: 3,
		MinISR:            2,
		MaxReplicationLag: 100,
		Replicas:          []string{"node-1", "node-2", "node-3"},
	})
	if err != nil {
		t.Fatalf("new partition group: %v", err)
	}
	defer pg.Close()

	if !pg.IsLeader() {
		t.Fatalf("expected partition group to report is leader")
	}

	// 1. Propose with current epoch
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := pg.Propose(ctx, pg.LeaderEpoch(), []byte("order-1")); err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// 2. Propose with stale epoch should be rejected
	pg.IncrementEpoch() // Epoch is now 2
	err = pg.Propose(ctx, 1, []byte("order-stale"))
	if !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("expected ErrStaleEpoch, got: %v", err)
	}

	// 3. ISR updates and lag threshold
	// Leader acknowledges index 500
	pg.UpdateReplicaProgress("node-1", 500)
	// Node-2 acknowledges index 450 (lag 50 <= 100 maxLag) -> remains in ISR
	pg.UpdateReplicaProgress("node-2", 450)
	// Node-3 acknowledges index 300 (lag 200 > 100 maxLag) -> dropped from ISR
	pg.UpdateReplicaProgress("node-3", 300)

	isr := pg.ISR()
	if len(isr) != 2 {
		t.Fatalf("expected 2 ISR members, got %d: %v", len(isr), isr)
	}

	// 4. If Node-2 also falls behind (lag 300 > 100) -> ISR drops to 1, which is < MinISR (2)
	pg.UpdateReplicaProgress("node-1", 800)
	pg.UpdateReplicaProgress("node-2", 450) // lag 350
	isr = pg.ISR()
	if len(isr) != 1 {
		t.Fatalf("expected 1 ISR member, got %d: %v", len(isr), isr)
	}

	// Propose should now fail with ErrQuorumNotMet
	err = pg.Propose(ctx, pg.LeaderEpoch(), []byte("order-unmet"))
	if !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("expected ErrQuorumNotMet, got: %v", err)
	}

	// Catch up node-2 -> ISR back to 2 -> propose succeeds
	pg.UpdateReplicaProgress("node-2", 800)
	if err := pg.Propose(ctx, pg.LeaderEpoch(), []byte("order-recovered")); err != nil {
		t.Fatalf("expected propose to succeed after catchup, got: %v", err)
	}
}

func TestControllerQuorumMetadataReplication(t *testing.T) {
	meta := NewClusterMetadata()
	sm := NewMetadataStateMachine(meta)

	ctrl, err := NewRaftController(RaftControllerConfig{
		ID:            1,
		NodeID:        "ctrl-1",
		Peers:         map[uint64]string{1: "ctrl-1"},
		ElectionTick:  10,
		HeartbeatTick: 1,
		TickInterval:  10 * time.Millisecond,
		StateMachine:  sm,
	})
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := ctrl.Start(); err != nil {
		t.Fatalf("start controller: %v", err)
	}
	defer ctrl.Stop()

	_ = ctrl.Campaign()
	deadline := time.Now().Add(2 * time.Second)
	for !ctrl.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	quorum, err := NewControllerQuorum(ControllerQuorumConfig{
		Controller: ctrl,
		Metadata:   meta,
	})
	if err != nil {
		t.Fatalf("new controller quorum: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Register nodes
	if err := quorum.RegisterNode(ctx, &NodeInfo{
		ID:      "broker-1",
		Address: "127.0.0.1:9092",
		State:   NodeActive,
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	nodes := quorum.GetNodes()
	if len(nodes) != 1 || nodes[0].ID != "broker-1" {
		t.Fatalf("unexpected nodes: %v", nodes)
	}

	// 2. Assign partition
	if err := quorum.AssignPartition(ctx, "payments", 0, []string{"broker-1", "broker-2", "broker-3"}, "broker-1"); err != nil {
		t.Fatalf("assign partition: %v", err)
	}

	pm, err := quorum.GetPartition("payments", 0)
	if err != nil {
		t.Fatalf("get partition: %v", err)
	}
	if pm.Leader != "broker-1" || pm.LeaderEpoch != 1 || len(pm.Replicas) != 3 {
		t.Fatalf("unexpected partition metadata: %+v", pm)
	}

	// 3. Advance epoch
	if err := quorum.AdvanceEpoch(ctx, "payments", 0, "broker-2"); err != nil {
		t.Fatalf("advance epoch: %v", err)
	}

	pm, err = quorum.GetPartition("payments", 0)
	if err != nil {
		t.Fatalf("get partition after epoch advance: %v", err)
	}
	if pm.Leader != "broker-2" || pm.LeaderEpoch != 2 {
		t.Fatalf("expected leader broker-2 with epoch 2, got %+v", pm)
	}

	// 4. Test Snapshot and Restore
	snap, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	newMeta := NewClusterMetadata()
	newSM := NewMetadataStateMachine(newMeta)
	if err := newSM.Restore(snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	if len(newMeta.Nodes) != 1 || len(newMeta.Partitions) != 1 {
		t.Fatalf("restored metadata mismatch: %+v", newMeta)
	}
}
