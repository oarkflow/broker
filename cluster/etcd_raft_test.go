package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

type testStateMachine struct {
	mu      sync.Mutex
	applied []*LogEntry
}

func (sm *testStateMachine) Apply(entry *LogEntry) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.applied = append(sm.applied, entry)
	return nil
}

func (sm *testStateMachine) Snapshot() ([]byte, error) {
	return nil, nil
}

func (sm *testStateMachine) Restore(data []byte) error {
	return nil
}

func (sm *testStateMachine) Count() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.applied)
}

func TestEtcdRaftControllerLifecycle(t *testing.T) {
	sm := &testStateMachine{}
	ctrl, err := NewRaftController(RaftControllerConfig{
		ID:           1,
		NodeID:       "node-1",
		Peers:        map[uint64]string{1: "node-1"},
		ElectionTick: 10,
		HeartbeatTick: 1,
		TickInterval: 10 * time.Millisecond,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatalf("failed to create RaftController: %v", err)
	}

	if err := ctrl.Start(); err != nil {
		t.Fatalf("failed to start controller: %v", err)
	}
	defer ctrl.Stop()

	// Single-node campaigns to become leader
	if err := ctrl.Campaign(); err != nil {
		t.Fatalf("campaign failed: %v", err)
	}

	// Wait until leader is established
	deadline := time.Now().Add(2 * time.Second)
	for !ctrl.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("node failed to become leader in time; leaderID=%d", ctrl.LeaderID())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Propose a command
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := []byte("hello-etcd-raft")
	if err := ctrl.Propose(ctx, cmd); err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Verify command applied to state machine
	deadline = time.Now().Add(2 * time.Second)
	for sm.Count() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("state machine did not receive applied command")
		}
		time.Sleep(10 * time.Millisecond)
	}

	sm.mu.Lock()
	entry := sm.applied[0]
	sm.mu.Unlock()

	if string(entry.Data) != "hello-etcd-raft" {
		t.Fatalf("expected payload 'hello-etcd-raft', got '%s'", string(entry.Data))
	}
}

func TestEtcdRaftController3NodeCluster(t *testing.T) {
	const numNodes = 3
	transports := make([]*MemoryTransport, numNodes)
	controllers := make([]*RaftController, numNodes)
	sms := make([]*testStateMachine, numNodes)

	peers := make(map[uint64]string, numNodes)
	for i := 1; i <= numNodes; i++ {
		peers[uint64(i)] = fmt.Sprintf("node-%d", i)
	}

	registry := make(map[string]*MemoryTransport)
	for i := 0; i < numNodes; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		transports[i] = NewMemoryTransport(id)
		registry[id] = transports[i]
	}
	for _, tr := range transports {
		tr.Register(registry)
	}

	for i := 0; i < numNodes; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		sms[i] = &testStateMachine{}
		ctrl, err := NewRaftController(RaftControllerConfig{
			ID:           uint64(i + 1),
			NodeID:       nodeID,
			Peers:        peers,
			ElectionTick: 10,
			HeartbeatTick: 1,
			TickInterval: 10 * time.Millisecond,
			Transport:    transports[i],
			StateMachine: sms[i],
		})
		if err != nil {
			t.Fatalf("failed to create RaftController %d: %v", i+1, err)
		}
		controllers[i] = ctrl

		if err := ctrl.RegisterTransportHandler(nodeID); err != nil {
			t.Fatalf("failed to register transport: %v", err)
		}
	}

	for _, c := range controllers {
		if err := c.Start(); err != nil {
			t.Fatalf("failed to start controller: %v", err)
		}
		defer c.Stop()
	}

	// Trigger election from node 1
	_ = controllers[0].Campaign()

	// Wait for any leader to be elected
	var leader *RaftController
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range controllers {
			if c.IsLeader() {
				leader = c
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if leader == nil {
		t.Fatalf("no leader elected among 3 nodes within deadline")
	}

	// Propose 3 messages from the leader
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for k := 1; k <= 3; k++ {
		payload := []byte(fmt.Sprintf("msg-%d", k))
		if err := leader.Propose(ctx, payload); err != nil {
			t.Fatalf("failed to propose %s: %v", string(payload), err)
		}
	}

	// Verify all state machines replicated all 3 messages
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		allReplicated := true
		for _, sm := range sms {
			if sm.Count() < 3 {
				allReplicated = false
				break
			}
		}
		if allReplicated {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	for i, sm := range sms {
		if sm.Count() < 3 {
			t.Fatalf("node %d replicated %d entries, want 3", i+1, sm.Count())
		}
	}
}

func TestEtcdRaftControllerConfChange(t *testing.T) {
	sm := &testStateMachine{}
	ctrl, err := NewRaftController(RaftControllerConfig{
		ID:           1,
		NodeID:       "node-1",
		Peers:        map[uint64]string{1: "node-1"},
		ElectionTick: 10,
		HeartbeatTick: 1,
		TickInterval: 10 * time.Millisecond,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatalf("failed to create RaftController: %v", err)
	}

	if err := ctrl.Start(); err != nil {
		t.Fatalf("failed to start controller: %v", err)
	}
	defer ctrl.Stop()

	_ = ctrl.Campaign()
	deadline := time.Now().Add(2 * time.Second)
	for !ctrl.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Propose adding node 2
	ccType := raftpb.ConfChangeAddNode
	nodeID := uint64(2)
	cc := raftpb.ConfChange{
		Type:   &ccType,
		NodeId: &nodeID,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := ctrl.ProposeConfChange(ctx, &cc); err != nil {
		t.Fatalf("failed to propose conf change: %v", err)
	}
}
