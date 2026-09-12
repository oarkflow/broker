package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	etcdraft "go.etcd.io/raft/v3"
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
		ID:            1,
		NodeID:        "node-1",
		Peers:         map[uint64]string{1: "node-1"},
		ElectionTick:  10,
		HeartbeatTick: 1,
		TickInterval:  10 * time.Millisecond,
		StateMachine:  sm,
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

func TestEtcdRaftControllerRecoversDurableLog(t *testing.T) {
	dir := t.TempDir()
	config := func(sm StateMachine) RaftControllerConfig {
		return RaftControllerConfig{
			ID: 1, NodeID: "node-1", Peers: map[uint64]string{1: "node-1"},
			ElectionTick: 10, HeartbeatTick: 1, TickInterval: 5 * time.Millisecond,
			StateMachine: sm, StorageDir: dir,
		}
	}

	firstSM := &testStateMachine{}
	first, err := NewRaftController(config(firstSM))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	if err := first.Campaign(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !first.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := first.Propose(ctx, []byte("durable-command")); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	secondSM := &testStateMachine{}
	second, err := NewRaftController(config(secondSM))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	defer second.Stop()
	deadline = time.Now().Add(2 * time.Second)
	for secondSM.Count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if secondSM.Count() != 1 {
		t.Fatalf("restarted state machine applied %d commands, want 1", secondSM.Count())
	}
	secondSM.mu.Lock()
	got := string(secondSM.applied[0].Data)
	secondSM.mu.Unlock()
	if got != "durable-command" {
		t.Fatalf("recovered command %q", got)
	}
}

func TestEtcdRaftStorageRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	storage, err := openPersistentRaftStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	term, vote, commit := uint64(1), uint64(1), uint64(1)
	if err := storage.persistReady(raftReadyForTest(
		&raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit},
		[]*raftpb.Entry{{Term: &term, Index: &commit, Data: []byte("command")}},
	)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "etcd-raft-state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var disk raftDiskState
	if err := json.Unmarshal(data, &disk); err != nil {
		t.Fatal(err)
	}
	disk.Entries[0][0] ^= 1
	data, err = json.Marshal(disk)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openPersistentRaftStorage(dir); err == nil {
		t.Fatal("expected corrupt durable raft state to be rejected")
	}
}

func raftReadyForTest(hardState *raftpb.HardState, entries []*raftpb.Entry) etcdraft.Ready {
	return etcdraft.Ready{HardState: hardState, Entries: entries}
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
			ID:            uint64(i + 1),
			NodeID:        nodeID,
			Peers:         peers,
			ElectionTick:  10,
			HeartbeatTick: 1,
			TickInterval:  10 * time.Millisecond,
			Transport:     transports[i],
			StateMachine:  sms[i],
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

func TestEtcdRaftQuorumLossLeaderChangeAndRecovery(t *testing.T) {
	peers := map[uint64]string{1: "partition-1", 2: "partition-2", 3: "partition-3"}
	registry := make(map[string]*MemoryTransport)
	controllers := make([]*RaftController, 3)
	transports := make([]*MemoryTransport, 3)
	states := make([]*testStateMachine, 3)
	for i := range transports {
		id := peers[uint64(i+1)]
		transports[i] = NewMemoryTransport(id)
		registry[id] = transports[i]
	}
	for _, transport := range transports {
		transport.Register(registry)
	}
	for i := range controllers {
		states[i] = &testStateMachine{}
		controller, err := NewRaftController(RaftControllerConfig{
			ID: uint64(i + 1), NodeID: peers[uint64(i+1)], Peers: peers,
			ElectionTick: 5, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond,
			Transport: transports[i], StateMachine: states[i], StorageDir: t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		controllers[i] = controller
		if err := controller.RegisterTransportHandler(peers[uint64(i+1)]); err != nil {
			t.Fatal(err)
		}
		if err := controller.Start(); err != nil {
			t.Fatal(err)
		}
		defer controller.Stop()
	}
	_ = controllers[0].Campaign()
	leaderIndex := waitForLeaderIndex(controllers, nil, 3*time.Second)
	if leaderIndex < 0 {
		t.Fatal("initial leader was not elected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := controllers[leaderIndex].Propose(ctx, []byte("before-partition")); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	for peerIndex := range transports {
		if peerIndex != leaderIndex {
			transports[leaderIndex].SetPartition(peers[uint64(peerIndex+1)], true)
		}
	}
	excluded := map[int]bool{leaderIndex: true}
	newLeader := waitForLeaderIndex(controllers, excluded, 3*time.Second)
	if newLeader < 0 {
		t.Fatal("majority partition did not elect a replacement leader")
	}
	isolatedCtx, isolatedCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	if err := controllers[leaderIndex].Propose(isolatedCtx, []byte("must-not-commit")); err == nil {
		isolatedCancel()
		t.Fatal("isolated former leader accepted a quorum-confirmed proposal")
	}
	isolatedCancel()
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	if err := controllers[newLeader].Propose(ctx, []byte("after-failover")); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	for peerIndex := range transports {
		if peerIndex != leaderIndex {
			transports[leaderIndex].SetPartition(peers[uint64(peerIndex+1)], false)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if states[0].Count() >= 2 && states[1].Count() >= 2 && states[2].Count() >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cluster did not converge after healing: counts=%d,%d,%d", states[0].Count(), states[1].Count(), states[2].Count())
}

func waitForLeaderIndex(controllers []*RaftController, excluded map[int]bool, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, controller := range controllers {
			if !excluded[i] && controller.IsLeader() {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return -1
}

func TestEtcdRaftControllerConfChange(t *testing.T) {
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
		Type:    &ccType,
		NodeId:  &nodeID,
		Context: []byte("node-2"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := ctrl.ProposeConfChange(ctx, &cc); err != nil {
		t.Fatalf("failed to propose conf change: %v", err)
	}
	if address, ok := ctrl.PeerAddress(2); !ok || address != "node-2" {
		t.Fatalf("configuration change did not update transport membership: address=%q ok=%v", address, ok)
	}
}

func TestEtcdRaftJoinExistingNode(t *testing.T) {
	transport1 := NewMemoryTransport("join-1")
	transport2 := NewMemoryTransport("join-2")
	registry := map[string]*MemoryTransport{"join-1": transport1, "join-2": transport2}
	transport1.Register(registry)
	transport2.Register(registry)
	state1 := &testStateMachine{}
	first, err := NewRaftController(RaftControllerConfig{
		ID: 1, NodeID: "join-1", Peers: map[uint64]string{1: "join-1"},
		ElectionTick: 5, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond,
		Transport: transport1, StateMachine: state1, StorageDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.RegisterTransportHandler("join-1"); err != nil {
		t.Fatal(err)
	}
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	defer first.Stop()
	_ = first.Campaign()
	deadline := time.Now().Add(2 * time.Second)
	for !first.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	id, err := first.AddPeer(ctx, "join-2")
	cancel()
	if err != nil || id != 2 {
		t.Fatalf("add joining peer: id=%d err=%v", id, err)
	}
	state2 := &testStateMachine{}
	second, err := NewRaftController(RaftControllerConfig{
		ID: 2, NodeID: "join-2", Peers: map[uint64]string{1: "join-1", 2: "join-2"}, SkipBootstrap: true,
		ElectionTick: 5, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond,
		Transport: transport2, StateMachine: state2, StorageDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.RegisterTransportHandler("join-2"); err != nil {
		t.Fatal(err)
	}
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	defer second.Stop()
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	if err := first.Propose(ctx, []byte("after-join")); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	deadline = time.Now().Add(2 * time.Second)
	for state2.Count() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if state2.Count() != 1 {
		t.Fatalf("joining node applied %d commands, want 1", state2.Count())
	}
}
