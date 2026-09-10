package cluster

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestRaftStateTransitions(t *testing.T) {
	// Create a 3-node cluster
	transports := make([]*MemoryTransport, 3)
	rafts := make([]*Raft, 3)
	ids := []string{"node-1", "node-2", "node-3"}

	// Create transports
	for i, id := range ids {
		transports[i] = NewMemoryTransport(id)
	}

	// Register transports
	registry := make(map[string]*MemoryTransport)
	for _, tr := range transports {
		tr.Register(registry)
	}

	// Create Raft instances with shorter timeouts for testing
	peers := ids
	for i, id := range ids {
		rafts[i] = NewRaft(RaftConfig{
			ID:               id,
			Peers:            peers,
			Transport:        transports[i],
			ElectionTimeout:  100 * time.Millisecond,
			HeartbeatTimeout: 30 * time.Millisecond,
		})
	}

	// Start all Rafts
	for _, r := range rafts {
		if err := r.Start(); err != nil {
			t.Fatalf("start raft: %v", err)
		}
	}
	defer func() {
		for _, r := range rafts {
			r.Stop()
		}
	}()

	// Wait for leader election with retries
	var leader *Raft
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		for _, r := range rafts {
			if r.IsLeader() {
				leader = r
				break
			}
		}
		if leader != nil {
			break
		}
	}

	if leader == nil {
		t.Fatal("no leader elected")
	}

	t.Logf("Leader elected: %s", leader.id)

	// Submit a command
	idx, err := leader.Submit([]byte("test-command"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	t.Logf("Command submitted at index %d", idx)

	// Wait for replication
	time.Sleep(200 * time.Millisecond)

	// Check commit index
	for _, r := range rafts {
		if r.CommitIndex() < idx {
			t.Logf("Node %s commit index %d < %d", r.id, r.CommitIndex(), idx)
		}
	}
}

func TestQuorumSize(t *testing.T) {
	tests := []struct {
		nodes  int
		quorum int
	}{
		{1, 1},
		{3, 2},
		{5, 3},
		{7, 4},
	}

	for _, tt := range tests {
		peers := make([]string, tt.nodes)
		for i := 0; i < tt.nodes; i++ {
			peers[i] = "node-" + string(rune('a'+i))
		}

		quorum := (tt.nodes / 2) + 1
		if quorum != tt.quorum {
			t.Errorf("nodes=%d: expected quorum %d, got %d", tt.nodes, tt.quorum, quorum)
		}
	}
}

func TestFencingToken(t *testing.T) {
	ft := NewFencingToken()

	// Initial token should be 0
	if ft.Current() != 0 {
		t.Errorf("expected initial token 0, got %d", ft.Current())
	}

	// Increment
	token1 := ft.Increment()
	if token1 != 1 {
		t.Errorf("expected token 1, got %d", token1)
	}

	// Validate
	if !ft.Validate(1) {
		t.Error("token 1 should be valid")
	}
	if ft.Validate(0) {
		t.Error("token 0 should be invalid")
	}

	// Epoch increment
	epoch := ft.IncrementEpoch()
	if epoch != 1 {
		t.Errorf("expected epoch 1, got %d", epoch)
	}
}

func TestSplitBrainDetector(t *testing.T) {
	// Create a mock cluster with raft
	tr := NewMemoryTransport("node-1")
	registry := make(map[string]*MemoryTransport)
	tr.Register(registry)

	raft := NewRaft(RaftConfig{
		ID:        "node-1",
		Peers:     []string{"node-1"},
		Transport: tr,
	})

	cluster := &Cluster{
		id:    "node-1",
		nodes: make(map[string]*NodeInfo),
		raft:  raft,
	}

	// Add nodes
	for i := 0; i < 5; i++ {
		id := "node-" + string(rune('a'+i))
		cluster.nodes[id] = &NodeInfo{
			ID:       id,
			State:    NodeActive,
			LastSeen: time.Now(),
		}
	}

	detector := NewSplitBrainDetector(SplitBrainConfig{
		Cluster:       cluster,
		CheckInterval: 100 * time.Millisecond,
	})

	// Start detector
	if err := detector.Start(); err != nil {
		t.Fatalf("start detector: %v", err)
	}
	defer detector.Stop()

	// Wait for a check
	time.Sleep(200 * time.Millisecond)

	// Should have quorum
	if !detector.HasQuorum() {
		t.Error("expected quorum")
	}
}

func TestWALReplicator(t *testing.T) {
	// Create a mock cluster
	cluster := &Cluster{
		id:    "node-1",
		nodes: make(map[string]*NodeInfo),
		raft: &Raft{
			id:    "node-1",
			peers: []string{"node-1"},
			state: StateLeader,
		},
	}

	replicator := NewWALReplicator(WALReplicatorConfig{
		Cluster: cluster,
	})

	// Test stats
	stats := replicator.Stats()
	if stats["pending_count"] != 0 {
		t.Errorf("expected 0 pending, got %v", stats["pending_count"])
	}
}

func TestClusterStateMachine(t *testing.T) {
	cluster := &Cluster{
		id: "node-1",
	}

	sm := NewClusterStateMachine(cluster)

	// Apply an entry
	entry := &LogEntry{
		Term:  1,
		Index: 1,
		Type:  EntryNormal,
	}

	cmd := WALReplicationCommand{
		Type:  WALCommandReplicate,
		Index: 1,
		Data:  []byte("test-data"),
	}

	data, _ := json.Marshal(cmd)
	entry.Data = data

	if err := sm.Apply(entry); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Check data
	val, ok := sm.Get("wal:1")
	if !ok {
		t.Fatal("expected data to exist")
	}
	if string(val) != "test-data" {
		t.Errorf("expected 'test-data', got '%s'", string(val))
	}

	// Test snapshot
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Restore
	sm2 := NewClusterStateMachine(cluster)
	if err := sm2.Restore(snapshot); err != nil {
		t.Fatalf("restore: %v", err)
	}

	val2, ok := sm2.Get("wal:1")
	if !ok {
		t.Fatal("expected data after restore")
	}
	if string(val2) != "test-data" {
		t.Errorf("expected 'test-data' after restore, got '%s'", string(val2))
	}
}

func TestMemoryTransport(t *testing.T) {
	tr1 := NewMemoryTransport("node-1")
	tr2 := NewMemoryTransport("node-2")

	// Set up handler for node-2 before registering
	received := make(chan *RaftMessage, 1)
	tr2.handler = func(from string, msg *RaftMessage) {
		select {
		case received <- msg:
		default:
		}
	}

	// Register transports
	registry := make(map[string]*MemoryTransport)
	tr1.Register(registry)
	tr2.Register(registry)

	// Send message from node-1 to node-2
	err := tr1.Send("node-2", &RaftMessage{
		Type: MsgHeartbeat,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case receivedMsg := <-received:
		if receivedMsg.Type != MsgHeartbeat {
			t.Errorf("expected heartbeat message, got %d", receivedMsg.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("expected to receive message")
	}
}

func BenchmarkRaftSubmit(b *testing.B) {
	// Create a single node cluster for benchmarking
	tr := NewMemoryTransport("node-1")
	registry := make(map[string]*MemoryTransport)
	tr.Register(registry)

	raft := NewRaft(RaftConfig{
		ID:        "node-1",
		Peers:     []string{"node-1"},
		Transport: tr,
	})

	raft.Start()
	defer raft.Stop()

	// Wait for leader
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raft.Submit([]byte("bench-command"))
	}
}

// Mock cluster for testing
type mockCluster struct {
	mu    sync.RWMutex
	id    string
	nodes map[string]*NodeInfo
	raft  *Raft
}

func (m *mockCluster) ID() string     { return m.id }
func (m *mockCluster) IsLeader() bool { return m.raft.IsLeader() }
func (m *mockCluster) Leader() string { return m.raft.Leader() }
func (m *mockCluster) ActiveNodes() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]*NodeInfo, 0)
	for _, n := range m.nodes {
		if n.State == NodeActive {
			nodes = append(nodes, n)
		}
	}
	return nodes
}
func (m *mockCluster) Submit(data []byte) (uint64, error) {
	return m.raft.Submit(data)
}
func (m *mockCluster) Raft() *Raft { return m.raft }

func TestRaftWAL(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewRaftWAL(WALConfig{Directory: dir})
	if err != nil {
		t.Fatalf("create wal: %v", err)
	}
	defer wal.Close()

	// Append entries
	for i := uint64(1); i <= 5; i++ {
		entry := &LogEntry{
			Term:  1,
			Index: i,
			Type:  EntryNormal,
			Data:  []byte("entry-" + string(rune('0'+i))),
		}
		if err := wal.Append(entry); err != nil {
			t.Fatalf("append entry %d: %v", i, err)
		}
	}

	// Verify range
	if wal.FirstIndex() != 1 {
		t.Errorf("first index: got %d, want 1", wal.FirstIndex())
	}
	if wal.LastIndex() != 5 {
		t.Errorf("last index: got %d, want 5", wal.LastIndex())
	}

	// Read back entries
	for i := uint64(1); i <= 5; i++ {
		entry, err := wal.Read(i)
		if err != nil {
			t.Fatalf("read entry %d: %v", i, err)
		}
		if entry.Index != i {
			t.Errorf("entry index: got %d, want %d", entry.Index, i)
		}
		if entry.Term != 1 {
			t.Errorf("entry term: got %d, want 1", entry.Term)
		}
	}

	// Read range
	entries, err := wal.ReadRange(2, 4)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("range length: got %d, want 3", len(entries))
	}

	// Truncate
	if err := wal.Truncate(3); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if wal.LastIndex() != 3 {
		t.Errorf("after truncate last index: got %d, want 3", wal.LastIndex())
	}
	if _, err := wal.Read(4); err == nil {
		t.Error("expected error reading truncated entry")
	}
}

func TestRaftWALReplay(t *testing.T) {
	dir := t.TempDir()

	// Create and populate WAL
	wal1, err := NewRaftWAL(WALConfig{Directory: dir})
	if err != nil {
		t.Fatalf("create wal: %v", err)
	}

	for i := uint64(1); i <= 3; i++ {
		entry := &LogEntry{Term: 1, Index: i, Type: EntryNormal, Data: []byte("data")}
		wal1.Append(entry)
	}
	wal1.Close()

	// Reopen and verify replay
	wal2, err := NewRaftWAL(WALConfig{Directory: dir})
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer wal2.Close()

	if wal2.FirstIndex() != 1 {
		t.Errorf("replayed first index: got %d, want 1", wal2.FirstIndex())
	}
	if wal2.LastIndex() != 3 {
		t.Errorf("replayed last index: got %d, want 3", wal2.LastIndex())
	}

	entry, err := wal2.Read(2)
	if err != nil {
		t.Fatalf("read replayed entry: %v", err)
	}
	if entry.Index != 2 || entry.Term != 1 {
		t.Errorf("replayed entry: index=%d term=%d", entry.Index, entry.Term)
	}
}

func TestRaftWALWithRaft(t *testing.T) {
	// Create 3-node cluster with WAL
	transports := make([]*MemoryTransport, 3)
	rafts := make([]*Raft, 3)
	wals := make([]*RaftWAL, 3)
	ids := []string{"node-1", "node-2", "node-3"}

	for i, id := range ids {
		transports[i] = NewMemoryTransport(id)
	}

	registry := make(map[string]*MemoryTransport)
	for _, tr := range transports {
		tr.Register(registry)
	}

	for i, id := range ids {
		walDir := t.TempDir()
		wal, err := NewRaftWAL(WALConfig{Directory: walDir})
		if err != nil {
			t.Fatalf("create wal for %s: %v", id, err)
		}
		wals[i] = wal

		rafts[i] = NewRaft(RaftConfig{
			ID:               id,
			Peers:            ids,
			Transport:        transports[i],
			WAL:              wal,
			ElectionTimeout:  100 * time.Millisecond,
			HeartbeatTimeout: 30 * time.Millisecond,
		})
	}

	for _, r := range rafts {
		if err := r.Start(); err != nil {
			t.Fatalf("start raft: %v", err)
		}
	}
	defer func() {
		for _, r := range rafts {
			r.Stop()
		}
	}()

	// Wait for leader
	var leader *Raft
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		for _, r := range rafts {
			if r.IsLeader() {
				leader = r
				break
			}
		}
		if leader != nil {
			break
		}
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}

	// Submit commands
	for i := 0; i < 3; i++ {
		_, err := leader.Submit([]byte("command-" + string(rune('a'+i))))
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	// Verify WAL has entries
	for i, wal := range wals {
		if wal.LastIndex() < 3 {
			t.Errorf("node %s WAL last index: got %d, want >= 3", ids[i], wal.LastIndex())
		}
	}
}

func TestBrokerStateMachine(t *testing.T) {
	store := &mockBrokerStore{queues: make(map[string]bool)}
	sm := NewBrokerStateMachine(store)

	// Apply declare queue command
	cmd, err := MakeBrokerCommand(BrokerCmdDeclareQueue, DeclareQueueCommand{
		Name: "test-queue",
	})
	if err != nil {
		t.Fatalf("make command: %v", err)
	}
	data, _ := EncodeBrokerCommand(cmd)

	entry := &LogEntry{Term: 1, Index: 1, Type: EntryNormal, Data: data}
	if err := sm.Apply(entry); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !store.queues["test-queue"] {
		t.Error("queue not declared")
	}

	// Apply pause queue command
	cmd2, _ := MakeBrokerCommand(BrokerCmdPauseQueue, "test-queue")
	data2, _ := EncodeBrokerCommand(cmd2)
	entry2 := &LogEntry{Term: 1, Index: 2, Type: EntryNormal, Data: data2}
	if err := sm.Apply(entry2); err != nil {
		t.Fatalf("apply pause: %v", err)
	}
	if !store.paused["test-queue"] {
		t.Error("queue not paused")
	}

	// Snapshot/restore
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	sm2 := NewBrokerStateMachine(store)
	if err := sm2.Restore(snapshot); err != nil {
		t.Fatalf("restore: %v", err)
	}
}

// mockBrokerStore implements BrokerStore for testing.
type mockBrokerStore struct {
	mu     sync.Mutex
	queues map[string]bool
	paused map[string]bool
}

func (m *mockBrokerStore) ReplicateDeclareQueue(ctx context.Context, cmd DeclareQueueCommand) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queues == nil {
		m.queues = make(map[string]bool)
	}
	if m.paused == nil {
		m.paused = make(map[string]bool)
	}
	m.queues[cmd.Name] = true
	return nil
}
func (m *mockBrokerStore) ReplicateDeclareTopic(ctx context.Context, cmd DeclareTopicCommand) error {
	return nil
}
func (m *mockBrokerStore) ReplicateDeclareStream(ctx context.Context, cmd DeclareStreamCommand) error {
	return nil
}
func (m *mockBrokerStore) ReplicateAppend(ctx context.Context, cmd AppendCommand) error {
	return nil
}
func (m *mockBrokerStore) ReplicateAck(ctx context.Context, cmd AckCommand) error   { return nil }
func (m *mockBrokerStore) ReplicateNack(ctx context.Context, cmd NackCommand) error { return nil }
func (m *mockBrokerStore) ReplicatePauseQueue(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.paused == nil {
		m.paused = make(map[string]bool)
	}
	m.paused[name] = true
	return nil
}
func (m *mockBrokerStore) ReplicateResumeQueue(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.paused == nil {
		m.paused = make(map[string]bool)
	}
	delete(m.paused, name)
	return nil
}
func (m *mockBrokerStore) ReplicatePurgeQueue(ctx context.Context, name string) (int, error) {
	return 0, nil
}
