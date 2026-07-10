package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// WALReplicator handles replication of the Write-Ahead Log across cluster nodes.
type WALReplicator struct {
	mu sync.RWMutex

	cluster    *Cluster
	fencingToken *FencingToken

	// State
	lastReplicated uint64
	replicated     map[uint64]time.Time
	pending        map[uint64]*WALReplication

	// Callbacks
	onReplicated func(index uint64, data []byte)

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// WALReplication represents a pending WAL replication.
type WALReplication struct {
	Index     uint64    `json:"index"`
	Data      []byte    `json:"data"`
	Replicas  []string  `json:"replicas"`
	Acks      int       `json:"acks"`
	Quorum    int       `json:"quorum"`
	CreatedAt time.Time `json:"created_at"`
}

// WALReplicatorConfig holds configuration for the WAL replicator.
type WALReplicatorConfig struct {
	Cluster      *Cluster
	FencingToken *FencingToken
	OnReplicated func(index uint64, data []byte)
}

// NewWALReplicator creates a new WAL replicator.
func NewWALReplicator(cfg WALReplicatorConfig) *WALReplicator {
	return &WALReplicator{
		cluster:      cfg.Cluster,
		fencingToken: cfg.FencingToken,
		replicated:   make(map[uint64]time.Time),
		pending:      make(map[uint64]*WALReplication),
		onReplicated: cfg.OnReplicated,
		stopCh:       make(chan struct{}),
	}
}

// Start starts the WAL replicator.
func (r *WALReplicator) Start() error {
	r.wg.Add(1)
	go r.replicationLoop()
	return nil
}

// Stop stops the WAL replicator.
func (r *WALReplicator) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

// replicationLoop handles ongoing replication.
func (r *WALReplicator) replicationLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.processPending()
		}
	}
}

// Replicate replicates a WAL entry to all replicas.
func (r *WALReplicator) Replicate(index uint64, data []byte) error {
	if !r.cluster.IsLeader() {
		return ErrNotLeader
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if already replicated
	if _, ok := r.replicated[index]; ok {
		return nil
	}

	nodes := r.cluster.ActiveNodes()
	replicas := make([]string, 0, len(nodes)-1)
	for _, node := range nodes {
		if node.ID != r.cluster.ID() {
			replicas = append(replicas, node.ID)
		}
	}

	if len(replicas) == 0 {
		// Single node - immediate replication
		r.replicated[index] = time.Now()
		if r.onReplicated != nil {
			go r.onReplicated(index, data)
		}
		return nil
	}

	quorumSize := (len(nodes) / 2) + 1

	replication := &WALReplication{
		Index:     index,
		Data:      data,
		Replicas:  replicas,
		Quorum:    quorumSize,
		CreatedAt: time.Now(),
	}

	r.pending[index] = replication

	// Send to replicas
	for _, replicaID := range replicas {
		go r.sendToReplica(replicaID, replication)
	}

	return nil
}

// sendToReplica sends a replication to a specific replica.
func (r *WALReplicator) sendToReplica(replicaID string, replication *WALReplication) {
	// Create a Raft message for WAL replication
	command := &WALReplicationCommand{
		Type:  WALCommandReplicate,
		Index: replication.Index,
		Data:  replication.Data,
	}

	data, err := json.Marshal(command)
	if err != nil {
		log.Printf("[wal-replicator] marshal error: %v", err)
		return
	}

	// Submit to Raft (which will replicate to all nodes)
	_, err = r.cluster.Submit(data)
	if err != nil {
		log.Printf("[wal-replicator] submit error for index %d: %v", replication.Index, err)
		return
	}

	// Mark as acked (in real implementation, wait for actual ack)
	r.mu.Lock()
	replication.Acks++
	if replication.Acks >= replication.Quorum {
		delete(r.pending, replication.Index)
		r.replicated[replication.Index] = time.Now()
		log.Printf("[wal-replicator] index %d replicated with %d acks", replication.Index, replication.Acks)

		if r.onReplicated != nil {
			go r.onReplicated(replication.Index, replication.Data)
		}
	}
	r.mu.Unlock()
}

// processPending processes pending replications.
func (r *WALReplicator) processPending() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	timeout := 10 * time.Second

	for index, replication := range r.pending {
		if now.Sub(replication.CreatedAt) > timeout {
			log.Printf("[wal-replicator] replication %d timed out, retrying", index)
			replication.CreatedAt = now
			for _, replicaID := range replication.Replicas {
				go r.sendToReplica(replicaID, replication)
			}
		}
	}
}

// Acknowledge acknowledges a replication from a replica.
func (r *WALReplicator) Acknowledge(index uint64, nodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	replication, ok := r.pending[index]
	if !ok {
		return nil // Already processed
	}

	replication.Acks++
	if replication.Acks >= replication.Quorum {
		delete(r.pending, index)
		r.replicated[index] = time.Now()
		log.Printf("[wal-replicator] index %d committed with %d acks", index, replication.Acks)

		if r.onReplicated != nil {
			go r.onReplicated(index, replication.Data)
		}
	}

	return nil
}

// IsReplicated returns true if an index has been replicated.
func (r *WALReplicator) IsReplicated(index uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.replicated[index]
	return ok
}

// LastReplicated returns the last replicated index.
func (r *WALReplicator) LastReplicated() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastReplicated
}

// Stats returns replication statistics.
func (r *WALReplicator) Stats() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return map[string]any{
		"last_replicated": r.lastReplicated,
		"replicated_count": len(r.replicated),
		"pending_count":   len(r.pending),
	}
}

// WALReplicationCommand is a command for WAL replication.
type WALReplicationCommand struct {
	Type  WALCommandType `json:"type"`
	Index uint64         `json:"index"`
	Data  []byte         `json:"data"`
}

// WALCommandType is the type of WAL command.
type WALCommandType string

const (
	WALCommandReplicate WALCommandType = "replicate"
	WALCommandAck       WALCommandType = "ack"
	WALCommandTrim      WALCommandType = "trim"
)

// ClusterStateMachine implements StateMachine for the cluster.
type ClusterStateMachine struct {
	mu       sync.RWMutex
	data     map[string][]byte
	cluster  *Cluster
}

// NewClusterStateMachine creates a new cluster state machine.
func NewClusterStateMachine(cluster *Cluster) *ClusterStateMachine {
	return &ClusterStateMachine{
		data:    make(map[string][]byte),
		cluster: cluster,
	}
}

// Apply applies a committed log entry.
func (sm *ClusterStateMachine) Apply(entry *LogEntry) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if entry.Type == EntryNormal {
		var cmd WALReplicationCommand
		if err := json.Unmarshal(entry.Data, &cmd); err != nil {
			return fmt.Errorf("unmarshal command: %w", err)
		}

		switch cmd.Type {
		case WALCommandReplicate:
			sm.data[fmt.Sprintf("wal:%d", cmd.Index)] = cmd.Data
		case WALCommandTrim:
			// Trim old entries
			delete(sm.data, fmt.Sprintf("wal:%d", cmd.Index))
		}
	}

	return nil
}

// Snapshot returns a snapshot of the current state.
func (sm *ClusterStateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return json.Marshal(sm.data)
}

// Restore restores the state from a snapshot.
func (sm *ClusterStateMachine) Restore(snapshot []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var data map[string][]byte
	if err := json.Unmarshal(snapshot, &data); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	sm.data = data
	return nil
}

// Get retrieves data by key.
func (sm *ClusterStateMachine) Get(key string) ([]byte, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	data, ok := sm.data[key]
	return data, ok
}

// Set sets a key-value pair.
func (sm *ClusterStateMachine) Set(key string, value []byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.data[key] = value
}
