package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PartitionGroupID uniquely identifies a stream or topic partition.
type PartitionGroupID struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

func (p PartitionGroupID) String() string {
	return fmt.Sprintf("%s/%d", p.Topic, p.Partition)
}

func (p PartitionGroupID) MarshalText() ([]byte, error) {
	return []byte(p.String()), nil
}

func (p *PartitionGroupID) UnmarshalText(text []byte) error {
	s := string(text)
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid partition group id: %s", s)
	}
	partNum, err := strconv.Atoi(parts[1])
	if err != nil {
		return err
	}
	p.Topic = parts[0]
	p.Partition = partNum
	return nil
}

// ReplicaState holds synchronization details for a single partition replica.
type ReplicaState struct {
	NodeID       string    `json:"node_id"`
	LastAckIndex uint64    `json:"last_ack_index"`
	LastAckTime  time.Time `json:"last_ack_time"`
	IsISR        bool      `json:"is_isr"`
}

// PartitionGroupConfig configures a replicated partition group.
type PartitionGroupConfig struct {
	ID                 PartitionGroupID
	Controller         *RaftController
	ReplicationFactor  int
	MinISR             int
	MaxReplicationLag  uint64
	ReplicationTimeout time.Duration
	Replicas           []string
}

// PartitionGroup manages consensus, replication and ISR membership for one partition.
type PartitionGroup struct {
	id          PartitionGroupID
	controller  *RaftController
	rf          int
	minISR      int
	maxLag      uint64
	timeout     time.Duration
	leaderEpoch atomic.Uint64

	mu       sync.RWMutex
	replicas map[string]*ReplicaState
	isr      map[string]struct{}
	closed   atomic.Bool
}

// NewPartitionGroup creates a PartitionGroup backed by etcd/raft.
func NewPartitionGroup(cfg PartitionGroupConfig) (*PartitionGroup, error) {
	if cfg.Controller == nil {
		return nil, fmt.Errorf("partition group %s: RaftController is required", cfg.ID)
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 3
	}
	if cfg.MinISR <= 0 {
		cfg.MinISR = (cfg.ReplicationFactor / 2) + 1
	}
	if cfg.MaxReplicationLag == 0 {
		cfg.MaxReplicationLag = 1000
	}
	if cfg.ReplicationTimeout <= 0 {
		cfg.ReplicationTimeout = 5 * time.Second
	}

	pg := &PartitionGroup{
		id:         cfg.ID,
		controller: cfg.Controller,
		rf:         cfg.ReplicationFactor,
		minISR:     cfg.MinISR,
		maxLag:     cfg.MaxReplicationLag,
		timeout:    cfg.ReplicationTimeout,
		replicas:   make(map[string]*ReplicaState),
		isr:        make(map[string]struct{}),
	}
	pg.leaderEpoch.Store(1)

	for _, node := range cfg.Replicas {
		pg.replicas[node] = &ReplicaState{
			NodeID:      node,
			LastAckTime: time.Now(),
			IsISR:       true,
		}
		pg.isr[node] = struct{}{}
	}

	return pg, nil
}

// ID returns the partition group identifier.
func (pg *PartitionGroup) ID() PartitionGroupID {
	return pg.id
}

// LeaderEpoch returns the current leader epoch for fencing.
func (pg *PartitionGroup) LeaderEpoch() uint64 {
	return pg.leaderEpoch.Load()
}

// IncrementEpoch advances the leader epoch upon leadership acquisition.
func (pg *PartitionGroup) IncrementEpoch() uint64 {
	return pg.leaderEpoch.Add(1)
}

// IsLeader checks if this node is currently the leader of this partition.
func (pg *PartitionGroup) IsLeader() bool {
	return pg.controller != nil && pg.controller.IsLeader()
}

// LeaderID returns the current leader Raft ID.
func (pg *PartitionGroup) LeaderID() uint64 {
	if pg.controller == nil {
		return 0
	}
	return pg.controller.LeaderID()
}

// Propose submits a payload to the partition consensus group, validating leader epoch.
func (pg *PartitionGroup) Propose(ctx context.Context, epoch uint64, data []byte) error {
	if pg.closed.Load() {
		return ErrClosed
	}

	curEpoch := pg.leaderEpoch.Load()
	if epoch > 0 && epoch < curEpoch {
		return fmt.Errorf("%w: requested epoch %d < current epoch %d", ErrStaleEpoch, epoch, curEpoch)
	}

	if !pg.IsLeader() {
		return ErrNotLeader
	}

	// Verify minimum in-sync replicas before accepting writes
	pg.mu.RLock()
	currentISRCount := len(pg.isr)
	pg.mu.RUnlock()

	if currentISRCount < pg.minISR {
		return fmt.Errorf("%w: active ISR count %d < min ISR %d", ErrQuorumNotMet, currentISRCount, pg.minISR)
	}

	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), pg.timeout)
		defer cancel()
	}

	return pg.controller.Propose(ctx, data)
}

// UpdateReplicaProgress records the latest acknowledged log index for a replica
// and adjusts ISR membership accordingly.
func (pg *PartitionGroup) UpdateReplicaProgress(nodeID string, lastIndex uint64) {
	pg.mu.Lock()
	defer pg.mu.Unlock()

	state, exists := pg.replicas[nodeID]
	if !exists {
		state = &ReplicaState{
			NodeID: nodeID,
		}
		pg.replicas[nodeID] = state
	}

	state.LastAckIndex = lastIndex
	state.LastAckTime = time.Now()

	// Find the highest committed or leader index from peers
	var leaderIndex uint64
	for _, r := range pg.replicas {
		if r.LastAckIndex > leaderIndex {
			leaderIndex = r.LastAckIndex
		}
	}

	// Evaluate lag
	var lag uint64
	if leaderIndex > lastIndex {
		lag = leaderIndex - lastIndex
	}

	if lag <= pg.maxLag {
		if !state.IsISR {
			state.IsISR = true
			pg.isr[nodeID] = struct{}{}
		}
	} else {
		if state.IsISR {
			state.IsISR = false
			delete(pg.isr, nodeID)
		}
	}
}

// ISR returns a list of node IDs currently in the in-sync replica set.
func (pg *PartitionGroup) ISR() []string {
	pg.mu.RLock()
	defer pg.mu.RUnlock()

	out := make([]string, 0, len(pg.isr))
	for node := range pg.isr {
		out = append(out, node)
	}
	return out
}

// Replicas returns a snapshot of all known replica states.
func (pg *PartitionGroup) Replicas() []*ReplicaState {
	pg.mu.RLock()
	defer pg.mu.RUnlock()

	out := make([]*ReplicaState, 0, len(pg.replicas))
	for _, r := range pg.replicas {
		copyState := *r
		out = append(out, &copyState)
	}
	return out
}

// Close marks the partition group as closed.
func (pg *PartitionGroup) Close() error {
	pg.closed.Store(true)
	return nil
}
