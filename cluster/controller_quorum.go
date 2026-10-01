package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrPartitionNotFound = errors.New("cluster: partition metadata not found")
)

// MetadataOpType identifies a cluster metadata mutation.
type MetadataOpType byte

const (
	OpRegisterNode    MetadataOpType = 1
	OpDeregisterNode  MetadataOpType = 2
	OpAssignPartition MetadataOpType = 3
	OpUpdateISR       MetadataOpType = 4
	OpAdvanceEpoch    MetadataOpType = 5
)

// MetadataCommand represents a serialized mutation submitted to the controller quorum.
type MetadataCommand struct {
	Op        MetadataOpType    `json:"op"`
	Node      *NodeInfo         `json:"node,omitempty"`
	Partition *PartitionGroupID `json:"partition,omitempty"`
	Replicas  []string          `json:"replicas,omitempty"`
	Leader    string            `json:"leader,omitempty"`
	ISR       []string          `json:"isr,omitempty"`
	Epoch     uint64            `json:"epoch,omitempty"`
}

// PartitionMetadata describes the current topology of a single partition.
type PartitionMetadata struct {
	ID          PartitionGroupID `json:"id"`
	Leader      string           `json:"leader"`
	LeaderEpoch uint64           `json:"leader_epoch"`
	Replicas    []string         `json:"replicas"`
	ISR         []string         `json:"isr"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

// ClusterMetadata holds the in-memory replicated topology.
type ClusterMetadata struct {
	mu         sync.RWMutex
	Nodes      map[string]*NodeInfo                    `json:"nodes"`
	Partitions map[PartitionGroupID]*PartitionMetadata `json:"partitions"`
}

func NewClusterMetadata() *ClusterMetadata {
	return &ClusterMetadata{
		Nodes:      make(map[string]*NodeInfo),
		Partitions: make(map[PartitionGroupID]*PartitionMetadata),
	}
}

// MetadataStateMachine applies metadata commands to ClusterMetadata.
type MetadataStateMachine struct {
	meta *ClusterMetadata
}

func NewMetadataStateMachine(meta *ClusterMetadata) *MetadataStateMachine {
	if meta == nil {
		meta = NewClusterMetadata()
	}
	return &MetadataStateMachine{meta: meta}
}

func (sm *MetadataStateMachine) Apply(entry *LogEntry) error {
	if len(entry.Data) == 0 {
		return nil
	}

	var cmd MetadataCommand
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return fmt.Errorf("decode metadata command: %w", err)
	}

	sm.meta.mu.Lock()
	defer sm.meta.mu.Unlock()

	switch cmd.Op {
	case OpRegisterNode:
		if cmd.Node != nil && cmd.Node.ID != "" {
			sm.meta.Nodes[cmd.Node.ID] = cmd.Node
		}

	case OpDeregisterNode:
		if cmd.Node != nil && cmd.Node.ID != "" {
			delete(sm.meta.Nodes, cmd.Node.ID)
		}

	case OpAssignPartition:
		if cmd.Partition != nil {
			pid := *cmd.Partition
			epoch := uint64(1)
			if existing, ok := sm.meta.Partitions[pid]; ok && existing.LeaderEpoch > 0 {
				epoch = existing.LeaderEpoch + 1
			}
			sm.meta.Partitions[pid] = &PartitionMetadata{
				ID:          pid,
				Leader:      cmd.Leader,
				LeaderEpoch: epoch,
				Replicas:    cmd.Replicas,
				ISR:         cmd.ISR,
				UpdatedAt:   time.Now(),
			}
		}

	case OpUpdateISR:
		if cmd.Partition != nil {
			pid := *cmd.Partition
			if pm, ok := sm.meta.Partitions[pid]; ok {
				pm.ISR = cmd.ISR
				pm.UpdatedAt = time.Now()
			}
		}

	case OpAdvanceEpoch:
		if cmd.Partition != nil {
			pid := *cmd.Partition
			if pm, ok := sm.meta.Partitions[pid]; ok {
				pm.LeaderEpoch++
				if cmd.Leader != "" {
					pm.Leader = cmd.Leader
				}
				pm.UpdatedAt = time.Now()
			}
		}
	}

	return nil
}

func (sm *MetadataStateMachine) Snapshot() ([]byte, error) {
	sm.meta.mu.RLock()
	defer sm.meta.mu.RUnlock()
	return json.Marshal(sm.meta)
}

func (sm *MetadataStateMachine) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var newMeta ClusterMetadata
	if err := json.Unmarshal(data, &newMeta); err != nil {
		return err
	}
	sm.meta.mu.Lock()
	defer sm.meta.mu.Unlock()
	sm.meta.Nodes = newMeta.Nodes
	sm.meta.Partitions = newMeta.Partitions
	return nil
}

// ControllerQuorum coordinates cluster topology and partition assignments.
type ControllerQuorum struct {
	controller *RaftController
	meta       *ClusterMetadata
	timeout    time.Duration
}

// ControllerQuorumConfig configures the metadata consensus quorum.
type ControllerQuorumConfig struct {
	Controller *RaftController
	Metadata   *ClusterMetadata
	Timeout    time.Duration
}

// NewControllerQuorum creates a metadata consensus controller.
func NewControllerQuorum(cfg ControllerQuorumConfig) (*ControllerQuorum, error) {
	if cfg.Controller == nil {
		return nil, errors.New("controller quorum: RaftController is required")
	}
	if cfg.Metadata == nil {
		cfg.Metadata = NewClusterMetadata()
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &ControllerQuorum{
		controller: cfg.Controller,
		meta:       cfg.Metadata,
		timeout:    cfg.Timeout,
	}, nil
}

// RegisterNode submits a node registration command through consensus.
func (cq *ControllerQuorum) RegisterNode(ctx context.Context, node *NodeInfo) error {
	if node == nil || node.ID == "" {
		return errors.New("invalid node info")
	}
	cmd := MetadataCommand{
		Op:   OpRegisterNode,
		Node: node,
	}
	return cq.propose(ctx, cmd)
}

// DeregisterNode removes a node registration through consensus.
func (cq *ControllerQuorum) DeregisterNode(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return errors.New("invalid node ID")
	}
	cmd := MetadataCommand{
		Op:   OpDeregisterNode,
		Node: &NodeInfo{ID: nodeID},
	}
	return cq.propose(ctx, cmd)
}

// AssignPartition creates or updates a partition assignment across the cluster.
func (cq *ControllerQuorum) AssignPartition(ctx context.Context, topic string, partition int, replicas []string, leader string) error {
	pid := PartitionGroupID{Topic: topic, Partition: partition}
	cmd := MetadataCommand{
		Op:        OpAssignPartition,
		Partition: &pid,
		Replicas:  replicas,
		Leader:    leader,
		ISR:       replicas,
	}
	return cq.propose(ctx, cmd)
}

// UpdateISR submits an updated in-sync replica set for a partition.
func (cq *ControllerQuorum) UpdateISR(ctx context.Context, topic string, partition int, isr []string) error {
	pid := PartitionGroupID{Topic: topic, Partition: partition}
	cmd := MetadataCommand{
		Op:        OpUpdateISR,
		Partition: &pid,
		ISR:       isr,
	}
	return cq.propose(ctx, cmd)
}

// AdvanceEpoch advances the leader epoch and sets a new leader if provided.
func (cq *ControllerQuorum) AdvanceEpoch(ctx context.Context, topic string, partition int, newLeader string) error {
	pid := PartitionGroupID{Topic: topic, Partition: partition}
	cmd := MetadataCommand{
		Op:        OpAdvanceEpoch,
		Partition: &pid,
		Leader:    newLeader,
	}
	return cq.propose(ctx, cmd)
}

// GetPartition retrieves current partition metadata from the replicated state.
func (cq *ControllerQuorum) GetPartition(topic string, partition int) (*PartitionMetadata, error) {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()

	pid := PartitionGroupID{Topic: topic, Partition: partition}
	pm, ok := cq.meta.Partitions[pid]
	if !ok {
		return nil, ErrPartitionNotFound
	}
	copyMeta := *pm
	return &copyMeta, nil
}

// ListPartitions returns all active partition metadata entries.
func (cq *ControllerQuorum) ListPartitions() []*PartitionMetadata {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()

	out := make([]*PartitionMetadata, 0, len(cq.meta.Partitions))
	for _, pm := range cq.meta.Partitions {
		copyMeta := *pm
		out = append(out, &copyMeta)
	}
	return out
}

// GetNodes returns all currently registered nodes.
func (cq *ControllerQuorum) GetNodes() []*NodeInfo {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()

	out := make([]*NodeInfo, 0, len(cq.meta.Nodes))
	for _, n := range cq.meta.Nodes {
		copyNode := *n
		out = append(out, &copyNode)
	}
	return out
}

// IsLeader checks if this controller is the active leader of the metadata quorum.
func (cq *ControllerQuorum) IsLeader() bool {
	return cq.controller.IsLeader()
}

func (cq *ControllerQuorum) propose(ctx context.Context, cmd MetadataCommand) error {
	if !cq.controller.IsLeader() {
		return ErrNotLeader
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), cq.timeout)
		defer cancel()
	}
	return cq.controller.Propose(ctx, data)
}
