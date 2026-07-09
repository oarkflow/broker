package broker

import (
	"context"
	"sort"
	"sync"
	"time"
)

type NodeState string

const (
	NodeJoining  NodeState = "joining"
	NodeActive   NodeState = "active"
	NodeDraining NodeState = "draining"
	NodeLeft     NodeState = "left"
	NodeFailed   NodeState = "failed"
)

type ClusterNode struct {
	ID        string            `json:"id"`
	Address   string            `json:"address,omitempty"`
	Zone      string            `json:"zone,omitempty"`
	State     NodeState         `json:"state"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	JoinedAt  time.Time         `json:"joined_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

type PartitionAssignment struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	Leader    string   `json:"leader"`
	Replicas  []string `json:"replicas"`
}

type Cluster struct {
	mu          sync.RWMutex
	self        string
	nodes       map[string]ClusterNode
	assignments map[string]PartitionAssignment
}

func NewCluster(self string) *Cluster {
	if self == "" {
		self = "node-1"
	}
	c := &Cluster{self: self, nodes: map[string]ClusterNode{}, assignments: map[string]PartitionAssignment{}}
	_ = c.Join(context.Background(), ClusterNode{ID: self})
	return c
}

func (c *Cluster) Self() string { c.mu.RLock(); defer c.mu.RUnlock(); return c.self }

func (c *Cluster) Join(ctx context.Context, node ClusterNode) error {
	if node.ID == "" {
		return errf(ErrInvalidMessage, "node id required")
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.nodes[node.ID]; ok && !old.JoinedAt.IsZero() {
		node.JoinedAt = old.JoinedAt
	} else if node.JoinedAt.IsZero() {
		node.JoinedAt = now
	}
	if node.State == "" || node.State == NodeJoining || node.State == NodeLeft {
		node.State = NodeActive
	}
	node.UpdatedAt = now
	node.Metadata = cloneMap(node.Metadata)
	c.nodes[node.ID] = node
	return nil
}

func (c *Cluster) MarkFailed(ctx context.Context, id string) error {
	return c.setNodeState(id, NodeFailed)
}
func (c *Cluster) Drain(ctx context.Context, id string) error {
	return c.setNodeState(id, NodeDraining)
}
func (c *Cluster) Leave(ctx context.Context, id string) error { return c.setNodeState(id, NodeLeft) }

func (c *Cluster) setNodeState(id string, st NodeState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return errf(ErrMessageNotFound, "node not found")
	}
	n.State = st
	n.UpdatedAt = time.Now()
	c.nodes[id] = n
	return nil
}

func (c *Cluster) Nodes() []ClusterNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]ClusterNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		n.Metadata = cloneMap(n.Metadata)
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *Cluster) ActiveNodes() []ClusterNode {
	nodes := c.Nodes()
	out := nodes[:0]
	for _, n := range nodes {
		if n.State == NodeActive {
			out = append(out, n)
		}
	}
	return out
}

func (c *Cluster) Leader() string {
	nodes := c.ActiveNodes()
	if len(nodes) == 0 {
		return ""
	}
	return nodes[0].ID
}

func (c *Cluster) IsLeader() bool { return c.Leader() == c.Self() }

func (c *Cluster) Assign(topic string, partitions, replicas int) ([]PartitionAssignment, error) {
	if topic == "" || partitions <= 0 {
		return nil, errf(ErrInvalidMessage, "topic and positive partitions are required")
	}
	nodes := c.ActiveNodes()
	if len(nodes) == 0 {
		return nil, errf(ErrBrokerUnavailable, "no active cluster nodes")
	}
	if replicas <= 0 {
		replicas = 1
	}
	if replicas > len(nodes) {
		replicas = len(nodes)
	}
	out := make([]PartitionAssignment, 0, partitions)
	c.mu.Lock()
	defer c.mu.Unlock()
	for p := 0; p < partitions; p++ {
		leaderIdx := p % len(nodes)
		replicaIDs := make([]string, 0, replicas)
		for i := 0; i < replicas; i++ {
			replicaIDs = append(replicaIDs, nodes[(leaderIdx+i)%len(nodes)].ID)
		}
		a := PartitionAssignment{Topic: topic, Partition: p, Leader: nodes[leaderIdx].ID, Replicas: replicaIDs}
		c.assignments[assignmentKey(topic, p)] = a
		out = append(out, a)
	}
	return out, nil
}

func (c *Cluster) Assignment(topic string, partition int) (PartitionAssignment, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	a, ok := c.assignments[assignmentKey(topic, partition)]
	if ok {
		a.Replicas = append([]string(nil), a.Replicas...)
	}
	return a, ok
}

func (c *Cluster) Assignments() []PartitionAssignment {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]PartitionAssignment, 0, len(c.assignments))
	for _, a := range c.assignments {
		a.Replicas = append([]string(nil), a.Replicas...)
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic == out[j].Topic {
			return out[i].Partition < out[j].Partition
		}
		return out[i].Topic < out[j].Topic
	})
	return out
}

func assignmentKey(topic string, partition int) string { return topic + ":" + itoa(partition) }
