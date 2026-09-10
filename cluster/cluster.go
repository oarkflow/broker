package cluster

import (
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"
)

// Cluster manages the Raft cluster and provides a high-level API.
type Cluster struct {
	mu sync.RWMutex

	// Node information
	id        string
	addr      string
	raft      *Raft
	transport Transport

	// State
	state  RaftState
	leader string
	term   uint64

	// Node registry
	nodes   map[string]*NodeInfo
	nodesMu sync.RWMutex

	// Health checking
	healthTicker  *time.Ticker
	healthTimeout time.Duration

	// Callbacks
	onLeaderChange func(leader string)
	onNodeJoin     func(node *NodeInfo)
	onNodeLeave    func(nodeID string)
	onNodeFail     func(nodeID string)

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NodeInfo contains information about a cluster node.
type NodeInfo struct {
	ID        string            `json:"id"`
	Address   string            `json:"address"`
	State     NodeState         `json:"state"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	JoinedAt  time.Time         `json:"joined_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	LastSeen  time.Time         `json:"last_seen"`
	IsLeader  bool              `json:"is_leader"`
}

// ClusterConfig holds configuration for the cluster.
type ClusterConfig struct {
	ID             string
	Address        string
	Peers          []string
	Transport      Transport
	StateMachine   StateMachine
	HealthTimeout  time.Duration
	OnLeaderChange func(leader string)
	OnNodeJoin     func(node *NodeInfo)
	OnNodeLeave    func(nodeID string)
	OnNodeFail     func(nodeID string)
}

// NewCluster creates a new cluster instance.
func NewCluster(cfg ClusterConfig) *Cluster {
	if cfg.HealthTimeout == 0 {
		cfg.HealthTimeout = 10 * time.Second
	}

	c := &Cluster{
		id:             cfg.ID,
		addr:           cfg.Address,
		transport:      cfg.Transport,
		nodes:          make(map[string]*NodeInfo),
		healthTimeout:  cfg.HealthTimeout,
		onLeaderChange: cfg.OnLeaderChange,
		onNodeJoin:     cfg.OnNodeJoin,
		onNodeLeave:    cfg.OnNodeLeave,
		onNodeFail:     cfg.OnNodeFail,
		stopCh:         make(chan struct{}),
	}

	// Create Raft instance
	raftCfg := RaftConfig{
		ID:             cfg.ID,
		Peers:          cfg.Peers,
		Transport:      cfg.Transport,
		StateMachine:   cfg.StateMachine,
		OnStateChange:  c.onRaftStateChange,
		OnLeaderChange: c.onRaftLeaderChange,
	}
	c.raft = NewRaft(raftCfg)

	// Register self
	c.nodes[c.id] = &NodeInfo{
		ID:        c.id,
		Address:   cfg.Address,
		State:     NodeActive,
		JoinedAt:  time.Now(),
		UpdatedAt: time.Now(),
		LastSeen:  time.Now(),
	}

	return c
}

// Start starts the cluster.
func (c *Cluster) Start() error {
	// Start Raft
	if err := c.raft.Start(); err != nil {
		return err
	}

	// Start health checker
	c.wg.Add(1)
	go c.healthChecker()

	// Start message handler
	if err := c.transport.Listen(c.addr, c.handleMessage); err != nil {
		return err
	}

	log.Printf("[cluster:%s] started on %s", c.id, c.addr)
	return nil
}

// Stop stops the cluster.
func (c *Cluster) Stop() error {
	close(c.stopCh)
	c.raft.Stop()
	c.transport.Close()
	c.wg.Wait()
	return nil
}

// healthChecker periodically checks node health.
func (c *Cluster) healthChecker() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.healthTimeout / 3)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.checkHealth()
		}
	}
}

// checkHealth checks the health of all nodes.
func (c *Cluster) checkHealth() {
	now := time.Now()

	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()

	for id, node := range c.nodes {
		if id == c.id {
			continue
		}

		if node.State == NodeActive && now.Sub(node.LastSeen) > c.healthTimeout {
			log.Printf("[cluster:%s] node %s unhealthy (last seen %v ago)", c.id, id, now.Sub(node.LastSeen))
			node.State = NodeFailed
			node.UpdatedAt = now

			if c.onNodeFail != nil {
				go c.onNodeFail(id)
			}
		}
	}
}

// handleMessage processes incoming messages.
func (c *Cluster) handleMessage(from string, msg *RaftMessage) {
	// Update last seen time
	c.nodesMu.Lock()
	if node, ok := c.nodes[from]; ok {
		node.LastSeen = time.Now()
		if node.State == NodeFailed {
			node.State = NodeActive
			node.UpdatedAt = time.Now()
			log.Printf("[cluster:%s] node %s recovered", c.id, from)
		}
	}
	c.nodesMu.Unlock()

	// Forward to Raft
	c.raft.HandleMessage(from, msg)
}

// onRaftStateChange handles Raft state changes.
func (c *Cluster) onRaftStateChange(state RaftState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = state
	log.Printf("[cluster:%s] state changed to %s", c.id, state)
}

// onRaftLeaderChange handles leader changes.
func (c *Cluster) onRaftLeaderChange(leader string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = leader

	// Update node info
	c.nodesMu.Lock()
	for _, node := range c.nodes {
		node.IsLeader = node.ID == leader
	}
	c.nodesMu.Unlock()

	log.Printf("[cluster:%s] leader changed to %s", c.id, leader)

	if c.onLeaderChange != nil {
		go c.onLeaderChange(leader)
	}
}

// Join adds a new node to the cluster.
func (c *Cluster) Join(node NodeInfo) error {
	if node.ID == "" {
		return errors.New("node ID required")
	}

	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()

	now := time.Now()
	if existing, ok := c.nodes[node.ID]; ok {
		existing.Address = node.Address
		existing.State = NodeActive
		existing.UpdatedAt = now
		existing.LastSeen = now
		if node.Metadata != nil {
			existing.Metadata = node.Metadata
		}
	} else {
		node.State = NodeActive
		node.JoinedAt = now
		node.UpdatedAt = now
		node.LastSeen = now
		c.nodes[node.ID] = &node

		if c.onNodeJoin != nil {
			go c.onNodeJoin(&node)
		}
	}

	log.Printf("[cluster:%s] node %s joined", c.id, node.ID)
	return nil
}

// Leave removes a node from the cluster.
func (c *Cluster) Leave(nodeID string) error {
	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()

	node, ok := c.nodes[nodeID]
	if !ok {
		return ErrNodeNotFound
	}

	node.State = NodeLeft
	node.UpdatedAt = time.Now()

	if c.onNodeLeave != nil {
		go c.onNodeLeave(nodeID)
	}

	log.Printf("[cluster:%s] node %s left", c.id, nodeID)
	return nil
}

// Nodes returns all nodes in the cluster.
func (c *Cluster) Nodes() []*NodeInfo {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(c.nodes))
	for _, node := range c.nodes {
		n := *node
		nodes = append(nodes, &n)
	}
	return nodes
}

// ActiveNodes returns all active nodes.
func (c *Cluster) ActiveNodes() []*NodeInfo {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()

	nodes := make([]*NodeInfo, 0)
	for _, node := range c.nodes {
		if node.State == NodeActive {
			n := *node
			nodes = append(nodes, &n)
		}
	}
	return nodes
}

// Leader returns the current leader.
func (c *Cluster) Leader() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader
}

// IsLeader returns true if this node is the leader.
func (c *Cluster) IsLeader() bool {
	return c.raft.IsLeader()
}

// State returns the current cluster state.
func (c *Cluster) State() RaftState {
	return c.raft.State()
}

// ID returns the node ID.
func (c *Cluster) ID() string {
	return c.id
}

// Submit submits a command to the Raft cluster.
func (c *Cluster) Submit(command []byte) (uint64, error) {
	return c.raft.Submit(command)
}

// Raft returns the underlying Raft instance.
func (c *Cluster) Raft() *Raft {
	return c.raft
}

// Stats returns cluster statistics.
func (c *Cluster) Stats() map[string]any {
	c.mu.RLock()
	leader := c.leader
	state := c.state
	c.mu.RUnlock()

	c.nodesMu.RLock()
	nodeCount := len(c.nodes)
	activeCount := 0
	for _, node := range c.nodes {
		if node.State == NodeActive {
			activeCount++
		}
	}
	c.nodesMu.RUnlock()

	return map[string]any{
		"id":           c.id,
		"leader":       leader,
		"state":        state.String(),
		"term":         c.raft.CurrentTerm(),
		"commit_index": c.raft.CommitIndex(),
		"last_applied": c.raft.LastApplied(),
		"node_count":   nodeCount,
		"active_nodes": activeCount,
		"quorum_size":  c.raft.QuorumSize(),
	}
}

// ClusterState represents the serialized cluster state.
type ClusterState struct {
	Nodes     []*NodeInfo `json:"nodes"`
	Leader    string      `json:"leader"`
	Term      uint64      `json:"term"`
	Committed uint64      `json:"committed"`
}

// MarshalJSON implements json.Marshaler.
func (c *Cluster) MarshalJSON() ([]byte, error) {
	c.mu.RLock()
	leader := c.leader
	c.mu.RUnlock()

	nodes := c.Nodes()

	return json.Marshal(map[string]any{
		"id":     c.id,
		"leader": leader,
		"nodes":  nodes,
		"stats":  c.Stats(),
	})
}
