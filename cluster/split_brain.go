package cluster

import (
	"log"
	"sync"
	"time"
)

// SplitBrainDetector detects and handles split-brain scenarios.
type SplitBrainDetector struct {
	mu sync.RWMutex

	cluster       *Cluster
	checkInterval time.Duration
	quorumSize    int

	// State
	lastCheck    time.Time
	lastQuorum   bool
	partitions   []Partition
	onSplitBrain func(partition Partition)

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// Partition represents a network partition.
type Partition struct {
	ID        string     `json:"id"`
	Nodes     []string   `json:"nodes"`
	StartTime time.Time  `json:"start_time"`
	EndTime   *time.Time `json:"end_time,omitempty"`
	Resolved  bool       `json:"resolved"`
}

// SplitBrainConfig holds configuration for split-brain detection.
type SplitBrainConfig struct {
	Cluster       *Cluster
	CheckInterval time.Duration
	OnSplitBrain  func(partition Partition)
}

// NewSplitBrainDetector creates a new split-brain detector.
func NewSplitBrainDetector(cfg SplitBrainConfig) *SplitBrainDetector {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 5 * time.Second
	}

	var quorumSize int
	if cfg.Cluster.Raft() != nil {
		quorumSize = cfg.Cluster.Raft().QuorumSize()
	} else {
		// Default quorum size
		quorumSize = 1
	}

	return &SplitBrainDetector{
		cluster:       cfg.Cluster,
		checkInterval: cfg.CheckInterval,
		quorumSize:    quorumSize,
		partitions:    make([]Partition, 0),
		onSplitBrain:  cfg.OnSplitBrain,
		stopCh:        make(chan struct{}),
	}
}

// Start starts the split-brain detector.
func (d *SplitBrainDetector) Start() error {
	d.wg.Add(1)
	go d.run()
	return nil
}

// Stop stops the split-brain detector.
func (d *SplitBrainDetector) Stop() {
	close(d.stopCh)
	d.wg.Wait()
}

// run is the main detection loop.
func (d *SplitBrainDetector) run() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.check()
		}
	}
}

// check performs a split-brain check.
func (d *SplitBrainDetector) check() {
	d.mu.Lock()
	defer d.mu.Unlock()

	nodes := d.cluster.ActiveNodes()
	leader := d.cluster.Leader()
	selfID := d.cluster.ID()

	// Check 1: Do we have quorum?
	quorum := len(nodes) >= d.quorumSize
	d.lastQuorum = quorum

	if !quorum {
		d.handleQuorumLost(nodes)
		return
	}

	// Check 2: Is there a leader?
	if leader == "" {
		d.handleNoLeader(nodes)
		return
	}

	// Check 3: Can we reach the leader?
	canReachLeader := leader == selfID
	if !canReachLeader {
		for _, node := range nodes {
			if node.ID == leader {
				canReachLeader = true
				break
			}
		}
	}

	if !canReachLeader {
		d.handlePartitionDetected(nodes, leader)
		return
	}

	// All good - resolve any existing partitions
	d.resolvePartitions()
}

// handleNoLeader handles the case where there's no leader.
func (d *SplitBrainDetector) handleNoLeader(nodes []*NodeInfo) {
	log.Printf("[split-brain] no leader detected with %d active nodes", len(nodes))

	// If we have quorum, trigger an election
	if len(nodes) >= d.quorumSize {
		log.Printf("[split-brain] triggering election to restore leadership")
		// The Raft layer will handle this automatically via election timeout
	}
}

// handleQuorumLost handles the case where quorum is lost.
func (d *SplitBrainDetector) handleQuorumLost(nodes []*NodeInfo) {
	log.Printf("[split-brain] quorum lost: %d active nodes, need %d", len(nodes), d.quorumSize)

	partition := Partition{
		ID:        time.Now().Format("20060102150405"),
		Nodes:     extractNodeIDs(nodes),
		StartTime: time.Now(),
	}

	d.partitions = append(d.partitions, partition)

	if d.onSplitBrain != nil {
		go d.onSplitBrain(partition)
	}
}

// handlePartitionDetected handles a detected partition.
func (d *SplitBrainDetector) handlePartitionDetected(nodes []*NodeInfo, leader string) {
	log.Printf("[split-brain] partition detected: leader %s not reachable", leader)

	partition := Partition{
		ID:        time.Now().Format("20060102150405"),
		Nodes:     extractNodeIDs(nodes),
		StartTime: time.Now(),
	}

	d.partitions = append(d.partitions, partition)

	if d.onSplitBrain != nil {
		go d.onSplitBrain(partition)
	}
}

// resolvePartitions resolves existing partitions.
func (d *SplitBrainDetector) resolvePartitions() {
	for i := range d.partitions {
		if !d.partitions[i].Resolved {
			now := time.Now()
			d.partitions[i].EndTime = &now
			d.partitions[i].Resolved = true
			log.Printf("[split-brain] partition %s resolved", d.partitions[i].ID)
		}
	}
}

// GetPartitions returns all detected partitions.
func (d *SplitBrainDetector) GetPartitions() []Partition {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make([]Partition, len(d.partitions))
	copy(result, d.partitions)
	return result
}

// HasQuorum returns true if the cluster currently has quorum.
func (d *SplitBrainDetector) HasQuorum() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastQuorum
}

// FencingToken is used to prevent stale leaders from acting.
type FencingToken struct {
	mu    sync.RWMutex
	token uint64
	epoch uint64
}

// NewFencingToken creates a new fencing token.
func NewFencingToken() *FencingToken {
	return &FencingToken{
		token: 0,
		epoch: 0,
	}
}

// Increment increments the fencing token.
func (ft *FencingToken) Increment() uint64 {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.token++
	return ft.token
}

// Validate validates a fencing token.
func (ft *FencingToken) Validate(token uint64) bool {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	return token >= ft.token
}

// Current returns the current fencing token.
func (ft *FencingToken) Current() uint64 {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	return ft.token
}

// Epoch returns the current epoch.
func (ft *FencingToken) Epoch() uint64 {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	return ft.epoch
}

// IncrementEpoch increments the epoch.
func (ft *FencingToken) IncrementEpoch() uint64 {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.epoch++
	ft.token = ft.epoch << 32
	return ft.epoch
}

// extractNodeIDs extracts IDs from a list of nodes.
func extractNodeIDs(nodes []*NodeInfo) []string {
	ids := make([]string, len(nodes))
	for i, node := range nodes {
		ids[i] = node.ID
	}
	return ids
}
