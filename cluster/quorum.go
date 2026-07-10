package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// QuorumQueue is a replicated queue that requires majority acknowledgment.
type QuorumQueue struct {
	mu sync.RWMutex

	name       string
	cluster    *Cluster
	replicas   []string
	quorumSize int

	// State
	messages   []ReplicatedMessage
	committed  uint64
	leader     string

	// Replication
	pending    map[uint64]*pendingReplication
	pendingMu  sync.RWMutex

	// Callbacks
	onReplicated func(msg *ReplicatedMessage)

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// ReplicatedMessage represents a message with replication metadata.
type ReplicatedMessage struct {
	ID        string    `json:"id"`
	Index     uint64    `json:"index"`
	Term      uint64    `json:"term"`
	Data      []byte    `json:"data"`
	Headers   map[string]string `json:"headers,omitempty"`
	Replicas  []string  `json:"replicas"`
	Acks      int       `json:"acks"`
	Committed bool      `json:"committed"`
	CreatedAt time.Time `json:"created_at"`
}

type pendingReplication struct {
	msg       *ReplicatedMessage
	ackCount  int
	replicas  []string
	startedAt time.Time
}

// QuorumQueueConfig holds configuration for a quorum queue.
type QuorumQueueConfig struct {
	Name     string
	Cluster  *Cluster
	Replicas []string
}

// NewQuorumQueue creates a new quorum queue.
func NewQuorumQueue(cfg QuorumQueueConfig) *QuorumQueue {
	if len(cfg.Replicas) == 0 {
		cfg.Replicas = []string{cfg.Cluster.ID()}
	}

	quorumSize := (len(cfg.Replicas) / 2) + 1

	qq := &QuorumQueue{
		name:       cfg.Name,
		cluster:    cfg.Cluster,
		replicas:   cfg.Replicas,
		quorumSize: quorumSize,
		messages:   make([]ReplicatedMessage, 0),
		pending:    make(map[uint64]*pendingReplication),
		stopCh:     make(chan struct{}),
	}

	return qq
}

// Start starts the quorum queue.
func (qq *QuorumQueue) Start() error {
	qq.wg.Add(1)
	go qq.replicationLoop()

	log.Printf("[quorum:%s] started with %d replicas, quorum=%d", qq.name, len(qq.replicas), qq.quorumSize)
	return nil
}

// Stop stops the quorum queue.
func (qq *QuorumQueue) Stop() {
	close(qq.stopCh)
	qq.wg.Wait()
}

// Publish publishes a message to the quorum queue.
func (qq *QuorumQueue) Publish(data []byte, headers map[string]string) (*ReplicatedMessage, error) {
	qq.mu.Lock()
	defer qq.mu.Unlock()

	if !qq.cluster.IsLeader() {
		return nil, ErrNotLeader
	}

	term := qq.cluster.Raft().CurrentTerm()

	// Create message
	msg := &ReplicatedMessage{
		ID:        fmt.Sprintf("qq-%d-%d", term, len(qq.messages)+1),
		Index:     uint64(len(qq.messages) + 1),
		Term:      term,
		Data:      data,
		Headers:   headers,
		Replicas:  qq.replicas,
		CreatedAt: time.Now(),
	}

	// Submit to Raft for consensus
	command, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	_, err = qq.cluster.Submit(command)
	if err != nil {
		return nil, err
	}

	// Wait for replication
	pending := &pendingReplication{
		msg:       msg,
		replicas:  qq.replicas,
		startedAt: time.Now(),
	}

	qq.pendingMu.Lock()
	qq.pending[msg.Index] = pending
	qq.pendingMu.Unlock()

	return msg, nil
}

// replicationLoop handles replication of committed entries.
func (qq *QuorumQueue) replicationLoop() {
	defer qq.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-qq.stopCh:
			return
		case <-ticker.C:
			qq.processCommittedEntries()
		}
	}
}

// processCommittedEntries processes committed entries from Raft.
func (qq *QuorumQueue) processCommittedEntries() {
	commitIndex := qq.cluster.Raft().CommitIndex()
	lastApplied := qq.cluster.Raft().LastApplied()

	if commitIndex <= lastApplied {
		return
	}

	// Apply committed entries
	qq.mu.Lock()
	defer qq.mu.Unlock()

	// In a real implementation, we'd read the entries from the Raft log
	// and apply them. For now, we just update the committed index.
	qq.committed = commitIndex
}

// Ack acknowledges a message replica.
func (qq *QuorumQueue) Ack(messageID string, nodeID string) error {
	qq.mu.Lock()
	defer qq.mu.Unlock()

	// Find the message
	for i := range qq.messages {
		if qq.messages[i].ID == messageID {
			qq.messages[i].Acks++

			if qq.messages[i].Acks >= qq.quorumSize && !qq.messages[i].Committed {
				qq.messages[i].Committed = true
				log.Printf("[quorum:%s] message %s committed with %d acks", qq.name, messageID, qq.messages[i].Acks)

				if qq.onReplicated != nil {
					go qq.onReplicated(&qq.messages[i])
				}
			}
			return nil
		}
	}

	return errors.New("message not found")
}

// Get retrieves a message from the quorum queue.
func (qq *QuorumQueue) Get(limit int) []ReplicatedMessage {
	qq.mu.RLock()
	defer qq.mu.RUnlock()

	if limit <= 0 || limit > len(qq.messages) {
		limit = len(qq.messages)
	}

	result := make([]ReplicatedMessage, limit)
	copy(result, qq.messages[:limit])
	return result
}

// CommitIndex returns the committed index.
func (qq *QuorumQueue) CommitIndex() uint64 {
	qq.mu.RLock()
	defer qq.mu.RUnlock()
	return qq.committed
}

// Stats returns quorum queue statistics.
func (qq *QuorumQueue) Stats() map[string]any {
	qq.mu.RLock()
	defer qq.mu.RUnlock()

	return map[string]any{
		"name":        qq.name,
		"replicas":    qq.replicas,
		"quorum_size": qq.quorumSize,
		"messages":    len(qq.messages),
		"committed":   qq.committed,
		"pending":     len(qq.pending),
	}
}
