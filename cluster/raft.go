package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ConfigChangeType represents the type of configuration change.
type ConfigChangeType uint8

const (
	ConfigAdd    ConfigChangeType = 0
	ConfigRemove ConfigChangeType = 1
)

// ConfigChange is the payload for EntryConfig log entries.
type ConfigChange struct {
	Type   ConfigChangeType `json:"type"`
	PeerID string           `json:"peer_id"`
}

// Raft implements the Raft consensus algorithm.
type Raft struct {
	mu sync.RWMutex

	// Persistent state on all servers
	currentTerm uint64
	votedFor    string
	log         []LogEntry

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64
	state       RaftState
	leaderID    string

	// Volatile state on leaders
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Cluster information
	id         string
	peers      []string
	quorumSize int

	// Components
	transport    Transport
	snapshot     *Snapshot
	stateMachine StateMachine
	wal          *RaftWAL

	// Channels
	heartbeatCh chan struct{}
	voteCh      chan bool
	commitCh    chan uint64

	// Control
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running atomic.Bool

	// Configuration
	electionTimeout   time.Duration
	heartbeatTimeout  time.Duration
	snapshotThreshold uint64
	lastSnapshotIndex uint64

	// ReadIndex
	readIndexCh chan uint64

	// Callbacks
	onStateChange  func(state RaftState)
	onLeaderChange func(leader string)
	onConfigChange func(peers []string)
}

// RaftConfig holds configuration for a Raft node.
type RaftConfig struct {
	ID                string
	Peers             []string
	Transport         Transport
	StateMachine      StateMachine
	WAL               *RaftWAL
	ElectionTimeout   time.Duration
	HeartbeatTimeout  time.Duration
	SnapshotThreshold uint64
	OnStateChange     func(state RaftState)
	OnLeaderChange    func(leader string)
	OnConfigChange    func(peers []string)
}

// NewRaft creates a new Raft instance.
func NewRaft(cfg RaftConfig) *Raft {
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 150 * time.Millisecond
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 50 * time.Millisecond
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 1000
	}

	quorumSize := (len(cfg.Peers) / 2) + 1

	r := &Raft{
		id:                cfg.ID,
		peers:             cfg.Peers,
		quorumSize:        quorumSize,
		transport:         cfg.Transport,
		stateMachine:      cfg.StateMachine,
		wal:               cfg.WAL,
		state:             StateFollower,
		nextIndex:         make(map[string]uint64),
		matchIndex:        make(map[string]uint64),
		heartbeatCh:       make(chan struct{}, 1),
		voteCh:            make(chan bool, 1),
		commitCh:          make(chan uint64, 100),
		readIndexCh:       make(chan uint64, 100),
		stopCh:            make(chan struct{}),
		electionTimeout:   cfg.ElectionTimeout,
		heartbeatTimeout:  cfg.HeartbeatTimeout,
		snapshotThreshold: cfg.SnapshotThreshold,
		onStateChange:     cfg.OnStateChange,
		onLeaderChange:    cfg.OnLeaderChange,
		onConfigChange:    cfg.OnConfigChange,
	}

	return r
}

// Start starts the Raft node.
func (r *Raft) Start() error {
	if r.running.Swap(true) {
		return errors.New("already running")
	}

	// Replay WAL to restore log if present
	if r.wal != nil {
		if err := r.replayWAL(); err != nil {
			return fmt.Errorf("raft: replay WAL: %w", err)
		}
	}

	// Register our message handler with the transport so incoming
	// Raft messages (RequestVote, AppendEntries, etc.) are routed to us.
	r.transport.Listen("", r.HandleMessage)

	log.Printf("[raft:%s] starting as follower", r.id)

	r.wg.Add(1)
	go r.run()

	return nil
}

// replayWAL restores the in-memory log from the WAL on startup.
func (r *Raft) replayWAL() error {
	firstIdx := r.wal.FirstIndex()
	lastIdx := r.wal.LastIndex()

	if firstIdx == 0 || lastIdx == 0 {
		return nil
	}

	entries, err := r.wal.ReadRange(firstIdx, lastIdx)
	if err != nil {
		return err
	}

	r.log = make([]LogEntry, 0, len(entries))
	for _, entry := range entries {
		r.log = append(r.log, *entry)
	}

	// Find the snapshot entry if any (last snapshot marker)
	for i := len(r.log) - 1; i >= 0; i-- {
		if r.log[i].Type == EntrySnapshot {
			r.lastSnapshotIndex = r.log[i].Index
			break
		}
	}

	log.Printf("[raft:%s] replayed WAL: %d entries, index [%d, %d]", r.id, len(r.log), firstIdx, lastIdx)
	return nil
}

// Stop stops the Raft node.
func (r *Raft) Stop() {
	if !r.running.Swap(false) {
		return
	}
	close(r.stopCh)
	r.wg.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = StateDead

	// Close WAL
	if r.wal != nil {
		r.wal.Close()
	}
}

// run is the main loop for the Raft node.
func (r *Raft) run() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		switch r.getState() {
		case StateFollower:
			r.runFollower()
		case StateCandidate:
			r.runCandidate()
		case StateLeader:
			r.runLeader()
		}
	}
}

// runFollower runs the follower state loop.
func (r *Raft) runFollower() {
	timeout := r.randomTimeout()

	for {
		select {
		case <-r.stopCh:
			return
		case <-r.heartbeatCh:
			// Reset timeout on heartbeat
			timeout = r.randomTimeout()
		case <-time.After(timeout):
			// Election timeout - become candidate
			log.Printf("[raft:%s] election timeout, becoming candidate", r.id)
			r.mu.Lock()
			r.setState(StateCandidate)
			r.mu.Unlock()
			return
		}
	}
}

// runCandidate runs the candidate state loop.
func (r *Raft) runCandidate() {
	r.mu.Lock()
	r.currentTerm++
	r.votedFor = r.id
	term := r.currentTerm
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := r.getLastLogTerm()
	r.mu.Unlock()

	log.Printf("[raft:%s] becoming candidate for term %d", r.id, term)

	// Vote for self
	votes := 1

	// Request votes from all peers
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}

		go func(peerID string) {
			resp, err := r.transport.SendRequestVote(peerID, &RequestVoteRequest{
				Term:         term,
				CandidateID:  r.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				return
			}

			if resp.Term > term {
				r.mu.Lock()
				if r.currentTerm < resp.Term {
					r.currentTerm = resp.Term
					r.votedFor = ""
					r.setState(StateFollower)
				}
				r.mu.Unlock()
				return
			}

			if resp.VoteGranted {
				select {
				case r.voteCh <- true:
				default:
				}
			}
		}(peer)
	}

	timeout := r.randomTimeout()

	for {
		select {
		case <-r.stopCh:
			return
		case <-r.voteCh:
			votes++
			if votes >= r.quorumSize {
				log.Printf("[raft:%s] won election for term %d with %d votes", r.id, term, votes)
				r.becomeLeader()
				return
			}
		case <-time.After(timeout):
			// Election timeout - restart election
			log.Printf("[raft:%s] election timeout in term %d", r.id, term)
			return
		}
	}
}

// becomeLeader transitions to leader state.
func (r *Raft) becomeLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state = StateLeader
	r.leaderID = r.id

	// Initialize nextIndex and matchIndex for all peers
	lastLogIndex := r.getLastLogIndex()
	for _, peer := range r.peers {
		if peer != r.id {
			r.nextIndex[peer] = lastLogIndex + 1
			r.matchIndex[peer] = 0
		}
	}

	if r.onStateChange != nil {
		go r.onStateChange(StateLeader)
	}
	if r.onLeaderChange != nil {
		go r.onLeaderChange(r.id)
	}
}

// runLeader runs the leader state loop.
func (r *Raft) runLeader() {
	ticker := time.NewTicker(r.heartbeatTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.sendHeartbeats()
		}
	}
}

// sendHeartbeats sends heartbeats to all peers.
func (r *Raft) sendHeartbeats() {
	r.mu.RLock()
	term := r.currentTerm
	leaderID := r.id
	commitIndex := r.commitIndex
	peers := make([]string, 0, len(r.peers)-1)
	for _, p := range r.peers {
		if p != r.id {
			peers = append(peers, p)
		}
	}
	r.mu.RUnlock()

	for _, peer := range peers {
		go func(peerID string) {
			r.sendAppendEntriesTo(peerID, term, leaderID, commitIndex)
		}(peer)
	}
}

// sendAppendEntriesTo sends AppendEntries to a specific peer.
func (r *Raft) sendAppendEntriesTo(peerID string, term uint64, leaderID string, commitIndex uint64) {
	r.mu.RLock()
	nextIdx := r.nextIndex[peerID]
	prevLogIndex := nextIdx - 1
	prevLogTerm := r.getTermAt(prevLogIndex)

	// Get entries to send
	var entries []LogEntry
	if nextIdx <= r.getLastLogIndex() {
		entries = make([]LogEntry, 0, r.getLastLogIndex()-nextIdx+1)
		for i := nextIdx; i <= r.getLastLogIndex(); i++ {
			entries = append(entries, r.log[i-1]) // log is 1-indexed in Raft, 0-indexed in slice
		}
	}
	r.mu.RUnlock()

	resp, err := r.transport.SendAppendEntries(peerID, &AppendEntriesRequest{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	})
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.votedFor = ""
		r.setState(StateFollower)
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex
		if len(entries) > 0 {
			r.nextIndex[peerID] = entries[len(entries)-1].Index + 1
			r.matchIndex[peerID] = entries[len(entries)-1].Index
		}
	} else {
		// Decrement nextIndex for fast backup
		if resp.LastLogIndex > 0 {
			r.nextIndex[peerID] = resp.LastLogIndex + 1
		} else {
			r.nextIndex[peerID] = max(1, r.nextIndex[peerID]-1)
		}
	}

	// Try to advance commit index
	r.advanceCommitIndexLocked()
}

// advanceCommitIndexLocked tries to advance the commit index.
// Must be called with r.mu held.
func (r *Raft) advanceCommitIndexLocked() {
	for n := r.commitIndex + 1; n <= r.getLastLogIndex(); n++ {
		if r.log[n-1].Term != r.currentTerm {
			continue
		}

		count := 1 // count self
		for _, peer := range r.peers {
			if peer != r.id && r.matchIndex[peer] >= n {
				count++
			}
		}

		if count >= r.quorumSize {
			r.commitIndex = n
			r.applyCommittedEntries()
		}
	}
}

// applyCommittedEntries applies committed entries to the state machine.
func (r *Raft) applyCommittedEntries() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		entry := &r.log[r.lastApplied-1]
		if entry.Type == EntryConfig {
			r.applyConfigChange(entry)
		}
		if r.stateMachine != nil {
			if err := r.stateMachine.Apply(entry); err != nil {
				log.Printf("[raft:%s] failed to apply entry %d: %v", r.id, r.lastApplied, err)
			}
		}
	}

	// Auto-snapshot and log compaction
	if r.lastApplied-r.lastSnapshotIndex >= r.snapshotThreshold {
		r.takeSnapshot()
	}
}

// takeSnapshot takes a snapshot of the current state and compacts the log.
func (r *Raft) takeSnapshot() {
	if r.stateMachine == nil {
		return
	}

	data, err := r.stateMachine.Snapshot()
	if err != nil {
		log.Printf("[raft:%s] snapshot failed: %v", r.id, err)
		return
	}

	lastIncludedIndex := r.lastApplied
	lastIncludedTerm := r.getTermAt(lastIncludedIndex)

	snapshot := &Snapshot{
		Term:              r.currentTerm,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}

	// Write snapshot to WAL
	if r.wal != nil {
		if err := r.wal.Snapshot(snapshot); err != nil {
			log.Printf("[raft:%s] wal snapshot error: %v", r.id, err)
			return
		}
		if err := r.wal.Compact(lastIncludedIndex + 1); err != nil {
			log.Printf("[raft:%s] wal compact error: %v", r.id, err)
		}
	}

	// Trim in-memory log
	r.log = r.log[:0]
	r.lastSnapshotIndex = lastIncludedIndex
	r.snapshot = snapshot

	log.Printf("[raft:%s] snapshot taken at index %d, term %d", r.id, lastIncludedIndex, r.currentTerm)
}

// HandleMessage processes an incoming Raft message.
func (r *Raft) HandleMessage(from string, msg *RaftMessage) {
	switch msg.Type {
	case MsgAppendEntries:
		r.handleAppendEntries(from, msg.AppendEntries)
	case MsgAppendEntriesResp:
		r.handleAppendEntriesResp(from, msg.AppendEntriesResp)
	case MsgRequestVote:
		r.handleRequestVote(from, msg.RequestVote)
	case MsgRequestVoteResp:
		r.handleRequestVoteResp(from, msg.RequestVoteResp)
	case MsgInstallSnapshot:
		r.handleInstallSnapshot(from, msg.InstallSnapshot)
	case MsgInstallSnapshotResp:
		r.handleInstallSnapshotResp(from, msg.InstallSnapshotResp)
	}
}

// handleAppendEntries processes an AppendEntries request.
func (r *Raft) handleAppendEntries(from string, req *AppendEntriesRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}

	// Reply false if term < currentTerm
	if req.Term < r.currentTerm {
		r.sendAppendEntriesResp(from, resp)
		return
	}

	// We have a leader
	r.leaderID = req.LeaderID

	// Reset election timeout
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.votedFor = ""
	}

	// If we're candidate, revert to follower
	if r.state == StateCandidate {
		r.setState(StateFollower)
	}

	// Signal heartbeat
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}

	// Check if log contains entry at prevLogIndex with prevLogTerm
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(r.log)) {
			r.sendAppendEntriesResp(from, resp)
			return
		}
		if r.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			// Delete conflicting entries
			r.log = r.log[:req.PrevLogIndex-1]
			r.sendAppendEntriesResp(from, resp)
			return
		}
	}

	// Append new entries
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		if idx-1 < uint64(len(r.log)) {
			if r.log[idx-1].Term != entry.Term {
				r.log = r.log[:idx-1]
				r.log = append(r.log, entry)
				r.walAppend(&entry)
			}
		} else {
			r.log = append(r.log, entry)
			r.walAppend(&entry)
		}
	}

	// Update commit index
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, r.getLastLogIndex())
		r.applyCommittedEntries()
	}

	resp.Success = true
	resp.LastLogIndex = r.getLastLogIndex()
	r.sendAppendEntriesResp(from, resp)
}

// handleAppendEntriesResp processes an AppendEntries response.
func (r *Raft) handleAppendEntriesResp(from string, resp *AppendEntriesResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.votedFor = ""
		r.setState(StateFollower)
		return
	}

	if r.state != StateLeader {
		return
	}

	if resp.Success {
		if resp.LastLogIndex > r.matchIndex[from] {
			r.matchIndex[from] = resp.LastLogIndex
			r.nextIndex[from] = resp.LastLogIndex + 1
			r.advanceCommitIndexLocked()
		}
	} else {
		// Decrement nextIndex
		r.nextIndex[from] = max(1, r.nextIndex[from]-1)
	}
}

// handleRequestVote processes a RequestVote request.
func (r *Raft) handleRequestVote(from string, req *RequestVoteRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}

	// Reply false if term < currentTerm
	if req.Term < r.currentTerm {
		r.sendRequestVoteResp(from, resp)
		return
	}

	// Update term
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.votedFor = ""
		r.setState(StateFollower)
	}

	// Check if we can vote for this candidate
	canVote := r.votedFor == "" || r.votedFor == from
	if !canVote {
		r.sendRequestVoteResp(from, resp)
		return
	}

	// Check if candidate's log is at least as up-to-date
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := r.getLastLogTerm()

	if req.LastLogTerm < lastLogTerm {
		r.sendRequestVoteResp(from, resp)
		return
	}
	if req.LastLogTerm == lastLogTerm && req.LastLogIndex < lastLogIndex {
		r.sendRequestVoteResp(from, resp)
		return
	}

	// Grant vote
	r.votedFor = from
	resp.VoteGranted = true

	// Reset election timeout
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}

	r.sendRequestVoteResp(from, resp)
}

// handleRequestVoteResp processes a RequestVote response.
func (r *Raft) handleRequestVoteResp(from string, resp *RequestVoteResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.votedFor = ""
		r.setState(StateFollower)
		return
	}

	if resp.VoteGranted && r.state == StateCandidate {
		select {
		case r.voteCh <- true:
		default:
		}
	}
}

// handleInstallSnapshot processes an InstallSnapshot request.
func (r *Raft) handleInstallSnapshot(from string, req *InstallSnapshotRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.currentTerm {
		r.sendInstallSnapshotResp(from, &InstallSnapshotResponse{Term: r.currentTerm})
		return
	}

	// Apply snapshot
	if r.stateMachine != nil {
		if err := r.stateMachine.Restore(req.Data); err != nil {
			log.Printf("[raft:%s] failed to restore snapshot: %v", r.id, err)
		}
	}

	// Update state
	r.lastApplied = req.LastIncludedIndex
	r.commitIndex = req.LastIncludedIndex
	r.lastSnapshotIndex = req.LastIncludedIndex

	// Trim log
	r.log = r.log[:0]

	// Write snapshot marker to WAL and compact
	if r.wal != nil {
		snapshot := &Snapshot{
			Term:              req.Term,
			LastIncludedIndex: req.LastIncludedIndex,
			LastIncludedTerm:  req.LastIncludedTerm,
			Data:              req.Data,
		}
		if err := r.wal.Snapshot(snapshot); err != nil {
			log.Printf("[raft:%s] wal snapshot error: %v", r.id, err)
		}
	}

	r.sendInstallSnapshotResp(from, &InstallSnapshotResponse{Term: r.currentTerm})
}

// walAppend writes an entry to the WAL if present. Must be called with r.mu held.
func (r *Raft) walAppend(entry *LogEntry) {
	if r.wal != nil {
		if err := r.wal.Append(entry); err != nil {
			log.Printf("[raft:%s] wal append error at index %d: %v", r.id, entry.Index, err)
		}
	}
}

// handleInstallSnapshotResp processes an InstallSnapshot response.
func (r *Raft) handleInstallSnapshotResp(from string, resp *InstallSnapshotResponse) {
	// Snapshot is applied, nothing more to do
}

// Helper methods

func (r *Raft) getState() RaftState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *Raft) setState(state RaftState) {
	r.state = state
	if r.onStateChange != nil {
		go r.onStateChange(state)
	}
}

func (r *Raft) getLastLogIndex() uint64 {
	return uint64(len(r.log))
}

func (r *Raft) getLastLogTerm() uint64 {
	if len(r.log) == 0 {
		return 0
	}
	return r.log[len(r.log)-1].Term
}

func (r *Raft) getTermAt(index uint64) uint64 {
	if index == 0 || index > uint64(len(r.log)) {
		return 0
	}
	return r.log[index-1].Term
}

func (r *Raft) randomTimeout() time.Duration {
	delta := time.Duration(rand.Int63n(int64(r.electionTimeout)))
	return r.electionTimeout + delta
}

// AppendEntry appends a new entry to the log (only works on leader).
func (r *Raft) AppendEntry(data []byte) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateLeader {
		return 0, ErrNotLeader
	}

	entry := LogEntry{
		Term:  r.currentTerm,
		Index: r.getLastLogIndex() + 1,
		Type:  EntryNormal,
		Data:  data,
	}

	r.log = append(r.log, entry)

	// Persist to WAL
	if r.wal != nil {
		if err := r.wal.Append(&entry); err != nil {
			log.Printf("[raft:%s] wal append error: %v", r.id, err)
		}
	}

	// Trigger replication
	go r.sendHeartbeats()

	return entry.Index, nil
}

// Submit submits a command to the Raft cluster.
func (r *Raft) Submit(command []byte) (uint64, error) {
	return r.AppendEntry(command)
}

// ReadIndex returns the current commit index after confirming leadership.
// If this node is not the leader, it returns ErrNotLeader.
// For linearizable reads, the caller should wait until its local state machine
// has applied up to the returned index before serving the read.
func (r *Raft) ReadIndex() (uint64, error) {
	r.mu.RLock()
	if r.state != StateLeader {
		r.mu.RUnlock()
		return 0, ErrNotLeader
	}
	term := r.currentTerm
	leaderID := r.id
	commitIndex := r.commitIndex
	peers := make([]string, 0, len(r.peers)-1)
	for _, p := range r.peers {
		if p != r.id {
			peers = append(peers, p)
		}
	}
	r.mu.RUnlock()

	// Confirm leadership by sending empty AppendEntries to a majority
	responses := make(chan bool, len(peers))
	for _, peer := range peers {
		go func(peerID string) {
			resp, err := r.transport.SendAppendEntries(peerID, &AppendEntriesRequest{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: commitIndex,
			})
			if err != nil {
				responses <- false
				return
			}
			if resp.Term > term {
				r.mu.Lock()
				if r.currentTerm < resp.Term {
					r.currentTerm = resp.Term
					r.votedFor = ""
					r.setState(StateFollower)
				}
				r.mu.Unlock()
				responses <- false
				return
			}
			responses <- resp.Success
		}(peer)
	}

	// Count confirmations (self + peers)
	confirmations := 1 // self
	for i := 0; i < len(peers); i++ {
		if <-responses {
			confirmations++
		}
	}

	r.mu.RLock()
	quorumSize := r.quorumSize
	r.mu.RUnlock()

	if confirmations < quorumSize {
		return 0, ErrNotLeader
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.commitIndex, nil
}

// AddPeer submits a configuration change to add a peer to the cluster.
func (r *Raft) AddPeer(peerID string) error {
	r.mu.Lock()
	if r.state != StateLeader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	for _, p := range r.peers {
		if p == peerID {
			r.mu.Unlock()
			return fmt.Errorf("peer %s already exists", peerID)
		}
	}
	r.mu.Unlock()

	cfgData, err := json.Marshal(&ConfigChange{Type: ConfigAdd, PeerID: peerID})
	if err != nil {
		return fmt.Errorf("marshal config change: %w", err)
	}

	entry := LogEntry{
		Term:  r.CurrentTerm(),
		Index: 0, // assigned by AppendEntry
		Type:  EntryConfig,
		Data:  cfgData,
	}

	_, err = r.AppendEntry(entry.Data)
	return err
}

// RemovePeer submits a configuration change to remove a peer from the cluster.
func (r *Raft) RemovePeer(peerID string) error {
	r.mu.Lock()
	if r.state != StateLeader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	if peerID == r.id {
		r.mu.Unlock()
		return fmt.Errorf("cannot remove self from cluster")
	}
	found := false
	for _, p := range r.peers {
		if p == peerID {
			found = true
			break
		}
	}
	if !found {
		r.mu.Unlock()
		return fmt.Errorf("peer %s not found", peerID)
	}
	r.mu.Unlock()

	cfgData, err := json.Marshal(&ConfigChange{Type: ConfigRemove, PeerID: peerID})
	if err != nil {
		return fmt.Errorf("marshal config change: %w", err)
	}

	entry := LogEntry{
		Term:  r.CurrentTerm(),
		Index: 0,
		Type:  EntryConfig,
		Data:  cfgData,
	}

	_, err = r.AppendEntry(entry.Data)
	return err
}

// applyConfigChange applies a configuration change entry. Must be called with r.mu held.
func (r *Raft) applyConfigChange(entry *LogEntry) {
	var cfg ConfigChange
	if err := json.Unmarshal(entry.Data, &cfg); err != nil {
		log.Printf("[raft:%s] failed to unmarshal config change: %v", r.id, err)
		return
	}

	switch cfg.Type {
	case ConfigAdd:
		// Check for duplicate
		for _, p := range r.peers {
			if p == cfg.PeerID {
				return
			}
		}
		r.peers = append(r.peers, cfg.PeerID)
	case ConfigRemove:
		for i, p := range r.peers {
			if p == cfg.PeerID {
				r.peers = append(r.peers[:i], r.peers[i+1:]...)
				delete(r.nextIndex, cfg.PeerID)
				delete(r.matchIndex, cfg.PeerID)
				break
			}
		}
	}

	r.quorumSize = (len(r.peers) / 2) + 1

	if r.onConfigChange != nil {
		go r.onConfigChange(r.Peers())
	}

	log.Printf("[raft:%s] config change applied: peers=%v quorum=%d", r.id, r.peers, r.quorumSize)
}

// GetEntry reads a log entry by index from the in-memory log.
func (r *Raft) GetEntry(index uint64) (*LogEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if index == 0 || index > uint64(len(r.log)) {
		return nil, fmt.Errorf("index %d out of range [1, %d]", index, len(r.log))
	}

	entry := r.log[index-1]
	return &entry, nil
}

// Leader returns the current leader ID.
func (r *Raft) Leader() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderID
}

// IsLeader returns true if this node is the leader.
func (r *Raft) IsLeader() bool {
	return r.getState() == StateLeader
}

// State returns the current Raft state.
func (r *Raft) State() RaftState {
	return r.getState()
}

// CurrentTerm returns the current term.
func (r *Raft) CurrentTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

// CommitIndex returns the current commit index.
func (r *Raft) CommitIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.commitIndex
}

// LastApplied returns the last applied index.
func (r *Raft) LastApplied() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastApplied
}

// QuorumSize returns the quorum size.
func (r *Raft) QuorumSize() int {
	return r.quorumSize
}

// Peers returns the list of peers.
func (r *Raft) Peers() []string {
	return append([]string(nil), r.peers...)
}

// sendAppendEntriesResp sends an AppendEntries response.
func (r *Raft) sendAppendEntriesResp(to string, resp *AppendEntriesResponse) {
	r.transport.Send(to, &RaftMessage{
		Type:              MsgAppendEntriesResp,
		AppendEntriesResp: resp,
	})
}

// sendRequestVoteResp sends a RequestVote response.
func (r *Raft) sendRequestVoteResp(to string, resp *RequestVoteResponse) {
	r.transport.Send(to, &RaftMessage{
		Type:            MsgRequestVoteResp,
		RequestVoteResp: resp,
	})
}

// sendInstallSnapshotResp sends an InstallSnapshot response.
func (r *Raft) sendInstallSnapshotResp(to string, resp *InstallSnapshotResponse) {
	r.transport.Send(to, &RaftMessage{
		Type:                MsgInstallSnapshotResp,
		InstallSnapshotResp: resp,
	})
}

// max returns the larger of two uint64 values.
func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// min returns the smaller of two uint64 values.
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// JSON serialization helpers for Raft messages.
func (m *RaftMessage) MarshalJSON() ([]byte, error) {
	type Alias RaftMessage
	return json.Marshal(struct {
		*Alias
	}{
		Alias: (*Alias)(m),
	})
}

func (m *RaftMessage) UnmarshalJSON(data []byte) error {
	type Alias RaftMessage
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(m),
	}
	return json.Unmarshal(data, aux)
}

// Ensure io.Writer is used.
var _ io.Writer = (*discardWriter)(nil)

type discardWriter struct{}

func (d *discardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
