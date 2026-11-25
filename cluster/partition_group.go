package cluster

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// PartitionResourceKind identifies the resource whose partition is replicated.
// The empty value is the legacy topic kind and remains wire-compatible with
// existing controller snapshots and partition group IDs.
type PartitionResourceKind string

const (
	PartitionResourceTopic  PartitionResourceKind = "topic"
	PartitionResourceStream PartitionResourceKind = "stream"
	PartitionResourceQueue  PartitionResourceKind = "queue"
)

func (k PartitionResourceKind) normalized() PartitionResourceKind {
	if k == "" {
		return PartitionResourceTopic
	}
	return k
}

// Canonical returns an ID with the legacy empty kind expanded to "topic".
func (p PartitionGroupID) Canonical() PartitionGroupID {
	p.Kind = p.Kind.normalized()
	return p
}

// PartitionGroupID uniquely identifies a resource partition. Topic remains
// the resource name for source compatibility; Kind disambiguates same-named
// topics, streams, and queues.
type PartitionGroupID struct {
	Topic     string                `json:"topic"`
	Partition int                   `json:"partition"`
	Kind      PartitionResourceKind `json:"kind,omitempty"`
}

func (p PartitionGroupID) String() string {
	if kind := p.Kind.normalized(); kind != PartitionResourceTopic {
		return fmt.Sprintf("%s/%s/%d", kind, p.Topic, p.Partition)
	}
	return fmt.Sprintf("%s/%d", p.Topic, p.Partition)
}

func (p PartitionGroupID) MarshalText() ([]byte, error) {
	p = p.Canonical()
	kind := p.Kind.normalized()
	if p.Topic == "" || strings.Contains(p.Topic, "/") || p.Partition < 0 || !utf8.ValidString(p.Topic) ||
		(kind != PartitionResourceTopic && kind != PartitionResourceStream && kind != PartitionResourceQueue) {
		return nil, fmt.Errorf("invalid partition group id: %s", p.String())
	}
	return []byte(p.String()), nil
}

func (p *PartitionGroupID) UnmarshalText(text []byte) error {
	s := string(text)
	parts := strings.Split(s, "/")
	if len(parts) != 2 && len(parts) != 3 {
		return fmt.Errorf("invalid partition group id: %s", s)
	}
	kind := PartitionResourceTopic
	name, partitionText := parts[0], parts[1]
	if len(parts) == 3 {
		kind = PartitionResourceKind(parts[0])
		if kind != PartitionResourceTopic && kind != PartitionResourceStream && kind != PartitionResourceQueue {
			return fmt.Errorf("invalid partition group id: %s", s)
		}
		name, partitionText = parts[1], parts[2]
	}
	if name == "" || !utf8.ValidString(name) {
		return fmt.Errorf("invalid partition group id: %s", s)
	}
	partNum, err := strconv.Atoi(partitionText)
	if err != nil || partNum < 0 {
		return fmt.Errorf("invalid partition group id: %s", s)
	}
	p.Topic = name
	p.Partition = partNum
	p.Kind = kind
	return nil
}

// ReplicaState holds synchronization details for a single partition replica.
type ReplicaState struct {
	NodeID       string    `json:"node_id"`
	LastAckIndex uint64    `json:"last_ack_index"`
	LastAckTime  time.Time `json:"last_ack_time"`
	IsISR        bool      `json:"is_isr"`
}

// ReplicaProgress is a leader-observed Raft replication snapshot for one
// logical replica. Lag is measured in committed log entries behind the
// partition leader's Match index.
type ReplicaProgress struct {
	ReplicaID     string `json:"replica_id"`
	ProgressKnown bool   `json:"progress_known"`
	MatchIndex    uint64 `json:"match_index"`
	Lag           uint64 `json:"lag"`
	RecentActive  bool   `json:"recent_active"`
	InSync        bool   `json:"in_sync"`
}

// PartitionProgress is a point-in-time replication snapshot. It is available
// only from the current Raft leader, whose progress table contains the
// authoritative Match and RecentActive values for every member.
type PartitionProgress struct {
	ID          PartitionGroupID  `json:"id"`
	Leader      string            `json:"leader"`
	LeaderMatch uint64            `json:"leader_match_index"`
	ObservedAt  time.Time         `json:"observed_at"`
	Replicas    []ReplicaProgress `json:"replicas"`
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
	// ReplicaPeerIDs maps stable logical replica IDs to transport-local Raft
	// IDs. It is required when logical IDs differ from peer addresses.
	ReplicaPeerIDs map[string]uint64
}

// PartitionGroup manages consensus, replication and ISR membership for one partition.
type PartitionGroup struct {
	id                 PartitionGroupID
	controller         *RaftController
	rf                 int
	minISR             int
	maxLag             uint64
	timeout            time.Duration
	leaderEpoch        atomic.Uint64
	minISRWaitCount    atomic.Uint64
	minISRWaitFailures atomic.Uint64
	minISRWaitNanos    atomic.Uint64

	mu       sync.RWMutex
	replicas map[string]*ReplicaState
	isr      map[string]struct{}
	peerIDs  map[string]uint64
	closed   atomic.Bool
}

// NewPartitionGroup creates a PartitionGroup backed by etcd/raft.
func NewPartitionGroup(cfg PartitionGroupConfig) (*PartitionGroup, error) {
	if cfg.Controller == nil {
		return nil, fmt.Errorf("partition group %s: RaftController is required", cfg.ID)
	}
	kind := cfg.ID.Kind.normalized()
	if cfg.ID.Topic == "" || strings.Contains(cfg.ID.Topic, "/") || !utf8.ValidString(cfg.ID.Topic) || cfg.ID.Partition < 0 ||
		(kind != PartitionResourceTopic && kind != PartitionResourceStream && kind != PartitionResourceQueue) {
		return nil, fmt.Errorf("%w: topic must be non-empty and partition non-negative", ErrInvalidPartitionAssignment)
	}
	cfg.ID = cfg.ID.Canonical()
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 3
	}
	if cfg.MinISR <= 0 {
		cfg.MinISR = (cfg.ReplicationFactor / 2) + 1
	}
	if cfg.MinISR > cfg.ReplicationFactor {
		return nil, fmt.Errorf("%w: min ISR %d exceeds replication factor %d", ErrInvalidPartitionAssignment, cfg.MinISR, cfg.ReplicationFactor)
	}
	if len(cfg.Replicas) != cfg.ReplicationFactor {
		return nil, fmt.Errorf("%w: replication factor %d does not match %d configured replicas", ErrInvalidPartitionAssignment, cfg.ReplicationFactor, len(cfg.Replicas))
	}
	seenReplicas := make(map[string]struct{}, len(cfg.Replicas))
	for _, replica := range cfg.Replicas {
		if replica == "" {
			return nil, fmt.Errorf("%w: replica ID must not be empty", ErrInvalidPartitionAssignment)
		}
		if _, exists := seenReplicas[replica]; exists {
			return nil, fmt.Errorf("%w: duplicate replica %q", ErrInvalidPartitionAssignment, replica)
		}
		seenReplicas[replica] = struct{}{}
	}
	peerIDs := make(map[string]uint64, len(cfg.ReplicaPeerIDs))
	seenPeerIDs := make(map[uint64]string, len(cfg.ReplicaPeerIDs))
	for replica, peerID := range cfg.ReplicaPeerIDs {
		if _, exists := seenReplicas[replica]; !exists || peerID == 0 {
			return nil, fmt.Errorf("%w: invalid Raft peer mapping for replica %q", ErrInvalidPartitionAssignment, replica)
		}
		if _, exists := cfg.Controller.PeerAddress(peerID); !exists {
			return nil, fmt.Errorf("%w: replica %q maps to unknown Raft peer %d", ErrInvalidPartitionAssignment, replica, peerID)
		}
		if previous, exists := seenPeerIDs[peerID]; exists {
			return nil, fmt.Errorf("%w: replicas %q and %q map to Raft peer %d", ErrInvalidPartitionAssignment, previous, replica, peerID)
		}
		peerIDs[replica] = peerID
		seenPeerIDs[peerID] = replica
	}
	if len(peerIDs) != 0 && len(peerIDs) != len(cfg.Replicas) {
		return nil, fmt.Errorf("%w: Raft peer mapping covers %d of %d replicas", ErrInvalidPartitionAssignment, len(peerIDs), len(cfg.Replicas))
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
		peerIDs:    peerIDs,
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

// MinimumISR returns the configured live-replica quorum required for writes.
func (pg *PartitionGroup) MinimumISR() int {
	return pg.minISR
}

// ReplicationFactor returns the configured number of voting replicas.
func (pg *PartitionGroup) ReplicationFactor() int {
	return pg.rf
}

// MinISRWaitStats reports local leader-side waits for the configured
// post-commit replication acknowledgement policy.
func (pg *PartitionGroup) MinISRWaitStats() (waits, failures uint64, duration time.Duration) {
	return pg.minISRWaitCount.Load(), pg.minISRWaitFailures.Load(), time.Duration(pg.minISRWaitNanos.Load())
}

// LeaderEpoch returns the current leader epoch for fencing.
func (pg *PartitionGroup) LeaderEpoch() uint64 {
	epoch := pg.leaderEpoch.Load()
	if pg.controller != nil && pg.controller.Term() > epoch {
		return pg.controller.Term()
	}
	return epoch
}

// ObserveLeaderEpoch advances the metadata side of the fencing epoch without
// ever moving it backwards. The effective epoch still follows the Raft term.
func (pg *PartitionGroup) ObserveLeaderEpoch(epoch uint64) uint64 {
	for {
		current := pg.leaderEpoch.Load()
		if current >= epoch || pg.leaderEpoch.CompareAndSwap(current, epoch) {
			return pg.LeaderEpoch()
		}
	}
}

// UpdateAssignment replaces the logical replica view after controller
// metadata advances. The caller must have verified the transition against the
// partition Raft group's live membership.
func (pg *PartitionGroup) UpdateAssignment(replicas []string, peerIDs map[string]uint64, epoch uint64) error {
	if pg == nil || pg.closed.Load() {
		return ErrClosed
	}
	if len(replicas) != pg.rf {
		return fmt.Errorf("partition %s assignment has %d replicas; expected %d", pg.id, len(replicas), pg.rf)
	}
	newReplicas := make(map[string]*ReplicaState, len(replicas))
	newISR := make(map[string]struct{}, len(replicas))
	newPeerIDs := make(map[string]uint64, len(replicas))
	seen := make(map[string]struct{}, len(replicas))
	for _, nodeID := range replicas {
		if _, duplicate := seen[nodeID]; duplicate {
			return fmt.Errorf("partition %s assignment repeats replica %q", pg.id, nodeID)
		}
		seen[nodeID] = struct{}{}
		peerID := peerIDs[nodeID]
		if nodeID == "" || peerID == 0 {
			return fmt.Errorf("partition %s assignment has no Raft peer for replica %q", pg.id, nodeID)
		}
		if role := pg.controller.PeerRole(peerID); role != "voter" {
			return fmt.Errorf("partition %s replica %q is not a Raft voter (role %q)", pg.id, nodeID, role)
		}
		newPeerIDs[nodeID] = peerID
	}
	pg.mu.Lock()
	for _, nodeID := range replicas {
		state := pg.replicas[nodeID]
		if state == nil {
			state = &ReplicaState{NodeID: nodeID}
		}
		newReplicas[nodeID] = state
		if state.IsISR {
			newISR[nodeID] = struct{}{}
		}
	}
	pg.replicas = newReplicas
	pg.isr = newISR
	pg.peerIDs = newPeerIDs
	pg.mu.Unlock()
	pg.ObserveLeaderEpoch(epoch)
	pg.refreshRaftReplicaProgress()
	return nil
}

// IncrementEpoch advances the local fencing epoch. In a Raft-backed group,
// LeaderEpoch also follows the Raft term so every election fences old requests.
func (pg *PartitionGroup) IncrementEpoch() uint64 {
	for {
		current := pg.LeaderEpoch()
		stored := pg.leaderEpoch.Load()
		if stored >= current {
			return pg.leaderEpoch.Add(1)
		}
		if pg.leaderEpoch.CompareAndSwap(stored, current+1) {
			return current + 1
		}
	}
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
	_, err := pg.ProposeWithResult(ctx, epoch, data)
	return err
}

// ProposeWithResult submits a fenced command and returns the state machine
// result produced by applying the committed entry on this leader.
func (pg *PartitionGroup) ProposeWithResult(ctx context.Context, epoch uint64, data []byte) (any, error) {
	if pg.closed.Load() {
		return nil, ErrClosed
	}

	term := pg.controller.Term()
	curEpoch := pg.LeaderEpoch()
	if epoch != curEpoch {
		return nil, fmt.Errorf("%w: requested epoch %d does not match current epoch %d", ErrStaleEpoch, epoch, curEpoch)
	}

	if !pg.IsLeader() {
		return nil, ErrNotLeader
	}

	// Refresh the local leader's durable progress and expire replicas whose
	// acknowledgements are older than the configured replication timeout.
	pg.refreshRaftReplicaProgress()
	pg.mu.Lock()
	pg.refreshLeaderProgressLocked(time.Now())
	pg.expireStaleReplicasLocked(time.Now())
	currentISRCount := len(pg.isr)
	pg.mu.Unlock()

	if currentISRCount < pg.minISR {
		return nil, fmt.Errorf("%w: active ISR count %d < min ISR %d", ErrQuorumNotMet, currentISRCount, pg.minISR)
	}

	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, pg.timeout)
	defer cancel()

	result, err := pg.controller.ProposeAtTermWithResult(ctx, term, data)
	if err != nil {
		return nil, err
	}
	if err := pg.waitForISRCommit(ctx, pg.controller.AppliedIndex()); err != nil {
		return nil, err
	}
	pg.refreshRaftReplicaProgress()
	return result, nil
}

// waitForISRCommit strengthens Raft's majority-commit guarantee when the
// configured MinISR is greater than the Raft quorum. A successful client
// acknowledgement then means every required assigned voter has durably
// replicated at least the committed entry.
func (pg *PartitionGroup) waitForISRCommit(ctx context.Context, index uint64) error {
	if pg.minISR <= 1 {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	started := time.Now()
	waiting := false
	finishWait := func(failed bool) {
		if !waiting {
			return
		}
		pg.minISRWaitNanos.Add(uint64(time.Since(started)))
		if failed {
			pg.minISRWaitFailures.Add(1)
		}
	}
	for {
		count := pg.replicasReplicatedAt(index)
		if count >= pg.minISR {
			finishWait(false)
			return nil
		}
		if !waiting {
			waiting = true
			pg.minISRWaitCount.Add(1)
		}
		select {
		case <-ctx.Done():
			finishWait(true)
			return fmt.Errorf("%w: only %d of %d replicas durably replicated index %d before deadline: %v", ErrQuorumNotMet, count, pg.minISR, index, ctx.Err())
		case <-ticker.C:
			if pg.closed.Load() {
				finishWait(true)
				return ErrClosed
			}
			if !pg.controller.IsLeader() {
				finishWait(true)
				return ErrNotLeader
			}
		}
	}
}

func (pg *PartitionGroup) replicasReplicatedAt(index uint64) int {
	pg.mu.RLock()
	peerIDs := make(map[string]uint64, len(pg.peerIDs))
	for replica, peerID := range pg.peerIDs {
		peerIDs[replica] = peerID
	}
	pg.mu.RUnlock()
	if len(peerIDs) == 0 {
		return len(pg.ISR())
	}
	status := pg.controller.Status()
	leaderID := pg.controller.LeaderID()
	count := 0
	for _, peerID := range peerIDs {
		if pg.controller.PeerRole(peerID) != "voter" {
			continue
		}
		if peerID == leaderID {
			if pg.controller.AppliedIndex() >= index {
				count++
			}
			continue
		}
		progress, ok := status.Progress[peerID]
		if ok && progress.RecentActive && progress.Match >= index {
			count++
		}
	}
	return count
}

// refreshRaftReplicaProgress derives ISR state from the local leader's Raft
// progress table. Raft's Match index and RecentActive bit are the authoritative
// evidence for replication and liveness; callers do not need to report progress
// through a separate, potentially stale control-plane API.
func (pg *PartitionGroup) refreshRaftReplicaProgress() {
	if pg.controller == nil || !pg.controller.IsLeader() {
		return
	}
	pg.mu.RLock()
	hasPeerIDs := len(pg.peerIDs) > 0
	pg.mu.RUnlock()
	if !hasPeerIDs {
		return
	}
	status := pg.controller.Status()
	leaderID := pg.controller.LeaderID()
	leaderProgress, ok := status.Progress[leaderID]
	if !ok {
		return
	}
	now := time.Now()
	pg.mu.Lock()
	defer pg.mu.Unlock()
	for peerID, progress := range status.Progress {
		replicaID, ok := pg.replicaForPeerID(peerID)
		if !ok {
			continue
		}
		state := pg.replicas[replicaID]
		if state == nil {
			continue
		}
		if progress.Match > state.LastAckIndex {
			state.LastAckIndex = progress.Match
		}
		active := peerID == leaderID || progress.RecentActive
		if active {
			state.LastAckTime = now
		}
		lag := uint64(0)
		if leaderProgress.Match > progress.Match {
			lag = leaderProgress.Match - progress.Match
		}
		state.IsISR = active && lag <= pg.maxLag
		if state.IsISR {
			pg.isr[replicaID] = struct{}{}
		} else {
			delete(pg.isr, replicaID)
		}
	}
	pg.expireStaleReplicasLocked(now)
}

func (pg *PartitionGroup) refreshLeaderProgressLocked(now time.Time) {
	if !pg.controller.IsLeader() {
		return
	}
	leaderID := pg.controller.LeaderID()
	if leaderID == 0 {
		return
	}
	leaderNodeID, ok := pg.replicaForPeerID(leaderID)
	if !ok {
		return
	}
	state := pg.replicas[leaderNodeID]
	if state == nil {
		return
	}
	if applied := pg.controller.AppliedIndex(); applied > state.LastAckIndex {
		state.LastAckIndex = applied
	}
	state.LastAckTime = now
	state.IsISR = true
	pg.isr[leaderNodeID] = struct{}{}
}

func (pg *PartitionGroup) replicaForPeerID(peerID uint64) (string, bool) {
	for replica, mappedPeerID := range pg.peerIDs {
		if mappedPeerID == peerID {
			return replica, true
		}
	}
	// Keep compatibility with callers whose replica IDs are the transport
	// addresses, while allowing controller metadata to use stable logical IDs.
	address, ok := pg.controller.PeerAddress(peerID)
	if !ok {
		return "", false
	}
	if _, exists := pg.replicas[address]; !exists {
		return "", false
	}
	return address, true
}

func (pg *PartitionGroup) expireStaleReplicasLocked(now time.Time) {
	for nodeID, state := range pg.replicas {
		if state.IsISR && now.Sub(state.LastAckTime) > pg.timeout {
			state.IsISR = false
			delete(pg.isr, nodeID)
		}
	}
}

// UpdateReplicaProgress records the latest acknowledged log index for a replica
// and adjusts ISR membership accordingly.
func (pg *PartitionGroup) UpdateReplicaProgress(nodeID string, lastIndex uint64) error {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	pg.refreshLeaderProgressLocked(time.Now())

	state, exists := pg.replicas[nodeID]
	if !exists {
		return ErrUnknownReplica
	}
	if lastIndex < state.LastAckIndex {
		return ErrReplicaProgressRegression
	}
	leaderID := pg.controller.LeaderID()
	if leaderID == 0 {
		return ErrNotLeader
	}
	leaderNodeID, ok := pg.replicaForPeerID(leaderID)
	if !ok {
		return ErrUnknownReplica
	}
	leaderState, ok := pg.replicas[leaderNodeID]
	if !ok {
		return ErrUnknownReplica
	}

	leaderIndex := leaderState.LastAckIndex
	if nodeID == leaderNodeID {
		leaderIndex = lastIndex
	} else if lastIndex > leaderIndex {
		return ErrReplicaProgressAhead
	}
	state.LastAckIndex = lastIndex
	state.LastAckTime = time.Now()

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
	return nil
}

// ISR returns a list of node IDs currently in the in-sync replica set.
func (pg *PartitionGroup) ISR() []string {
	pg.refreshRaftReplicaProgress()
	pg.mu.Lock()
	defer pg.mu.Unlock()
	now := time.Now()
	pg.refreshLeaderProgressLocked(now)
	pg.expireStaleReplicasLocked(now)

	out := make([]string, 0, len(pg.isr))
	for node := range pg.isr {
		out = append(out, node)
	}
	sort.Strings(out)
	return out
}

// LiveISRForPeers evaluates Raft liveness for an assignment that may include
// newly promoted reassignment targets not yet present in the group's committed
// logical replica map. The local Raft leader's progress table is authoritative.
func (pg *PartitionGroup) LiveISRForPeers(peerIDs map[string]uint64) []string {
	if pg == nil || pg.controller == nil || !pg.controller.IsLeader() || len(peerIDs) == 0 {
		return nil
	}
	status := pg.controller.Status()
	leaderID := pg.controller.LeaderID()
	leaderProgress, ok := status.Progress[leaderID]
	if !ok {
		return nil
	}
	var out []string
	for replica, peerID := range peerIDs {
		progress, exists := status.Progress[peerID]
		if !exists || (peerID != leaderID && !progress.RecentActive) {
			continue
		}
		lag := uint64(0)
		if leaderProgress.Match > progress.Match {
			lag = leaderProgress.Match - progress.Match
		}
		if lag <= pg.maxLag {
			out = append(out, replica)
		}
	}
	sort.Strings(out)
	return out
}

// LiveProgress returns authoritative per-replica log lag from the local Raft
// leader. Followers return nil because their Raft status does not contain the
// leader's complete peer progress table.
func (pg *PartitionGroup) LiveProgress() *PartitionProgress {
	if pg == nil || pg.controller == nil || !pg.controller.IsLeader() {
		return nil
	}
	pg.mu.RLock()
	peerIDs := make(map[string]uint64, len(pg.peerIDs))
	for replicaID, peerID := range pg.peerIDs {
		peerIDs[replicaID] = peerID
	}
	pg.mu.RUnlock()
	if len(peerIDs) == 0 {
		return nil
	}
	status := pg.controller.Status()
	leaderID := pg.controller.LeaderID()
	leader, ok := status.Progress[leaderID]
	if !ok || leaderID == 0 {
		return nil
	}
	progress := &PartitionProgress{
		ID: pg.id, LeaderMatch: leader.Match, ObservedAt: time.Now().UTC(),
		Replicas: make([]ReplicaProgress, 0, len(peerIDs)),
	}
	for replicaID, peerID := range peerIDs {
		peer, exists := status.Progress[peerID]
		lag := uint64(0)
		if exists && leader.Match > peer.Match {
			lag = leader.Match - peer.Match
		}
		active := exists && (peerID == leaderID || peer.RecentActive)
		progress.Replicas = append(progress.Replicas, ReplicaProgress{
			ReplicaID: replicaID, ProgressKnown: exists, MatchIndex: peer.Match, Lag: lag,
			RecentActive: active, InSync: active && lag <= pg.maxLag,
		})
		if peerID == leaderID {
			progress.Leader = replicaID
		}
	}
	if progress.Leader == "" {
		return nil
	}
	sort.Slice(progress.Replicas, func(i, j int) bool {
		return progress.Replicas[i].ReplicaID < progress.Replicas[j].ReplicaID
	})
	return progress
}

// Replicas returns a snapshot of all known replica states.
func (pg *PartitionGroup) Replicas() []*ReplicaState {
	pg.refreshRaftReplicaProgress()
	pg.mu.Lock()
	defer pg.mu.Unlock()
	now := time.Now()
	pg.refreshLeaderProgressLocked(now)
	pg.expireStaleReplicasLocked(now)

	out := make([]*ReplicaState, 0, len(pg.replicas))
	for _, r := range pg.replicas {
		copyState := *r
		out = append(out, &copyState)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// Close marks the partition group as closed.
func (pg *PartitionGroup) Close() error {
	pg.closed.Store(true)
	return nil
}
