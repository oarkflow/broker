package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// EtcdRaftCoreConfig configures the consensus core shared by controller and
// partition groups.
type EtcdRaftCoreConfig struct {
	ID                        uint64
	Peers                     []uint64
	ElectionTick              int
	HeartbeatTick             int
	MaxMessageBytes           uint64
	MaxInflightMessages       int
	MaxUncommittedEntryBytes  uint64
	DisableProposalForwarding bool
	StorageDir                string
	SkipBootstrap             bool
}

// EtcdRaftCore is the audited etcd/raft state machine foundation.
type EtcdRaftCore struct {
	RawNode *etcdraft.RawNode
	Storage *etcdraft.MemoryStorage
	durable *persistentRaftStorage
}

func NewEtcdRaftCore(cfg EtcdRaftCoreConfig) (*EtcdRaftCore, error) {
	if cfg.ID == 0 {
		return nil, fmt.Errorf("etcd raft: node ID must be non-zero")
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = 1
	}
	if cfg.ElectionTick <= cfg.HeartbeatTick {
		cfg.ElectionTick = 10 * cfg.HeartbeatTick
	}
	if cfg.MaxMessageBytes == 0 {
		cfg.MaxMessageBytes = 4 << 20
	}
	if cfg.MaxInflightMessages <= 0 {
		cfg.MaxInflightMessages = 256
	}
	if cfg.MaxUncommittedEntryBytes == 0 {
		cfg.MaxUncommittedEntryBytes = 64 << 20
	}
	durable, err := openPersistentRaftStorage(cfg.StorageDir)
	if err != nil {
		return nil, err
	}
	storage := durable.memory
	snapshot, err := storage.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("etcd raft: read initial snapshot: %w", err)
	}
	raw, err := etcdraft.NewRawNode(&etcdraft.Config{
		ID:                        cfg.ID,
		ElectionTick:              cfg.ElectionTick,
		HeartbeatTick:             cfg.HeartbeatTick,
		Storage:                   storage,
		MaxSizePerMsg:             cfg.MaxMessageBytes,
		MaxCommittedSizePerReady:  cfg.MaxMessageBytes,
		MaxUncommittedEntriesSize: cfg.MaxUncommittedEntryBytes,
		MaxInflightMsgs:           cfg.MaxInflightMessages,
		MaxInflightBytes:          cfg.MaxMessageBytes * uint64(cfg.MaxInflightMessages),
		CheckQuorum:               true,
		PreVote:                   true,
		ReadOnlyOption:            etcdraft.ReadOnlySafe,
		DisableProposalForwarding: cfg.DisableProposalForwarding,
		Applied:                   snapshot.Metadata.GetIndex(),
	})
	if err != nil {
		return nil, err
	}
	if len(cfg.Peers) > 0 && !durable.loaded && !cfg.SkipBootstrap {
		peers := make([]etcdraft.Peer, 0, len(cfg.Peers))
		seen := make(map[uint64]struct{}, len(cfg.Peers))
		for _, id := range cfg.Peers {
			if id == 0 {
				return nil, fmt.Errorf("etcd raft: invalid peer ID %d", id)
			}
			if _, exists := seen[id]; exists {
				return nil, fmt.Errorf("etcd raft: duplicate peer ID %d", id)
			}
			seen[id] = struct{}{}
			peers = append(peers, etcdraft.Peer{ID: id})
		}
		if err := raw.Bootstrap(peers); err != nil {
			return nil, err
		}
	}
	return &EtcdRaftCore{RawNode: raw, Storage: storage, durable: durable}, nil
}

// RaftControllerConfig configures an active RaftController.
type RaftControllerConfig struct {
	ID                        uint64
	GroupID                   string
	NodeID                    string
	Peers                     map[uint64]string // ID -> address/nodeID
	ElectionTick              int
	HeartbeatTick             int
	TickInterval              time.Duration
	ReadIndexTimeout          time.Duration
	MaxMessageBytes           uint64
	MaxInflightMessages       int
	MaxUncommittedEntryBytes  uint64
	DisableProposalForwarding bool
	Transport                 Transport
	StateMachine              StateMachine
	StorageDir                string
	SkipBootstrap             bool
	// SnapshotEntries controls automatic state-machine snapshots and Raft log
	// compaction. Zero uses 10,000 committed entries.
	SnapshotEntries uint64
	// SnapshotCatchUpEntries are retained behind the snapshot for lagging peers.
	// Zero uses 1,024 entries.
	SnapshotCatchUpEntries uint64
}

// RaftController runs an etcd/raft consensus engine with network transport,
// local state machine application, and linearizable command proposals.
type RaftController struct {
	id                uint64
	groupID           string
	nodeID            string
	core              *EtcdRaftCore
	transport         Transport
	sm                StateMachine
	peers             map[uint64]string
	peerIDs           map[string]uint64
	peersMu           sync.RWMutex
	proposals         map[uint64]chan raftProposalResult
	propMu            sync.Mutex
	propSeq           atomic.Uint64
	readSeq           atomic.Uint64
	readMu            sync.Mutex
	readWaiters       map[uint64]chan error
	pendingReadStates []etcdraft.ReadState
	nodeMu            sync.Mutex
	stopCh            chan struct{}
	doneCh            chan struct{}
	tickDur           time.Duration
	readIndexTimeout  time.Duration
	leaderID          atomic.Uint64
	term              atomic.Uint64
	running           atomic.Bool
	promotionRunning  atomic.Bool
	fatalMu           sync.RWMutex
	fatalErr          error
	confMu            sync.Mutex
	confState         *raftpb.ConfState
	appliedIndex      uint64
	lastSnapshotIndex uint64
	snapshotEntries   uint64
	snapshotCatchUp   uint64
}

type raftProposalResult struct {
	value any
	err   error
}

// NewRaftController creates a complete Raft controller backed by etcd/raft.
func NewRaftController(cfg RaftControllerConfig) (*RaftController, error) {
	if cfg.ID == 0 {
		return nil, fmt.Errorf("etcd controller: ID must be non-zero")
	}
	if len(cfg.GroupID) > MaxRaftGroupIDBytes || !utf8.ValidString(cfg.GroupID) {
		return nil, fmt.Errorf("etcd controller: group ID must be valid UTF-8 and at most %d bytes", MaxRaftGroupIDBytes)
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 50 * time.Millisecond
	}
	if cfg.ReadIndexTimeout <= 0 {
		cfg.ReadIndexTimeout = 5 * time.Second
	}
	if cfg.SnapshotEntries == 0 {
		cfg.SnapshotEntries = 10_000
	}
	if cfg.SnapshotCatchUpEntries == 0 {
		cfg.SnapshotCatchUpEntries = 1_024
	}

	peerIDs := make([]uint64, 0, len(cfg.Peers))
	peerMap := make(map[uint64]string, len(cfg.Peers))
	peerRev := make(map[string]uint64, len(cfg.Peers))

	for id, addr := range cfg.Peers {
		peerIDs = append(peerIDs, id)
		peerMap[id] = addr
		peerRev[addr] = id
	}

	storageDir := cfg.StorageDir
	if cfg.GroupID != "" && storageDir != "" {
		digest := sha256.Sum256([]byte(cfg.GroupID))
		storageDir = filepath.Join(storageDir, "groups", hex.EncodeToString(digest[:16]))
	}
	core, err := NewEtcdRaftCore(EtcdRaftCoreConfig{
		ID:                        cfg.ID,
		Peers:                     peerIDs,
		ElectionTick:              cfg.ElectionTick,
		HeartbeatTick:             cfg.HeartbeatTick,
		MaxMessageBytes:           cfg.MaxMessageBytes,
		MaxInflightMessages:       cfg.MaxInflightMessages,
		MaxUncommittedEntryBytes:  cfg.MaxUncommittedEntryBytes,
		DisableProposalForwarding: cfg.DisableProposalForwarding,
		StorageDir:                storageDir,
		SkipBootstrap:             cfg.SkipBootstrap,
	})
	if err != nil {
		return nil, err
	}
	initialSnapshot, err := core.Storage.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("etcd controller: read snapshot: %w", err)
	}
	if !etcdraft.IsEmptySnap(initialSnapshot) && cfg.StateMachine != nil {
		if err := cfg.StateMachine.Restore(initialSnapshot.Data); err != nil {
			return nil, fmt.Errorf("etcd controller: restore snapshot: %w", err)
		}
	}

	c := &RaftController{
		id:                cfg.ID,
		groupID:           cfg.GroupID,
		nodeID:            cfg.NodeID,
		core:              core,
		transport:         cfg.Transport,
		sm:                cfg.StateMachine,
		peers:             peerMap,
		peerIDs:           peerRev,
		proposals:         make(map[uint64]chan raftProposalResult),
		readWaiters:       make(map[uint64]chan error),
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
		tickDur:           cfg.TickInterval,
		readIndexTimeout:  cfg.ReadIndexTimeout,
		confState:         initialSnapshot.Metadata.GetConfState(),
		appliedIndex:      initialSnapshot.Metadata.GetIndex(),
		lastSnapshotIndex: initialSnapshot.Metadata.GetIndex(),
		snapshotEntries:   cfg.SnapshotEntries,
		snapshotCatchUp:   cfg.SnapshotCatchUpEntries,
	}

	return c, nil
}

// Start launches the background event and tick loops.
func (c *RaftController) Start() error {
	if !c.running.CompareAndSwap(false, true) {
		return nil
	}
	go c.run()
	return nil
}

// Stop cleanly terminates the controller.
func (c *RaftController) Stop() error {
	if !c.running.CompareAndSwap(true, false) {
		return nil
	}
	close(c.stopCh)
	<-c.doneCh
	// Wait for synchronous operations that passed their running check before
	// Stop flipped it to false. New operations are fenced under nodeMu.
	c.nodeMu.Lock()
	c.nodeMu.Unlock()
	c.propMu.Lock()
	for seq, ch := range c.proposals {
		select {
		case ch <- raftProposalResult{err: ErrClosed}:
		default:
		}
		delete(c.proposals, seq)
	}
	c.propMu.Unlock()
	c.failReadWaiters(ErrClosed)
	return nil
}

// Step handles an incoming etcd/raft message received from a peer.
func (c *RaftController) Step(ctx context.Context, msg *raftpb.Message) error {
	if msg == nil {
		return errors.New("cannot step nil message")
	}
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	if err := c.operationalErrorLocked(); err != nil {
		return err
	}
	err := c.core.RawNode.Step(msg)
	if c.core.RawNode.HasReady() {
		readyErr := c.handleReadyLocked()
		if readyErr != nil {
			c.setFatalError(readyErr)
		}
		err = errors.Join(err, readyErr)
	}
	return err
}

// HandleMessage decodes and processes an incoming transport message carrying etcd/raft frames.
func (c *RaftController) HandleMessage(from string, msg *RaftMessage) error {
	if msg == nil || msg.Type != MsgEtcdRaft || len(msg.EtcdMessage) == 0 {
		return nil
	}
	if len(msg.GroupID) > MaxRaftGroupIDBytes || !utf8.ValidString(msg.GroupID) {
		return fmt.Errorf("Raft message group ID is invalid or exceeds %d bytes", MaxRaftGroupIDBytes)
	}
	if msg.GroupID != c.groupID {
		return fmt.Errorf("Raft message group %q does not match controller group %q", msg.GroupID, c.groupID)
	}
	var m raftpb.Message
	if err := proto.Unmarshal(msg.EtcdMessage, &m); err != nil {
		return err
	}
	c.peersMu.RLock()
	peerID, knownPeer := c.peerIDs[from]
	c.peersMu.RUnlock()
	if !knownPeer {
		return fmt.Errorf("Raft message arrived from unknown transport peer %q", from)
	}
	if m.GetFrom() != peerID {
		return fmt.Errorf("Raft message sender %d does not match authenticated peer %q (node %d)", m.GetFrom(), from, peerID)
	}
	if m.GetTo() != c.id {
		return fmt.Errorf("Raft message destination %d does not match local node %d", m.GetTo(), c.id)
	}
	return c.Step(context.Background(), &m)
}

// RegisterTransportHandler attaches this controller to a network transport.
func (c *RaftController) RegisterTransportHandler(addr string) error {
	if c.transport == nil {
		return nil
	}
	return c.transport.Listen(addr, func(from string, msg *RaftMessage) {
		if err := c.HandleMessage(from, msg); err != nil {
			log.Printf("[raft] rejected peer message from %q: %v", from, err)
		}
	})
}

// Campaign forces the Raft controller to campaign for leadership.
func (c *RaftController) Campaign() error {
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	if err := c.operationalErrorLocked(); err != nil {
		return err
	}
	err := c.core.RawNode.Campaign()
	if c.core.RawNode.HasReady() {
		readyErr := c.handleReadyLocked()
		if readyErr != nil {
			c.setFatalError(readyErr)
		}
		err = errors.Join(err, readyErr)
	}
	return err
}

func (c *RaftController) run() {
	defer close(c.doneCh)
	ticker := time.NewTicker(c.tickDur)
	defer ticker.Stop()

	for {
		if c.FatalError() != nil {
			return
		}
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.nodeMu.Lock()
			if c.FatalError() != nil {
				c.nodeMu.Unlock()
				return
			}
			c.core.RawNode.Tick()
			if c.core.RawNode.HasReady() {
				if err := c.handleReadyLocked(); err != nil {
					c.nodeMu.Unlock()
					c.setFatalError(err)
					return
				}
			}
			c.nodeMu.Unlock()
			c.scheduleCaughtUpLearnerPromotion()
		default:
		}

		c.nodeMu.Lock()
		if c.FatalError() != nil {
			c.nodeMu.Unlock()
			return
		}
		if c.core.RawNode.HasReady() {
			if err := c.handleReadyLocked(); err != nil {
				c.nodeMu.Unlock()
				c.setFatalError(err)
				return
			}
			c.nodeMu.Unlock()
		} else {
			c.nodeMu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}
}

func (c *RaftController) scheduleCaughtUpLearnerPromotion() {
	if !c.running.Load() || !c.IsLeader() || !c.promotionRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.promotionRunning.Store(false)
		c.promoteCaughtUpLearner()
	}()
}

func (c *RaftController) promoteCaughtUpLearner() {
	if !c.confMu.TryLock() {
		return
	}
	defer c.confMu.Unlock()
	if !c.running.Load() || !c.IsLeader() {
		return
	}
	status := c.Status()
	if status.Config.AutoLeave || status.HardState == nil {
		return
	}
	learnerIDs := make([]uint64, 0, len(status.Config.Learners))
	for id := range status.Config.Learners {
		learnerIDs = append(learnerIDs, id)
	}
	sort.Slice(learnerIDs, func(i, j int) bool { return learnerIDs[i] < learnerIDs[j] })
	commitIndex := status.HardState.GetCommit()
	for _, id := range learnerIDs {
		progress, ok := status.Progress[id]
		if !ok || progress.Match < commitIndex {
			continue
		}
		changeType := raftpb.ConfChangeAddNode
		transition := raftpb.ConfChangeTransition_ConfChangeTransitionJointImplicit
		change := &raftpb.ConfChangeV2{
			Transition: &transition,
			Changes:    []*raftpb.ConfChangeSingle{{Type: &changeType, NodeId: &id}},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.proposeConfChangeV2(ctx, change); err != nil && !errors.Is(err, ErrConfChangePending) && !errors.Is(err, ErrClosed) {
			log.Printf("[raft] auto-promote caught-up learner %d failed: %v", id, err)
		}
		cancel()
		return
	}
}

func (c *RaftController) handleReadyLocked() error {
	rd := c.core.RawNode.Ready()
	// Raft's durability contract requires HardState, entries, and snapshots to
	// be synced before messages are sent or committed entries are applied.
	if err := c.core.durable.persistReady(rd); err != nil {
		return fmt.Errorf("persist raft ready: %w", err)
	}

	if rd.SoftState != nil {
		c.leaderID.Store(rd.SoftState.Lead)
	}

	if rd.HardState != nil && !etcdraft.IsEmptyHardState(rd.HardState) {
		c.term.Store(rd.HardState.GetTerm())
	}

	// Send messages to peers via transport
	if len(rd.Messages) > 0 {
		for _, m := range rd.Messages {
			c.sendMessage(m)
		}
	}

	// Install a received snapshot before applying entries that follow it.
	if rd.Snapshot != nil && !etcdraft.IsEmptySnap(rd.Snapshot) {
		if c.sm != nil {
			if err := c.sm.Restore(rd.Snapshot.Data); err != nil {
				return fmt.Errorf("restore raft snapshot: %w", err)
			}
		}
		c.confState = rd.Snapshot.Metadata.GetConfState()
		if rd.Snapshot.Metadata.GetIndex() > c.appliedIndex {
			c.appliedIndex = rd.Snapshot.Metadata.GetIndex()
		}
		c.lastSnapshotIndex = rd.Snapshot.Metadata.GetIndex()
	}

	// Apply committed entries to state machine.
	if len(rd.CommittedEntries) > 0 {
		for _, entry := range rd.CommittedEntries {
			switch entry.GetType() {
			case raftpb.EntryNormal:
				data := entry.GetData()
				if len(data) == 0 {
					if entry.GetIndex() > c.appliedIndex {
						c.appliedIndex = entry.GetIndex()
					}
					continue
				}
				// Entry format: [Seq:8][Payload:N]
				seq := uint64(0)
				payload := data
				if len(data) >= 8 {
					seq = binary.BigEndian.Uint64(data[:8])
					payload = data[8:]
				}

				var result any
				var applyErr error
				if c.sm != nil {
					applied := &LogEntry{
						Term:  entry.GetTerm(),
						Index: entry.GetIndex(),
						Data:  payload,
					}
					if resultSM, ok := c.sm.(ResultStateMachine); ok {
						result, applyErr = resultSM.ApplyWithResult(applied)
					} else {
						applyErr = c.sm.Apply(applied)
					}
				}

				c.propMu.Lock()
				if ch, ok := c.proposals[seq]; ok {
					ch <- raftProposalResult{value: result, err: applyErr}
					delete(c.proposals, seq)
				}
				c.propMu.Unlock()
				if applyErr != nil {
					return fmt.Errorf("apply committed entry %d: %w", entry.GetIndex(), applyErr)
				}

			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				applyErr := proto.Unmarshal(entry.GetData(), &cc)
				if applyErr == nil {
					c.confState = c.core.RawNode.ApplyConfChange(&cc)
					applyErr = c.applyPeerChange(&cc)
				}
				c.completeConfChangeProposal(cc.GetId(), applyErr)
				if applyErr != nil {
					return fmt.Errorf("apply configuration entry %d: %w", entry.GetIndex(), applyErr)
				}
			case raftpb.EntryConfChangeV2:
				var cc raftpb.ConfChangeV2
				applyErr := proto.Unmarshal(entry.GetData(), &cc)
				seq, context := decodeConfChangeContext(cc.GetContext())
				cc.Context = context
				previousState := c.confState
				if applyErr == nil {
					c.confState = c.core.RawNode.ApplyConfChange(&cc)
					applyErr = c.applyPeerChangeV2(&cc, previousState, c.confState)
				}
				if applyErr != nil {
					c.completeConfChangeProposal(seq, applyErr)
					return fmt.Errorf("apply configuration entry %d: %w", entry.GetIndex(), applyErr)
				}
				c.completeConfChangeProposal(seq, nil)
			}
			if entry.GetIndex() > c.appliedIndex {
				c.appliedIndex = entry.GetIndex()
			}
		}
	}
	c.pendingReadStates = append(c.pendingReadStates, rd.ReadStates...)
	c.completeAppliedReadStatesLocked()

	c.core.RawNode.Advance(rd)
	if err := c.maybeSnapshotLocked(); err != nil {
		return err
	}
	return nil
}

func (c *RaftController) completeAppliedReadStatesLocked() {
	remaining := c.pendingReadStates[:0]
	for _, state := range c.pendingReadStates {
		if state.Index > c.appliedIndex {
			remaining = append(remaining, state)
			continue
		}
		if len(state.RequestCtx) != 8 {
			continue
		}
		seq := binary.BigEndian.Uint64(state.RequestCtx)
		c.readMu.Lock()
		if ch, ok := c.readWaiters[seq]; ok {
			ch <- nil
			delete(c.readWaiters, seq)
		}
		c.readMu.Unlock()
	}
	c.pendingReadStates = remaining
}

func (c *RaftController) failReadWaiters(err error) {
	if err == nil {
		return
	}
	c.readMu.Lock()
	for seq, ch := range c.readWaiters {
		select {
		case ch <- err:
		default:
		}
		delete(c.readWaiters, seq)
	}
	c.readMu.Unlock()
}

// ReadIndex waits for a quorum-confirmed read point to be applied locally.
func (c *RaftController) ReadIndex(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && c.readIndexTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.readIndexTimeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.FatalError(); err != nil {
		return err
	}
	if !c.running.Load() {
		return ErrClosed
	}
	seq := c.readSeq.Add(1)
	requestCtx := make([]byte, 8)
	binary.BigEndian.PutUint64(requestCtx, seq)
	waiter := make(chan error, 1)
	c.readMu.Lock()
	c.readWaiters[seq] = waiter
	c.readMu.Unlock()

	c.nodeMu.Lock()
	if err := c.operationalErrorLocked(); err != nil {
		c.nodeMu.Unlock()
		c.removeReadWaiter(seq)
		return err
	}
	if c.core.RawNode.Status().Lead == 0 {
		c.nodeMu.Unlock()
		c.removeReadWaiter(seq)
		return ErrNotLeader
	}
	c.core.RawNode.ReadIndex(requestCtx)
	var err error
	if c.core.RawNode.HasReady() {
		err = c.handleReadyLocked()
		if err != nil {
			c.setFatalError(err)
		}
	}
	c.nodeMu.Unlock()
	if err != nil {
		c.removeReadWaiter(seq)
		return err
	}

	select {
	case err := <-waiter:
		return err
	case <-ctx.Done():
		c.removeReadWaiter(seq)
		return ctx.Err()
	case <-c.doneCh:
		c.removeReadWaiter(seq)
		if err := c.FatalError(); err != nil {
			return err
		}
		return ErrClosed
	}
}

// operationalErrorLocked rejects RawNode operations after shutdown or a fatal
// Ready-processing failure. The caller must hold nodeMu.
func (c *RaftController) operationalErrorLocked() error {
	if !c.running.Load() {
		return ErrClosed
	}
	if err := c.FatalError(); err != nil {
		return fmt.Errorf("etcd raft controller failed: %w", err)
	}
	return nil
}

func (c *RaftController) removeReadWaiter(seq uint64) {
	c.readMu.Lock()
	delete(c.readWaiters, seq)
	c.readMu.Unlock()
}

func (c *RaftController) completeConfChangeProposal(seq uint64, err error) {
	if seq == 0 {
		return
	}
	c.propMu.Lock()
	if ch, ok := c.proposals[seq]; ok {
		ch <- raftProposalResult{err: err}
		delete(c.proposals, seq)
	}
	c.propMu.Unlock()
	c.failReadWaiters(err)
}

func encodeConfChangeContext(seq uint64, context []byte) []byte {
	encoded := make([]byte, 8+len(context))
	binary.BigEndian.PutUint64(encoded[:8], seq)
	copy(encoded[8:], context)
	return encoded
}

func decodeConfChangeContext(encoded []byte) (uint64, []byte) {
	if len(encoded) < 8 {
		return 0, encoded
	}
	return binary.BigEndian.Uint64(encoded[:8]), encoded[8:]
}

func (c *RaftController) applyPeerChangeV2(change *raftpb.ConfChangeV2, previous, current *raftpb.ConfState) error {
	if change.LeaveJoint() {
		currentVoters := make(map[uint64]struct{}, len(current.GetVoters())+len(current.GetLearners()))
		for _, id := range current.GetVoters() {
			currentVoters[id] = struct{}{}
		}
		for _, id := range current.GetLearners() {
			currentVoters[id] = struct{}{}
		}
		for _, id := range previous.GetVotersOutgoing() {
			if _, remains := currentVoters[id]; remains {
				continue
			}
			c.removePeerMapping(id)
		}
		return nil
	}
	autoLeave, joint := change.EnterJoint()
	for _, single := range change.GetChanges() {
		// Keep removed peers in the transport allowlist until the auto-leave
		// entry commits. They may still be needed to satisfy the outgoing
		// configuration's quorum during the joint phase.
		if single.GetType() == raftpb.ConfChangeRemoveNode && joint && autoLeave {
			continue
		}
		id := single.GetNodeId()
		legacy := &raftpb.ConfChange{Type: single.GetType().Enum(), NodeId: &id, Context: change.GetContext()}
		if err := c.applyPeerChange(legacy); err != nil {
			return err
		}
	}
	return nil
}

func (c *RaftController) removePeerMapping(id uint64) {
	c.peersMu.Lock()
	previous := c.peers[id]
	if previous != "" {
		delete(c.peerIDs, previous)
	}
	delete(c.peers, id)
	c.peersMu.Unlock()
	if previous != "" {
		if identityTransport, ok := c.transport.(interface{ RemovePeerIdentity(string) }); ok {
			identityTransport.RemovePeerIdentity(previous)
		}
	}
}

func (c *RaftController) applyPeerChange(change *raftpb.ConfChange) error {
	address := string(change.GetContext())
	switch change.GetType() {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeUpdateNode:
		if address == "" {
			c.peersMu.RLock()
			address = c.peers[change.GetNodeId()]
			c.peersMu.RUnlock()
			if address == "" {
				return fmt.Errorf("Raft membership change for node %d has no transport address", change.GetNodeId())
			}
		}
		// Initial RawNode.Bootstrap entries contain peer IDs but no transport
		// contexts. The local identity is already configured and must not be
		// added to the remote-peer allowlist.
		if change.GetNodeId() == c.id {
			return nil
		}
		c.peersMu.RLock()
		currentAddress := c.peers[change.GetNodeId()]
		c.peersMu.RUnlock()
		// Reapplying an update with the same address must not acquire another
		// transport identity reference. The group host holds one reference per
		// membership entry and releases it when that entry is removed.
		if currentAddress == address {
			return nil
		}
		if validator, ok := c.transport.(interface{ ValidatePeerIdentity(string) error }); ok {
			if err := validator.ValidatePeerIdentity(address); err != nil {
				return fmt.Errorf("validate Raft peer %d address: %w", change.GetNodeId(), err)
			}
		}
		if identityTransport, ok := c.transport.(interface{ AllowPeerIdentity(string) error }); ok {
			if err := identityTransport.AllowPeerIdentity(address); err != nil {
				return fmt.Errorf("allow Raft peer %d transport identity: %w", change.GetNodeId(), err)
			}
		}
		c.peersMu.Lock()
		previous := c.peers[change.GetNodeId()]
		if previous != "" && previous != address {
			delete(c.peerIDs, previous)
		}
		c.peers[change.GetNodeId()] = address
		c.peerIDs[address] = change.GetNodeId()
		c.peersMu.Unlock()
		if previous != "" && previous != address {
			if identityTransport, ok := c.transport.(interface{ RemovePeerIdentity(string) }); ok {
				identityTransport.RemovePeerIdentity(previous)
			}
		}
	case raftpb.ConfChangeRemoveNode:
		c.peersMu.Lock()
		previous := c.peers[change.GetNodeId()]
		if previous != "" {
			delete(c.peerIDs, previous)
		}
		delete(c.peers, change.GetNodeId())
		c.peersMu.Unlock()
		if previous != "" {
			if identityTransport, ok := c.transport.(interface{ RemovePeerIdentity(string) }); ok {
				identityTransport.RemovePeerIdentity(previous)
			}
		}
	}
	return nil
}

func (c *RaftController) maybeSnapshotLocked() error {
	if c.sm == nil || c.snapshotEntries == 0 || c.appliedIndex-c.lastSnapshotIndex < c.snapshotEntries {
		return nil
	}
	data, err := c.sm.Snapshot()
	if err != nil {
		return fmt.Errorf("snapshot state machine at index %d: %w", c.appliedIndex, err)
	}
	if _, err := c.core.Storage.CreateSnapshot(c.appliedIndex, c.confState, data); err != nil {
		return fmt.Errorf("create raft snapshot at index %d: %w", c.appliedIndex, err)
	}
	compactIndex := uint64(1)
	if c.appliedIndex > c.snapshotCatchUp {
		compactIndex = c.appliedIndex - c.snapshotCatchUp
	}
	firstIndex, err := c.core.Storage.FirstIndex()
	if err != nil {
		return err
	}
	if compactIndex > firstIndex {
		if err := c.core.Storage.Compact(compactIndex); err != nil && !errors.Is(err, etcdraft.ErrCompacted) {
			return fmt.Errorf("compact raft log at index %d: %w", compactIndex, err)
		}
	}
	if err := c.core.durable.sync(); err != nil {
		return fmt.Errorf("persist raft snapshot at index %d: %w", c.appliedIndex, err)
	}
	c.lastSnapshotIndex = c.appliedIndex
	return nil
}

func (c *RaftController) sendMessage(m *raftpb.Message) {
	if m == nil {
		return
	}
	c.peersMu.RLock()
	targetNode, ok := c.peers[m.GetTo()]
	c.peersMu.RUnlock()

	if !ok || c.transport == nil {
		return
	}

	data, err := proto.Marshal(m)
	if err != nil {
		return
	}

	_ = c.transport.Send(targetNode, &RaftMessage{
		GroupID:     c.groupID,
		Type:        MsgEtcdRaft,
		EtcdMessage: data,
	})
}

// Propose submits a command to the Raft cluster and waits for quorum commit.
func (c *RaftController) Propose(ctx context.Context, data []byte) error {
	_, err := c.proposeAtTermWithResult(ctx, 0, data)
	return err
}

// ProposeWithResult submits a command and returns the local state machine's
// result for that committed entry, when it implements ResultStateMachine.
func (c *RaftController) ProposeWithResult(ctx context.Context, data []byte) (any, error) {
	return c.proposeAtTermWithResult(ctx, 0, data)
}

// ProposeAtTerm submits a command only if this node is still leader in the
// supplied term. The check and RawNode proposal are serialized under nodeMu,
// preventing a leadership change between a caller's fencing check and admit.
func (c *RaftController) ProposeAtTerm(ctx context.Context, term uint64, data []byte) error {
	_, err := c.ProposeAtTermWithResult(ctx, term, data)
	return err
}

// ProposeAtTermWithResult submits only if this node is still leader in the
// supplied term and returns the result produced while applying the entry.
func (c *RaftController) ProposeAtTermWithResult(ctx context.Context, term uint64, data []byte) (any, error) {
	if term == 0 {
		return nil, fmt.Errorf("%w: proposal term must be non-zero", ErrStaleEpoch)
	}
	return c.proposeAtTermWithResult(ctx, term, data)
}

func (c *RaftController) proposeAtTermWithResult(ctx context.Context, requiredTerm uint64, data []byte) (any, error) {
	if !c.running.Load() {
		return nil, ErrClosed
	}
	if err := c.FatalError(); err != nil {
		return nil, fmt.Errorf("etcd raft controller failed: %w", err)
	}

	seq := c.propSeq.Add(1)
	ch := make(chan raftProposalResult, 1)

	c.propMu.Lock()
	c.proposals[seq] = ch
	c.propMu.Unlock()

	// Frame proposal with seq: [Seq:8][Data:N]
	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(buf[:8], seq)
	copy(buf[8:], data)

	c.nodeMu.Lock()
	err := c.operationalErrorLocked()
	if err == nil && requiredTerm != 0 {
		status := c.core.RawNode.Status()
		currentTerm := uint64(0)
		if status.Term != nil {
			currentTerm = *status.Term
		}
		if currentTerm != requiredTerm {
			err = fmt.Errorf("%w: requested term %d, current term %d", ErrStaleEpoch, requiredTerm, currentTerm)
		} else if status.Lead != c.id {
			err = ErrNotLeader
		}
	}
	if err == nil {
		err = c.core.RawNode.Propose(buf)
	}
	if err == nil && c.core.RawNode.HasReady() {
		err = c.handleReadyLocked()
		if err != nil {
			c.setFatalError(err)
		}
	}
	c.nodeMu.Unlock()

	if err != nil {
		c.propMu.Lock()
		delete(c.proposals, seq)
		c.propMu.Unlock()
		return nil, err
	}

	select {
	case <-ctx.Done():
		c.propMu.Lock()
		delete(c.proposals, seq)
		c.propMu.Unlock()
		return nil, ctx.Err()
	case result := <-ch:
		return result.value, result.err
	}
}

func (c *RaftController) setFatalError(err error) {
	if err == nil {
		return
	}
	c.fatalMu.Lock()
	if c.fatalErr == nil {
		c.fatalErr = err
	}
	c.fatalMu.Unlock()

	// Unblock every caller waiting for a proposal that can no longer commit.
	c.propMu.Lock()
	for seq, ch := range c.proposals {
		ch <- raftProposalResult{err: err}
		delete(c.proposals, seq)
	}
	c.propMu.Unlock()
}

// FatalError reports a terminal persistence or state-machine failure. Once
// set, the controller rejects new writes until it is restarted and recovered.
func (c *RaftController) FatalError() error {
	c.fatalMu.RLock()
	defer c.fatalMu.RUnlock()
	return c.fatalErr
}

// IsLeader reports whether this node is currently the cluster leader.
func (c *RaftController) IsLeader() bool {
	return c.leaderID.Load() == c.id
}

// Running reports whether the controller's Raft event loop is active.
func (c *RaftController) Running() bool {
	return c.running.Load()
}

// GroupID identifies the independent Raft group routed by this controller.
// The empty ID is the legacy/default group.
func (c *RaftController) GroupID() string {
	return c.groupID
}

// LeaderID returns the current known cluster leader ID.
func (c *RaftController) LeaderID() uint64 {
	return c.leaderID.Load()
}

// Term returns the current election term.
func (c *RaftController) Term() uint64 {
	return c.term.Load()
}

// AppliedIndex is the highest committed entry applied to the broker state.
func (c *RaftController) AppliedIndex() uint64 {
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	return c.appliedIndex
}

// SnapshotIndex is the highest applied index captured in a durable snapshot.
func (c *RaftController) SnapshotIndex() uint64 {
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	return c.lastSnapshotIndex
}

// Status returns the current status of the Raft core.
func (c *RaftController) Status() etcdraft.Status {
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	return c.core.RawNode.Status()
}

// TransferLeadership transfers leadership to a caught-up voter and waits until
// the target becomes leader or the context expires.
func (c *RaftController) TransferLeadership(ctx context.Context, transferee uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if transferee == 0 || transferee == c.id {
		return errors.New("leadership transferee must be a different non-zero peer ID")
	}
	if !c.confMu.TryLock() {
		return ErrConfChangePending
	}
	defer c.confMu.Unlock()
	if !c.running.Load() {
		return ErrClosed
	}
	if !c.IsLeader() {
		return ErrNotLeader
	}
	status := c.Status()
	if status.Config.AutoLeave {
		return ErrConfChangePending
	}
	if _, learner := status.Config.Learners[transferee]; learner {
		return ErrNodeNotFound
	}
	progress, ok := status.Progress[transferee]
	if !ok {
		return ErrNodeNotFound
	}
	if status.HardState == nil || progress.Match < status.HardState.GetCommit() {
		return ErrReplicaLagging
	}
	c.nodeMu.Lock()
	if !c.IsLeader() {
		c.nodeMu.Unlock()
		return ErrNotLeader
	}
	c.core.RawNode.TransferLeader(transferee)
	c.nodeMu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !c.running.Load() {
			return ErrClosed
		}
		leader := c.LeaderID()
		if leader == transferee {
			return nil
		}
		if leader != 0 && leader != c.id {
			return ErrLeaderLost
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// ProposeConfChange proposes a membership change.
func (c *RaftController) ProposeConfChange(ctx context.Context, cc *raftpb.ConfChange) error {
	if cc == nil {
		return errors.New("cannot propose nil configuration change")
	}
	change := cc.AsV2()
	transition := raftpb.ConfChangeTransition_ConfChangeTransitionJointImplicit
	change.Transition = &transition
	return c.ProposeConfChangeV2(ctx, change)
}

// ProposeConfChangeV2 proposes a joint-consensus membership change and returns
// after the joint configuration applies. etcd/raft transitions out once both
// configurations have committed the automatic leave entry. Further changes are
// rejected with ErrConfChangePending until that transition completes.
func (c *RaftController) ProposeConfChangeV2(ctx context.Context, change *raftpb.ConfChangeV2) error {
	if change == nil {
		return errors.New("cannot propose nil configuration change")
	}
	change = proto.Clone(change).(*raftpb.ConfChangeV2)
	transition := raftpb.ConfChangeTransition_ConfChangeTransitionJointImplicit
	change.Transition = &transition
	c.confMu.Lock()
	defer c.confMu.Unlock()
	return c.proposeConfChangeV2(ctx, change)
}

func (c *RaftController) proposeConfChangeV2(ctx context.Context, change *raftpb.ConfChangeV2) error {
	if len(change.GetChanges()) == 0 {
		return errors.New("configuration change must contain at least one membership operation")
	}
	if !c.running.Load() {
		return ErrClosed
	}
	if !c.IsLeader() {
		return ErrNotLeader
	}
	if err := c.FatalError(); err != nil {
		return fmt.Errorf("etcd raft controller failed: %w", err)
	}
	seq := c.propSeq.Add(1)
	copyChange := proto.Clone(change).(*raftpb.ConfChangeV2)
	copyChange.Context = encodeConfChangeContext(seq, copyChange.GetContext())
	ch := make(chan raftProposalResult, 1)
	c.propMu.Lock()
	c.proposals[seq] = ch
	c.propMu.Unlock()
	c.nodeMu.Lock()
	if err := c.operationalErrorLocked(); err != nil {
		c.nodeMu.Unlock()
		c.propMu.Lock()
		delete(c.proposals, seq)
		c.propMu.Unlock()
		return err
	}
	if c.confState.GetAutoLeave() {
		c.nodeMu.Unlock()
		c.propMu.Lock()
		delete(c.proposals, seq)
		c.propMu.Unlock()
		return ErrConfChangePending
	}
	err := c.core.RawNode.ProposeConfChange(copyChange)
	if err == nil && c.core.RawNode.HasReady() {
		err = c.handleReadyLocked()
		if err != nil {
			c.setFatalError(err)
		}
	}
	c.nodeMu.Unlock()
	if err != nil {
		c.propMu.Lock()
		delete(c.proposals, seq)
		c.propMu.Unlock()
		return err
	}
	select {
	case <-ctx.Done():
		c.propMu.Lock()
		delete(c.proposals, seq)
		c.propMu.Unlock()
		return ctx.Err()
	case result := <-ch:
		return result.err
	}
}

// AddLearner adds a non-voting peer so it can catch up before promotion.
func (c *RaftController) AddLearner(ctx context.Context, address string) (uint64, error) {
	return c.addLearner(ctx, address, 0)
}

// AddLearnerWithID adds a learner using a caller-selected ID. Shared multi-
// group hosts use the stable controller peer ID so a joining broker can
// provision the same address-to-ID mapping before it receives its first
// partition Raft message.
func (c *RaftController) AddLearnerWithID(ctx context.Context, address string, preferredID uint64) (uint64, error) {
	if preferredID == 0 {
		return 0, errors.New("preferred learner ID must be non-zero")
	}
	return c.addLearner(ctx, address, preferredID)
}

func (c *RaftController) addLearner(ctx context.Context, address string, preferredID uint64) (uint64, error) {
	c.confMu.Lock()
	defer c.confMu.Unlock()
	if address == "" {
		return 0, errors.New("peer address is required")
	}
	if validator, ok := c.transport.(interface{ ValidatePeerIdentity(string) error }); ok {
		if err := validator.ValidatePeerIdentity(address); err != nil {
			return 0, err
		}
	}
	c.peersMu.RLock()
	if id := c.peerIDs[address]; id != 0 {
		c.peersMu.RUnlock()
		if preferredID != 0 && id != preferredID {
			return 0, fmt.Errorf("peer %q already uses Raft ID %d, not requested ID %d", address, id, preferredID)
		}
		return id, nil
	}
	id := preferredID
	if id == 0 {
		id = 1
		for existing := range c.peers {
			if existing >= id {
				id = existing + 1
			}
		}
	} else if existingAddress := c.peers[id]; existingAddress != "" {
		c.peersMu.RUnlock()
		return 0, fmt.Errorf("preferred Raft ID %d is already assigned to peer %q", id, existingAddress)
	}
	c.peersMu.RUnlock()
	changeType := raftpb.ConfChangeAddLearnerNode
	change := &raftpb.ConfChangeV2{
		Changes: []*raftpb.ConfChangeSingle{{Type: &changeType, NodeId: &id}},
		Context: []byte(address),
	}
	if err := c.proposeConfChangeV2(ctx, change); err != nil {
		return 0, err
	}
	return id, nil
}

// PromoteLearner promotes a fully caught-up learner to voting membership.
func (c *RaftController) PromoteLearner(ctx context.Context, peerID uint64) error {
	c.confMu.Lock()
	defer c.confMu.Unlock()
	if peerID == 0 {
		return errors.New("peer ID must be non-zero")
	}
	if !c.IsLeader() {
		return ErrNotLeader
	}
	status := c.Status()
	if _, ok := status.Config.Learners[peerID]; !ok {
		if _, isVoter := status.Config.Voters.IDs()[peerID]; isVoter {
			return nil
		}
		return ErrNodeNotFound
	}
	progress, ok := status.Progress[peerID]
	if !ok || status.HardState == nil || progress.Match < status.HardState.GetCommit() {
		return ErrReplicaLagging
	}
	changeType := raftpb.ConfChangeAddNode
	transition := raftpb.ConfChangeTransition_ConfChangeTransitionJointImplicit
	change := &raftpb.ConfChangeV2{
		Transition: &transition,
		Changes:    []*raftpb.ConfChangeSingle{{Type: &changeType, NodeId: &peerID}},
	}
	return c.proposeConfChangeV2(ctx, change)
}

// PeerRole reports whether a configured peer is a voter, learner, or pending
// learner; it returns an empty string for an unknown ID.
func (c *RaftController) PeerRole(peerID uint64) string {
	status := c.Status()
	if _, ok := status.Config.Learners[peerID]; ok {
		return "learner"
	}
	if _, ok := status.Config.LearnersNext[peerID]; ok {
		return "learner_next"
	}
	if _, ok := status.Config.Voters.IDs()[peerID]; ok {
		return "voter"
	}
	return ""
}

// AddPeer adds a voting member and returns its allocated Raft ID.
func (c *RaftController) AddPeer(ctx context.Context, address string) (uint64, error) {
	c.confMu.Lock()
	defer c.confMu.Unlock()
	if address == "" {
		return 0, errors.New("peer address is required")
	}
	if validator, ok := c.transport.(interface{ ValidatePeerIdentity(string) error }); ok {
		if err := validator.ValidatePeerIdentity(address); err != nil {
			return 0, err
		}
	}
	c.peersMu.RLock()
	if id := c.peerIDs[address]; id != 0 {
		c.peersMu.RUnlock()
		return id, nil
	}
	var id uint64 = 1
	for existing := range c.peers {
		if existing >= id {
			id = existing + 1
		}
	}
	c.peersMu.RUnlock()
	changeType := raftpb.ConfChangeAddNode
	transition := raftpb.ConfChangeTransition_ConfChangeTransitionJointImplicit
	change := &raftpb.ConfChangeV2{
		Transition: &transition,
		Changes:    []*raftpb.ConfChangeSingle{{Type: &changeType, NodeId: &id}},
		Context:    []byte(address),
	}
	if err := c.proposeConfChangeV2(ctx, change); err != nil {
		return 0, err
	}
	return id, nil
}

// RemovePeer removes a voting member by numeric Raft ID.
func (c *RaftController) RemovePeer(ctx context.Context, id uint64) error {
	c.confMu.Lock()
	defer c.confMu.Unlock()
	if id == 0 {
		return errors.New("peer ID must be non-zero")
	}
	status := c.Status()
	if status.Config.AutoLeave {
		return ErrConfChangePending
	}
	_, learner := status.Config.Learners[id]
	if !learner {
		if _, voter := status.Config.Voters.IDs()[id]; !voter {
			return ErrNodeNotFound
		}
	}
	changeType := raftpb.ConfChangeRemoveNode
	transition := raftpb.ConfChangeTransition_ConfChangeTransitionJointImplicit
	if learner {
		transition = raftpb.ConfChangeTransition_ConfChangeTransitionAuto
	}
	change := &raftpb.ConfChangeV2{
		Transition: &transition,
		Changes:    []*raftpb.ConfChangeSingle{{Type: &changeType, NodeId: &id}},
	}
	return c.proposeConfChangeV2(ctx, change)
}

// PeerAddress resolves a Raft ID to its current transport address.
func (c *RaftController) PeerAddress(id uint64) (string, bool) {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	address, ok := c.peers[id]
	return address, ok
}

// PeerAddresses returns a stable snapshot of the addresses currently known to
// this Raft group, including configured learners and voters.
func (c *RaftController) PeerAddresses() map[uint64]string {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	out := make(map[uint64]string, len(c.peers))
	for id, address := range c.peers {
		out[id] = address
	}
	return out
}

// PeerCount returns the number of configured Raft members, including the
// local node, voters, and learners.
func (c *RaftController) PeerCount() int {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	return len(c.peers)
}

// JointConsensusPending reports whether this group is still using joint voter
// configurations or waiting for the automatic leave entry to apply.
func (c *RaftController) JointConsensusPending() bool {
	status := c.Status()
	return status.Config.AutoLeave || (len(status.Config.Voters) > 1 && len(status.Config.Voters[1]) != 0)
}

// PeerID resolves a transport address to its current Raft ID.
func (c *RaftController) PeerID(address string) (uint64, bool) {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	id, ok := c.peerIDs[address]
	return id, ok
}
