package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
	NodeID                    string
	Peers                     map[uint64]string // ID -> address/nodeID
	ElectionTick              int
	HeartbeatTick             int
	TickInterval              time.Duration
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
	nodeID            string
	core              *EtcdRaftCore
	transport         Transport
	sm                StateMachine
	peers             map[uint64]string
	peerIDs           map[string]uint64
	peersMu           sync.RWMutex
	proposals         map[uint64]chan error
	propMu            sync.Mutex
	propSeq           atomic.Uint64
	nodeMu            sync.Mutex
	stopCh            chan struct{}
	doneCh            chan struct{}
	tickDur           time.Duration
	leaderID          atomic.Uint64
	term              atomic.Uint64
	running           atomic.Bool
	fatalMu           sync.RWMutex
	fatalErr          error
	confMu            sync.Mutex
	confState         *raftpb.ConfState
	appliedIndex      uint64
	lastSnapshotIndex uint64
	snapshotEntries   uint64
	snapshotCatchUp   uint64
}

// NewRaftController creates a complete Raft controller backed by etcd/raft.
func NewRaftController(cfg RaftControllerConfig) (*RaftController, error) {
	if cfg.ID == 0 {
		return nil, fmt.Errorf("etcd controller: ID must be non-zero")
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 50 * time.Millisecond
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

	core, err := NewEtcdRaftCore(EtcdRaftCoreConfig{
		ID:                        cfg.ID,
		Peers:                     peerIDs,
		ElectionTick:              cfg.ElectionTick,
		HeartbeatTick:             cfg.HeartbeatTick,
		MaxMessageBytes:           cfg.MaxMessageBytes,
		MaxInflightMessages:       cfg.MaxInflightMessages,
		MaxUncommittedEntryBytes:  cfg.MaxUncommittedEntryBytes,
		DisableProposalForwarding: cfg.DisableProposalForwarding,
		StorageDir:                cfg.StorageDir,
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
		nodeID:            cfg.NodeID,
		core:              core,
		transport:         cfg.Transport,
		sm:                cfg.StateMachine,
		peers:             peerMap,
		peerIDs:           peerRev,
		proposals:         make(map[uint64]chan error),
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
		tickDur:           cfg.TickInterval,
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
	c.propMu.Lock()
	for seq, ch := range c.proposals {
		select {
		case ch <- ErrClosed:
		default:
		}
		delete(c.proposals, seq)
	}
	c.propMu.Unlock()
	return nil
}

// Step handles an incoming etcd/raft message received from a peer.
func (c *RaftController) Step(ctx context.Context, msg *raftpb.Message) error {
	if msg == nil {
		return errors.New("cannot step nil message")
	}
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	err := c.core.RawNode.Step(msg)
	if c.core.RawNode.HasReady() {
		err = errors.Join(err, c.handleReadyLocked())
	}
	return err
}

// HandleMessage decodes and processes an incoming transport message carrying etcd/raft frames.
func (c *RaftController) HandleMessage(from string, msg *RaftMessage) error {
	if msg == nil || msg.Type != MsgEtcdRaft || len(msg.EtcdMessage) == 0 {
		return nil
	}
	var m raftpb.Message
	if err := proto.Unmarshal(msg.EtcdMessage, &m); err != nil {
		return err
	}
	return c.Step(context.Background(), &m)
}

// RegisterTransportHandler attaches this controller to a network transport.
func (c *RaftController) RegisterTransportHandler(addr string) error {
	if c.transport == nil {
		return nil
	}
	return c.transport.Listen(addr, func(from string, msg *RaftMessage) {
		_ = c.HandleMessage(from, msg)
	})
}

// Campaign forces the Raft controller to campaign for leadership.
func (c *RaftController) Campaign() error {
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	err := c.core.RawNode.Campaign()
	if c.core.RawNode.HasReady() {
		err = errors.Join(err, c.handleReadyLocked())
	}
	return err
}

func (c *RaftController) run() {
	defer close(c.doneCh)
	ticker := time.NewTicker(c.tickDur)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.nodeMu.Lock()
			c.core.RawNode.Tick()
			if c.core.RawNode.HasReady() {
				if err := c.handleReadyLocked(); err != nil {
					c.nodeMu.Unlock()
					c.setFatalError(err)
					return
				}
			}
			c.nodeMu.Unlock()
		default:
		}

		c.nodeMu.Lock()
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
					continue
				}
				// Entry format: [Seq:8][Payload:N]
				seq := uint64(0)
				payload := data
				if len(data) >= 8 {
					seq = binary.BigEndian.Uint64(data[:8])
					payload = data[8:]
				}

				var applyErr error
				if c.sm != nil {
					applyErr = c.sm.Apply(&LogEntry{
						Term:  entry.GetTerm(),
						Index: entry.GetIndex(),
						Data:  payload,
					})
				}

				c.propMu.Lock()
				if ch, ok := c.proposals[seq]; ok {
					ch <- applyErr
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
					c.applyPeerChange(&cc)
				}
				c.propMu.Lock()
				if ch, ok := c.proposals[cc.GetId()]; ok {
					ch <- applyErr
					delete(c.proposals, cc.GetId())
				}
				c.propMu.Unlock()
				if applyErr != nil {
					return fmt.Errorf("apply configuration entry %d: %w", entry.GetIndex(), applyErr)
				}
			}
			if entry.GetIndex() > c.appliedIndex {
				c.appliedIndex = entry.GetIndex()
			}
		}
	}

	c.core.RawNode.Advance(rd)
	if err := c.maybeSnapshotLocked(); err != nil {
		return err
	}
	return nil
}

func (c *RaftController) applyPeerChange(change *raftpb.ConfChange) {
	address := string(change.GetContext())
	c.peersMu.Lock()
	defer c.peersMu.Unlock()
	switch change.GetType() {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeUpdateNode:
		if address != "" {
			if previous := c.peers[change.GetNodeId()]; previous != "" {
				delete(c.peerIDs, previous)
			}
			c.peers[change.GetNodeId()] = address
			c.peerIDs[address] = change.GetNodeId()
		}
	case raftpb.ConfChangeRemoveNode:
		if previous := c.peers[change.GetNodeId()]; previous != "" {
			delete(c.peerIDs, previous)
		}
		delete(c.peers, change.GetNodeId())
	}
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
		Type:        MsgEtcdRaft,
		EtcdMessage: data,
	})
}

// Propose submits a command to the Raft cluster and waits for quorum commit.
func (c *RaftController) Propose(ctx context.Context, data []byte) error {
	if !c.running.Load() {
		return ErrClosed
	}
	if err := c.FatalError(); err != nil {
		return fmt.Errorf("etcd raft controller failed: %w", err)
	}

	seq := c.propSeq.Add(1)
	ch := make(chan error, 1)

	c.propMu.Lock()
	c.proposals[seq] = ch
	c.propMu.Unlock()

	// Frame proposal with seq: [Seq:8][Data:N]
	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(buf[:8], seq)
	copy(buf[8:], data)

	c.nodeMu.Lock()
	err := c.core.RawNode.Propose(buf)
	if err == nil && c.core.RawNode.HasReady() {
		err = c.handleReadyLocked()
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
	case err := <-ch:
		return err
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
		ch <- err
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

// TransferLeadership transfers leadership to transferee.
func (c *RaftController) TransferLeadership(ctx context.Context, transferee uint64) {
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	c.core.RawNode.TransferLeader(transferee)
}

// ProposeConfChange proposes a membership change.
func (c *RaftController) ProposeConfChange(ctx context.Context, cc *raftpb.ConfChange) error {
	if cc == nil {
		return errors.New("cannot propose nil configuration change")
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
	copyChange := proto.Clone(cc).(*raftpb.ConfChange)
	copyChange.Id = &seq
	ch := make(chan error, 1)
	c.propMu.Lock()
	c.proposals[seq] = ch
	c.propMu.Unlock()
	c.nodeMu.Lock()
	err := c.core.RawNode.ProposeConfChange(copyChange)
	if err == nil && c.core.RawNode.HasReady() {
		err = c.handleReadyLocked()
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
	case err := <-ch:
		return err
	}
}

// AddPeer adds a voting member and returns its allocated Raft ID.
func (c *RaftController) AddPeer(ctx context.Context, address string) (uint64, error) {
	c.confMu.Lock()
	defer c.confMu.Unlock()
	if address == "" {
		return 0, errors.New("peer address is required")
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
	change := &raftpb.ConfChange{Type: &changeType, NodeId: &id, Context: []byte(address)}
	if err := c.ProposeConfChange(ctx, change); err != nil {
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
	changeType := raftpb.ConfChangeRemoveNode
	return c.ProposeConfChange(ctx, &raftpb.ConfChange{Type: &changeType, NodeId: &id})
}

// PeerAddress resolves a Raft ID to its current transport address.
func (c *RaftController) PeerAddress(id uint64) (string, bool) {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	address, ok := c.peers[id]
	return address, ok
}

// PeerID resolves a transport address to its current Raft ID.
func (c *RaftController) PeerID(address string) (uint64, bool) {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	id, ok := c.peerIDs[address]
	return id, ok
}
