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
}

// EtcdRaftCore is the audited etcd/raft state machine foundation.
type EtcdRaftCore struct {
	RawNode *etcdraft.RawNode
	Storage *etcdraft.MemoryStorage
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
	storage := etcdraft.NewMemoryStorage()
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
	})
	if err != nil {
		return nil, err
	}
	if len(cfg.Peers) > 0 {
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
	return &EtcdRaftCore{RawNode: raw, Storage: storage}, nil
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
}

// RaftController runs an etcd/raft consensus engine with network transport,
// local state machine application, and linearizable command proposals.
type RaftController struct {
	id        uint64
	nodeID    string
	core      *EtcdRaftCore
	transport Transport
	sm        StateMachine
	peers     map[uint64]string
	peerIDs   map[string]uint64
	peersMu   sync.RWMutex
	proposals map[uint64]chan error
	propMu    sync.Mutex
	propSeq   atomic.Uint64
	nodeMu    sync.Mutex
	stopCh    chan struct{}
	doneCh    chan struct{}
	tickDur   time.Duration
	leaderID  atomic.Uint64
	term      atomic.Uint64
	running   atomic.Bool
}

// NewRaftController creates a complete Raft controller backed by etcd/raft.
func NewRaftController(cfg RaftControllerConfig) (*RaftController, error) {
	if cfg.ID == 0 {
		return nil, fmt.Errorf("etcd controller: ID must be non-zero")
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 50 * time.Millisecond
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
	})
	if err != nil {
		return nil, err
	}

	c := &RaftController{
		id:        cfg.ID,
		nodeID:    cfg.NodeID,
		core:      core,
		transport: cfg.Transport,
		sm:        cfg.StateMachine,
		peers:     peerMap,
		peerIDs:   peerRev,
		proposals: make(map[uint64]chan error),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		tickDur:   cfg.TickInterval,
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
		c.handleReadyLocked()
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
		c.handleReadyLocked()
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
				c.handleReadyLocked()
			}
			c.nodeMu.Unlock()
		default:
		}

		c.nodeMu.Lock()
		if c.core.RawNode.HasReady() {
			c.handleReadyLocked()
			c.nodeMu.Unlock()
		} else {
			c.nodeMu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}
}

func (c *RaftController) handleReadyLocked() {
	rd := c.core.RawNode.Ready()

	if rd.SoftState != nil {
		c.leaderID.Store(rd.SoftState.Lead)
	}

	if rd.HardState != nil && !etcdraft.IsEmptyHardState(rd.HardState) {
		_ = c.core.Storage.SetHardState(rd.HardState)
		c.term.Store(rd.HardState.GetTerm())
	}

	if len(rd.Entries) > 0 {
		_ = c.core.Storage.Append(rd.Entries)
	}

	// Send messages to peers via transport
	if len(rd.Messages) > 0 {
		for _, m := range rd.Messages {
			c.sendMessage(m)
		}
	}

	// Apply committed entries to state machine
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

			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				if err := proto.Unmarshal(entry.GetData(), &cc); err == nil {
					c.core.RawNode.ApplyConfChange(&cc)
				}
			}
		}
	}

	// Apply snapshot if available
	if rd.Snapshot != nil && !etcdraft.IsEmptySnap(rd.Snapshot) {
		_ = c.core.Storage.ApplySnapshot(rd.Snapshot)
		if c.sm != nil {
			_ = c.sm.Restore(rd.Snapshot.Data)
		}
	}

	c.core.RawNode.Advance(rd)
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
		c.handleReadyLocked()
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
func (c *RaftController) ProposeConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	return c.core.RawNode.ProposeConfChange(&cc)
}
