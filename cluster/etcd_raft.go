package cluster

import (
	"fmt"

	etcdraft "go.etcd.io/raft/v3"
)

// EtcdRaftCoreConfig configures the consensus core shared by controller and
// partition groups. Persistence and transport remain responsibilities of the
// group runtime that drains RawNode.Ready; it must durably store Ready state
// before calling Advance.
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

// EtcdRaftCore is the audited etcd/raft state machine foundation. It is kept
// separate from the legacy custom Raft type while the durable group runtime is
// completed.
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
