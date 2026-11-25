package broker

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/oarkflow/broker/cluster"
	"github.com/oarkflow/fh"
)

var (
	ErrCircuitOpen = errors.New("cluster: circuit breaker open, write rejected")
)

type DistributedConfig struct {
	Peers      []string
	ListenAddr string
	WALDir     string
	// TLSConfig for multi-node etcd/raft must require and verify client
	// certificates. Peer addresses are also bound to certificate SANs.
	TLSConfig *tls.Config
	Cluster   *cluster.Cluster
	// AllowExperimentalCustomRaft explicitly enables the legacy custom
	// consensus path. It is unsafe for production and will be removed when the
	// etcd/raft controller and partition groups replace it.
	AllowExperimentalCustomRaft bool

	// UseEtcdRaft enables the audited production etcd/raft consensus engine.
	UseEtcdRaft        bool
	RaftNodeID         uint64
	RaftPeers          map[uint64]string
	ReplicationFactor  int
	MinInSyncReplicas  int
	MaxReplicationLag  uint64
	ReplicationTimeout time.Duration
	// PartitionStoreFactory opens an isolated, snapshot-capable store for each
	// locally assigned partition. DistributedBroker owns and closes returned
	// stores. If nil, callers must provide a PartitionStateMachineFactory to
	// ReconcilePartitionGroups.
	PartitionStoreFactory PartitionStoreFactory
	// PartitionStoragePath is the local root containing partition stores. When
	// set, the broker measures filesystem usage for this path, including before
	// the first local partition store is created.
	PartitionStoragePath   string
	ControllerAddrs        []string
	SnapshotEntries        uint64
	SnapshotCatchUpEntries uint64
	ReadIndexTimeout       time.Duration
	// AllowStaleReads permits follower-local reads. It is false by default;
	// callers requiring linearizable reads must use the leader.
	AllowStaleReads bool
	// JoinExisting starts a fresh node without bootstrapping a separate Raft
	// cluster. Add the node through AddRaftNode first and use its returned ID.
	JoinExisting bool

	SubmitTimeout  time.Duration
	ForwardTimeout time.Duration

	OnLeaderChange func(leader string)
	OnStateChange  func(state string)
	OnNodeJoin     func(nodeID string)
	OnNodeLeave    func(nodeID string)

	// TransactionTimeout bounds coordinator-backed Broker.CommitOffsetAndAppend
	// transactions. It defaults to 30 seconds.
	TransactionTimeout time.Duration

	// TransactionResolver enables leader-only replay of durable coordinator
	// decisions. Implementations must honor the supplied context and make
	// participant resolution idempotent.
	TransactionResolver         cluster.TransactionParticipantResolver
	TransactionRecoveryInterval time.Duration
	OnTransactionRecoveryError  func(error)
}

// PartitionStoreFactory opens the local store for one partition. Each call
// must return a distinct store with durable state isolated from every other
// partition; the broker closes it when that Raft group is removed or stopped.
type PartitionStoreFactory func(ctx context.Context, partition *cluster.PartitionMetadata) (Store, error)

// NewSegmentedWALPartitionStoreFactory creates a durable BWL2 store factory
// with a stable directory for each resource partition. Configure root
// separately from the broker-wide store directory.
func NewSegmentedWALPartitionStoreFactory(root string, cfg SegmentedWALConfig) (PartitionStoreFactory, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("partition WAL root directory is required")
	}
	return func(ctx context.Context, partition *cluster.PartitionMetadata) (Store, error) {
		if partition == nil || partition.ID.Topic == "" || partition.ID.Partition < 0 {
			return nil, fmt.Errorf("invalid partition metadata for partition WAL store")
		}
		id := partition.ID.Canonical()
		identity := fmt.Sprintf("%s\x00%s\x00%d", id.Kind, id.Topic, id.Partition)
		digest := sha256.Sum256([]byte(identity))
		partitionCfg := cfg
		partitionCfg.Dir = filepath.Join(root, hex.EncodeToString(digest[:]))
		if id.Kind != cluster.PartitionResourceQueue {
			if _, statErr := os.Stat(partitionCfg.Dir); errors.Is(statErr, os.ErrNotExist) {
				// Before resource kinds were part of the directory key, topic and
				// stream partitions with the same name and index shared one WAL. Keep
				// the old path for stores that contain exactly one resource kind.
				legacyDigest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", id.Topic, id.Partition)))
				legacyDir := filepath.Join(root, hex.EncodeToString(legacyDigest[:]))
				if _, legacyErr := os.Stat(legacyDir); legacyErr == nil {
					legacyCfg := cfg
					legacyCfg.Dir = legacyDir
					legacyStore, openErr := NewSegmentedWALStore(legacyCfg)
					if openErr != nil {
						return nil, fmt.Errorf("inspect legacy partition store for %s: %w", id, openErr)
					}
					snapshot := legacyStore.Snapshot()
					_, hasTopic := snapshot.Topics[id.Topic]
					_, hasStream := snapshot.Streams[id.Topic]
					if hasTopic && hasStream {
						_ = legacyStore.Close()
						return nil, fmt.Errorf("legacy partition store for %s contains both a topic and stream; separate their data before opening resource-scoped stores", id)
					}
					matches := (id.Kind == cluster.PartitionResourceTopic && hasTopic) ||
						(id.Kind == cluster.PartitionResourceStream && hasStream)
					if matches {
						return legacyStore, nil
					}
					if closeErr := legacyStore.Close(); closeErr != nil {
						return nil, fmt.Errorf("close unrelated legacy partition store for %s: %w", id, closeErr)
					}
				} else if !errors.Is(legacyErr, os.ErrNotExist) {
					return nil, fmt.Errorf("inspect legacy partition directory for %s: %w", id, legacyErr)
				}
			} else if statErr != nil {
				return nil, fmt.Errorf("inspect partition directory for %s: %w", id, statErr)
			}
		}
		store, err := NewSegmentedWALStore(partitionCfg)
		if err != nil {
			return nil, fmt.Errorf("open partition WAL store for %s: %w", partition.ID, err)
		}
		return store, nil
	}, nil
}

type distributedStateStore interface {
	Store
	prepareAppendAtomic(context.Context, []AppendEntry) ([]Message, error)
	prepareLease(context.Context, LeaseRequest) ([]Message, error)
	applyLease(context.Context, []Message, LeaseRequest) error
	validateExtendLease(context.Context, LeaseRequest) (Message, error)
	prepareSweepLeases(context.Context, time.Time) ([]Message, error)
	prepareMove(context.Context, MoveRequest) (map[string]string, MoveResult, error)
	applyMovePlan(context.Context, string, MoveRequest, map[string]string) (MoveResult, error)
	appendRecoveredAtomic([]AppendEntry) error
	Snapshot() MemoryStoreSnapshot
	RestoreExact(MemoryStoreSnapshot)
}

// PartitionStateMachineFactory creates the local state machine for an assigned
// partition. skipBootstrap must be true when this local group is joining a
// partition group that already has durable history; initial creation on every
// assigned replica uses false.
type PartitionStateMachineFactory func(ctx context.Context, partition *cluster.PartitionMetadata) (stateMachine cluster.StateMachine, skipBootstrap bool, err error)

// NewPartitionStateMachine adapts a snapshot-capable broker store to the
// standard broker command state machine used by partition Raft groups. The
// supplied store must be dedicated to this partition; each group's snapshots
// replace its store state during recovery. Reusing the broker-wide store across
// partition groups can overwrite data committed by other groups.
func NewPartitionStateMachine(store Store) (cluster.StateMachine, error) {
	if isNilPartitionStore(store) {
		return nil, fmt.Errorf("partition state machine requires a store")
	}
	if _, ok := store.(interface {
		Snapshot() MemoryStoreSnapshot
		RestoreExact(MemoryStoreSnapshot)
	}); !ok {
		return nil, fmt.Errorf("%w: partition state machine requires a snapshot-capable store", ErrUnsupportedMode)
	}
	return cluster.NewBrokerStateMachine(&brokerStoreAdapter{store: store}), nil
}

func isNilPartitionStore(store Store) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func samePartitionStoreInstance(a, b Store) bool {
	if a == nil || b == nil {
		return false
	}
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	return av.Type() == bv.Type() && av.Kind() == reflect.Pointer && !av.IsNil() && !bv.IsNil() && av.Pointer() == bv.Pointer()
}

type circuitBreaker struct {
	mu               sync.Mutex
	consecutiveFails int
	state            string // "closed", "open", "half-open"
	trippedAt        time.Time
	threshold        int
	tripDuration     time.Duration
	cooldownExpiry   func() bool
}

func newCircuitBreaker(threshold int, tripDuration time.Duration) *circuitBreaker {
	cb := &circuitBreaker{
		state:        "closed",
		threshold:    threshold,
		tripDuration: tripDuration,
	}
	cb.cooldownExpiry = func() bool {
		cb.mu.Lock()
		defer cb.mu.Unlock()
		return cb.state == "open" && time.Since(cb.trippedAt) >= cb.tripDuration
	}
	return cb
}

func (cb *circuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == "closed" {
		return true
	}
	if cb.state == "open" {
		if time.Since(cb.trippedAt) >= cb.tripDuration {
			cb.state = "half-open"
			return true
		}
		return false
	}
	// half-open: allow probe
	return true
}

func (cb *circuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.consecutiveFails = 0
	if cb.state == "half-open" {
		cb.state = "closed"
	}
}

func (cb *circuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.consecutiveFails++
	if cb.consecutiveFails >= cb.threshold {
		cb.state = "open"
		cb.trippedAt = time.Now()
	}
}

func (cb *circuitBreaker) getState() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *circuitBreaker) getConsecutiveFailures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.consecutiveFails
}

type DistributedBroker struct {
	*Broker
	distCfg                    DistributedConfig
	raft                       *cluster.Raft
	raftCtrl                   *cluster.RaftController
	raftMux                    *cluster.RaftMultiplexer
	raftGroups                 map[string]*cluster.RaftController
	partitionGroups            map[string]*cluster.PartitionGroup
	partitionStores            map[string]Store
	partitionAdapters          map[string]*brokerStoreAdapter
	controllerQuorum           *cluster.ControllerQuorum
	wal                        *cluster.RaftWAL
	transport                  *cluster.TCPTransport
	startMu                    sync.Mutex
	reconcileMu                sync.Mutex
	placementMu                sync.Mutex
	queueListSequence          atomic.Uint64
	started                    bool
	closed                     bool
	stopping                   bool
	cb                         *circuitBreaker
	lastSubmitAt               atomic.Value // time.Time
	pending                    atomic.Int64
	startTime                  time.Time
	txRecoveryCancel           context.CancelFunc
	txRecoveryDone             chan struct{}
	partitionForwardClient     *fh.Client
	localPlacementSignalsMu    sync.Mutex
	localPlacementSignalsCache *localPlacementSignalsCacheEntry
}

func NewDistributed(cfg Config, distCfg DistributedConfig) (*DistributedBroker, error) {
	if !distCfg.UseEtcdRaft && !distCfg.AllowExperimentalCustomRaft {
		return nil, fmt.Errorf("%w: distributed mode requires either UseEtcdRaft: true (production etcd/raft consensus) or AllowExperimentalCustomRaft: true", ErrUnsupportedMode)
	}
	if distCfg.TransactionResolver != nil && !distCfg.UseEtcdRaft {
		return nil, fmt.Errorf("%w: automatic transaction recovery requires the etcd/raft controller quorum", ErrUnsupportedMode)
	}
	if distCfg.UseEtcdRaft && distCfg.TransactionRecoveryInterval <= 0 {
		distCfg.TransactionRecoveryInterval = 5 * time.Second
	}
	if distCfg.TransactionTimeout <= 0 {
		distCfg.TransactionTimeout = 30 * time.Second
	}
	if distCfg.UseEtcdRaft && distCfg.WALDir == "" {
		return nil, fmt.Errorf("%w: etcd/raft distributed mode requires WALDir for durable consensus state", ErrUnsupportedMode)
	}
	if distCfg.ReplicationFactor <= 0 {
		distCfg.ReplicationFactor = 3
	}
	if distCfg.MinInSyncReplicas <= 0 {
		distCfg.MinInSyncReplicas = distCfg.ReplicationFactor/2 + 1
	}
	if distCfg.MinInSyncReplicas > distCfg.ReplicationFactor {
		return nil, fmt.Errorf("%w: minimum in-sync replicas %d exceeds replication factor %d", cluster.ErrInvalidPartitionAssignment, distCfg.MinInSyncReplicas, distCfg.ReplicationFactor)
	}
	if distCfg.MaxReplicationLag == 0 {
		distCfg.MaxReplicationLag = 1000
	}
	if distCfg.ReplicationTimeout <= 0 {
		distCfg.ReplicationTimeout = 5 * time.Second
	}
	if distCfg.JoinExisting {
		if !distCfg.UseEtcdRaft || distCfg.RaftNodeID == 0 || len(distCfg.RaftPeers) < 2 {
			return nil, fmt.Errorf("%w: joining requires etcd/raft, a non-zero RaftNodeID, and the current peer map", ErrUnsupportedMode)
		}
		if _, ok := distCfg.RaftPeers[distCfg.RaftNodeID]; !ok {
			return nil, fmt.Errorf("%w: joining peer map does not contain RaftNodeID %d", ErrUnsupportedMode, distCfg.RaftNodeID)
		}
	}
	// Distributed construction is intentionally explicit; Config.Mode alone
	// must never make a local broker appear clustered.
	cfg.Mode = ModeEmbedded
	b, err := New(cfg)
	if err != nil {
		return nil, err
	}
	b.cfg.Mode = ModeDistributed
	if distCfg.UseEtcdRaft {
		if _, ok := b.store.(distributedStateStore); !ok {
			return nil, fmt.Errorf("%w: distributed etcd/raft requires a snapshot-capable deterministic state store", ErrUnsupportedMode)
		}
	}

	if distCfg.SubmitTimeout <= 0 {
		distCfg.SubmitTimeout = 5 * time.Second
	}
	if distCfg.ForwardTimeout <= 0 {
		distCfg.ForwardTimeout = 3 * time.Second
	}

	transport := cluster.NewTCPTransport()
	transport.TLSConfig = distCfg.TLSConfig

	var wal *cluster.RaftWAL
	if !distCfg.UseEtcdRaft && distCfg.WALDir != "" {
		wal, err = cluster.NewRaftWAL(cluster.WALConfig{Directory: distCfg.WALDir})
		if err != nil {
			return nil, fmt.Errorf("raft wal: %w", err)
		}
	}

	storeAdapter := &brokerStoreAdapter{store: b.store}
	brokerSM := cluster.NewBrokerStateMachine(storeAdapter)
	metadata := cluster.NewClusterMetadata()
	metadataSM := cluster.NewMetadataStateMachine(metadata)
	sm, err := cluster.NewBrokerMetadataStateMachine(brokerSM, metadataSM)
	if err != nil {
		return nil, fmt.Errorf("composite broker state machine: %w", err)
	}

	var (
		raft     *cluster.Raft
		raftCtrl *cluster.RaftController
		raftMux  *cluster.RaftMultiplexer
		cq       *cluster.ControllerQuorum
	)

	if distCfg.UseEtcdRaft {
		raftMux, err = cluster.NewRaftMultiplexer(transport)
		if err != nil {
			return nil, fmt.Errorf("Raft multiplexer: %w", err)
		}
		nodeID := distCfg.RaftNodeID
		if nodeID == 0 {
			nodeID = 1
		}
		peers := distCfg.RaftPeers
		if len(peers) == 0 {
			peers = map[uint64]string{nodeID: cfg.NodeID}
		}
		if _, ok := peers[nodeID]; !ok {
			return nil, fmt.Errorf("%w: Raft peer map is missing local node %d", ErrUnsupportedMode, nodeID)
		}
		if len(peers) > 1 && distCfg.TLSConfig == nil {
			return nil, fmt.Errorf("%w: multi-node etcd/raft requires mutual TLS peer authentication", ErrUnsupportedMode)
		}
		if distCfg.TLSConfig != nil {
			localAddress, ok := peers[nodeID]
			if !ok {
				return nil, fmt.Errorf("%w: peer identity map is missing local Raft node %d", ErrUnsupportedMode, nodeID)
			}
			peerAddresses := make([]string, 0, len(peers)-1)
			for peerID, address := range peers {
				if peerID != nodeID {
					peerAddresses = append(peerAddresses, address)
				}
			}
			if err := transport.ConfigurePeerIdentities(localAddress, peerAddresses); err != nil {
				return nil, fmt.Errorf("configure Raft peer identities: %w", err)
			}
		}
		controllerTransport, err := raftMux.GroupTransport("")
		if err != nil {
			return nil, fmt.Errorf("controller group transport: %w", err)
		}
		ctrl, err := cluster.NewRaftController(cluster.RaftControllerConfig{
			ID:                     nodeID,
			GroupID:                "",
			NodeID:                 cfg.NodeID,
			Peers:                  peers,
			ElectionTick:           10,
			HeartbeatTick:          1,
			TickInterval:           50 * time.Millisecond,
			ReadIndexTimeout:       distCfg.ReadIndexTimeout,
			Transport:              controllerTransport,
			StateMachine:           sm,
			StorageDir:             distCfg.WALDir,
			SkipBootstrap:          distCfg.JoinExisting,
			SnapshotEntries:        distCfg.SnapshotEntries,
			SnapshotCatchUpEntries: distCfg.SnapshotCatchUpEntries,
		})
		if err != nil {
			return nil, fmt.Errorf("etcd raft controller: %w", err)
		}
		if err := raftMux.Register(ctrl); err != nil {
			_ = ctrl.Stop()
			return nil, fmt.Errorf("register controller Raft group: %w", err)
		}
		raftCtrl = ctrl
		cq, err = cluster.NewControllerQuorum(cluster.ControllerQuorumConfig{
			Controller:       raftCtrl,
			Metadata:         metadata,
			PeerIDForAddress: raftCtrl.PeerID,
			PeerRoleForAddress: func(address string) string {
				peerID, ok := raftCtrl.PeerID(address)
				if !ok {
					return ""
				}
				return raftCtrl.PeerRole(peerID)
			},
			RequireClientAddress: true,
		})
		if err != nil {
			return nil, fmt.Errorf("controller quorum: %w", err)
		}
	} else {
		raftCfg := cluster.RaftConfig{
			ID:             cfg.NodeID,
			Peers:          distCfg.Peers,
			Transport:      transport,
			StateMachine:   sm,
			WAL:            wal,
			OnLeaderChange: distCfg.OnLeaderChange,
			OnStateChange:  wrapStateCallback(distCfg.OnStateChange),
			OnConfigChange: nil,
		}
		raft = cluster.NewRaft(raftCfg)
	}

	origStore := b.store
	b.store = &ReplicatedStore{
		Store:           origStore,
		raft:            raft,
		raftCtrl:        raftCtrl,
		commitTimeout:   distCfg.SubmitTimeout,
		forwardTimeout:  distCfg.ForwardTimeout,
		cb:              newCircuitBreaker(5, 10*time.Second),
		allowStaleReads: distCfg.AllowStaleReads,
	}

	db := &DistributedBroker{
		Broker:            b,
		distCfg:           distCfg,
		raft:              raft,
		raftCtrl:          raftCtrl,
		raftMux:           raftMux,
		raftGroups:        make(map[string]*cluster.RaftController),
		partitionGroups:   make(map[string]*cluster.PartitionGroup),
		partitionStores:   make(map[string]Store),
		partitionAdapters: make(map[string]*brokerStoreAdapter),
		controllerQuorum:  cq,
		wal:               wal,
		transport:         transport,
		cb:                newCircuitBreaker(5, 10*time.Second),
	}
	if distCfg.TLSConfig != nil {
		forwardTLS := distCfg.TLSConfig.Clone()
		forwardTLS.ServerName = ""
		forwardTimeout := distCfg.ForwardTimeout
		if distCfg.SubmitTimeout > forwardTimeout {
			forwardTimeout = distCfg.SubmitTimeout
		}
		db.partitionForwardClient = NewHTTPClient(HTTPClientConfig{
			Timeout:          forwardTimeout,
			TLSConfig:        forwardTLS,
			AllowInsecureTLS: forwardTLS.InsecureSkipVerify,
			DisableRedirects: true,
		})
	}
	b.distributedPartitions = db
	if distCfg.UseEtcdRaft && b.cfg.SchemaRegistry == nil && cq != nil {
		b.cfg.SchemaRegistry = &ControllerSchemaRegistry{Controller: cq}
	}
	if replicated, ok := b.store.(*ReplicatedStore); ok && distCfg.UseEtcdRaft {
		replicated.partitionRouter = db
	}

	db.lastSubmitAt.Store(time.Time{})

	return db, nil
}

// ControllerQuorum returns the replicated cluster metadata API when the
// distributed broker uses etcd/raft. It is nil on the experimental legacy
// consensus path.
func (db *DistributedBroker) ControllerQuorum() *cluster.ControllerQuorum {
	return db.controllerQuorum
}

// AddRaftGroup registers an independent etcd/raft state machine on the
// distributed broker's authenticated shared transport. The controller group
// remains the legacy empty group ID; application groups must use non-empty IDs.
func (db *DistributedBroker) AddRaftGroup(cfg cluster.RaftGroupConfig) (*cluster.RaftController, error) {
	db.startMu.Lock()
	defer db.startMu.Unlock()
	if db.closed || db.stopping {
		return nil, ErrClosed
	}
	if db.raftMux == nil {
		return nil, fmt.Errorf("%w: independent Raft groups require etcd/raft mode", ErrUnsupportedMode)
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("Raft group ID must not be empty; the empty ID is reserved for the controller group")
	}
	if cfg.ID == 0 || len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("Raft group %q requires a non-zero local peer ID and a complete peer map", cfg.GroupID)
	}
	if len(cfg.Peers) > 1 && !db.transport.PeerIdentityValidationEnabled() {
		return nil, fmt.Errorf("%w: multi-node Raft groups require mutual TLS peer authentication", ErrUnsupportedMode)
	}
	localAddress, exists := cfg.Peers[cfg.ID]
	if !exists {
		return nil, fmt.Errorf("Raft group %q peer map does not contain local ID %d", cfg.GroupID, cfg.ID)
	}
	seenAddresses := make(map[string]uint64, len(cfg.Peers))
	for peerID, address := range cfg.Peers {
		if peerID == 0 || address == "" {
			return nil, fmt.Errorf("Raft group %q has an empty peer ID or address", cfg.GroupID)
		}
		if previousID, duplicate := seenAddresses[address]; duplicate {
			return nil, fmt.Errorf("Raft group %q maps peer IDs %d and %d to the same address", cfg.GroupID, previousID, peerID)
		}
		seenAddresses[address] = peerID
	}
	controllerID := db.distCfg.RaftNodeID
	if controllerID == 0 {
		controllerID = 1
	}
	controllerAddress := db.distCfg.RaftPeers[controllerID]
	if controllerAddress == "" {
		controllerAddress = db.Broker.cfg.NodeID
	}
	if localAddress != controllerAddress {
		return nil, fmt.Errorf("Raft group %q local address %q does not match shared transport identity %q", cfg.GroupID, localAddress, controllerAddress)
	}
	if _, exists := db.raftGroups[cfg.GroupID]; exists {
		return nil, fmt.Errorf("Raft group %q already exists", cfg.GroupID)
	}
	groupTransport, err := db.raftMux.GroupTransport(cfg.GroupID)
	if err != nil {
		return nil, err
	}
	controller, err := cluster.NewRaftController(cluster.RaftControllerConfig{
		ID: cfg.ID, GroupID: cfg.GroupID, NodeID: cfg.NodeID, Peers: cfg.Peers,
		ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		TickInterval: 50 * time.Millisecond, ReadIndexTimeout: db.distCfg.ReadIndexTimeout,
		Transport: groupTransport, StateMachine: cfg.StateMachine,
		StorageDir: db.distCfg.WALDir, SkipBootstrap: cfg.SkipBootstrap,
		SnapshotEntries: db.distCfg.SnapshotEntries, SnapshotCatchUpEntries: db.distCfg.SnapshotCatchUpEntries,
	})
	if err != nil {
		return nil, err
	}
	if err := db.raftMux.Register(controller); err != nil {
		_ = controller.Stop()
		return nil, err
	}
	db.raftGroups[cfg.GroupID] = controller
	if db.started {
		if err := controller.Start(); err != nil {
			delete(db.raftGroups, cfg.GroupID)
			_ = db.raftMux.Unregister(controller)
			return nil, err
		}
		if controller.PeerCount() <= 1 {
			if err := controller.Campaign(); err != nil {
				_ = controller.Stop()
				delete(db.raftGroups, cfg.GroupID)
				_ = db.raftMux.Unregister(controller)
				return nil, err
			}
		}
	}
	return controller, nil
}

// RemoveRaftGroup stops and unregisters an application Raft group.
func (db *DistributedBroker) RemoveRaftGroup(groupID string) error {
	db.startMu.Lock()
	defer db.startMu.Unlock()
	if db.closed || db.stopping {
		return ErrClosed
	}
	controller := db.raftGroups[groupID]
	if controller == nil {
		return fmt.Errorf("Raft group %q not found", groupID)
	}
	if err := controller.Stop(); err != nil {
		return err
	}
	if err := db.raftMux.Unregister(controller); err != nil {
		return err
	}
	if partitionGroup := db.partitionGroups[groupID]; partitionGroup != nil {
		_ = partitionGroup.Close()
		delete(db.partitionGroups, groupID)
	}
	delete(db.raftGroups, groupID)
	delete(db.partitionAdapters, groupID)
	if store := db.partitionStores[groupID]; store != nil {
		delete(db.partitionStores, groupID)
		return store.Close()
	}
	return nil
}

// PartitionGroup returns the reconciled partition quorum and fencing view for
// one logical partition. It is nil until controller metadata has been
// reconciled on this broker.
func (db *DistributedBroker) PartitionGroup(id cluster.PartitionGroupID) (*cluster.PartitionGroup, bool) {
	groupID := "partition/" + id.String()
	db.startMu.Lock()
	defer db.startMu.Unlock()
	group, ok := db.partitionGroups[groupID]
	return group, ok
}

// LivePartitionProgress returns leader-observed replication snapshots for
// partition groups led by this broker. Each snapshot includes logical replica
// IDs, match indexes, lag, and recent Raft activity. Other brokers expose their
// own leader snapshots through the same authenticated cluster status API.
func (db *DistributedBroker) LivePartitionProgress() []cluster.PartitionProgress {
	db.startMu.Lock()
	groups := make([]*cluster.PartitionGroup, 0, len(db.partitionGroups))
	for _, group := range db.partitionGroups {
		groups = append(groups, group)
	}
	db.startMu.Unlock()

	progress := make([]cluster.PartitionProgress, 0, len(groups))
	for _, group := range groups {
		if snapshot := group.LiveProgress(); snapshot != nil {
			progress = append(progress, *snapshot)
		}
	}
	sort.Slice(progress, func(i, j int) bool {
		return progress[i].ID.String() < progress[j].ID.String()
	})
	return progress
}

func (db *DistributedBroker) partitionGroupNotLeaderError(ctx context.Context, controller *cluster.RaftController) error {
	if controller == nil {
		return ErrNotLeader
	}
	if ctx == nil {
		ctx = context.Background()
	}
	peerAddress, ok := controller.PeerAddress(controller.LeaderID())
	if !ok || peerAddress == "" || db == nil || db.controllerQuorum == nil {
		return NotLeaderError("", controller.Term())
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return NotLeaderError("", controller.Term())
	}
	for _, node := range snapshot.Nodes {
		if node != nil && node.Address == peerAddress {
			// Raft transport addresses are private to inter-broker replication.
			// Clients must only be directed to an advertised broker endpoint.
			return NotLeaderError(node.ClientAddress, controller.Term())
		}
	}
	return NotLeaderError("", controller.Term())
}

// AssignPartition validates the configured replica count before committing an
// initial placement. Existing placement changes require the safe reassignment
// protocol and are rejected by the controller quorum.
func (db *DistributedBroker) AssignPartition(ctx context.Context, topic string, partition int, replicas []string, leader string) error {
	return db.AssignResourcePartition(ctx, cluster.PartitionResourceTopic, topic, partition, replicas, leader)
}

// AssignResourcePartition commits an initial placement for a topic or stream
// partition through the controller quorum.
func (db *DistributedBroker) AssignResourcePartition(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int, replicas []string, leader string) error {
	if db.controllerQuorum == nil {
		return ErrUnsupportedMode
	}
	if len(replicas) != db.distCfg.ReplicationFactor {
		return fmt.Errorf("%w: partition has %d replicas; configured replication factor is %d", cluster.ErrInvalidPartitionAssignment, len(replicas), db.distCfg.ReplicationFactor)
	}
	return db.controllerQuorum.AssignResourcePartition(ctx, kind, name, partition, replicas, leader)
}

func placementTieScore(key, nodeID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(nodeID))
	return h.Sum64()
}

func weightedPlacementLoadLess(countA int, weightA uint32, countB int, weightB uint32) bool {
	wA, wB := int(weightA), int(weightB)
	qA, rA := countA/wA, countA%wA
	qB, rB := countB/wB, countB%wB
	if qA != qB {
		return qA < qB
	}
	return rA*wB < rB*wA
}

// PlanResourcePartitionPlacement picks active authenticated nodes with the
// lowest replica counts relative to their declared capacity weights, preferring
// healthy and caught-up voters and using complete live partition/workload
// signals as tie-breakers when available.
// Deterministic hash tie-breaking spreads adjacent partition IDs without
// requiring local mutable placement state.
func (db *DistributedBroker) PlanResourcePartitionPlacement(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int) (*PartitionPlacementPlan, error) {
	kind = (cluster.PartitionGroupID{Kind: kind}).Canonical().Kind
	if db.controllerQuorum == nil || db.raftCtrl == nil {
		return nil, ErrUnsupportedMode
	}
	if name == "" || partition < 0 {
		return nil, fmt.Errorf("%w: resource name and non-negative partition are required", cluster.ErrInvalidPartitionAssignment)
	}
	if _, err := db.controllerQuorum.GetResourcePartitionLinearizable(ctx, kind, name, partition); err == nil {
		return nil, fmt.Errorf("%w: partition %s/%d is already assigned", cluster.ErrPartitionReassignmentRequired, name, partition)
	} else if !errors.Is(err, cluster.ErrPartitionNotFound) {
		return nil, err
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		node *cluster.NodeInfo
		load NodePartitionLoad
		tie  uint64
	}
	raftStatus := db.raftCtrl.Status()
	controllerCommit := raftStatus.HardState.GetCommit()
	candidates := make([]candidate, 0, len(snapshot.Nodes))
	byID := make(map[string]int, len(snapshot.Nodes))
	addresses := make(map[string]string, len(snapshot.Nodes))
	activeNodes := make([]*cluster.NodeInfo, 0, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if node == nil || node.State != cluster.NodeActive || node.Address == "" || node.ClientAddress == "" {
			continue
		}
		peerID, ok := db.raftCtrl.PeerID(node.Address)
		if !ok || db.raftCtrl.PeerRole(peerID) != "voter" {
			continue
		}
		if previous, duplicate := addresses[node.Address]; duplicate {
			return nil, fmt.Errorf("%w: logical nodes %q and %q share one Raft address", cluster.ErrInvalidPartitionAssignment, previous, node.ID)
		}
		addresses[node.Address] = node.ID
		activeNodes = append(activeNodes, node)
		byID[node.ID] = len(candidates)
		load := NodePartitionLoad{
			NodeID: node.ID, FailureDomain: strings.TrimSpace(node.Metadata[cluster.NodeMetadataFailureDomain]),
			CapacityWeight: cluster.NodeCapacityWeight(node),
		}
		if progress, known := raftStatus.Progress[peerID]; known {
			load.ControllerProgressKnown = true
			load.ControllerRecentActive = progress.RecentActive
			if controllerCommit > progress.Match {
				load.ControllerLag = controllerCommit - progress.Match
			}
		}
		candidates = append(candidates, candidate{node: node, load: load})
	}
	for _, assigned := range snapshot.Partitions {
		for _, replica := range assigned.Replicas {
			if index, exists := byID[replica]; exists {
				candidates[index].load.ReplicaCount++
			}
		}
		if index, exists := byID[assigned.Leader]; exists {
			candidates[index].load.LeaderCount++
		}
	}
	if len(candidates) < db.distCfg.ReplicationFactor {
		return nil, fmt.Errorf("%w: %d active authenticated nodes with client addresses are available; replication factor is %d", cluster.ErrInvalidPartitionAssignment, len(candidates), db.distCfg.ReplicationFactor)
	}
	liveSignals, liveWorkloadUsed, livePartitionLagUsed, liveDiskCapacityUsed, _ := db.gatherPlacementSignals(ctx, activeNodes, snapshot.Partitions)
	if liveWorkloadUsed || livePartitionLagUsed || liveDiskCapacityUsed {
		latest, readErr := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
		if readErr != nil || !samePlacementTopology(snapshot, latest) {
			liveSignals, liveWorkloadUsed, livePartitionLagUsed, liveDiskCapacityUsed = nil, false, false, false
		}
	}
	for i := range candidates {
		if signals, known := liveSignals[candidates[i].node.ID]; known {
			candidates[i].load.WorkloadKnown = signals.WorkloadKnown
			candidates[i].load.WorkloadMessages = signals.WorkloadMessages
			candidates[i].load.WorkloadLeased = signals.WorkloadLeased
			candidates[i].load.DiskUsageKnown = signals.DiskUsageKnown
			candidates[i].load.DiskUsedBytes = signals.DiskUsedBytes
			candidates[i].load.DiskAvailableBytes = signals.DiskAvailableBytes
			candidates[i].load.DiskCapacityBytes = signals.DiskCapacityBytes
			candidates[i].load.PartitionProgressKnown = signals.PartitionProgressKnown
			candidates[i].load.PartitionRecentActive = signals.PartitionRecentActive
			candidates[i].load.PartitionInSync = signals.PartitionInSync
			candidates[i].load.PartitionLag = signals.PartitionLag
		}
	}
	key := fmt.Sprintf("%s/%s/%d", kind, name, partition)
	for i := range candidates {
		candidates[i].tie = placementTieScore(key, candidates[i].node.ID)
	}
	placementLess := func(a, b candidate) bool {
		if weightedPlacementLoadLess(a.load.ReplicaCount, a.load.CapacityWeight, b.load.ReplicaCount, b.load.CapacityWeight) {
			return true
		}
		if weightedPlacementLoadLess(b.load.ReplicaCount, b.load.CapacityWeight, a.load.ReplicaCount, a.load.CapacityWeight) {
			return false
		}
		if a.load.PartitionProgressKnown != b.load.PartitionProgressKnown {
			return a.load.PartitionProgressKnown
		}
		if a.load.PartitionRecentActive != b.load.PartitionRecentActive {
			return a.load.PartitionRecentActive
		}
		if a.load.PartitionInSync != b.load.PartitionInSync {
			return a.load.PartitionInSync
		}
		if a.load.PartitionLag != b.load.PartitionLag {
			return a.load.PartitionLag < b.load.PartitionLag
		}
		if a.load.DiskUsageKnown != b.load.DiskUsageKnown {
			return a.load.DiskUsageKnown
		}
		if a.load.DiskUsageKnown && a.load.DiskAvailableBytes != b.load.DiskAvailableBytes {
			return a.load.DiskAvailableBytes > b.load.DiskAvailableBytes
		}
		if a.load.WorkloadKnown != b.load.WorkloadKnown {
			return a.load.WorkloadKnown
		}
		if a.load.WorkloadKnown {
			if weightedPlacementLoadLess(a.load.WorkloadMessages, a.load.CapacityWeight, b.load.WorkloadMessages, b.load.CapacityWeight) {
				return true
			}
			if weightedPlacementLoadLess(b.load.WorkloadMessages, b.load.CapacityWeight, a.load.WorkloadMessages, a.load.CapacityWeight) {
				return false
			}
			if weightedPlacementLoadLess(a.load.WorkloadLeased, a.load.CapacityWeight, b.load.WorkloadLeased, b.load.CapacityWeight) {
				return true
			}
			if weightedPlacementLoadLess(b.load.WorkloadLeased, b.load.CapacityWeight, a.load.WorkloadLeased, a.load.CapacityWeight) {
				return false
			}
		}
		if a.load.ControllerProgressKnown != b.load.ControllerProgressKnown {
			return a.load.ControllerProgressKnown
		}
		if a.load.ControllerRecentActive != b.load.ControllerRecentActive {
			return a.load.ControllerRecentActive
		}
		if a.load.ControllerLag != b.load.ControllerLag {
			return a.load.ControllerLag < b.load.ControllerLag
		}
		if weightedPlacementLoadLess(a.load.LeaderCount, a.load.CapacityWeight, b.load.LeaderCount, b.load.CapacityWeight) {
			return true
		}
		if weightedPlacementLoadLess(b.load.LeaderCount, b.load.CapacityWeight, a.load.LeaderCount, a.load.CapacityWeight) {
			return false
		}
		if a.tie != b.tie {
			return a.tie < b.tie
		}
		return a.node.ID < b.node.ID
	}
	selectReplica := func(pool []candidate) []candidate {
		selected := make([]candidate, 0, minInt(db.distCfg.ReplicationFactor, len(pool)))
		used := make(map[string]struct{}, db.distCfg.ReplicationFactor)
		for len(selected) < db.distCfg.ReplicationFactor {
			best := -1
			bestDomainPenalty := 3
			for i := range pool {
				if pool[i].load.Selected {
					continue
				}
				penalty := 1 // unknown failure domain
				if domain := pool[i].load.FailureDomain; domain != "" {
					penalty = 0
					if _, exists := used[domain]; exists {
						penalty = 2
					}
				}
				if best < 0 || penalty < bestDomainPenalty || (penalty == bestDomainPenalty && placementLess(pool[i], pool[best])) {
					best, bestDomainPenalty = i, penalty
				}
			}
			if best < 0 {
				break
			}
			pool[best].load.Selected = true
			selected = append(selected, pool[best])
			if domain := pool[best].load.FailureDomain; domain != "" {
				used[domain] = struct{}{}
			}
		}
		return selected
	}
	selected := selectReplica(candidates)
	leader := selected[0]
	for _, candidate := range selected[1:] {
		betterLeader := false
		switch {
		case weightedPlacementLoadLess(candidate.load.LeaderCount, candidate.load.CapacityWeight, leader.load.LeaderCount, leader.load.CapacityWeight):
			betterLeader = true
		case weightedPlacementLoadLess(leader.load.LeaderCount, leader.load.CapacityWeight, candidate.load.LeaderCount, candidate.load.CapacityWeight):
			betterLeader = false
		case candidate.load.PartitionProgressKnown != leader.load.PartitionProgressKnown:
			betterLeader = candidate.load.PartitionProgressKnown
		case candidate.load.PartitionRecentActive != leader.load.PartitionRecentActive:
			betterLeader = candidate.load.PartitionRecentActive
		case candidate.load.PartitionInSync != leader.load.PartitionInSync:
			betterLeader = candidate.load.PartitionInSync
		case candidate.load.PartitionLag != leader.load.PartitionLag:
			betterLeader = candidate.load.PartitionLag < leader.load.PartitionLag
		case candidate.load.DiskUsageKnown != leader.load.DiskUsageKnown:
			betterLeader = candidate.load.DiskUsageKnown
		case candidate.load.DiskUsageKnown && candidate.load.DiskAvailableBytes != leader.load.DiskAvailableBytes:
			betterLeader = candidate.load.DiskAvailableBytes > leader.load.DiskAvailableBytes
		case candidate.load.WorkloadKnown && leader.load.WorkloadKnown && candidate.load.WorkloadMessages != leader.load.WorkloadMessages:
			betterLeader = weightedPlacementLoadLess(candidate.load.WorkloadMessages, candidate.load.CapacityWeight, leader.load.WorkloadMessages, leader.load.CapacityWeight)
		case candidate.load.WorkloadKnown && leader.load.WorkloadKnown && candidate.load.WorkloadLeased != leader.load.WorkloadLeased:
			betterLeader = weightedPlacementLoadLess(candidate.load.WorkloadLeased, candidate.load.CapacityWeight, leader.load.WorkloadLeased, leader.load.CapacityWeight)
		case candidate.load.ControllerProgressKnown != leader.load.ControllerProgressKnown:
			betterLeader = candidate.load.ControllerProgressKnown
		case candidate.load.ControllerRecentActive != leader.load.ControllerRecentActive:
			betterLeader = candidate.load.ControllerRecentActive
		case candidate.load.ControllerLag != leader.load.ControllerLag:
			betterLeader = candidate.load.ControllerLag < leader.load.ControllerLag
		default:
			betterLeader = candidate.tie < leader.tie
		}
		if betterLeader {
			leader = candidate
		}
	}
	plan := &PartitionPlacementPlan{
		ID:                   (cluster.PartitionGroupID{Kind: kind, Topic: name, Partition: partition}).Canonical(),
		LivePartitionLagUsed: livePartitionLagUsed, LiveWorkloadUsed: liveWorkloadUsed,
		LiveDiskCapacityUsed: liveDiskCapacityUsed,
	}
	for i := range selected {
		selected[i].load.Selected = true
		selected[i].load.Leader = selected[i].node.ID == leader.node.ID
		plan.Replicas = append(plan.Replicas, selected[i].node.ID)
	}
	plan.Leader = leader.node.ID
	for _, candidate := range candidates {
		plan.Loads = append(plan.Loads, candidate.load)
	}
	sort.Strings(plan.Replicas)
	sort.Slice(plan.Loads, func(i, j int) bool { return plan.Loads[i].NodeID < plan.Loads[j].NodeID })
	for i := range plan.Loads {
		if plan.Loads[i].NodeID == leader.node.ID {
			plan.Loads[i].Leader = true
			break
		}
	}
	return plan, nil
}

// AssignResourcePartitionBalanced commits a deterministic load-aware initial
// placement. Retries return an existing assignment unchanged.
func (db *DistributedBroker) AssignResourcePartitionBalanced(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int) (*cluster.PartitionMetadata, error) {
	if db.controllerQuorum == nil {
		return nil, ErrUnsupportedMode
	}
	// Serialize local auto-assignments so each placement sees the committed
	// result of the previous one instead of reusing the same stale load snapshot.
	db.placementMu.Lock()
	defer db.placementMu.Unlock()
	kind = (cluster.PartitionGroupID{Kind: kind}).Canonical().Kind
	if current, err := db.controllerQuorum.GetResourcePartitionLinearizable(ctx, kind, name, partition); err == nil {
		return current, nil
	} else if !errors.Is(err, cluster.ErrPartitionNotFound) {
		return nil, err
	}
	plan, err := db.PlanResourcePartitionPlacement(ctx, kind, name, partition)
	if err != nil {
		if current, readErr := db.controllerQuorum.GetResourcePartitionLinearizable(ctx, kind, name, partition); readErr == nil {
			return current, nil
		}
		return nil, err
	}
	candidates := make([]cluster.PartitionPlacementCandidate, 0, len(plan.Loads))
	for _, load := range plan.Loads {
		candidates = append(candidates, cluster.PartitionPlacementCandidate{
			NodeID: load.NodeID, ReplicationLag: load.ControllerLag,
			ProgressKnown: load.ControllerProgressKnown, RecentActive: load.ControllerRecentActive,
			PartitionLag: load.PartitionLag, PartitionProgressKnown: load.PartitionProgressKnown,
			PartitionRecentActive: load.PartitionRecentActive, PartitionInSync: load.PartitionInSync,
			WorkloadMessages: load.WorkloadMessages, WorkloadLeased: load.WorkloadLeased,
			WorkloadKnown: load.WorkloadKnown, DiskUsageKnown: load.DiskUsageKnown,
			DiskAvailableBytes: load.DiskAvailableBytes,
		})
	}
	if err := db.controllerQuorum.AssignBalancedResourcePartition(ctx, kind, name, partition, db.distCfg.ReplicationFactor, candidates); err != nil {
		if current, readErr := db.controllerQuorum.GetResourcePartitionLinearizable(ctx, kind, name, partition); readErr == nil {
			return current, nil
		}
		return nil, err
	}
	return db.controllerQuorum.GetResourcePartitionLinearizable(ctx, kind, name, partition)
}

// PlanResourcePartitionReassignment validates a desired replica set and
// describes the learner, catch-up, leader-transfer, metadata-commit, and removal
// sequence. It does not mutate Raft membership or controller metadata.
func (db *DistributedBroker) PlanResourcePartitionReassignment(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int, replicas []string, leader string) (*PartitionReassignmentPlan, error) {
	kind = (cluster.PartitionGroupID{Kind: kind}).Canonical().Kind
	if db.controllerQuorum == nil || db.raftCtrl == nil {
		return nil, ErrUnsupportedMode
	}
	if name == "" || partition < 0 || len(replicas) != db.distCfg.ReplicationFactor {
		return nil, fmt.Errorf("%w: name, non-negative partition, and exactly %d desired replicas are required", cluster.ErrInvalidPartitionAssignment, db.distCfg.ReplicationFactor)
	}
	desired := append([]string(nil), replicas...)
	sort.Strings(desired)
	for i, id := range desired {
		if id == "" || (i > 0 && desired[i-1] == id) {
			return nil, fmt.Errorf("%w: desired replica IDs must be non-empty and unique", cluster.ErrInvalidPartitionAssignment)
		}
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	var current *cluster.PartitionMetadata
	nodesByID := make(map[string]*cluster.NodeInfo, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodesByID[node.ID] = node
	}
	for _, candidate := range snapshot.Partitions {
		if candidate.ID.Canonical().Kind == kind && candidate.ID.Topic == name && candidate.ID.Partition == partition {
			current = candidate
			break
		}
	}
	if current == nil {
		return nil, cluster.ErrPartitionNotFound
	}
	if leader == "" {
		if current.Reassignment != nil {
			leader = current.Reassignment.DesiredLeader
		} else {
			leader = current.Leader
		}
	}
	if !slices.Contains(desired, leader) {
		return nil, fmt.Errorf("%w: desired leader must be in the desired replica set", cluster.ErrInvalidPartitionAssignment)
	}
	pending := current.Reassignment
	if pending != nil && (pending.DesiredLeader != leader || !sameStringSet(pending.DesiredReplicas, desired)) {
		return nil, cluster.ErrPartitionReassignmentRequired
	}
	plan := &PartitionReassignmentPlan{
		ID: current.ID.Canonical(), CurrentReplicas: append([]string(nil), current.Replicas...),
		DesiredReplicas: desired, CurrentLeader: current.Leader, DesiredLeader: leader,
		LiveISR: append([]string(nil), current.ISR...), TransferLeader: leader != current.Leader,
		ReadyToStart: pending == nil, InProgress: pending != nil,
	}
	if pending != nil {
		plan.Phase = pending.Phase
		plan.ReadyToContinue = true
		plan.CurrentReplicas = append([]string(nil), pending.ExpectedReplicas...)
		plan.CurrentLeader = pending.ExpectedLeader
		plan.TransferLeader = pending.DesiredLeader != pending.ExpectedLeader
	}
	sourceReplicas := current.Replicas
	if pending != nil {
		sourceReplicas = pending.ExpectedReplicas
	}
	currentSet := make(map[string]struct{}, len(sourceReplicas))
	desiredSet := make(map[string]struct{}, len(desired))
	for _, id := range sourceReplicas {
		currentSet[id] = struct{}{}
	}
	peerOwners := make(map[uint64]string, len(desired))
	for _, id := range desired {
		desiredSet[id] = struct{}{}
		if _, exists := currentSet[id]; !exists {
			plan.AddLearners = append(plan.AddLearners, id)
			plan.PromoteAfterCatchup = append(plan.PromoteAfterCatchup, id)
		}
		node := nodesByID[id]
		alreadyAssigned := pending != nil && slices.Contains(pending.ExpectedReplicas, id)
		removingDuringAbort := pending != nil && pending.Phase == cluster.PartitionReassignmentAborting && !alreadyAssigned
		if node == nil || node.Address == "" || (node.State != cluster.NodeActive && !alreadyAssigned && !removingDuringAbort) {
			plan.ReadyToStart = false
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("desired replica %q is not registered and active", id))
			continue
		}
		if node.ClientAddress == "" {
			plan.ReadyToStart = false
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("desired replica %q has no advertised client address", id))
			continue
		}
		peerID, ok := db.raftCtrl.PeerID(node.Address)
		if !ok || db.raftCtrl.PeerRole(peerID) != "voter" {
			plan.ReadyToStart = false
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("desired replica %q is not an authenticated Raft voter", id))
			continue
		}
		if previous, duplicate := peerOwners[peerID]; duplicate {
			plan.ReadyToStart = false
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("desired replicas %q and %q map to the same Raft peer", previous, id))
		} else {
			peerOwners[peerID] = id
		}
	}
	for _, id := range sourceReplicas {
		if _, remains := desiredSet[id]; !remains {
			plan.RemoveAfterCommit = append(plan.RemoveAfterCommit, id)
		}
	}
	if pending != nil {
		plan.ReadyToStart = false
		plan.ReadyToContinue = len(plan.Blockers) == 0
	}
	sort.Strings(plan.AddLearners)
	sort.Strings(plan.PromoteAfterCatchup)
	sort.Strings(plan.RemoveAfterCommit)
	groupID := "partition/" + current.ID.String()
	if controller, exists := db.RaftGroup(groupID); exists {
		plan.JointChangePending = controller.JointConsensusPending()
		if plan.JointChangePending {
			plan.ReadyToStart = false
			plan.ReadyToContinue = false
			plan.Blockers = append(plan.Blockers, "partition group has a joint membership change in progress")
		}
		if !controller.IsLeader() {
			plan.ReadyToStart = false
			plan.ReadyToContinue = false
			plan.Blockers = append(plan.Blockers, "reassignment must be advanced on the current partition leader")
		}
		if group, ok := db.PartitionGroup(current.ID); ok {
			if pending == nil {
				plan.LiveISR = group.ISR()
			} else {
				desiredPeerIDs := make(map[string]uint64, len(pending.DesiredReplicas))
				for _, replica := range pending.DesiredReplicas {
					if node := nodesByID[replica]; node != nil {
						if peerID, member := controller.PeerID(node.Address); member {
							desiredPeerIDs[replica] = peerID
						}
					}
				}
				plan.LiveISR = group.LiveISRForPeers(desiredPeerIDs)
			}
			plan.MinimumISR = group.MinimumISR()
			if len(plan.LiveISR) < plan.MinimumISR {
				plan.ReadyToStart = false
				if pending == nil || pending.Phase == cluster.PartitionReassignmentCopying {
					plan.ReadyToContinue = false
					plan.Blockers = append(plan.Blockers, fmt.Sprintf("live ISR has %d replicas; at least %d are required", len(plan.LiveISR), plan.MinimumISR))
				}
			}
			groupReplicas := current.Replicas
			if pending != nil {
				groupReplicas = sortedUniqueStrings(append(append([]string(nil), pending.ExpectedReplicas...), pending.DesiredReplicas...))
			}
			if err := validateReconciledPartitionMembership(controller, current, groupReplicas, nodesByID); err != nil {
				plan.ReadyToStart = false
				if pending != nil {
					plan.ReadyToContinue = false
				}
				plan.Blockers = append(plan.Blockers, err.Error())
			}
			if pending == nil && controller.PeerCount() != len(current.Replicas) {
				plan.ReadyToStart = false
				plan.Blockers = append(plan.Blockers, "live Raft membership does not match the current controller assignment")
			}
			if pending != nil {
				switch pending.Phase {
				case cluster.PartitionReassignmentCopying:
					status := controller.Status()
					commit := uint64(0)
					if status.HardState != nil {
						commit = status.HardState.GetCommit()
					}
					plan.AddLearners = nil
					plan.PromoteAfterCatchup = nil
					for _, id := range pending.DesiredReplicas {
						if slices.Contains(pending.ExpectedReplicas, id) {
							continue
						}
						node := nodesByID[id]
						peerID, member := uint64(0), false
						if node != nil {
							peerID, member = controller.PeerID(node.Address)
						}
						if !member {
							plan.AddLearners = append(plan.AddLearners, id)
							plan.PromoteAfterCatchup = append(plan.PromoteAfterCatchup, id)
							plan.WaitingFor = append(plan.WaitingFor, id)
							continue
						}
						role := controller.PeerRole(peerID)
						if role == "learner" || role == "learner_next" {
							plan.PromoteAfterCatchup = append(plan.PromoteAfterCatchup, id)
						}
						if role != "voter" && role != "learner" && role != "learner_next" {
							plan.ReadyToContinue = false
							plan.Blockers = append(plan.Blockers, fmt.Sprintf("target replica %q has unexpected Raft role %q", id, role))
						}
						if role != "voter" || status.Progress[peerID].Match < commit {
							plan.WaitingFor = append(plan.WaitingFor, id)
						}
					}
				case cluster.PartitionReassignmentCleanup:
					plan.AddLearners = nil
					plan.PromoteAfterCatchup = nil
					plan.RemoveAfterCommit = nil
					for _, id := range pending.ExpectedReplicas {
						if slices.Contains(pending.DesiredReplicas, id) {
							continue
						}
						node := nodesByID[id]
						if node != nil {
							if peerID, member := controller.PeerID(node.Address); member && controller.PeerRole(peerID) != "" {
								plan.RemoveAfterCommit = append(plan.RemoveAfterCommit, id)
								plan.WaitingFor = append(plan.WaitingFor, id)
							}
						}
					}
				case cluster.PartitionReassignmentAborting:
					plan.AddLearners = nil
					plan.PromoteAfterCatchup = nil
					plan.RemoveAfterCommit = nil
					for _, id := range pending.DesiredReplicas {
						if slices.Contains(pending.ExpectedReplicas, id) {
							continue
						}
						node := nodesByID[id]
						if node != nil {
							if peerID, member := controller.PeerID(node.Address); member && controller.PeerRole(peerID) != "" {
								plan.RemoveDuringAbort = append(plan.RemoveDuringAbort, id)
								plan.WaitingFor = append(plan.WaitingFor, id)
							}
						}
					}
				default:
					plan.ReadyToContinue = false
					plan.Blockers = append(plan.Blockers, fmt.Sprintf("unknown durable reassignment phase %q", pending.Phase))
				}
				sort.Strings(plan.WaitingFor)
				sort.Strings(plan.RemoveAfterCommit)
				sort.Strings(plan.RemoveDuringAbort)
			}
			leaderNode := nodesByID[current.Leader]
			if leaderNode == nil {
				plan.ReadyToStart = false
				plan.Blockers = append(plan.Blockers, "current controller leader has no registered node")
			} else if expectedLeader, mapped := controller.PeerID(leaderNode.Address); !mapped || controller.LeaderID() != expectedLeader {
				plan.ReadyToStart = false
				plan.Blockers = append(plan.Blockers, "live Raft leader does not match the current controller assignment")
			}
		} else {
			plan.ReadyToStart = false
			plan.ReadyToContinue = false
			plan.Blockers = append(plan.Blockers, "partition quorum view is not reconciled on this broker")
		}
	} else {
		plan.ReadyToStart = false
		plan.ReadyToContinue = false
		plan.Blockers = append(plan.Blockers, "partition group is not reconciled on this broker")
	}
	return plan, nil
}

// PartitionReassignmentProgress reports one idempotent step of an online
// reassignment. Call the method again after it returns incomplete progress;
// catch-up and joint Raft changes can take longer than one request deadline.
type PartitionReassignmentProgress struct {
	Partition  *cluster.PartitionMetadata `json:"partition"`
	Phase      string                     `json:"phase"`
	WaitingFor []string                   `json:"waiting_for,omitempty"`
	Complete   bool                       `json:"complete"`
}

// ReassignResourcePartition advances a durable partition reassignment by one
// safe phase. It adds target replicas as learners, waits for them to catch up,
// promotes them, commits the desired controller assignment, removes old
// members, and clears the transition. It may transfer leadership to a desired
// replica after that replica catches up and enters the live ISR. Controller
// metadata proposals use authenticated Raft peer forwarding, so the two Raft
// groups need not share a local leader. Every step is derived from quorum
// metadata and live Raft membership, making retries resumable after failures.
func (db *DistributedBroker) ReassignResourcePartition(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int, replicas []string, leader string) (*PartitionReassignmentProgress, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	kind = (cluster.PartitionGroupID{Kind: kind}).Canonical().Kind
	if db == nil || db.controllerQuorum == nil || db.raftCtrl == nil {
		return nil, ErrUnsupportedMode
	}
	desired := append([]string(nil), replicas...)
	sort.Strings(desired)
	if len(desired) != db.distCfg.ReplicationFactor {
		return nil, fmt.Errorf("%w: exactly %d desired replicas are required", cluster.ErrInvalidPartitionAssignment, db.distCfg.ReplicationFactor)
	}
	for i := 1; i < len(desired); i++ {
		if desired[i] == desired[i-1] {
			return nil, fmt.Errorf("%w: desired replica IDs must be unique", cluster.ErrInvalidPartitionAssignment)
		}
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	var current *cluster.PartitionMetadata
	nodes := make(map[string]*cluster.NodeInfo, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodes[node.ID] = node
	}
	for _, candidate := range snapshot.Partitions {
		if candidate.ID.Canonical().Kind == kind && candidate.ID.Topic == name && candidate.ID.Partition == partition {
			current = candidate
			break
		}
	}
	if current == nil {
		return nil, cluster.ErrPartitionNotFound
	}
	if leader == "" {
		if current.Reassignment != nil {
			leader = current.Reassignment.DesiredLeader
		} else {
			leader = current.Leader
		}
	}
	if !slices.Contains(desired, leader) {
		return nil, fmt.Errorf("%w: desired leader must belong to the desired replica set", cluster.ErrInvalidPartitionAssignment)
	}
	if current.Reassignment == nil {
		if current.Leader == leader && sameStringSet(current.Replicas, desired) {
			return &PartitionReassignmentProgress{Partition: current, Phase: "complete", Complete: true}, nil
		}
		plan, planErr := db.PlanResourcePartitionReassignment(ctx, kind, name, partition, desired, leader)
		if planErr != nil {
			return nil, planErr
		}
		if !plan.ReadyToStart {
			return nil, fmt.Errorf("%w: reassignment cannot start: %s", cluster.ErrNodeUnavailable, strings.Join(plan.Blockers, "; "))
		}
		if err := db.controllerQuorum.BeginResourcePartitionReassignment(ctx, kind, name, partition, current.Replicas, current.Leader, current.LeaderEpoch, desired, leader); err != nil {
			return nil, err
		}
		return db.partitionReassignmentState(ctx, kind, name, partition, "copying", nil, false)
	}

	pending := current.Reassignment
	if pending.DesiredLeader != leader || !sameStringSet(pending.DesiredReplicas, desired) {
		return nil, cluster.ErrPartitionReassignmentRequired
	}
	groupID := "partition/" + current.ID.String()
	controller, ok := db.RaftGroup(groupID)
	if !ok {
		return nil, fmt.Errorf("%w: partition group %s is not reconciled on this broker", ErrBrokerUnavailable, current.ID)
	}
	group, ok := db.PartitionGroup(current.ID)
	if !ok {
		return nil, fmt.Errorf("%w: partition quorum view %s is not reconciled on this broker", ErrBrokerUnavailable, current.ID)
	}
	if !controller.IsLeader() {
		return nil, db.partitionGroupNotLeaderError(ctx, controller)
	}
	if controller.JointConsensusPending() {
		return nil, cluster.ErrConfChangePending
	}

	if pending.Phase == cluster.PartitionReassignmentCopying {
		if len(group.ISR()) < group.MinimumISR() {
			return nil, fmt.Errorf("%w: live ISR fell below the minimum during reassignment", cluster.ErrNodeUnavailable)
		}
		for _, replica := range pending.DesiredReplicas {
			if slices.Contains(pending.ExpectedReplicas, replica) {
				continue
			}
			node := nodes[replica]
			if node == nil || node.Address == "" {
				return nil, fmt.Errorf("%w: desired replica %q has no registered Raft address", cluster.ErrNodeNotFound, replica)
			}
			stablePeerID, registered := db.raftCtrl.PeerID(node.Address)
			if !registered || db.raftCtrl.PeerRole(stablePeerID) != "voter" {
				return nil, fmt.Errorf("%w: desired replica %q is not an authenticated controller voter", cluster.ErrNodeUnavailable, replica)
			}
			peerID, member := controller.PeerID(node.Address)
			if !member {
				if _, err := controller.AddLearnerWithID(ctx, node.Address, stablePeerID); err != nil {
					return nil, db.normalizePartitionRaftError(ctx, controller, err)
				}
				return db.partitionReassignmentState(ctx, kind, name, partition, "copying", []string{replica}, false)
			}
			switch controller.PeerRole(peerID) {
			case "learner":
				status := controller.Status()
				progress := status.Progress[peerID]
				if status.HardState == nil || progress.Match < status.HardState.GetCommit() {
					return db.partitionReassignmentState(ctx, kind, name, partition, "copying", []string{replica}, false)
				}
				if err := controller.PromoteLearner(ctx, peerID); err != nil {
					return nil, db.normalizePartitionRaftError(ctx, controller, err)
				}
				return db.partitionReassignmentState(ctx, kind, name, partition, "copying", []string{replica}, false)
			case "voter":
			case "learner_next":
				return db.partitionReassignmentState(ctx, kind, name, partition, "copying", []string{replica}, false)
			default:
				return nil, fmt.Errorf("%w: target replica %q has Raft role %q", cluster.ErrNodeUnavailable, replica, controller.PeerRole(peerID))
			}
		}

		status := controller.Status()
		if status.HardState == nil {
			return nil, db.partitionGroupNotLeaderError(ctx, controller)
		}
		commit := status.HardState.GetCommit()
		peerIDs := make(map[string]uint64, len(pending.DesiredReplicas))
		var waiting []string
		for _, replica := range pending.DesiredReplicas {
			node := nodes[replica]
			if node == nil {
				return nil, fmt.Errorf("%w: desired replica %q is no longer registered", cluster.ErrNodeNotFound, replica)
			}
			peerID, member := controller.PeerID(node.Address)
			if !member || controller.PeerRole(peerID) != "voter" {
				waiting = append(waiting, replica)
				continue
			}
			peerIDs[replica] = peerID
			if status.Progress[peerID].Match < commit {
				waiting = append(waiting, replica)
			}
		}
		if len(waiting) > 0 {
			return db.partitionReassignmentState(ctx, kind, name, partition, "copying", waiting, false)
		}
		liveISR := group.LiveISRForPeers(peerIDs)
		if len(liveISR) < group.MinimumISR() {
			return nil, fmt.Errorf("%w: desired live ISR fell below the minimum before reassignment commit", cluster.ErrNodeUnavailable)
		}
		if !slices.Contains(liveISR, pending.DesiredLeader) {
			return db.partitionReassignmentState(ctx, kind, name, partition, "copying", []string{pending.DesiredLeader}, false)
		}
		if controller.LeaderID() != peerIDs[pending.DesiredLeader] {
			if err := controller.TransferLeadership(ctx, peerIDs[pending.DesiredLeader]); err != nil {
				return nil, db.normalizePartitionRaftError(ctx, controller, err)
			}
			return db.partitionReassignmentState(ctx, kind, name, partition, "copying", nil, false)
		}
		resultEpoch := controller.Term()
		if resultEpoch < pending.ExpectedEpoch {
			return nil, cluster.ErrStaleEpoch
		}
		if err := db.controllerQuorum.CommitResourcePartitionReassignment(ctx, kind, name, partition, pending.ExpectedReplicas, pending.ExpectedLeader, pending.ExpectedEpoch, pending.DesiredReplicas, pending.DesiredLeader, resultEpoch, liveISR); err != nil {
			return nil, err
		}
		if err := group.UpdateAssignment(pending.DesiredReplicas, peerIDs, resultEpoch); err != nil {
			return nil, err
		}
		return db.partitionReassignmentState(ctx, kind, name, partition, "cleanup", pending.ExpectedReplicas, false)
	}

	if pending.Phase != cluster.PartitionReassignmentCleanup {
		return nil, cluster.ErrPartitionReassignmentRequired
	}
	desiredPeerIDs := make(map[string]uint64, len(pending.DesiredReplicas))
	for _, replica := range pending.DesiredReplicas {
		node := nodes[replica]
		if node == nil {
			return nil, fmt.Errorf("%w: desired replica %q is no longer registered", cluster.ErrNodeNotFound, replica)
		}
		peerID, member := controller.PeerID(node.Address)
		if !member || controller.PeerRole(peerID) != "voter" {
			return nil, fmt.Errorf("%w: desired replica %q is not a live Raft voter", cluster.ErrNodeUnavailable, replica)
		}
		desiredPeerIDs[replica] = peerID
	}
	if controller.LeaderID() != desiredPeerIDs[pending.DesiredLeader] {
		if err := controller.TransferLeadership(ctx, desiredPeerIDs[pending.DesiredLeader]); err != nil {
			return nil, db.normalizePartitionRaftError(ctx, controller, err)
		}
		return db.partitionReassignmentState(ctx, kind, name, partition, "cleanup", nil, false)
	}
	if err := group.UpdateAssignment(pending.DesiredReplicas, desiredPeerIDs, current.LeaderEpoch); err != nil {
		return nil, err
	}
	for _, replica := range pending.ExpectedReplicas {
		if slices.Contains(pending.DesiredReplicas, replica) {
			continue
		}
		node := nodes[replica]
		if node == nil {
			return nil, fmt.Errorf("%w: old replica %q is no longer registered", cluster.ErrNodeNotFound, replica)
		}
		peerID, member := controller.PeerID(node.Address)
		if !member || controller.PeerRole(peerID) == "" {
			continue
		}
		if err := controller.RemovePeer(ctx, peerID); err != nil {
			return nil, db.normalizePartitionRaftError(ctx, controller, err)
		}
		return db.partitionReassignmentState(ctx, kind, name, partition, "cleanup", []string{replica}, false)
	}
	if controller.JointConsensusPending() {
		return nil, cluster.ErrConfChangePending
	}
	if controller.PeerCount() != len(pending.DesiredReplicas) {
		return nil, fmt.Errorf("%w: partition group still has members outside the desired replica set", cluster.ErrPartitionReassignmentRequired)
	}
	resultEpoch := current.LeaderEpoch
	if err := db.controllerQuorum.FinishResourcePartitionReassignment(ctx, kind, name, partition, pending.DesiredReplicas, pending.DesiredLeader, resultEpoch); err != nil {
		return nil, err
	}
	return db.partitionReassignmentState(ctx, kind, name, partition, "complete", nil, true)
}

func (db *DistributedBroker) partitionReassignmentState(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int, phase string, waiting []string, complete bool) (*PartitionReassignmentProgress, error) {
	metadata, err := db.controllerQuorum.GetResourcePartitionLinearizable(ctx, kind, name, partition)
	if err != nil {
		return nil, err
	}
	return &PartitionReassignmentProgress{Partition: metadata, Phase: phase, WaitingFor: append([]string(nil), waiting...), Complete: complete}, nil
}

// CancelResourcePartitionReassignment aborts a reassignment only before its
// desired assignment is committed. It records the abort phase first, removes
// every target-only member, then clears the pending metadata. Repeating it is
// safe after a timeout or process restart.
func (db *DistributedBroker) CancelResourcePartitionReassignment(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int) (*PartitionReassignmentProgress, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	kind = (cluster.PartitionGroupID{Kind: kind}).Canonical().Kind
	if db == nil || db.controllerQuorum == nil || db.raftCtrl == nil {
		return nil, ErrUnsupportedMode
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	var current *cluster.PartitionMetadata
	nodes := make(map[string]*cluster.NodeInfo, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodes[node.ID] = node
	}
	for _, candidate := range snapshot.Partitions {
		if candidate.ID.Canonical().Kind == kind && candidate.ID.Topic == name && candidate.ID.Partition == partition {
			current = candidate
			break
		}
	}
	if current == nil {
		return nil, cluster.ErrPartitionNotFound
	}
	pending := current.Reassignment
	if pending == nil {
		return &PartitionReassignmentProgress{Partition: current, Phase: "complete", Complete: true}, nil
	}
	if pending.Phase == cluster.PartitionReassignmentCleanup {
		return nil, cluster.ErrPartitionReassignmentRequired
	}
	groupID := "partition/" + current.ID.String()
	controller, ok := db.RaftGroup(groupID)
	if !ok {
		return nil, fmt.Errorf("%w: partition group %s is not reconciled on this broker", ErrBrokerUnavailable, current.ID)
	}
	group, ok := db.PartitionGroup(current.ID)
	if !ok {
		return nil, fmt.Errorf("%w: partition quorum view %s is not reconciled on this broker", ErrBrokerUnavailable, current.ID)
	}
	if !controller.IsLeader() {
		return nil, db.partitionGroupNotLeaderError(ctx, controller)
	}
	if controller.JointConsensusPending() {
		return nil, cluster.ErrConfChangePending
	}
	expectedPeerIDs := make(map[string]uint64, len(pending.ExpectedReplicas))
	for _, replica := range pending.ExpectedReplicas {
		node := nodes[replica]
		if node == nil {
			return nil, fmt.Errorf("%w: source replica %q is no longer registered", cluster.ErrNodeNotFound, replica)
		}
		peerID, member := controller.PeerID(node.Address)
		if !member || controller.PeerRole(peerID) != "voter" {
			return nil, fmt.Errorf("%w: source replica %q is not a live Raft voter", cluster.ErrNodeUnavailable, replica)
		}
		expectedPeerIDs[replica] = peerID
	}
	expectedLeaderID, exists := expectedPeerIDs[pending.ExpectedLeader]
	if !exists {
		return nil, fmt.Errorf("%w: source leader %q is not in the source replica set", cluster.ErrInvalidPartitionAssignment, pending.ExpectedLeader)
	}
	liveSourceISR := group.LiveISRForPeers(expectedPeerIDs)
	if len(liveSourceISR) < group.MinimumISR() {
		return nil, fmt.Errorf("%w: source ISR fell below the minimum during reassignment abort", cluster.ErrNodeUnavailable)
	}
	if !slices.Contains(liveSourceISR, pending.ExpectedLeader) {
		return nil, fmt.Errorf("%w: original source leader is not in the live source ISR; reassignment cannot safely abort", cluster.ErrPartitionTransferRequired)
	}
	if controller.LeaderID() != expectedLeaderID {
		if err := controller.TransferLeadership(ctx, expectedLeaderID); err != nil {
			return nil, db.normalizePartitionRaftError(ctx, controller, err)
		}
		return db.partitionReassignmentState(ctx, kind, name, partition, string(pending.Phase), nil, false)
	}
	if pending.Phase == cluster.PartitionReassignmentCopying {
		if err := db.controllerQuorum.BeginResourcePartitionReassignmentAbort(ctx, kind, name, partition, pending.ExpectedReplicas, pending.ExpectedLeader, pending.ExpectedEpoch); err != nil {
			return nil, err
		}
		return db.partitionReassignmentState(ctx, kind, name, partition, "aborting", nil, false)
	}
	if pending.Phase != cluster.PartitionReassignmentAborting {
		return nil, cluster.ErrPartitionReassignmentRequired
	}
	for _, replica := range pending.DesiredReplicas {
		if slices.Contains(pending.ExpectedReplicas, replica) {
			continue
		}
		node := nodes[replica]
		if node == nil {
			return nil, fmt.Errorf("%w: target replica %q is no longer registered", cluster.ErrNodeNotFound, replica)
		}
		peerID, exists := controller.PeerID(node.Address)
		if !exists || controller.PeerRole(peerID) == "" {
			continue
		}
		if err := controller.RemovePeer(ctx, peerID); err != nil {
			return nil, db.normalizePartitionRaftError(ctx, controller, err)
		}
		return db.partitionReassignmentState(ctx, kind, name, partition, "aborting", []string{replica}, false)
	}
	if controller.JointConsensusPending() {
		return nil, cluster.ErrConfChangePending
	}
	for _, replica := range pending.ExpectedReplicas {
		node := nodes[replica]
		if node == nil {
			return nil, fmt.Errorf("%w: source replica %q is no longer registered", cluster.ErrNodeNotFound, replica)
		}
		peerID, exists := controller.PeerID(node.Address)
		if !exists || controller.PeerRole(peerID) != "voter" {
			return nil, fmt.Errorf("%w: source replica %q is not a Raft voter after abort cleanup", cluster.ErrNodeUnavailable, replica)
		}
	}
	if controller.PeerCount() != len(pending.ExpectedReplicas) {
		return nil, fmt.Errorf("%w: target members remain in partition group during abort", cluster.ErrPartitionReassignmentRequired)
	}
	if !controller.IsLeader() || controller.LeaderID() != expectedLeaderID {
		return nil, cluster.ErrPartitionTransferRequired
	}
	if len(liveSourceISR) < group.MinimumISR() {
		return nil, fmt.Errorf("%w: source ISR fell below the minimum before abort completion", cluster.ErrNodeUnavailable)
	}
	if err := db.controllerQuorum.FinishResourcePartitionReassignmentAbort(ctx, kind, name, partition, pending.ExpectedReplicas, pending.ExpectedLeader, pending.ExpectedEpoch); err != nil {
		return nil, err
	}
	return db.partitionReassignmentState(ctx, kind, name, partition, "complete", nil, true)
}

func (db *DistributedBroker) hasResourcePartitions(ctx context.Context, kind cluster.PartitionResourceKind, name string) (bool, error) {
	if db == nil || db.controllerQuorum == nil || name == "" {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return db.controllerQuorum.HasResourcePartitionsLinearizable(ctx, kind, name)
}

// PartitionRoute describes the controller-assigned route and, when this broker
// hosts the group, the leader currently observed by its local Raft controller.
type PartitionRoute struct {
	Partition             *cluster.PartitionMetadata `json:"partition"`
	LeaderAddress         string                     `json:"leader_address"`
	LeaderClientAddress   string                     `json:"leader_client_address,omitempty"`
	LeaderPeerID          uint64                     `json:"leader_peer_id"`
	FencingEpoch          uint64                     `json:"fencing_epoch"`
	ObservedLeaderAddress string                     `json:"observed_leader_address,omitempty"`
	ObservedLeaderPeerID  uint64                     `json:"observed_leader_peer_id,omitempty"`
	RaftTerm              uint64                     `json:"raft_term,omitempty"`
	LocalReplica          bool                       `json:"local_replica"`
	LocalGroupReady       bool                       `json:"local_group_ready"`
	LocalLeader           bool                       `json:"local_leader"`
	MetadataMatchesRaft   bool                       `json:"metadata_matches_raft"`
}

// PartitionReassignmentPlan describes a safe replica-set change without
// mutating controller metadata or Raft membership.
type PartitionReassignmentPlan struct {
	ID                  cluster.PartitionGroupID `json:"id"`
	CurrentReplicas     []string                 `json:"current_replicas"`
	DesiredReplicas     []string                 `json:"desired_replicas"`
	CurrentLeader       string                   `json:"current_leader"`
	DesiredLeader       string                   `json:"desired_leader"`
	LiveISR             []string                 `json:"live_isr,omitempty"`
	MinimumISR          int                      `json:"minimum_isr"`
	AddLearners         []string                 `json:"add_learners,omitempty"`
	PromoteAfterCatchup []string                 `json:"promote_after_catchup,omitempty"`
	TransferLeader      bool                     `json:"transfer_leader"`
	RemoveAfterCommit   []string                 `json:"remove_after_commit,omitempty"`
	JointChangePending  bool                     `json:"joint_change_pending"`
	ReadyToStart        bool                     `json:"ready_to_start"`
	InProgress          bool                     `json:"in_progress"`
	Phase               string                   `json:"phase,omitempty"`
	ReadyToContinue     bool                     `json:"ready_to_continue,omitempty"`
	WaitingFor          []string                 `json:"waiting_for,omitempty"`
	RemoveDuringAbort   []string                 `json:"remove_during_abort,omitempty"`
	Blockers            []string                 `json:"blockers,omitempty"`
}

type NodePartitionLoad struct {
	NodeID                  string `json:"node_id"`
	FailureDomain           string `json:"failure_domain,omitempty"`
	CapacityWeight          uint32 `json:"capacity_weight"`
	ReplicaCount            int    `json:"replica_count"`
	LeaderCount             int    `json:"leader_count"`
	WorkloadMessages        int    `json:"workload_messages,omitempty"`
	WorkloadLeased          int    `json:"workload_leased,omitempty"`
	WorkloadKnown           bool   `json:"workload_known"`
	DiskUsedBytes           uint64 `json:"disk_used_bytes,omitempty"`
	DiskAvailableBytes      uint64 `json:"disk_available_bytes,omitempty"`
	DiskCapacityBytes       uint64 `json:"disk_capacity_bytes,omitempty"`
	DiskUsageKnown          bool   `json:"disk_usage_known"`
	PartitionLag            uint64 `json:"partition_lag,omitempty"`
	PartitionProgressKnown  bool   `json:"partition_progress_known"`
	PartitionRecentActive   bool   `json:"partition_recent_active"`
	PartitionInSync         bool   `json:"partition_in_sync"`
	ControllerLag           uint64 `json:"controller_lag"`
	ControllerProgressKnown bool   `json:"controller_progress_known"`
	ControllerRecentActive  bool   `json:"controller_recent_active"`
	Selected                bool   `json:"selected"`
	Leader                  bool   `json:"leader"`
}

type PartitionPlacementPlan struct {
	ID                   cluster.PartitionGroupID `json:"id"`
	Replicas             []string                 `json:"replicas"`
	Leader               string                   `json:"leader"`
	LivePartitionLagUsed bool                     `json:"live_partition_lag_used"`
	LiveWorkloadUsed     bool                     `json:"live_workload_used"`
	LiveDiskCapacityUsed bool                     `json:"live_disk_capacity_used"`
	Loads                []NodePartitionLoad      `json:"loads"`
}

// PartitionRoute returns a quorum-confirmed controller assignment plus local
// Raft observation when this broker hosts the partition group. Clients can use
// the assigned address as the routing target and compare it with live group
// leadership when available.
func (db *DistributedBroker) PartitionRoute(ctx context.Context, topic string, partition int) (*PartitionRoute, error) {
	return db.PartitionResourceRoute(ctx, cluster.PartitionResourceTopic, topic, partition)
}

// PartitionResourceRoute returns the quorum-confirmed route for a topic or
// stream partition, including local Raft observation when this broker hosts it.
func (db *DistributedBroker) PartitionResourceRoute(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int) (*PartitionRoute, error) {
	kind = (cluster.PartitionGroupID{Kind: kind}).Canonical().Kind
	if ctx == nil {
		ctx = context.Background()
	}
	if db.controllerQuorum == nil || db.raftCtrl == nil {
		return nil, ErrUnsupportedMode
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	var metadata *cluster.PartitionMetadata
	for _, candidate := range snapshot.Partitions {
		if candidate.ID.Canonical().Kind == kind && candidate.ID.Topic == name && candidate.ID.Partition == partition {
			metadata = candidate
			break
		}
	}
	if metadata == nil {
		return nil, cluster.ErrPartitionNotFound
	}
	var leader *cluster.NodeInfo
	for _, node := range snapshot.Nodes {
		if node.ID == metadata.Leader {
			leader = node
			break
		}
	}
	if leader == nil || leader.Address == "" {
		return nil, fmt.Errorf("%w: partition leader %q has no registered address", cluster.ErrNodeUnavailable, metadata.Leader)
	}
	leaderPeerID, ok := db.raftCtrl.PeerID(leader.Address)
	if !ok || db.raftCtrl.PeerRole(leaderPeerID) != "voter" {
		return nil, fmt.Errorf("%w: partition leader %q is not a Raft voter", cluster.ErrNodeUnavailable, metadata.Leader)
	}
	localPeerID := db.distCfg.RaftNodeID
	if localPeerID == 0 {
		localPeerID = 1
	}
	localAddress, _ := db.raftCtrl.PeerAddress(localPeerID)
	localNodeID := ""
	for _, node := range snapshot.Nodes {
		if node.Address == localAddress {
			localNodeID = node.ID
			break
		}
	}
	route := &PartitionRoute{
		Partition: metadata, LeaderAddress: leader.Address, LeaderPeerID: leaderPeerID,
		LeaderClientAddress: leader.ClientAddress,
		FencingEpoch:        metadata.LeaderEpoch,
		LocalReplica:        localNodeID != "" && slices.Contains(metadata.Replicas, localNodeID),
	}
	if !route.LocalReplica {
		return route, nil
	}
	controller, exists := db.RaftGroup("partition/" + metadata.ID.String())
	if !exists {
		return route, nil
	}
	route.LocalGroupReady = controller.Running() && controller.FatalError() == nil && controller.LeaderID() != 0
	route.LocalLeader = route.LocalGroupReady && controller.IsLeader()
	route.RaftTerm = controller.Term()
	if route.RaftTerm > route.FencingEpoch {
		route.FencingEpoch = route.RaftTerm
	}
	route.ObservedLeaderPeerID = controller.LeaderID()
	route.ObservedLeaderAddress, _ = controller.PeerAddress(route.ObservedLeaderPeerID)
	route.MetadataMatchesRaft = route.ObservedLeaderPeerID == leaderPeerID
	return route, nil
}

// ReadPartitionTopic reads one assigned topic partition after a quorum-backed
// Raft ReadIndex. It is intended for clients that route each partition request
// to the advertised client endpoint returned by PartitionRoute.
func (db *DistributedBroker) ReadPartitionTopic(ctx context.Context, topic string, partition int, afterID string, limit int) ([]Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	route, err := db.PartitionRoute(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	if !route.LocalReplica {
		return nil, NotLeaderError(route.LeaderClientAddress, 0)
	}
	groupID := "partition/" + route.Partition.ID.String()
	controller, exists := db.RaftGroup(groupID)
	if !exists || !controller.Running() || controller.FatalError() != nil {
		return nil, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	assignedLeaderID, exists := controller.PeerID(route.LeaderAddress)
	if !exists || controller.PeerRole(assignedLeaderID) != "voter" {
		return nil, fmt.Errorf("%w: assigned leader for partition %s is not a Raft voter", cluster.ErrNodeUnavailable, route.Partition.ID)
	}
	if controller.LeaderID() != assignedLeaderID {
		return nil, db.partitionGroupNotLeaderError(ctx, controller)
	}
	if err := controller.ReadIndex(ctx); err != nil {
		if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
			return nil, db.partitionGroupNotLeaderError(ctx, controller)
		}
		return nil, err
	}
	db.startMu.Lock()
	store := db.partitionStores[groupID]
	db.startMu.Unlock()
	if store == nil {
		return nil, fmt.Errorf("%w: partition %s has no managed local store", ErrBrokerUnavailable, route.Partition.ID)
	}
	if afterID != "" {
		checkpoint, found, err := store.Get(ctx, afterID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errf(ErrCursorExpired, "message %q is no longer retained in topic %q partition %d", afterID, topic, partition)
		}
		if checkpoint.Topic != topic || checkpoint.Partition != partition {
			return nil, errf(ErrInvalidMessage, "cursor message %q does not belong to topic %q partition %d", afterID, topic, partition)
		}
	}
	return store.ReadTopic(ctx, topic, afterID, limit)
}

// ReadPartitionStream reads one assigned stream partition after a quorum-backed
// ReadIndex. afterOffset is local to this partition; clients merging the whole
// stream must retain a separate offset for every assigned partition.
func (db *DistributedBroker) ReadPartitionStream(ctx context.Context, stream string, partition int, afterOffset uint64, limit int) ([]Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	route, err := db.PartitionResourceRoute(ctx, cluster.PartitionResourceStream, stream, partition)
	if err != nil {
		return nil, err
	}
	if !route.LocalReplica {
		return nil, NotLeaderError(route.LeaderClientAddress, 0)
	}
	groupID := "partition/" + route.Partition.ID.String()
	controller, exists := db.RaftGroup(groupID)
	if !exists || !controller.Running() || controller.FatalError() != nil {
		return nil, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	assignedLeaderID, exists := controller.PeerID(route.LeaderAddress)
	if !exists || controller.PeerRole(assignedLeaderID) != "voter" {
		return nil, fmt.Errorf("%w: assigned leader for partition %s is not a Raft voter", cluster.ErrNodeUnavailable, route.Partition.ID)
	}
	if controller.LeaderID() != assignedLeaderID {
		return nil, db.partitionGroupNotLeaderError(ctx, controller)
	}
	if err := controller.ReadIndex(ctx); err != nil {
		if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
			return nil, db.partitionGroupNotLeaderError(ctx, controller)
		}
		return nil, err
	}
	db.startMu.Lock()
	store := db.partitionStores[groupID]
	db.startMu.Unlock()
	if store == nil {
		return nil, fmt.Errorf("%w: partition %s has no managed local store", ErrBrokerUnavailable, route.Partition.ID)
	}
	return store.ReadStream(ctx, StreamReadRequest{Stream: stream, AfterOffset: afterOffset, Limit: limit})
}

func (db *DistributedBroker) PartitionStreamLag(ctx context.Context, stream string, partition int, consumerGroup string) (PartitionStreamLag, error) {
	route, err := db.PartitionResourceRoute(ctx, cluster.PartitionResourceStream, stream, partition)
	if err != nil {
		return PartitionStreamLag{}, err
	}
	if !route.LocalLeader {
		return PartitionStreamLag{}, NotLeaderError(route.LeaderClientAddress, 0)
	}
	groupID := "partition/" + route.Partition.ID.String()
	controller, ok := db.RaftGroup(groupID)
	if !ok || !controller.Running() || controller.FatalError() != nil {
		return PartitionStreamLag{}, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	if err := controller.ReadIndex(ctx); err != nil {
		if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
			return PartitionStreamLag{}, db.partitionGroupNotLeaderError(ctx, controller)
		}
		return PartitionStreamLag{}, err
	}
	db.startMu.Lock()
	store := db.partitionStores[groupID]
	db.startMu.Unlock()
	if store == nil {
		return PartitionStreamLag{}, fmt.Errorf("%w: partition %s has no managed local store", ErrBrokerUnavailable, route.Partition.ID)
	}
	lagStore, ok := store.(streamLagStore)
	if !ok {
		return PartitionStreamLag{}, ErrUnsupportedMode
	}
	commit := OffsetCommit{Stream: stream, ConsumerGroup: consumerGroup, Partition: partition, Partitioned: true}
	committed, err := store.ConsumerOffset(ctx, commit)
	if err != nil {
		return PartitionStreamLag{}, err
	}
	head, err := lagStore.StreamHeadOffsetContext(ctx, stream, "")
	if err != nil {
		return PartitionStreamLag{}, err
	}
	lag, err := lagStore.StreamLagContext(ctx, stream, consumerGroup, "")
	if err != nil {
		return PartitionStreamLag{}, err
	}
	return PartitionStreamLag{Stream: stream, Partition: partition, ConsumerGroup: consumerGroup, HeadOffset: head, CommittedOffset: committed, Lag: lag}, nil
}

// PartitionStreamOffset reads one consumer group's checkpoint from the state
// machine for an assigned stream partition.
func (db *DistributedBroker) PartitionStreamOffset(ctx context.Context, commit OffsetCommit) (uint64, error) {
	if !commit.Partitioned {
		return 0, errf(ErrInvalidMessage, "partitioned stream offset requires a partition")
	}
	route, err := db.PartitionResourceRoute(ctx, cluster.PartitionResourceStream, commit.Stream, commit.Partition)
	if err != nil {
		return 0, err
	}
	if !route.LocalReplica {
		return 0, NotLeaderError(route.LeaderClientAddress, 0)
	}
	if !route.LocalLeader {
		return 0, NotLeaderError(route.LeaderClientAddress, 0)
	}
	groupID := "partition/" + route.Partition.ID.String()
	controller, exists := db.RaftGroup(groupID)
	if !exists || !controller.Running() || controller.FatalError() != nil {
		return 0, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	if err := controller.ReadIndex(ctx); err != nil {
		if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
			return 0, db.partitionGroupNotLeaderError(ctx, controller)
		}
		return 0, err
	}
	db.startMu.Lock()
	store := db.partitionStores[groupID]
	db.startMu.Unlock()
	if store == nil {
		return 0, fmt.Errorf("%w: partition %s has no managed local store", ErrBrokerUnavailable, route.Partition.ID)
	}
	return store.ConsumerOffset(ctx, commit)
}

func (db *DistributedBroker) commitPartitionStreamOffset(ctx context.Context, commit OffsetCommit) error {
	if !commit.Partitioned {
		return errf(ErrInvalidMessage, "partitioned stream offset requires a partition")
	}
	_, route, routed, err := db.partitionStoreForResourceWrite(ctx, cluster.PartitionResourceStream, commit.Stream, commit.Partition)
	if err != nil {
		return err
	}
	if !routed {
		return cluster.ErrPartitionNotFound
	}
	persisted, err := durableOffsetCommit(ctx, commit)
	if err != nil {
		return err
	}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{
		Type:    cluster.BrokerCmdCommitOffset,
		Payload: mustMarshal(replicatedOffsetCommit{Version: 2, Commit: persisted}),
	})
	if err != nil {
		return err
	}
	if err := db.ProposeResourcePartition(ctx, cluster.PartitionResourceStream, commit.Stream, commit.Partition, route.FencingEpoch, data); err != nil {
		return err
	}
	return nil
}

func (db *DistributedBroker) commitPartitionStreamOffsetAndAppend(ctx context.Context, commit OffsetCommit, entries []AppendEntry) ([]Message, error) {
	if !commit.Partitioned {
		return nil, errf(ErrInvalidMessage, "partitioned stream offset requires a partition")
	}
	store, route, routed, err := db.partitionStoreForResourceWrite(ctx, cluster.PartitionResourceStream, commit.Stream, commit.Partition)
	if err != nil {
		return nil, err
	}
	if !routed {
		return nil, cluster.ErrPartitionNotFound
	}
	if len(entries) == 0 {
		return nil, errf(ErrInvalidMessage, "offset-plus-append transaction requires output entries")
	}
	for _, entry := range entries {
		kind, name, single := singlePartitionDestination(entry.Destinations)
		if len(entry.Destinations) == 0 {
			if entry.Message.Stream != "" {
				kind, name, single = cluster.PartitionResourceStream, entry.Message.Stream, true
			} else if entry.Message.Topic != "" {
				kind, name, single = cluster.PartitionResourceTopic, entry.Message.Topic, true
			}
		}
		if !single || kind != cluster.PartitionResourceStream || name != commit.Stream || entry.Message.Partition != commit.Partition {
			return nil, fmt.Errorf("%w: partitioned offset-plus-append must write only to the same stream partition", ErrUnsupportedMode)
		}
	}
	commit, entries, fingerprint, err := prepareOffsetAppendRequest(ctx, commit, entries)
	if err != nil {
		return nil, err
	}
	if _, err := store.ConsumerOffset(ctx, commit); err != nil {
		return nil, err
	}
	if commit.TransactionID != "" {
		lookup, ok := store.(interface {
			lookupOffsetAppendTransaction(OffsetCommit, string) (OffsetAppendTransaction, bool, error)
		})
		if !ok {
			return nil, fmt.Errorf("%w: backing store cannot check offset append transaction identity", ErrUnsupportedMode)
		}
		if transaction, found, err := lookup.lookupOffsetAppendTransaction(commit, fingerprint); err != nil {
			return nil, err
		} else if found {
			return transaction.Messages, nil
		}
	}
	persistedOffset, err := durableOffsetCommit(ctx, commit)
	if err != nil {
		return nil, err
	}
	preparer, ok := store.(interface {
		prepareAppendAtomic(context.Context, []AppendEntry) ([]Message, error)
	})
	if !ok {
		return nil, fmt.Errorf("%w: backing store cannot prepare a replicated offset append", ErrUnsupportedMode)
	}
	prepared, err := preparer.prepareAppendAtomic(ctx, entries)
	if err != nil {
		return nil, err
	}
	transaction := OffsetAppendTransaction{}
	if fingerprint != "" {
		transaction = OffsetAppendTransaction{Fingerprint: fingerprint, Messages: compactOffsetAppendMessages(prepared), CreatedAt: time.Now()}
	}
	payload := replicatedOffsetAppend{Offset: persistedOffset, Entries: storedAppendEntries(prepared), Transaction: transaction}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{Type: cluster.BrokerCmdCommitOffsetAppend, Payload: mustMarshal(payload)})
	if err != nil {
		return nil, err
	}
	if err := db.ProposeResourcePartition(ctx, cluster.PartitionResourceStream, commit.Stream, commit.Partition, route.FencingEpoch, data); err != nil {
		return nil, err
	}
	return prepared, nil
}

func (db *DistributedBroker) syncQueueConfigOnPartition(ctx context.Context, localStore Store, queue string, partition int, epoch uint64) error {
	lookup, ok := db.Broker.store.(QueueConfigLookup)
	if !ok {
		return fmt.Errorf("%w: distributed queue partitions require queue configuration lookup", ErrUnsupportedMode)
	}
	cfg, found, err := lookup.LookupQueueConfig(ctx, queue)
	if err != nil {
		return err
	}
	if !found {
		cfg = QueueConfig{Name: queue, VisibilityTimeout: 30 * time.Second, RetryPolicy: DefaultRetryPolicy()}
	}
	localLookup, ok := localStore.(QueueConfigLookup)
	if !ok {
		return fmt.Errorf("%w: partition store does not expose queue configuration", ErrUnsupportedMode)
	}
	localCfg, localFound, err := localLookup.LookupQueueConfig(ctx, queue)
	if err != nil {
		return err
	}
	if localFound && reflect.DeepEqual(localCfg, cfg) {
		return nil
	}
	cmd := cluster.DeclareQueueCommand{
		Name: cfg.Name, Durable: cfg.Durable, Workers: cfg.Workers, MaxInFlight: cfg.MaxInFlight,
		MaxDepth: cfg.MaxDepth, VisibilityTimeout: cfg.VisibilityTimeout,
		AckMode: string(cfg.AckMode), RetryMaxAttempts: cfg.RetryPolicy.MaxAttempts,
		RetryInitial: cfg.RetryPolicy.Initial, RetryMax: cfg.RetryPolicy.Max,
		RetryFactor: cfg.RetryPolicy.Factor, DLQ: cfg.DLQ,
		Ordering: string(cfg.Ordering), Backpressure: string(cfg.Backpressure),
		Strategy: string(cfg.Strategy),
	}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{Type: cluster.BrokerCmdDeclareQueue, Payload: mustMarshal(cmd)})
	if err != nil {
		return err
	}
	return db.ProposeResourcePartition(ctx, cluster.PartitionResourceQueue, queue, partition, epoch, data)
}

// LeasePartitionQueue leases available work from one assigned queue partition
// and commits the lease transition through that partition's Raft group.
func (db *DistributedBroker) LeasePartitionQueue(ctx context.Context, req LeaseRequest) ([]Message, error) {
	if req.Queue == "" || req.Partition < 0 {
		return nil, errf(ErrInvalidMessage, "queue and non-negative partition are required")
	}
	store, route, routed, err := db.partitionStoreForResourceWrite(ctx, cluster.PartitionResourceQueue, req.Queue, req.Partition)
	if err != nil {
		return nil, err
	}
	if !routed {
		return nil, cluster.ErrPartitionNotFound
	}
	if err := db.syncQueueConfigOnPartition(ctx, store, req.Queue, req.Partition, route.FencingEpoch); err != nil {
		return nil, err
	}
	req.Partitioned = true
	preparer, ok := store.(interface {
		prepareLease(context.Context, LeaseRequest) ([]Message, error)
	})
	if !ok {
		return nil, fmt.Errorf("%w: partition store cannot prepare a queue lease", ErrUnsupportedMode)
	}
	messages, err := preparer.prepareLease(ctx, req)
	if err != nil || len(messages) == 0 {
		return messages, err
	}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{
		Type:    cluster.BrokerCmdLease,
		Payload: mustMarshal(replicatedLease{Request: req, Messages: messages}),
	})
	if err != nil {
		return nil, err
	}
	if err := db.ProposeResourcePartition(ctx, cluster.PartitionResourceQueue, req.Queue, req.Partition, route.FencingEpoch, data); err != nil {
		return nil, err
	}
	for i := range messages {
		messages[i].Partition = req.Partition
		messages[i].Partitioned = true
	}
	return messages, nil
}

// AckPartitionQueue acknowledges one leased message in its assigned queue
// partition state machine.
func (db *DistributedBroker) AckPartitionQueue(ctx context.Context, req AckRequest) error {
	if req.Queue == "" || req.Partition < 0 {
		return errf(ErrInvalidMessage, "queue and non-negative partition are required")
	}
	store, route, routed, err := db.partitionStoreForResourceWrite(ctx, cluster.PartitionResourceQueue, req.Queue, req.Partition)
	if err != nil {
		return err
	}
	if !routed {
		return cluster.ErrPartitionNotFound
	}
	if err := db.syncQueueConfigOnPartition(ctx, store, req.Queue, req.Partition, route.FencingEpoch); err != nil {
		return err
	}
	cmd := cluster.AckCommand{MessageID: req.MessageID, Queue: req.Queue, LeaseToken: req.LeaseToken, ConsumerID: req.ConsumerID}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{Type: cluster.BrokerCmdAck, Payload: mustMarshal(cmd)})
	if err != nil {
		return err
	}
	return db.ProposeResourcePartition(ctx, cluster.PartitionResourceQueue, req.Queue, req.Partition, route.FencingEpoch, data)
}

// NackPartitionQueue applies retry, reject, or dead-letter handling within one
// assigned queue partition state machine.
func (db *DistributedBroker) NackPartitionQueue(ctx context.Context, req NackRequest) error {
	if req.Queue == "" || req.Partition < 0 {
		return errf(ErrInvalidMessage, "queue and non-negative partition are required")
	}
	store, route, routed, err := db.partitionStoreForResourceWrite(ctx, cluster.PartitionResourceQueue, req.Queue, req.Partition)
	if err != nil {
		return err
	}
	if !routed {
		return cluster.ErrPartitionNotFound
	}
	if err := db.syncQueueConfigOnPartition(ctx, store, req.Queue, req.Partition, route.FencingEpoch); err != nil {
		return err
	}
	cmd := cluster.NackCommand{
		MessageID: req.MessageID, Queue: req.Queue, LeaseToken: req.LeaseToken,
		ConsumerID: req.ConsumerID, Action: string(req.Action), Reason: req.Reason,
		Error: req.Error, Delay: req.Delay, DLQ: req.DLQ,
		ResetLease: req.ResetLease, IncrementAttempt: req.IncrementAttempt,
	}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{Type: cluster.BrokerCmdNack, Payload: mustMarshal(cmd)})
	if err != nil {
		return err
	}
	return db.ProposeResourcePartition(ctx, cluster.PartitionResourceQueue, req.Queue, req.Partition, route.FencingEpoch, data)
}

func (db *DistributedBroker) ExtendPartitionQueueLease(ctx context.Context, req LeaseRequest) error {
	if req.Queue == "" || req.Partition < 0 {
		return errf(ErrInvalidMessage, "queue and non-negative partition are required")
	}
	store, route, routed, err := db.partitionStoreForResourceWrite(ctx, cluster.PartitionResourceQueue, req.Queue, req.Partition)
	if err != nil {
		return err
	}
	if !routed {
		return cluster.ErrPartitionNotFound
	}
	if err := db.syncQueueConfigOnPartition(ctx, store, req.Queue, req.Partition, route.FencingEpoch); err != nil {
		return err
	}
	validator, ok := store.(interface {
		validateExtendLease(context.Context, LeaseRequest) (Message, error)
	})
	if !ok {
		return fmt.Errorf("%w: partition store cannot validate a lease extension", ErrUnsupportedMode)
	}
	message, err := validator.validateExtendLease(ctx, req)
	if err != nil {
		return err
	}
	if req.VisibilityTimeout <= 0 {
		req.VisibilityTimeout = 30 * time.Second
	}
	message.LeaseUntil = time.Now().Add(req.VisibilityTimeout)
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{Type: cluster.BrokerCmdUpdate, Payload: mustMarshal(message)})
	if err != nil {
		return err
	}
	return db.ProposeResourcePartition(ctx, cluster.PartitionResourceQueue, req.Queue, req.Partition, route.FencingEpoch, data)
}

func (db *DistributedBroker) GetPartitionQueueMessage(ctx context.Context, queue string, partition int, id string) (Message, bool, error) {
	route, err := db.PartitionResourceRoute(ctx, cluster.PartitionResourceQueue, queue, partition)
	if err != nil {
		return Message{}, false, err
	}
	if !route.LocalLeader {
		return Message{}, false, NotLeaderError(route.LeaderClientAddress, 0)
	}
	groupID := "partition/" + route.Partition.ID.String()
	controller, ok := db.RaftGroup(groupID)
	if !ok || !controller.Running() || controller.FatalError() != nil {
		return Message{}, false, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	if err := controller.ReadIndex(ctx); err != nil {
		if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
			return Message{}, false, db.partitionGroupNotLeaderError(ctx, controller)
		}
		return Message{}, false, err
	}
	db.startMu.Lock()
	store := db.partitionStores[groupID]
	db.startMu.Unlock()
	if store == nil {
		return Message{}, false, fmt.Errorf("%w: partition %s has no managed local store", ErrBrokerUnavailable, route.Partition.ID)
	}
	return store.Get(ctx, id)
}

func (db *DistributedBroker) ListPartitionQueue(ctx context.Context, queue string, partition, limit int) ([]Message, error) {
	route, err := db.PartitionResourceRoute(ctx, cluster.PartitionResourceQueue, queue, partition)
	if err != nil {
		return nil, err
	}
	if !route.LocalLeader {
		return nil, NotLeaderError(route.LeaderClientAddress, 0)
	}
	groupID := "partition/" + route.Partition.ID.String()
	controller, ok := db.RaftGroup(groupID)
	if !ok || !controller.Running() || controller.FatalError() != nil {
		return nil, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	if err := controller.ReadIndex(ctx); err != nil {
		if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
			return nil, db.partitionGroupNotLeaderError(ctx, controller)
		}
		return nil, err
	}
	db.startMu.Lock()
	store := db.partitionStores[groupID]
	db.startMu.Unlock()
	if store == nil {
		return nil, fmt.Errorf("%w: partition %s has no managed local store", ErrBrokerUnavailable, route.Partition.ID)
	}
	return store.ListQueue(ctx, queue, limit)
}

func (db *DistributedBroker) setPartitionQueuePaused(ctx context.Context, queue string, partition int, paused bool) error {
	store, route, routed, err := db.partitionStoreForResourceWrite(ctx, cluster.PartitionResourceQueue, queue, partition)
	if err != nil {
		return err
	}
	if !routed {
		return cluster.ErrPartitionNotFound
	}
	if err := db.syncQueueConfigOnPartition(ctx, store, queue, partition, route.FencingEpoch); err != nil {
		return err
	}
	commandType := cluster.BrokerCmdPauseQueue
	if !paused {
		commandType = cluster.BrokerCmdResumeQueue
	}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{Type: commandType, Payload: mustMarshal(queue)})
	if err != nil {
		return err
	}
	return db.ProposeResourcePartition(ctx, cluster.PartitionResourceQueue, queue, partition, route.FencingEpoch, data)
}

func (db *DistributedBroker) PausePartitionQueue(ctx context.Context, queue string, partition int) error {
	return db.setPartitionQueuePaused(ctx, queue, partition, true)
}

func (db *DistributedBroker) ResumePartitionQueue(ctx context.Context, queue string, partition int) error {
	return db.setPartitionQueuePaused(ctx, queue, partition, false)
}

func (db *DistributedBroker) PurgePartitionQueue(ctx context.Context, queue string, partition int) (int, error) {
	return db.queueControlFromLeader(ctx, queue, partition, "purge")
}

func (db *DistributedBroker) purgePartitionQueueOnLeader(ctx context.Context, queue string, partition int) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	store, route, routed, err := db.partitionStoreForResourceWrite(ctx, cluster.PartitionResourceQueue, queue, partition)
	if err != nil {
		return 0, err
	}
	if !routed {
		return 0, cluster.ErrPartitionNotFound
	}
	if !route.LocalLeader {
		return 0, NotLeaderError(route.LeaderClientAddress, route.RaftTerm)
	}
	if err := db.syncQueueConfigOnPartition(ctx, store, queue, partition, route.FencingEpoch); err != nil {
		return 0, err
	}
	groupID := "partition/" + route.Partition.ID.String()
	controller, ok := db.RaftGroup(groupID)
	if !ok || !controller.Running() || controller.FatalError() != nil {
		return 0, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{Type: cluster.BrokerCmdPurgeQueue, Payload: mustMarshal(queue)})
	if err != nil {
		return 0, err
	}
	group, ok := db.PartitionGroup(route.Partition.ID)
	if !ok {
		return 0, fmt.Errorf("%w: local partition quorum view %q is not reconciled", ErrBrokerUnavailable, groupID)
	}
	result, err := group.ProposeWithResult(ctx, route.FencingEpoch, data)
	if err != nil {
		return 0, db.normalizePartitionRaftError(ctx, controller, err)
	}
	purged, ok := result.(int)
	if !ok || purged < 0 {
		return 0, fmt.Errorf("partition queue purge did not return a valid committed count")
	}
	return purged, nil
}

// ProposePartition submits a fenced state-machine command to the assigned
// topic partition leader, forwarding to that broker when necessary.
func (db *DistributedBroker) ProposePartition(ctx context.Context, topic string, partition int, epoch uint64, data []byte) error {
	return db.ProposeResourcePartition(ctx, cluster.PartitionResourceTopic, topic, partition, epoch, data)
}

// ProposeResourcePartition submits a fenced state-machine command to the
// assigned Raft leader for a topic or stream partition.
func (db *DistributedBroker) ProposeResourcePartition(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int, epoch uint64, data []byte) error {
	return db.proposeResourcePartition(ctx, kind, name, partition, epoch, data, true)
}

// proposeResourcePartitionFromPeer accepts a proposal delivered over the
// authenticated broker peer API. It never forwards again, preventing loops if
// the controller assignment changes while a request is in flight.
func (db *DistributedBroker) proposeResourcePartitionFromPeer(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int, epoch uint64, data []byte) error {
	return db.proposeResourcePartition(ctx, kind, name, partition, epoch, data, false)
}

func (db *DistributedBroker) proposeResourcePartition(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int, epoch uint64, data []byte, allowForward bool) error {
	kind = (cluster.PartitionGroupID{Kind: kind}).Canonical().Kind
	if ctx == nil {
		ctx = context.Background()
	}
	if db.controllerQuorum == nil || db.raftCtrl == nil {
		return ErrUnsupportedMode
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return err
	}
	var metadata *cluster.PartitionMetadata
	var nodesByID = make(map[string]*cluster.NodeInfo, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodesByID[node.ID] = node
	}
	for _, candidate := range snapshot.Partitions {
		if candidate.ID.Canonical().Kind == kind && candidate.ID.Topic == name && candidate.ID.Partition == partition {
			metadata = candidate
			break
		}
	}
	if metadata == nil {
		return cluster.ErrPartitionNotFound
	}
	leader := nodesByID[metadata.Leader]
	if leader == nil || leader.Address == "" {
		return fmt.Errorf("%w: partition leader %q has no registered address", cluster.ErrNodeUnavailable, metadata.Leader)
	}
	assignedLeaderID, ok := db.raftCtrl.PeerID(leader.Address)
	if !ok || db.raftCtrl.PeerRole(assignedLeaderID) != "voter" {
		return fmt.Errorf("%w: partition leader %q is not a Raft voter", cluster.ErrNodeUnavailable, metadata.Leader)
	}
	groupID := "partition/" + metadata.ID.String()
	controller, ok := db.RaftGroup(groupID)
	if !ok {
		if allowForward && assignedLeaderID != db.localRaftPeerID() {
			return db.forwardPartitionProposal(ctx, leader.ClientAddress, kind, name, partition, epoch, data)
		}
		return fmt.Errorf("%w: local partition group %q is not reconciled", ErrBrokerUnavailable, groupID)
	}
	if !controller.Running() || controller.FatalError() != nil {
		if allowForward && assignedLeaderID != db.localRaftPeerID() {
			return db.forwardPartitionProposal(ctx, leader.ClientAddress, kind, name, partition, epoch, data)
		}
		return fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	if controller.LeaderID() != assignedLeaderID || !controller.IsLeader() {
		if allowForward && assignedLeaderID != db.localRaftPeerID() {
			return db.forwardPartitionProposal(ctx, leader.ClientAddress, kind, name, partition, epoch, data)
		}
		return db.partitionGroupNotLeaderError(ctx, controller)
	}
	group, ok := db.PartitionGroup(metadata.ID)
	if !ok {
		return fmt.Errorf("%w: local partition quorum view %q is not reconciled", ErrBrokerUnavailable, groupID)
	}
	fencingEpoch := group.ObserveLeaderEpoch(metadata.LeaderEpoch)
	if epoch != fencingEpoch {
		return StaleEpochError(fencingEpoch)
	}
	if err := group.Propose(ctx, epoch, data); err != nil {
		return db.normalizePartitionRaftError(ctx, controller, err)
	}
	return nil
}

func (db *DistributedBroker) localRaftPeerID() uint64 {
	if db.distCfg.RaftNodeID == 0 {
		return 1
	}
	return db.distCfg.RaftNodeID
}

func (db *DistributedBroker) partitionStoreForWrite(ctx context.Context, topic string, partition int) (Store, *PartitionRoute, bool, error) {
	return db.partitionStoreForResourceWrite(ctx, cluster.PartitionResourceTopic, topic, partition)
}

func (db *DistributedBroker) partitionStoreForResourceWrite(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int) (Store, *PartitionRoute, bool, error) {
	if name == "" {
		return nil, nil, false, nil
	}
	if partition < 0 {
		return nil, nil, true, errf(ErrInvalidMessage, "partition must be non-negative")
	}
	route, err := db.PartitionResourceRoute(ctx, kind, name, partition)
	if errors.Is(err, cluster.ErrPartitionNotFound) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, true, err
	}
	groupID := "partition/" + route.Partition.ID.String()
	db.startMu.Lock()
	store := db.partitionStores[groupID]
	db.startMu.Unlock()
	if store == nil {
		return nil, route, true, fmt.Errorf("%w: assigned partition %s has no managed local store", ErrBrokerUnavailable, route.Partition.ID)
	}
	return store, route, true, nil
}

func singlePartitionDestination(dests []Destination) (cluster.PartitionResourceKind, string, bool) {
	if len(dests) != 1 || dests[0].Name == "" {
		return "", "", false
	}
	switch dests[0].Kind {
	case DestinationTopic:
		return cluster.PartitionResourceTopic, dests[0].Name, true
	case DestinationStream:
		return cluster.PartitionResourceStream, dests[0].Name, true
	case DestinationQueue:
		return cluster.PartitionResourceQueue, dests[0].Name, true
	default:
		return "", "", false
	}
}

type localTopicPartition struct {
	metadata   *cluster.PartitionMetadata
	controller *cluster.RaftController
	store      Store
}

func (db *DistributedBroker) localTopicPartitions(ctx context.Context, topic string) ([]localTopicPartition, bool, error) {
	return db.localResourcePartitions(ctx, cluster.PartitionResourceTopic, topic)
}

func (db *DistributedBroker) localResourcePartitions(ctx context.Context, kind cluster.PartitionResourceKind, name string) ([]localTopicPartition, bool, error) {
	if db.controllerQuorum == nil || db.raftCtrl == nil {
		return nil, false, nil
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, false, err
	}
	localPeerID := db.distCfg.RaftNodeID
	if localPeerID == 0 {
		localPeerID = 1
	}
	localAddress, _ := db.raftCtrl.PeerAddress(localPeerID)
	localNodeID := ""
	nodesByID := make(map[string]*cluster.NodeInfo, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodesByID[node.ID] = node
		if node.Address == localAddress {
			localNodeID = node.ID
		}
	}
	partitions := make([]*cluster.PartitionMetadata, 0)
	for _, partition := range snapshot.Partitions {
		if partition.ID.Canonical().Kind == kind && partition.ID.Topic == name {
			partitions = append(partitions, partition)
		}
	}
	if len(partitions) == 0 {
		return nil, false, nil
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].ID.Partition < partitions[j].ID.Partition
	})
	local := make([]localTopicPartition, 0, len(partitions))
	for _, partition := range partitions {
		leader := nodesByID[partition.Leader]
		if leader == nil || leader.Address == "" {
			return nil, true, fmt.Errorf("%w: partition %s has no registered leader address", cluster.ErrNodeUnavailable, partition.ID)
		}
		if localNodeID == "" || !slices.Contains(partition.Replicas, localNodeID) {
			return nil, true, NotLeaderError(leader.Address, 0)
		}
		groupID := "partition/" + partition.ID.String()
		controller, exists := db.RaftGroup(groupID)
		if !exists || !controller.Running() || controller.FatalError() != nil {
			return nil, true, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
		}
		assignedLeaderID, exists := controller.PeerID(leader.Address)
		if !exists || controller.PeerRole(assignedLeaderID) != "voter" {
			return nil, true, fmt.Errorf("%w: assigned leader for partition %s is not a Raft voter", cluster.ErrNodeUnavailable, partition.ID)
		}
		if controller.LeaderID() != assignedLeaderID {
			return nil, true, db.partitionGroupNotLeaderError(ctx, controller)
		}
		db.startMu.Lock()
		store := db.partitionStores[groupID]
		db.startMu.Unlock()
		if store == nil {
			return nil, true, fmt.Errorf("%w: partition %s has no managed local store", ErrBrokerUnavailable, partition.ID)
		}
		if err := controller.ReadIndex(ctx); err != nil {
			if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
				return nil, true, db.partitionGroupNotLeaderError(ctx, controller)
			}
			return nil, true, err
		}
		local = append(local, localTopicPartition{metadata: partition, controller: controller, store: store})
	}
	return local, true, nil
}

func (db *DistributedBroker) listPartitionedTopic(ctx context.Context, topic string, limit int) ([]Message, bool, error) {
	partitions, handled, err := db.localTopicPartitions(ctx, topic)
	if !handled || err != nil {
		return nil, handled, err
	}
	if limit <= 0 {
		limit = 100
	}
	messages := make([]Message, 0)
	for _, partition := range partitions {
		part, err := partition.store.ListTopic(ctx, topic, limit)
		if err != nil {
			return nil, true, err
		}
		messages = append(messages, part...)
	}
	sort.SliceStable(messages, func(i, j int) bool {
		if messages[i].CreatedAt.Equal(messages[j].CreatedAt) {
			if messages[i].Partition == messages[j].Partition {
				return messages[i].ID < messages[j].ID
			}
			return messages[i].Partition < messages[j].Partition
		}
		return messages[i].CreatedAt.Before(messages[j].CreatedAt)
	})
	if len(messages) > limit {
		messages = messages[:limit]
	}
	return messages, true, nil
}

func (db *DistributedBroker) readPartitionedTopic(ctx context.Context, topic, afterID string, limit int) ([]Message, bool, error) {
	partitions, handled, err := db.localTopicPartitions(ctx, topic)
	if !handled || err != nil {
		return nil, handled, err
	}
	if limit <= 0 {
		limit = 100
	}
	var anchor *Message
	anchorPartition := -1
	if afterID != "" {
		foundInOtherPartition := false
		for i, partition := range partitions {
			message, exists, getErr := partition.store.Get(ctx, afterID)
			if getErr != nil {
				return nil, true, getErr
			}
			if exists {
				if message.Topic != topic || message.Partition != partition.metadata.ID.Partition {
					foundInOtherPartition = true
					continue
				}
				anchor = &message
				anchorPartition = i
				break
			}
		}
		if anchor == nil {
			if foundInOtherPartition {
				return nil, true, errf(ErrInvalidMessage, "cursor message %q does not belong to topic %q", afterID, topic)
			}
			return nil, true, errf(ErrCursorExpired, "message %q is no longer retained in topic %q", afterID, topic)
		}
	}

	// At most limit messages per partition enter the merge buffer. Older data is
	// scanned in bounded pages only when an anchor falls partway through a topic.
	merged := make([]Message, 0)
	for index, partition := range partitions {
		cursor := ""
		if anchor != nil && index == anchorPartition {
			cursor = afterID
		}
		collected := 0
		for collected < limit {
			pageSize := limit - collected
			page, readErr := partition.store.ReadTopic(ctx, topic, cursor, pageSize)
			if readErr != nil {
				return nil, true, readErr
			}
			if len(page) == 0 {
				break
			}
			for _, message := range page {
				cursor = message.ID
				if anchor != nil && !topicMessageLess(*anchor, message) {
					continue
				}
				merged = append(merged, message)
				collected++
				if collected == limit {
					break
				}
			}
			if len(page) < pageSize {
				break
			}
			// A full page containing only messages before the global anchor must
			// advance the local cursor and continue scanning.
		}
	}
	sort.SliceStable(merged, func(i, j int) bool { return topicMessageLess(merged[i], merged[j]) })
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged, true, nil
}

func (db *DistributedBroker) readPartitionedStream(ctx context.Context, req StreamReadRequest) ([]Message, bool, error) {
	partitions, handled, err := db.localResourcePartitions(ctx, cluster.PartitionResourceStream, req.Stream)
	if !handled || err != nil {
		return nil, handled, err
	}
	if req.ConsumerGroup != "" || req.AfterOffset != 0 {
		return nil, true, fmt.Errorf("%w: partitioned stream reads require Client.ReadStreamPage with a per-partition cursor", ErrUnsupportedMode)
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	type streamEntry struct {
		partition int
		position  int
		message   Message
	}
	entries := make([]streamEntry, 0)
	for partitionIndex, partition := range partitions {
		partReq := req
		partReq.ConsumerGroup = ""
		partReq.AfterOffset = 0
		partReq.Limit = limit
		messages, readErr := partition.store.ReadStream(ctx, partReq)
		if readErr != nil {
			return nil, true, readErr
		}
		for position, message := range messages {
			entries = append(entries, streamEntry{partition: partitionIndex, position: position, message: message})
		}
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].position != entries[j].position {
			return entries[i].position < entries[j].position
		}
		return entries[i].partition < entries[j].partition
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}
	messages := make([]Message, len(entries))
	for i := range entries {
		messages[i] = entries[i].message
	}
	return messages, true, nil
}

func topicMessageLess(a, b Message) bool {
	if a.CreatedAt.Equal(b.CreatedAt) {
		if a.Partition == b.Partition {
			return a.ID < b.ID
		}
		return a.Partition < b.Partition
	}
	return a.CreatedAt.Before(b.CreatedAt)
}

// appendToAssignedPartition commits a topic append to its independently
// replicated partition group when controller metadata assigns that partition.
// It reports routed=true for assigned partitions even when the local broker is
// not their leader, preventing accidental fallback to the broker-wide group.
func (db *DistributedBroker) appendToAssignedPartition(ctx context.Context, msg Message, dests []Destination) ([]Message, bool, error) {
	kind, name, singleDestination := singlePartitionDestination(dests)
	if !singleDestination {
		if msg.Topic != "" {
			kind, name = cluster.PartitionResourceTopic, msg.Topic
		} else if msg.Stream != "" {
			kind, name = cluster.PartitionResourceStream, msg.Stream
		} else if msg.Queue != "" {
			kind, name = cluster.PartitionResourceQueue, msg.Queue
		} else {
			return nil, false, nil
		}
	}
	store, route, routed, err := db.partitionStoreForResourceWrite(ctx, kind, name, msg.Partition)
	if err != nil {
		return nil, routed, err
	}
	if !routed {
		assigned, checkErr := db.hasResourcePartitions(ctx, kind, name)
		if checkErr != nil {
			return nil, true, checkErr
		}
		if assigned {
			return nil, true, fmt.Errorf("%w: %s partition %s/%d has no controller assignment", cluster.ErrPartitionNotFound, kind, name, msg.Partition)
		}
		return nil, false, nil
	}
	if !singleDestination || kind != route.Partition.ID.Canonical().Kind || name != route.Partition.ID.Topic {
		return nil, true, fmt.Errorf("%w: an assigned partition append must target exactly one assigned topic, stream, or queue", ErrUnsupportedMode)
	}
	if kind == cluster.PartitionResourceQueue {
		if err := db.syncQueueConfigOnPartition(ctx, store, name, msg.Partition, route.FencingEpoch); err != nil {
			return nil, true, err
		}
	}
	commandDestinations := make([]cluster.CommandDestination, len(dests))
	for i, destination := range dests {
		commandDestinations[i] = cluster.CommandDestination{Kind: string(destination.Kind), Name: destination.Name}
	}
	command := struct {
		Message
		Destinations []cluster.CommandDestination `json:"destinations"`
	}{Message: msg, Destinations: commandDestinations}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{
		Type: cluster.BrokerCmdAppend, Payload: mustMarshal(command),
	})
	if err != nil {
		return nil, true, err
	}
	if err := db.ProposeResourcePartition(ctx, kind, name, msg.Partition, route.FencingEpoch, data); err != nil {
		return nil, true, err
	}
	stored, exists, err := store.Get(ctx, msg.ID)
	if err != nil {
		return nil, true, err
	}
	if exists {
		return []Message{stored}, true, nil
	}
	return []Message{msg}, true, nil
}

func (db *DistributedBroker) appendAtomicToAssignedPartition(ctx context.Context, entries []AppendEntry) ([]Message, bool, error) {
	if len(entries) == 0 {
		return nil, false, nil
	}
	var store Store
	var route *PartitionRoute
	var resourceKind cluster.PartitionResourceKind
	var resourceName string
	routedCount := 0
	for _, entry := range entries {
		kind, name, singleDestination := singlePartitionDestination(entry.Destinations)
		if !singleDestination {
			if entry.Message.Topic != "" {
				kind, name = cluster.PartitionResourceTopic, entry.Message.Topic
			} else if entry.Message.Stream != "" {
				kind, name = cluster.PartitionResourceStream, entry.Message.Stream
			} else if entry.Message.Queue != "" {
				kind, name = cluster.PartitionResourceQueue, entry.Message.Queue
			} else {
				continue
			}
		}
		currentStore, currentRoute, routed, err := db.partitionStoreForResourceWrite(ctx, kind, name, entry.Message.Partition)
		if err != nil {
			return nil, true, err
		}
		if !routed {
			assigned, checkErr := db.hasResourcePartitions(ctx, kind, name)
			if checkErr != nil {
				return nil, true, checkErr
			}
			if assigned {
				return nil, true, fmt.Errorf("%w: %s partition %s/%d has no controller assignment", cluster.ErrPartitionNotFound, kind, name, entry.Message.Partition)
			}
			continue
		}
		routedCount++
		if !singleDestination || kind != currentRoute.Partition.ID.Canonical().Kind || name != currentRoute.Partition.ID.Topic {
			return nil, true, fmt.Errorf("%w: an assigned partition append must target exactly one assigned topic, stream, or queue", ErrUnsupportedMode)
		}
		if route == nil {
			store, route = currentStore, currentRoute
			resourceKind, resourceName = kind, name
			continue
		}
		if currentRoute.Partition.ID.Canonical() != route.Partition.ID.Canonical() || !samePartitionStoreInstance(currentStore, store) {
			return nil, true, fmt.Errorf("%w: atomic append spans multiple partition groups", ErrUnsupportedMode)
		}
	}
	if routedCount == 0 {
		return nil, false, nil
	}
	if routedCount != len(entries) {
		return nil, true, fmt.Errorf("%w: atomic append spans assigned and unassigned partitions", ErrUnsupportedMode)
	}
	if resourceKind == cluster.PartitionResourceQueue {
		if err := db.syncQueueConfigOnPartition(ctx, store, resourceName, route.Partition.ID.Partition, route.FencingEpoch); err != nil {
			return nil, true, err
		}
	}
	preparer, ok := store.(interface {
		prepareAppendAtomic(context.Context, []AppendEntry) ([]Message, error)
	})
	if !ok {
		return nil, true, fmt.Errorf("%w: partition store cannot prepare an atomic append", ErrUnsupportedMode)
	}
	prepared, err := preparer.prepareAppendAtomic(ctx, entries)
	if err != nil {
		return nil, true, err
	}
	command, err := cluster.MakeBrokerCommand(cluster.BrokerCmdAppendAtomic, storedAppendEntries(prepared))
	if err != nil {
		return nil, true, err
	}
	data, err := cluster.EncodeBrokerCommand(command)
	if err != nil {
		return nil, true, err
	}
	if err := db.ProposeResourcePartition(ctx, resourceKind, resourceName, route.Partition.ID.Partition, route.FencingEpoch, data); err != nil {
		return nil, true, err
	}
	return prepared, true, nil
}

// TransferPartitionLeader moves Raft leadership to an in-sync assigned replica
// before committing the new logical leader and epoch in controller metadata.
// If the transfer succeeds but metadata quorum is temporarily unavailable, a
// retry with the same target completes the metadata step idempotently.
func (db *DistributedBroker) TransferPartitionLeader(ctx context.Context, topic string, partition int, newLeader string) (*cluster.PartitionMetadata, error) {
	return db.TransferResourcePartitionLeader(ctx, cluster.PartitionResourceTopic, topic, partition, newLeader)
}

// TransferResourcePartitionLeader moves Raft leadership to an in-sync assigned
// replica before committing typed resource metadata.
func (db *DistributedBroker) TransferResourcePartitionLeader(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int, newLeader string) (*cluster.PartitionMetadata, error) {
	kind = (cluster.PartitionGroupID{Kind: kind}).Canonical().Kind
	if ctx == nil {
		ctx = context.Background()
	}
	if db.controllerQuorum == nil || db.raftCtrl == nil {
		return nil, ErrUnsupportedMode
	}
	if newLeader == "" {
		return nil, fmt.Errorf("partition leader must not be empty")
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	var current *cluster.PartitionMetadata
	for _, candidate := range snapshot.Partitions {
		if candidate.ID.Canonical().Kind == kind && candidate.ID.Topic == name && candidate.ID.Partition == partition {
			current = candidate
			break
		}
	}
	if current == nil {
		return nil, cluster.ErrPartitionNotFound
	}
	if !slices.Contains(current.Replicas, newLeader) {
		return nil, fmt.Errorf("%w: target leader %q is not assigned to %s", cluster.ErrInvalidPartitionAssignment, newLeader, current.ID)
	}
	if !slices.Contains(current.ISR, newLeader) {
		return nil, fmt.Errorf("%w: target leader %q is not in the ISR for %s", cluster.ErrNodeUnavailable, newLeader, current.ID)
	}
	nodesByID := make(map[string]*cluster.NodeInfo, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if node != nil {
			nodesByID[node.ID] = node
		}
	}
	for _, replica := range current.Replicas {
		node := nodesByID[replica]
		if node == nil || node.ClientAddress == "" {
			return nil, fmt.Errorf("%w: replica %q has no advertised client address", cluster.ErrInvalidNodeClientAddress, replica)
		}
	}
	var targetNode *cluster.NodeInfo
	targetNode = nodesByID[newLeader]
	if targetNode == nil || targetNode.Address == "" {
		return nil, fmt.Errorf("%w: target leader %q has no registered address", cluster.ErrNodeUnavailable, newLeader)
	}
	groupID := "partition/" + current.ID.String()
	controller, ok := db.RaftGroup(groupID)
	if !ok {
		return nil, fmt.Errorf("%w: local partition group %q is not reconciled", ErrBrokerUnavailable, groupID)
	}
	partitionGroup, ok := db.PartitionGroup(current.ID)
	if !ok {
		return nil, fmt.Errorf("%w: partition quorum view %q is not reconciled", ErrBrokerUnavailable, groupID)
	}
	targetPeerID, ok := controller.PeerID(targetNode.Address)
	if !ok || controller.PeerRole(targetPeerID) != "voter" {
		return nil, fmt.Errorf("%w: target leader %q is not a Raft voter in %s", cluster.ErrNodeUnavailable, newLeader, current.ID)
	}
	if controller.JointConsensusPending() {
		return nil, cluster.ErrConfChangePending
	}
	if controller.LeaderID() != targetPeerID {
		if !controller.IsLeader() {
			return nil, db.partitionGroupNotLeaderError(ctx, controller)
		}
		if !slices.Contains(partitionGroup.ISR(), newLeader) {
			return nil, fmt.Errorf("%w: target leader %q is outside the live in-sync replica set", cluster.ErrNodeUnavailable, newLeader)
		}
		if err := controller.TransferLeadership(ctx, targetPeerID); err != nil {
			if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
				return nil, db.partitionGroupNotLeaderError(ctx, controller)
			}
			return nil, err
		}
	}
	if controller.LeaderID() != targetPeerID {
		return nil, db.partitionGroupNotLeaderError(ctx, controller)
	}
	if current.Leader != newLeader {
		if err := db.controllerQuorum.CommitResourcePartitionLeaderTransfer(ctx, kind, name, partition, current.Leader, current.LeaderEpoch, newLeader); err != nil {
			return nil, err
		}
	}
	return db.controllerQuorum.GetResourcePartitionLinearizable(ctx, kind, name, partition)
}

// UpdatePartitionISR commits only an ISR observed by the reconciled partition
// group's current Raft leader. Admin callers cannot publish a fabricated ISR.
func (db *DistributedBroker) UpdatePartitionISR(ctx context.Context, topic string, partition int, reportedISR []string) (*cluster.PartitionMetadata, error) {
	return db.UpdateResourcePartitionISR(ctx, cluster.PartitionResourceTopic, topic, partition, reportedISR)
}

// UpdateResourcePartitionISR commits only the live ISR observed by the
// reconciled topic or stream partition group's leader.
func (db *DistributedBroker) UpdateResourcePartitionISR(ctx context.Context, kind cluster.PartitionResourceKind, name string, partition int, reportedISR []string) (*cluster.PartitionMetadata, error) {
	kind = (cluster.PartitionGroupID{Kind: kind}).Canonical().Kind
	if ctx == nil {
		ctx = context.Background()
	}
	if db.controllerQuorum == nil {
		return nil, ErrUnsupportedMode
	}
	partitionID := (cluster.PartitionGroupID{Topic: name, Partition: partition, Kind: kind}).Canonical()
	group, ok := db.PartitionGroup(partitionID)
	if !ok {
		return nil, fmt.Errorf("%w: partition quorum view is not reconciled", ErrBrokerUnavailable)
	}
	controller, ok := db.RaftGroup("partition/" + partitionID.String())
	if !ok {
		return nil, fmt.Errorf("%w: partition Raft group is not reconciled", ErrBrokerUnavailable)
	}
	if !group.IsLeader() {
		return nil, db.partitionGroupNotLeaderError(ctx, controller)
	}
	liveISR := group.ISR()
	if !group.IsLeader() {
		return nil, db.partitionGroupNotLeaderError(ctx, controller)
	}
	requested := append([]string(nil), reportedISR...)
	sort.Strings(requested)
	if !slices.Equal(requested, liveISR) {
		return nil, fmt.Errorf("%w: supplied ISR does not match live Raft progress", cluster.ErrInvalidISR)
	}
	if err := db.controllerQuorum.UpdateResourceISR(ctx, kind, name, partition, liveISR); err != nil {
		return nil, err
	}
	return db.controllerQuorum.GetResourcePartitionLinearizable(ctx, kind, name, partition)
}

func (db *DistributedBroker) registerPartitionGroup(partition *cluster.PartitionMetadata, controller *cluster.RaftController, replicaPeerIDs map[string]uint64) error {
	groupID := "partition/" + partition.ID.String()
	group, err := cluster.NewPartitionGroup(cluster.PartitionGroupConfig{
		ID:         partition.ID.Canonical(),
		Controller: controller, ReplicationFactor: db.distCfg.ReplicationFactor,
		MinISR: db.distCfg.MinInSyncReplicas, MaxReplicationLag: db.distCfg.MaxReplicationLag,
		ReplicationTimeout: db.distCfg.ReplicationTimeout,
		Replicas:           append([]string(nil), partition.Replicas...), ReplicaPeerIDs: replicaPeerIDs,
	})
	if err != nil {
		return err
	}
	db.startMu.Lock()
	defer db.startMu.Unlock()
	if db.raftGroups[groupID] != controller {
		_ = group.Close()
		return fmt.Errorf("partition %s Raft group changed during reconciliation", partition.ID)
	}
	if existing := db.partitionGroups[groupID]; existing != nil {
		_ = group.Close()
		if err := existing.UpdateAssignment(partition.Replicas, replicaPeerIDs, partition.LeaderEpoch); err != nil {
			return err
		}
		return nil
	}
	db.partitionGroups[groupID] = group
	return nil
}

// RaftGroup returns an application group registered on this broker.
func (db *DistributedBroker) RaftGroup(groupID string) (*cluster.RaftController, bool) {
	db.startMu.Lock()
	defer db.startMu.Unlock()
	controller, ok := db.raftGroups[groupID]
	return controller, ok
}

// RaftGroupIDs returns the registered application group IDs in stable order.
func (db *DistributedBroker) RaftGroupIDs() []string {
	db.startMu.Lock()
	defer db.startMu.Unlock()
	ids := make([]string, 0, len(db.raftGroups))
	for id := range db.raftGroups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// ReconcilePartitionGroups provisions missing local groups from one
// quorum-confirmed controller topology snapshot. During a durable reassignment
// it also provisions target replicas so they can join as Raft learners. The
// state-machine factory must make the same schema available on every assigned
// replica. If factory is nil, DistributedConfig.PartitionStoreFactory supplies
// isolated stores adapted to the standard broker state machine. Reconciliation
// does not mutate Raft membership; ReassignResourcePartition advances that
// protocol after each local broker has reconciled the pending assignment.
func (db *DistributedBroker) ReconcilePartitionGroups(ctx context.Context, factory PartitionStateMachineFactory) error {
	if factory == nil && db.distCfg.PartitionStoreFactory == nil {
		return fmt.Errorf("partition group reconciliation requires a state-machine factory")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db.reconcileMu.Lock()
	defer db.reconcileMu.Unlock()
	if db.controllerQuorum == nil || db.raftCtrl == nil {
		return fmt.Errorf("%w: partition group reconciliation requires etcd/raft controller metadata", ErrUnsupportedMode)
	}
	createdStores := make(map[string]Store)
	createdAdapters := make(map[string]*brokerStoreAdapter)
	defer func() {
		for _, store := range createdStores {
			_ = store.Close()
		}
	}()
	if factory == nil {
		factory = func(ctx context.Context, partition *cluster.PartitionMetadata) (cluster.StateMachine, bool, error) {
			store, err := db.distCfg.PartitionStoreFactory(ctx, partition)
			if err != nil {
				return nil, false, err
			}
			if isNilPartitionStore(store) {
				return nil, false, fmt.Errorf("partition store factory returned nil for %s", partition.ID)
			}
			for otherID, otherStore := range createdStores {
				if samePartitionStoreInstance(store, otherStore) {
					return nil, false, fmt.Errorf("partition store factory reused one store for %s and %s", otherID, partition.ID)
				}
			}
			db.startMu.Lock()
			for otherID, otherStore := range db.partitionStores {
				if samePartitionStoreInstance(store, otherStore) {
					db.startMu.Unlock()
					return nil, false, fmt.Errorf("partition store factory reused the store for %s as well as %s", otherID, partition.ID)
				}
			}
			db.startMu.Unlock()
			var declareErr error
			switch partition.ID.Canonical().Kind {
			case cluster.PartitionResourceTopic:
				declareErr = store.DeclareTopic(ctx, TopicConfig{
					Name: partition.ID.Topic, Partitions: partition.ID.Partition + 1,
				})
			case cluster.PartitionResourceStream:
				declareErr = store.DeclareStream(ctx, StreamConfig{
					Name: partition.ID.Topic, Partitions: partition.ID.Partition + 1,
				})
			case cluster.PartitionResourceQueue:
				declareErr = store.DeclareQueue(ctx, QueueConfig{Name: partition.ID.Topic})
			default:
				declareErr = fmt.Errorf("%w: unsupported partition resource kind %q", ErrUnsupportedMode, partition.ID.Kind)
			}
			if declareErr != nil {
				_ = store.Close()
				return nil, false, fmt.Errorf("declare local resource state for partition %s: %w", partition.ID, declareErr)
			}
			adapter := &brokerStoreAdapter{store: store}
			stateMachine := cluster.NewBrokerStateMachine(adapter)
			groupID := "partition/" + partition.ID.String()
			createdStores[groupID] = store
			createdAdapters[groupID] = adapter
			return stateMachine, db.distCfg.JoinExisting, nil
		}
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return err
	}
	localPeerID := db.distCfg.RaftNodeID
	if localPeerID == 0 {
		localPeerID = 1
	}
	localAddress, ok := db.raftCtrl.PeerAddress(localPeerID)
	if !ok {
		return fmt.Errorf("local Raft peer %d has no configured address", localPeerID)
	}
	nodesByID := make(map[string]*cluster.NodeInfo, len(snapshot.Nodes))
	logicalLocalID := ""
	for _, node := range snapshot.Nodes {
		if node == nil || node.ID == "" {
			return fmt.Errorf("controller topology contains an invalid node record")
		}
		nodesByID[node.ID] = node
		if node.Address == localAddress {
			if logicalLocalID != "" {
				return fmt.Errorf("multiple logical nodes use local Raft address %q", localAddress)
			}
			logicalLocalID = node.ID
		}
	}
	if logicalLocalID == "" {
		return fmt.Errorf("local Raft address %q is not registered in controller metadata", localAddress)
	}
	desiredLocalGroups := make(map[string]struct{})
	for _, partition := range snapshot.Partitions {
		if err := ctx.Err(); err != nil {
			return err
		}
		if partition == nil || partition.ID.Topic == "" || partition.ID.Partition < 0 {
			return fmt.Errorf("controller topology contains an invalid partition record")
		}
		groupID := "partition/" + partition.ID.String()
		groupReplicas := append([]string(nil), partition.Replicas...)
		localReplicas := append([]string(nil), partition.Replicas...)
		if pending := partition.Reassignment; pending != nil {
			groupReplicas = append(groupReplicas, pending.ExpectedReplicas...)
			groupReplicas = append(groupReplicas, pending.DesiredReplicas...)
			groupReplicas = sortedUniqueStrings(groupReplicas)
			if pending.Phase == cluster.PartitionReassignmentAborting {
				localReplicas = append([]string(nil), pending.ExpectedReplicas...)
			}
		}
		localAssigned := false
		for _, replica := range localReplicas {
			if replica == logicalLocalID {
				localAssigned = true
				break
			}
		}
		if localAssigned && partition.Reassignment != nil && partition.Reassignment.Phase == cluster.PartitionReassignmentCleanup &&
			!slices.Contains(partition.Reassignment.DesiredReplicas, logicalLocalID) {
			if group, exists := db.RaftGroup(groupID); !exists || group.PeerRole(localPeerID) == "" {
				localAssigned = false
			}
		}
		if !localAssigned {
			continue
		}
		if len(partition.Replicas) != db.distCfg.ReplicationFactor {
			return fmt.Errorf("partition %s has replication factor %d; configured factor is %d", partition.ID, len(partition.Replicas), db.distCfg.ReplicationFactor)
		}
		desiredLocalGroups[groupID] = struct{}{}
		peers := make(map[uint64]string, len(groupReplicas))
		replicaPeerIDs := make(map[string]uint64, len(partition.Replicas))
		for _, replica := range groupReplicas {
			node := nodesByID[replica]
			if node == nil || node.Address == "" {
				return fmt.Errorf("partition %s references replica %q without a registered address", partition.ID, replica)
			}
			peerID, exists := db.raftCtrl.PeerID(node.Address)
			if !exists {
				return fmt.Errorf("partition %s replica %q address %q is not an authenticated controller peer", partition.ID, replica, node.Address)
			}
			if _, duplicate := peers[peerID]; duplicate {
				return fmt.Errorf("partition %s maps multiple logical replicas to Raft peer %d", partition.ID, peerID)
			}
			peers[peerID] = node.Address
			if slices.Contains(partition.Replicas, replica) {
				replicaPeerIDs[replica] = peerID
			}
		}
		if peers[localPeerID] != localAddress {
			return fmt.Errorf("partition %s does not map local logical node %q to Raft peer %d", partition.ID, logicalLocalID, localPeerID)
		}
		if existing, exists := db.RaftGroup(groupID); exists {
			if existing.JointConsensusPending() {
				return fmt.Errorf("partition %s Raft group still has a joint membership change in progress", partition.ID)
			}
			if err := validateReconciledPartitionMembership(existing, partition, groupReplicas, nodesByID); err != nil {
				return err
			}
			if err := db.registerPartitionGroup(partition, existing, replicaPeerIDs); err != nil {
				return fmt.Errorf("initialize partition %s quorum tracking: %w", partition.ID, err)
			}
			continue
		}
		stateMachine, skipBootstrap, err := factory(ctx, partition)
		if err != nil {
			return fmt.Errorf("create state machine for partition %s: %w", partition.ID, err)
		}
		if stateMachine == nil {
			return fmt.Errorf("state-machine factory returned nil for partition %s", partition.ID)
		}
		if partition.Reassignment != nil {
			skipBootstrap = true
		}
		controller, err := db.AddRaftGroup(cluster.RaftGroupConfig{
			GroupID: groupID, ID: localPeerID, NodeID: logicalLocalID,
			Peers: peers, StateMachine: stateMachine, SkipBootstrap: skipBootstrap,
		})
		if err != nil {
			return fmt.Errorf("provision Raft group for partition %s: %w", partition.ID, err)
		}
		if err := db.registerPartitionGroup(partition, controller, replicaPeerIDs); err != nil {
			cleanupErr := db.RemoveRaftGroup(groupID)
			return errors.Join(fmt.Errorf("initialize partition %s quorum tracking: %w", partition.ID, err), cleanupErr)
		}
		if store := createdStores[groupID]; store != nil {
			db.startMu.Lock()
			db.partitionStores[groupID] = store
			if adapter := createdAdapters[groupID]; adapter != nil {
				db.partitionAdapters[groupID] = adapter
			}
			db.startMu.Unlock()
			delete(createdStores, groupID)
			delete(createdAdapters, groupID)
		}
	}
	for _, groupID := range db.RaftGroupIDs() {
		if strings.HasPrefix(groupID, "partition/") {
			if _, assigned := desiredLocalGroups[groupID]; !assigned {
				group, exists := db.RaftGroup(groupID)
				if !exists {
					continue
				}
				if role := group.PeerRole(localPeerID); role != "" {
					return fmt.Errorf("local Raft group %q is no longer assigned in controller metadata but still includes local peer %d as %s; safe reassignment and removal are required", groupID, localPeerID, role)
				}
				if err := db.RemoveRaftGroup(groupID); err != nil {
					return fmt.Errorf("remove stale local Raft group %q after local peer removal: %w", groupID, err)
				}
			}
		}
	}
	return nil
}

func sortedUniqueStrings(values []string) []string {
	sort.Strings(values)
	if len(values) < 2 {
		return values
	}
	unique := values[:1]
	for _, value := range values[1:] {
		if value != unique[len(unique)-1] {
			unique = append(unique, value)
		}
	}
	return unique
}

func sameStringSet(a, b []string) bool {
	return slices.Equal(sortedUniqueStrings(append([]string(nil), a...)), sortedUniqueStrings(append([]string(nil), b...)))
}

func validateReconciledPartitionMembership(controller *cluster.RaftController, partition *cluster.PartitionMetadata, groupReplicas []string, nodes map[string]*cluster.NodeInfo) error {
	addresses := controller.PeerAddresses()
	addressOwners := make(map[string]string, len(groupReplicas))
	for _, replica := range groupReplicas {
		node := nodes[replica]
		if node == nil || node.Address == "" {
			return fmt.Errorf("partition %s references reassignment replica %q without a registered address", partition.ID, replica)
		}
		if previous, duplicate := addressOwners[node.Address]; duplicate && previous != replica {
			return fmt.Errorf("partition %s maps logical replicas %q and %q to the same address", partition.ID, previous, replica)
		}
		addressOwners[node.Address] = replica
	}
	if partition.Reassignment == nil && len(addresses) != len(groupReplicas) {
		return fmt.Errorf("partition %s group membership differs from controller assignment; safe reassignment is required", partition.ID)
	}
	for _, address := range addresses {
		if _, ok := addressOwners[address]; !ok {
			return fmt.Errorf("partition %s group has unexpected peer %q outside its durable assignment transition", partition.ID, address)
		}
	}
	for _, replica := range partition.Replicas {
		node := nodes[replica]
		if node == nil || node.Address == "" {
			return fmt.Errorf("partition %s replica %q has no registered address", partition.ID, replica)
		}
		peerID, ok := controller.PeerID(node.Address)
		if !ok || controller.PeerRole(peerID) != "voter" {
			return fmt.Errorf("partition %s replica %q is not a Raft voter", partition.ID, replica)
		}
	}
	if pending := partition.Reassignment; pending != nil {
		switch pending.Phase {
		case cluster.PartitionReassignmentCopying, cluster.PartitionReassignmentAborting:
			for _, replica := range pending.ExpectedReplicas {
				node := nodes[replica]
				if node == nil || node.Address == "" {
					return fmt.Errorf("partition %s source replica %q has no registered address", partition.ID, replica)
				}
				peerID, ok := controller.PeerID(node.Address)
				if !ok || controller.PeerRole(peerID) != "voter" {
					return fmt.Errorf("partition %s source replica %q is not a live Raft voter", partition.ID, replica)
				}
			}
		case cluster.PartitionReassignmentCleanup:
			for _, replica := range pending.DesiredReplicas {
				node := nodes[replica]
				if node == nil || node.Address == "" {
					return fmt.Errorf("partition %s target replica %q has no registered address", partition.ID, replica)
				}
				peerID, ok := controller.PeerID(node.Address)
				if !ok || controller.PeerRole(peerID) != "voter" {
					return fmt.Errorf("partition %s target replica %q is not a live Raft voter", partition.ID, replica)
				}
			}
		}
	}
	leaderCandidates := []string{partition.Leader}
	if pending := partition.Reassignment; pending != nil && (pending.Phase == cluster.PartitionReassignmentCopying || pending.Phase == cluster.PartitionReassignmentAborting) {
		leaderCandidates = append(leaderCandidates, pending.ExpectedReplicas...)
		leaderCandidates = append(leaderCandidates, pending.DesiredLeader)
	}
	leaderCandidates = sortedUniqueStrings(leaderCandidates)
	leaderMatches := false
	for _, logicalID := range leaderCandidates {
		node := nodes[logicalID]
		if node == nil {
			continue
		}
		peerID, ok := controller.PeerID(node.Address)
		if ok && controller.LeaderID() == peerID {
			leaderMatches = true
			break
		}
	}
	if !leaderMatches {
		return fmt.Errorf("partition %s Raft leader does not match its active or pending controller assignment", partition.ID)
	}
	return nil
}

func wrapStateCallback(fn func(state string)) func(state cluster.RaftState) {
	if fn == nil {
		return nil
	}
	return func(state cluster.RaftState) {
		fn(state.String())
	}
}

func (db *DistributedBroker) Start(ctx context.Context) error {
	db.startMu.Lock()
	defer db.startMu.Unlock()
	if db.closed || db.stopping {
		return ErrClosed
	}
	if db.started {
		return fmt.Errorf("already started")
	}

	db.startTime = time.Now()

	// Accept peer traffic before starting elections so a node cannot campaign
	// while its transport is still unreachable.
	if db.distCfg.ListenAddr != "" {
		var err error
		if db.raftMux != nil {
			err = db.raftMux.Listen(db.distCfg.ListenAddr)
		} else {
			err = db.transport.Listen(db.distCfg.ListenAddr, db.handleMessage)
		}
		if err != nil {
			return fmt.Errorf("transport: %w", err)
		}
	}

	if db.raftCtrl != nil {
		if err := db.raftCtrl.Start(); err != nil {
			return errors.Join(fmt.Errorf("etcd raft: %w", err), db.abortConsensusStart())
		}
		if len(db.distCfg.RaftPeers) <= 1 {
			if err := db.raftCtrl.Campaign(); err != nil {
				cleanupErr := db.abortConsensusStart()
				return errors.Join(fmt.Errorf("campaign controller Raft group: %w", err), cleanupErr)
			}
		}
		for _, controller := range db.raftGroups {
			if err := controller.Start(); err != nil {
				cleanupErr := db.abortConsensusStart()
				return errors.Join(fmt.Errorf("start application Raft group %q: %w", controller.GroupID(), err), cleanupErr)
			}
			if controller.PeerCount() <= 1 {
				if err := controller.Campaign(); err != nil {
					cleanupErr := db.abortConsensusStart()
					return errors.Join(fmt.Errorf("campaign application Raft group %q: %w", controller.GroupID(), err), cleanupErr)
				}
			}
		}
	} else if db.raft != nil {
		if err := db.raft.Start(); err != nil {
			_ = db.transport.Close()
			db.closed = true
			return fmt.Errorf("raft: %w", err)
		}
	}

	if err := db.Broker.Start(ctx); err != nil {
		return errors.Join(fmt.Errorf("broker: %w", err), db.abortConsensusStart())
	}

	db.started = true
	if db.controllerQuorum != nil {
		recoveryCtx, cancel := context.WithCancel(context.Background())
		db.txRecoveryCancel = cancel
		db.txRecoveryDone = make(chan struct{})
		go db.runTransactionRecovery(recoveryCtx, db.txRecoveryDone)
	}
	return nil
}

func (db *DistributedBroker) runTransactionRecovery(ctx context.Context, done chan struct{}) {
	defer close(done)
	onError := db.distCfg.OnTransactionRecoveryError
	if onError == nil {
		onError = func(err error) { log.Printf("[cluster] transaction recovery: %v", err) }
	}
	if err := db.controllerQuorum.RunTransactionRecovery(ctx, db.distCfg.TransactionRecoveryInterval, db.TransactionParticipantResolver(), onError); err != nil && !errors.Is(err, context.Canceled) {
		onError(err)
	}
}

func (db *DistributedBroker) abortConsensusStart() error {
	var cleanupErr error
	db.closePartitionGroups()
	for _, controller := range db.raftGroups {
		cleanupErr = errors.Join(cleanupErr, controller.Stop())
	}
	if db.raftCtrl != nil {
		cleanupErr = errors.Join(cleanupErr, db.raftCtrl.Stop())
	}
	if db.raftMux != nil {
		for _, controller := range db.raftGroups {
			cleanupErr = errors.Join(cleanupErr, db.raftMux.Unregister(controller))
		}
		if db.raftCtrl != nil {
			cleanupErr = errors.Join(cleanupErr, db.raftMux.Unregister(db.raftCtrl))
		}
	}
	cleanupErr = errors.Join(cleanupErr, db.closePartitionStores())
	if db.raft != nil {
		db.raft.Stop()
	}
	if db.transport != nil {
		cleanupErr = errors.Join(cleanupErr, db.transport.Close())
	}
	if db.partitionForwardClient != nil {
		_ = db.partitionForwardClient.Close()
	}
	db.closed = true
	return cleanupErr
}

// closePartitionGroups is called while the broker lifecycle lock is held.
func (db *DistributedBroker) closePartitionGroups() {
	for groupID, group := range db.partitionGroups {
		_ = group.Close()
		delete(db.partitionGroups, groupID)
	}
}

func (db *DistributedBroker) closePartitionStores() error {
	var closeErr error
	for groupID, store := range db.partitionStores {
		closeErr = errors.Join(closeErr, store.Close())
		delete(db.partitionStores, groupID)
		delete(db.partitionAdapters, groupID)
	}
	return closeErr
}

func (db *DistributedBroker) Stop(ctx context.Context) error {
	db.startMu.Lock()
	if ctx == nil {
		ctx = context.Background()
	}
	if db.closed || (!db.started && !db.stopping) {
		db.startMu.Unlock()
		return nil
	}
	if !db.stopping {
		db.stopping = true
		if db.txRecoveryCancel != nil {
			db.txRecoveryCancel()
		}
	}
	recoveryDone := db.txRecoveryDone
	db.startMu.Unlock()

	// A participant resolver may be finishing a durable decision against local
	// partition stores. Join it before closing those stores. The resolver is
	// required to honor cancellation; callers may retry Stop after a deadline.
	if recoveryDone != nil {
		select {
		case <-recoveryDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	db.startMu.Lock()
	defer db.startMu.Unlock()
	if db.closed {
		return nil
	}

	var stopErr error
	db.closePartitionGroups()
	// Stop consensus before closing its transport. Otherwise the active Raft
	// loop can race transport shutdown by dialing a peer while Close is waiting
	// for connection goroutines to exit.
	if db.raftCtrl != nil {
		stopErr = errors.Join(stopErr, db.raftCtrl.Stop())
	}
	for _, controller := range db.raftGroups {
		stopErr = errors.Join(stopErr, controller.Stop())
	}
	if db.raftMux != nil {
		for _, controller := range db.raftGroups {
			stopErr = errors.Join(stopErr, db.raftMux.Unregister(controller))
		}
		if db.raftCtrl != nil {
			stopErr = errors.Join(stopErr, db.raftMux.Unregister(db.raftCtrl))
		}
	}
	stopErr = errors.Join(stopErr, db.closePartitionStores())
	if db.raft != nil {
		db.raft.Stop()
	}
	if db.transport != nil {
		stopErr = errors.Join(stopErr, db.transport.Close())
	}
	if db.partitionForwardClient != nil {
		_ = db.partitionForwardClient.Close()
	}
	stopErr = errors.Join(stopErr, db.Broker.Stop(ctx))
	if db.wal != nil {
		stopErr = errors.Join(stopErr, db.wal.Close())
	}

	db.started = false
	db.closed = true
	return stopErr
}

func (db *DistributedBroker) handleMessage(from string, msg *cluster.RaftMessage) {
	if db.raftMux != nil {
		if err := db.raftMux.HandleMessage(from, msg); err != nil {
			log.Printf("[raft] rejected multiplexed peer message from %q: %v", from, err)
		}
		return
	}
	if db.raftCtrl != nil {
		if err := db.raftCtrl.HandleMessage(from, msg); err != nil {
			log.Printf("[raft] rejected peer message from %q: %v", from, err)
		}
		return
	}
	if db.raft != nil {
		db.raft.HandleMessage(from, msg)
	}
}

func (db *DistributedBroker) forwardToLeader() error {
	leader := db.Leader()
	if leader == "" {
		return fmt.Errorf("cluster: no leader known; cannot forward request")
	}
	if db.IsLeader() {
		return nil
	}
	return NotLeaderError(leader, db.raftTerm())
}

// raftTerm reports the current term from whichever Raft implementation is
// configured, so a not-leader error can carry a fencing epoch.
func (db *DistributedBroker) raftTerm() uint64 {
	if db.raftCtrl != nil {
		return db.raftCtrl.Term()
	}
	if db.raft != nil {
		return db.raft.CurrentTerm()
	}
	return 0
}

func (db *DistributedBroker) DegradedMode() bool {
	return !db.IsLeader() && db.Leader() != "" && db.CanServeReads()
}

func (db *DistributedBroker) CanServeReads() bool {
	if db.raftCtrl != nil {
		return db.raftCtrl.IsLeader() || db.distCfg.AllowStaleReads
	}
	if db.raft != nil {
		state := db.raft.State()
		return state == cluster.StateFollower || state == cluster.StateLeader
	}
	return false
}

func (db *DistributedBroker) SubmitToRaft(command []byte) (uint64, error) {
	if !db.cb.allow() {
		return 0, ErrCircuitOpen
	}

	db.pending.Add(1)
	db.lastSubmitAt.Store(time.Now())

	if db.raftCtrl != nil {
		ctx, cancel := context.WithTimeout(context.Background(), db.distCfg.SubmitTimeout)
		defer cancel()
		if err := db.raftCtrl.Propose(ctx, command); err != nil {
			db.cb.recordFailure()
			db.pending.Add(-1)
			return 0, err
		}
		db.cb.recordSuccess()
		db.pending.Add(-1)
		return db.raftCtrl.Term(), nil
	}

	if db.raft != nil {
		idx, err := db.raft.Submit(command)
		if err != nil {
			db.cb.recordFailure()
			db.pending.Add(-1)
			return 0, err
		}

		db.cb.recordSuccess()
		db.pending.Add(-1)
		return idx, nil
	}

	db.pending.Add(-1)
	return 0, ErrClosed
}

func (db *DistributedBroker) IsLeader() bool {
	if db.raftCtrl != nil {
		return db.raftCtrl.IsLeader()
	}
	if db.raft != nil {
		return db.raft.IsLeader()
	}
	return false
}

func (db *DistributedBroker) Leader() string {
	if db.raftCtrl != nil {
		leadID := db.raftCtrl.LeaderID()
		if leadID == 0 {
			return ""
		}
		if addr, ok := db.raftCtrl.PeerAddress(leadID); ok {
			return addr
		}
		return fmt.Sprintf("%d", leadID)
	}
	if db.raft != nil {
		return db.raft.Leader()
	}
	return ""
}

func (db *DistributedBroker) RaftState() string {
	if db.raftCtrl != nil {
		if db.raftCtrl.IsLeader() {
			return "leader"
		}
		return "follower"
	}
	if db.raft != nil {
		return db.raft.State().String()
	}
	return "unknown"
}

func (db *DistributedBroker) AddNode(addr string) error {
	if db.raftCtrl != nil {
		_, err := db.AddRaftNode(context.Background(), addr)
		return err
	}
	if db.raft != nil {
		return db.raft.AddPeer(addr)
	}
	return fmt.Errorf("%w: no Raft engine is running", ErrClosed)
}

// AddRaftNode commits a direct voting-member addition and returns the ID that
// the joining process must use with JoinExisting enabled. Online onboarding
// should use AddRaftLearner and PromoteRaftLearner instead.
func (db *DistributedBroker) AddRaftNode(ctx context.Context, addr string) (uint64, error) {
	if db.raftCtrl == nil {
		return 0, fmt.Errorf("%w: etcd/raft controller is not enabled", ErrUnsupportedMode)
	}
	ctx, cancel := db.membershipContext(ctx)
	defer cancel()
	peerID, err := db.raftCtrl.AddPeer(ctx, addr)
	if err != nil {
		return 0, db.normalizeRaftError(err)
	}
	return peerID, nil
}

// AddRaftLearner adds a non-voting Raft peer and returns the ID to use when
// starting the joining process. Promote it only after it catches up.
func (db *DistributedBroker) AddRaftLearner(ctx context.Context, addr string) (uint64, error) {
	if db.raftCtrl == nil {
		return 0, fmt.Errorf("%w: etcd/raft controller is not enabled", ErrUnsupportedMode)
	}
	ctx, cancel := db.membershipContext(ctx)
	defer cancel()
	peerID, err := db.raftCtrl.AddLearner(ctx, addr)
	if err != nil {
		return 0, db.normalizeRaftError(err)
	}
	return peerID, nil
}

// PromoteRaftLearner promotes a learner after the leader confirms it has
// replicated through the current commit index.
func (db *DistributedBroker) PromoteRaftLearner(ctx context.Context, peerID uint64) error {
	if db.raftCtrl == nil {
		return fmt.Errorf("%w: etcd/raft controller is not enabled", ErrUnsupportedMode)
	}
	ctx, cancel := db.membershipContext(ctx)
	defer cancel()
	return db.normalizeRaftError(db.raftCtrl.PromoteLearner(ctx, peerID))
}

// RaftPeerRole reports a peer's current etcd/raft membership role.
func (db *DistributedBroker) RaftPeerRole(peerID uint64) string {
	if db.raftCtrl == nil {
		return ""
	}
	return db.raftCtrl.PeerRole(peerID)
}

// ControllerPeerAddresses returns the controller Raft group's current peer
// address map, including learners added after this broker started.
func (db *DistributedBroker) ControllerPeerAddresses() map[uint64]string {
	if db == nil || db.raftCtrl == nil {
		return nil
	}
	return db.raftCtrl.PeerAddresses()
}

func (db *DistributedBroker) RemoveNode(id string) error {
	if db.raftCtrl != nil {
		peerID, ok := db.raftCtrl.PeerID(id)
		if !ok {
			return cluster.ErrNodeNotFound
		}
		return db.RemoveRaftNode(context.Background(), peerID)
	}
	if db.raft != nil {
		return db.raft.RemovePeer(id)
	}
	return fmt.Errorf("%w: no Raft engine is running", ErrClosed)
}

// RaftNodeRemovalPlan describes partition assignments that must be moved
// before a controller Raft peer can be removed.
type RaftNodeRemovalPlan struct {
	PeerID               uint64                     `json:"peer_id"`
	Address              string                     `json:"address"`
	Role                 string                     `json:"role"`
	ControllerLeader     bool                       `json:"controller_leader"`
	LocalPeerLeader      bool                       `json:"local_peer_leader"`
	JointChangePending   bool                       `json:"joint_change_pending"`
	LogicalNodeID        string                     `json:"logical_node_id,omitempty"`
	LogicalNodeState     cluster.NodeState          `json:"logical_node_state,omitempty"`
	AssignedPartitions   []cluster.PartitionGroupID `json:"assigned_partitions"`
	PendingReassignments []cluster.PartitionGroupID `json:"pending_reassignments,omitempty"`
	AssignmentFree       bool                       `json:"assignment_free"`
	ReadyForRemoval      bool                       `json:"ready_for_removal"`
	Blockers             []string                   `json:"blockers,omitempty"`
}

// PlanRaftNodeRemoval returns a quorum-confirmed list of partition assignments
// that prevent removal of a controller Raft peer.
func (db *DistributedBroker) PlanRaftNodeRemoval(ctx context.Context, peerID uint64) (*RaftNodeRemovalPlan, error) {
	if db.raftCtrl == nil {
		return nil, fmt.Errorf("%w: etcd/raft controller is not enabled", ErrUnsupportedMode)
	}
	address, ok := db.raftCtrl.PeerAddress(peerID)
	if !ok {
		return nil, cluster.ErrNodeNotFound
	}
	if db.controllerQuorum == nil {
		return nil, fmt.Errorf("%w: controller quorum is required to verify partition assignments", ErrUnsupportedMode)
	}
	ctx, cancel := db.membershipContext(ctx)
	defer cancel()
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	plan := &RaftNodeRemovalPlan{
		PeerID: peerID, Address: address, Role: db.raftCtrl.PeerRole(peerID),
		ControllerLeader: db.raftCtrl.LeaderID() == peerID, LocalPeerLeader: db.raftCtrl.IsLeader(),
		JointChangePending: db.raftCtrl.JointConsensusPending(),
	}
	for _, node := range snapshot.Nodes {
		if node != nil && node.Address == address {
			if plan.LogicalNodeID != "" && plan.LogicalNodeID != node.ID {
				return nil, fmt.Errorf("%w: Raft address %q maps to multiple logical nodes", cluster.ErrInvalidPartitionAssignment, address)
			}
			plan.LogicalNodeID = node.ID
			plan.LogicalNodeState = node.State
		}
	}
	if plan.LogicalNodeID != "" {
		for _, partition := range snapshot.Partitions {
			if partition == nil {
				continue
			}
			if slices.Contains(partition.Replicas, plan.LogicalNodeID) {
				plan.AssignedPartitions = append(plan.AssignedPartitions, partition.ID.Canonical())
			}
			if partition.Reassignment != nil && (slices.Contains(partition.Reassignment.ExpectedReplicas, plan.LogicalNodeID) || slices.Contains(partition.Reassignment.DesiredReplicas, plan.LogicalNodeID)) {
				plan.PendingReassignments = append(plan.PendingReassignments, partition.ID.Canonical())
			}
		}
	}
	sort.Slice(plan.AssignedPartitions, func(i, j int) bool {
		return plan.AssignedPartitions[i].String() < plan.AssignedPartitions[j].String()
	})
	sort.Slice(plan.PendingReassignments, func(i, j int) bool {
		return plan.PendingReassignments[i].String() < plan.PendingReassignments[j].String()
	})
	plan.AssignmentFree = len(plan.AssignedPartitions) == 0 && len(plan.PendingReassignments) == 0
	if len(plan.AssignedPartitions) > 0 {
		plan.Blockers = append(plan.Blockers, "peer hosts a logical node assigned to one or more partitions")
	}
	if len(plan.PendingReassignments) > 0 {
		plan.Blockers = append(plan.Blockers, "peer is still referenced by an in-progress partition reassignment")
	}
	if plan.Role != "voter" && plan.Role != "learner" {
		plan.Blockers = append(plan.Blockers, fmt.Sprintf("peer has no removable membership role (current role %q)", plan.Role))
	}
	if plan.LogicalNodeID == "" || plan.LogicalNodeState != cluster.NodeLeft {
		plan.Blockers = append(plan.Blockers, "logical node must be registered, reassigned, and marked left before removal")
	}
	if plan.ControllerLeader {
		plan.Blockers = append(plan.Blockers, "controller leadership must transfer before removal")
	}
	if !plan.LocalPeerLeader {
		plan.Blockers = append(plan.Blockers, "removal must be proposed by the current controller leader")
	}
	if plan.JointChangePending {
		plan.Blockers = append(plan.Blockers, "a joint controller membership change is in progress")
	}
	plan.ReadyForRemoval = len(plan.Blockers) == 0
	return plan, nil
}

// BeginRaftNodeDrain marks the logical node for a controller peer as draining.
// The replicated state change prevents new partition assignments from adding
// the node while existing assignments are being moved.
func (db *DistributedBroker) BeginRaftNodeDrain(ctx context.Context, peerID uint64) (*cluster.NodeInfo, error) {
	if db.raftCtrl == nil || db.controllerQuorum == nil {
		return nil, fmt.Errorf("%w: controller quorum membership is not enabled", ErrUnsupportedMode)
	}
	address, ok := db.raftCtrl.PeerAddress(peerID)
	if !ok {
		return nil, cluster.ErrNodeNotFound
	}
	ctx, cancel := db.membershipContext(ctx)
	defer cancel()
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	var target *cluster.NodeInfo
	for _, node := range snapshot.Nodes {
		if node == nil || node.Address != address {
			continue
		}
		if target != nil {
			return nil, fmt.Errorf("%w: Raft address %q maps to multiple logical nodes", cluster.ErrInvalidPartitionAssignment, address)
		}
		target = node
	}
	if target == nil {
		return nil, cluster.ErrNodeNotFound
	}
	switch target.State {
	case cluster.NodeDraining:
		return target, nil
	case cluster.NodeActive, cluster.NodeJoining:
		target.State = cluster.NodeDraining
	default:
		return nil, fmt.Errorf("%w: node %q in state %q cannot begin a drain", cluster.ErrInvalidNodeState, target.ID, target.State)
	}
	if err := db.controllerQuorum.RegisterNode(ctx, target); err != nil {
		return nil, err
	}
	nodes, err := db.controllerQuorum.GetNodesLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if node.ID == target.ID {
			return node, nil
		}
	}
	return nil, cluster.ErrNodeNotFound
}

// CompleteRaftNodeDrain marks the logical node left after its current
// assignments are gone. The metadata state machine rechecks assignments while
// applying the state change, so a concurrent placement cannot slip past this
// preflight.
func (db *DistributedBroker) CompleteRaftNodeDrain(ctx context.Context, peerID uint64) (*cluster.NodeInfo, error) {
	if db.raftCtrl == nil || db.controllerQuorum == nil {
		return nil, fmt.Errorf("%w: controller quorum membership is not enabled", ErrUnsupportedMode)
	}
	address, ok := db.raftCtrl.PeerAddress(peerID)
	if !ok {
		return nil, cluster.ErrNodeNotFound
	}
	ctx, cancel := db.membershipContext(ctx)
	defer cancel()
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	var target *cluster.NodeInfo
	for _, node := range snapshot.Nodes {
		if node == nil || node.Address != address {
			continue
		}
		if target != nil {
			return nil, fmt.Errorf("%w: Raft address %q maps to multiple logical nodes", cluster.ErrInvalidPartitionAssignment, address)
		}
		target = node
	}
	if target == nil {
		return nil, cluster.ErrNodeNotFound
	}
	for _, partition := range snapshot.Partitions {
		if partition != nil && partitionReferencesLogicalNode(partition, target.ID) {
			return nil, fmt.Errorf("%w: logical node %q remains assigned or referenced by a reassignment for partition %s", cluster.ErrNodeInUse, target.ID, partition.ID)
		}
	}
	switch target.State {
	case cluster.NodeLeft:
		return target, nil
	case cluster.NodeDraining:
		target.State = cluster.NodeLeft
	default:
		return nil, fmt.Errorf("%w: node %q must be draining before it can be marked left", cluster.ErrInvalidNodeState, target.ID)
	}
	if err := db.controllerQuorum.RegisterNode(ctx, target); err != nil {
		return nil, err
	}
	nodes, err := db.controllerQuorum.GetNodesLinearizable(ctx)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if node.ID == target.ID {
			return node, nil
		}
	}
	return nil, cluster.ErrNodeNotFound
}

func partitionReferencesLogicalNode(partition *cluster.PartitionMetadata, nodeID string) bool {
	if partition == nil {
		return false
	}
	if slices.Contains(partition.Replicas, nodeID) {
		return true
	}
	return partition.Reassignment != nil &&
		(slices.Contains(partition.Reassignment.ExpectedReplicas, nodeID) || slices.Contains(partition.Reassignment.DesiredReplicas, nodeID))
}

// RemoveRaftNode commits removal of a voting member by its numeric Raft ID.
func (db *DistributedBroker) RemoveRaftNode(ctx context.Context, peerID uint64) error {
	plan, err := db.PlanRaftNodeRemoval(ctx, peerID)
	if err != nil {
		return err
	}
	if !plan.AssignmentFree {
		if len(plan.AssignedPartitions) > 0 {
			return fmt.Errorf("%w: Raft peer %d hosts logical node %q assigned to partition %s", cluster.ErrNodeInUse, peerID, plan.LogicalNodeID, plan.AssignedPartitions[0])
		}
		return fmt.Errorf("%w: Raft peer %d hosts logical node %q referenced by pending reassignment of partition %s", cluster.ErrNodeInUse, peerID, plan.LogicalNodeID, plan.PendingReassignments[0])
	}
	if plan.Role != "voter" && plan.Role != "learner" {
		return cluster.ErrNodeNotFound
	}
	if plan.LogicalNodeID == "" || plan.LogicalNodeState != cluster.NodeLeft {
		return fmt.Errorf("%w: peer %d logical node must be registered, reassigned, and marked left before removal", cluster.ErrInvalidNodeState, peerID)
	}
	if plan.ControllerLeader {
		return fmt.Errorf("%w: controller leader peer %d must transfer leadership before removal", cluster.ErrPartitionTransferRequired, peerID)
	}
	if !plan.LocalPeerLeader {
		return db.normalizeRaftError(cluster.ErrNotLeader)
	}
	if plan.JointChangePending {
		return cluster.ErrConfChangePending
	}
	ctx, cancel := db.membershipContext(ctx)
	defer cancel()
	if err := db.raftCtrl.RemovePeer(ctx, peerID); err != nil {
		return db.normalizeRaftError(err)
	}
	return nil
}

// TransferRaftLeadership moves control to a caught-up Raft voter.
func (db *DistributedBroker) TransferRaftLeadership(ctx context.Context, peerID uint64) error {
	if db.raftCtrl == nil {
		return fmt.Errorf("%w: etcd/raft controller is not enabled", ErrUnsupportedMode)
	}
	ctx, cancel := db.membershipContext(ctx)
	defer cancel()
	return db.normalizeRaftError(db.raftCtrl.TransferLeadership(ctx, peerID))
}

func (db *DistributedBroker) normalizeRaftError(err error) error {
	if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
		return NotLeaderError(db.controllerLeaderClientAddress(), db.raftTerm())
	}
	return err
}

func (db *DistributedBroker) normalizePartitionRaftError(ctx context.Context, controller *cluster.RaftController, err error) error {
	if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
		return db.partitionGroupNotLeaderError(ctx, controller)
	}
	return db.normalizeRaftError(err)
}

// controllerLeaderClientAddress maps the current Raft leader's private peer
// address to its advertised broker endpoint. The local metadata copy is used
// because a follower cannot perform a linearizable read while handling a
// not-leader response; this value is only a retry hint.
func (db *DistributedBroker) controllerLeaderClientAddress() string {
	if db == nil || db.raftCtrl == nil || db.controllerQuorum == nil {
		return ""
	}
	leaderID := db.raftCtrl.LeaderID()
	peerAddress, ok := db.raftCtrl.PeerAddress(leaderID)
	if !ok || peerAddress == "" {
		return ""
	}
	for _, node := range db.controllerQuorum.GetNodes() {
		if node != nil && node.Address == peerAddress {
			return node.ClientAddress
		}
	}
	return ""
}

func (db *DistributedBroker) membershipContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := db.distCfg.SubmitTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func (db *DistributedBroker) ClusterStats() map[string]any {
	lastSubmit, _ := db.lastSubmitAt.Load().(time.Time)

	stats := map[string]any{
		"raft_state":           db.RaftState(),
		"is_leader":            db.IsLeader(),
		"leader":               db.Leader(),
		"pending_submissions":  db.pending.Load(),
		"circuit_state":        db.cb.getState(),
		"consecutive_failures": db.cb.getConsecutiveFailures(),
		"last_submit_time":     lastSubmit,
		"uptime":               time.Since(db.startTime).String(),
	}
	if db.raftCtrl != nil {
		stats["consensus_engine"] = "etcd/raft"
		stats["term"] = db.raftCtrl.Term()
		stats["applied_index"] = db.raftCtrl.AppliedIndex()
		stats["snapshot_index"] = db.raftCtrl.SnapshotIndex()
	} else if db.raft != nil {
		stats["consensus_engine"] = "legacy"
		stats["term"] = db.raft.CurrentTerm()
		stats["commit_index"] = db.raft.CommitIndex()
		stats["last_applied"] = db.raft.LastApplied()
	}
	if db.wal != nil {
		for k, v := range db.wal.Stats() {
			stats["wal_"+k] = v
		}
	}
	return stats
}

func (db *DistributedBroker) HealthCheck() map[string]any {
	db.startMu.Lock()
	started := db.started
	db.startMu.Unlock()
	health := map[string]any{
		"leader":    db.Leader(),
		"state":     db.RaftState(),
		"is_leader": db.IsLeader(),
		"started":   started,
	}
	if db.raftCtrl != nil {
		if err := db.raftCtrl.FatalError(); err != nil {
			health["healthy"] = false
			health["error"] = err.Error()
			return health
		}
	}
	health["healthy"] = started && db.CanServeReads()
	return health
}

// PartitionReadiness verifies that every partition assigned to this broker has
// a running local Raft group and that no stale local partition group remains.
// It uses one linearizable topology snapshot so readiness does not compare
// assignments and node identities from different metadata revisions.
func (db *DistributedBroker) PartitionReadiness(ctx context.Context) error {
	if db == nil || db.raftCtrl == nil || db.controllerQuorum == nil {
		return nil
	}
	if !db.raftCtrl.Running() {
		return fmt.Errorf("controller Raft group is not running")
	}
	if err := db.raftCtrl.FatalError(); err != nil {
		return fmt.Errorf("controller Raft group failed: %w", err)
	}
	snapshot, err := db.controllerQuorum.TopologySnapshotLinearizable(ctx)
	if err != nil {
		return fmt.Errorf("read partition topology: %w", err)
	}
	localPeerID := db.distCfg.RaftNodeID
	if localPeerID == 0 {
		localPeerID = 1
	}
	localAddress, ok := db.raftCtrl.PeerAddress(localPeerID)
	if !ok {
		return fmt.Errorf("local controller peer %d has no configured address", localPeerID)
	}
	localNodeID := ""
	nodesByID := make(map[string]*cluster.NodeInfo, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if node != nil {
			nodesByID[node.ID] = node
		}
		if node != nil && node.Address == localAddress {
			if localNodeID != "" && localNodeID != node.ID {
				return fmt.Errorf("local Raft address %q is registered to multiple logical nodes", localAddress)
			}
			localNodeID = node.ID
		}
	}
	desired := make(map[string]*cluster.PartitionMetadata)
	if localNodeID != "" {
		for _, partition := range snapshot.Partitions {
			if partition == nil {
				continue
			}
			for _, replica := range partition.Replicas {
				if replica == localNodeID {
					id := partition.ID.Canonical()
					desired["partition/"+id.String()] = partition
					break
				}
			}
		}
	}
	desiredIDs := make([]string, 0, len(desired))
	for groupID := range desired {
		desiredIDs = append(desiredIDs, groupID)
	}
	sort.Strings(desiredIDs)
	for _, groupID := range desiredIDs {
		partition := desired[groupID]
		id := partition.ID.Canonical()
		controller, exists := db.RaftGroup(groupID)
		if !exists {
			return fmt.Errorf("assigned partition %s has no local Raft group", id)
		}
		if !controller.Running() {
			return fmt.Errorf("assigned partition %s Raft group is not running", id)
		}
		if err := controller.FatalError(); err != nil {
			return fmt.Errorf("assigned partition %s Raft group failed: %w", id, err)
		}
		group, exists := db.PartitionGroup(id)
		if !exists {
			return fmt.Errorf("assigned partition %s has no local quorum tracker", id)
		}
		trackedReplicas := group.Replicas()
		trackedReplicaIDs := make([]string, len(trackedReplicas))
		for i, replica := range trackedReplicas {
			trackedReplicaIDs[i] = replica.NodeID
		}
		assignedReplicaIDs := append([]string(nil), partition.Replicas...)
		sort.Strings(trackedReplicaIDs)
		sort.Strings(assignedReplicaIDs)
		if !slices.Equal(trackedReplicaIDs, assignedReplicaIDs) {
			return fmt.Errorf("assigned partition %s quorum tracker membership differs from metadata", id)
		}
		if controller.PeerCount() != len(partition.Replicas) {
			return fmt.Errorf("assigned partition %s local Raft membership differs from metadata", id)
		}
		for _, replica := range partition.Replicas {
			node := nodesByID[replica]
			if node == nil {
				return fmt.Errorf("assigned partition %s references unregistered replica %q", id, replica)
			}
			peerID, ok := controller.PeerID(node.Address)
			if !ok || controller.PeerRole(peerID) != "voter" {
				return fmt.Errorf("assigned partition %s replica %q is not a local Raft voter", id, replica)
			}
		}
		leaderNode := nodesByID[partition.Leader]
		if leaderNode == nil {
			return fmt.Errorf("assigned partition %s leader %q is not registered", id, partition.Leader)
		}
		leaderPeerID, ok := controller.PeerID(leaderNode.Address)
		if !ok || controller.LeaderID() != leaderPeerID {
			return fmt.Errorf("assigned partition %s Raft leader differs from metadata", id)
		}
	}
	for _, groupID := range db.RaftGroupIDs() {
		if strings.HasPrefix(groupID, "partition/") {
			if _, exists := desired[groupID]; !exists {
				return fmt.Errorf("local partition Raft group %q is no longer assigned", groupID)
			}
		}
	}
	return nil
}

type ReplicatedStore struct {
	Store
	raft            *cluster.Raft
	raftCtrl        *cluster.RaftController
	commitTimeout   time.Duration
	forwardTimeout  time.Duration
	cb              *circuitBreaker
	mutationMu      sync.Mutex
	allowStaleReads bool
	partitionRouter *DistributedBroker
}

func (s *ReplicatedStore) checkReadable() error {
	if s.raftCtrl != nil && !s.raftCtrl.IsLeader() && !s.allowStaleReads {
		return cluster.ErrNotLeader
	}
	return nil
}

func (s *ReplicatedStore) Get(ctx context.Context, id string) (Message, bool, error) {
	if err := s.checkReadable(); err != nil {
		return Message{}, false, err
	}
	return s.Store.Get(ctx, id)
}

func (s *ReplicatedStore) History(ctx context.Context, id string) ([]MessageHistory, error) {
	if err := s.checkReadable(); err != nil {
		return nil, err
	}
	return s.Store.History(ctx, id)
}

func (s *ReplicatedStore) ListQueue(ctx context.Context, name string, limit int) ([]Message, error) {
	if s.partitionRouter != nil {
		assigned, err := s.partitionRouter.hasResourcePartitions(ctx, cluster.PartitionResourceQueue, name)
		if err != nil {
			return nil, err
		}
		if assigned {
			return nil, fmt.Errorf("%w: queue %q must be read through its assigned partition leaders", ErrUnsupportedMode, name)
		}
	}
	if err := s.checkReadable(); err != nil {
		return nil, err
	}
	return s.Store.ListQueue(ctx, name, limit)
}

func (s *ReplicatedStore) QueueReadyCount(ctx context.Context, name string) (int, error) {
	if s.partitionRouter != nil {
		assigned, err := s.partitionRouter.hasResourcePartitions(ctx, cluster.PartitionResourceQueue, name)
		if err != nil {
			return 0, err
		}
		if assigned {
			return 0, fmt.Errorf("%w: queue %q must be counted through its assigned partition leaders", ErrUnsupportedMode, name)
		}
	}
	if err := s.checkReadable(); err != nil {
		return 0, err
	}
	counter, ok := s.Store.(QueueReadyCounter)
	if !ok {
		return 0, fmt.Errorf("%w: backing store does not provide queue ready counts", ErrUnsupportedMode)
	}
	return counter.QueueReadyCount(ctx, name)
}

func (s *ReplicatedStore) ListDLQ(ctx context.Context, name string, limit int) ([]Message, error) {
	if err := s.checkReadable(); err != nil {
		return nil, err
	}
	return s.Store.ListDLQ(ctx, name, limit)
}

func (s *ReplicatedStore) ReadStream(ctx context.Context, req StreamReadRequest) ([]Message, error) {
	if err := s.checkReadable(); err != nil {
		return nil, err
	}
	return s.Store.ReadStream(ctx, req)
}

func (s *ReplicatedStore) ConsumerOffset(ctx context.Context, commit OffsetCommit) (uint64, error) {
	if s.partitionRouter != nil && commit.Partitioned {
		return s.partitionRouter.PartitionStreamOffset(ctx, commit)
	}
	if err := s.checkReadable(); err != nil {
		return 0, err
	}
	return s.Store.ConsumerOffset(ctx, commit)
}

func (s *ReplicatedStore) ConsumerOffsets(ctx context.Context, stream, tenant string) (map[string]uint64, error) {
	if err := s.checkReadable(); err != nil {
		return nil, err
	}
	lister, ok := s.Store.(ConsumerOffsetLister)
	if !ok {
		return nil, ErrUnsupportedMode
	}
	return lister.ConsumerOffsets(ctx, stream, tenant)
}

func (s *ReplicatedStore) Stats(ctx context.Context) (StoreStats, error) {
	if err := s.checkReadable(); err != nil {
		return StoreStats{}, err
	}
	return s.Store.Stats(ctx)
}

type replicatedLease struct {
	Request  LeaseRequest `json:"request"`
	Messages []Message    `json:"messages"`
}

type replicatedMove struct {
	OperationID string            `json:"operation_id"`
	Request     MoveRequest       `json:"request"`
	PlannedIDs  map[string]string `json:"planned_ids,omitempty"`
}

// ConfirmationLevel returns ConfirmReplicated when backed by etcd/raft consensus.
func (s *ReplicatedStore) ConfirmationLevel() ConfirmLevel {
	if s.raftCtrl != nil {
		return ConfirmReplicated
	}
	return confirmationLevel(s.Store)
}

func (s *ReplicatedStore) proposeAndCommit(ctx context.Context, data []byte) error {
	if s.raftCtrl != nil {
		if !s.raftCtrl.IsLeader() {
			leadID := s.raftCtrl.LeaderID()
			if leadID != 0 {
				return NotLeaderError(strconv.FormatUint(uint64(leadID), 10), s.raftCtrl.Term())
			}
			return NotLeaderError("", 0)
		}
		if !s.cb.allow() {
			return ErrCircuitOpen
		}
		timeout := s.commitTimeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		propCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if err := s.raftCtrl.Propose(propCtx, data); err != nil {
			s.cb.recordFailure()
			return err
		}
		s.cb.recordSuccess()
		return nil
	}

	if s.raft == nil {
		return ErrClosed
	}

	if !s.raft.IsLeader() {
		leader := s.raft.Leader()
		if leader != "" {
			return NotLeaderError(leader, s.raft.CurrentTerm())
		}
		return NotLeaderError("", 0)
	}
	if !s.cb.allow() {
		return ErrCircuitOpen
	}
	idx, err := s.raft.Submit(data)
	if err != nil {
		s.cb.recordFailure()
		return err
	}
	s.cb.recordSuccess()
	return s.waitForCommit(ctx, idx)
}

func (s *ReplicatedStore) submit(ctx context.Context, cmdType cluster.BrokerCommandType, payload any) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.submitUnlocked(ctx, cmdType, payload)
}

func (s *ReplicatedStore) submitUnlocked(ctx context.Context, cmdType cluster.BrokerCommandType, payload any) error {
	cmd, err := cluster.MakeBrokerCommand(cmdType, payload)
	if err != nil {
		return err
	}
	data, err := cluster.EncodeBrokerCommand(cmd)
	if err != nil {
		return err
	}
	return s.proposeAndCommit(ctx, data)
}

func (s *ReplicatedStore) waitForCommit(ctx context.Context, index uint64) error {
	if s.raft == nil {
		return nil
	}
	timeout := s.commitTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		if s.raft.CommitIndex() >= index {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("replicated store: timeout waiting for commit of index %d", index)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *ReplicatedStore) DeclareQueue(ctx context.Context, cfg QueueConfig) error {
	cmd := cluster.DeclareQueueCommand{
		Name:              cfg.Name,
		Durable:           cfg.Durable,
		Workers:           cfg.Workers,
		MaxInFlight:       cfg.MaxInFlight,
		MaxDepth:          cfg.MaxDepth,
		VisibilityTimeout: cfg.VisibilityTimeout,
		AckMode:           string(cfg.AckMode),
		RetryMaxAttempts:  cfg.RetryPolicy.MaxAttempts,
		RetryInitial:      cfg.RetryPolicy.Initial,
		RetryMax:          cfg.RetryPolicy.Max,
		RetryFactor:       cfg.RetryPolicy.Factor,
		DLQ:               cfg.DLQ,
		Ordering:          string(cfg.Ordering),
		Backpressure:      string(cfg.Backpressure),
		Strategy:          string(cfg.Strategy),
	}
	return s.submit(ctx, cluster.BrokerCmdDeclareQueue, cmd)
}

func (s *ReplicatedStore) LookupQueueConfig(ctx context.Context, name string) (QueueConfig, bool, error) {
	lookup, ok := s.Store.(QueueConfigLookup)
	if !ok {
		return QueueConfig{}, false, ErrUnsupportedMode
	}
	return lookup.LookupQueueConfig(ctx, name)
}

func (s *ReplicatedStore) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	cmd := cluster.DeclareTopicCommand{
		Name:       cfg.Name,
		Partitions: cfg.Partitions,
		Retention:  cfg.Retention,
		Paused:     cfg.Paused,
	}
	return s.submit(ctx, cluster.BrokerCmdDeclareTopic, cmd)
}

func (s *ReplicatedStore) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	cmd := cluster.DeclareStreamCommand{
		Name:       cfg.Name,
		Partitions: cfg.Partitions,
		Retention:  cfg.Retention,
		Compact:    cfg.Compact,
	}
	return s.submit(ctx, cluster.BrokerCmdDeclareStream, cmd)
}

func (s *ReplicatedStore) Append(ctx context.Context, msg Message, dests []Destination) ([]Message, error) {
	if s.partitionRouter != nil {
		if stored, routed, err := s.partitionRouter.appendToAssignedPartition(ctx, msg, dests); routed || err != nil {
			return stored, err
		}
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	cmd := cluster.AppendCommand{
		ID:               msg.ID,
		ParentID:         msg.ParentID,
		Topic:            msg.Topic,
		Queue:            msg.Queue,
		Stream:           msg.Stream,
		Partition:        msg.Partition,
		Offset:           msg.Offset,
		Key:              msg.Key,
		RoutingKey:       msg.RoutingKey,
		Type:             msg.Type,
		ContentType:      msg.ContentType,
		Schema:           msg.Schema,
		SchemaVersion:    msg.SchemaVersion,
		Payload:          msg.Payload,
		Headers:          msg.Headers,
		Metadata:         msg.Metadata,
		TraceID:          msg.TraceID,
		SpanID:           msg.SpanID,
		CorrelationID:    msg.CorrelationID,
		CausationID:      msg.CausationID,
		RequestID:        msg.RequestID,
		ReplyTo:          msg.ReplyTo,
		ReplyGroup:       msg.ReplyGroup,
		IdempotencyKey:   msg.IdempotencyKey,
		DeduplicationID:  msg.DeduplicationID,
		Checksum:         msg.Checksum,
		Priority:         msg.Priority,
		CreatedAt:        msg.CreatedAt,
		AvailableAt:      msg.AvailableAt,
		ExpiresAt:        msg.ExpiresAt,
		Attempts:         msg.Attempts,
		MaxAttempts:      msg.MaxAttempts,
		Redeliveries:     msg.Redeliveries,
		TenantID:         msg.TenantID,
		ProducerID:       msg.ProducerID,
		ProducerEpoch:    msg.ProducerEpoch,
		ProducerSequence: msg.ProducerSequence,
		ConsumerID:       msg.ConsumerID,
		ConsumerGroup:    msg.ConsumerGroup,
		LeaseToken:       msg.LeaseToken,
		LeaseUntil:       msg.LeaseUntil,
		OriginalTopic:    msg.OriginalTopic,
		OriginalQueue:    msg.OriginalQueue,
		OriginalStream:   msg.OriginalStream,
		OriginalOffset:   msg.OriginalOffset,
		Status:           string(msg.Status),
		LastError:        msg.LastError,
		DeadLetterReason: msg.DeadLetterReason,
		TaskID:           msg.TaskID,
		TaskName:         msg.TaskName,
		Destinations:     make([]cluster.CommandDestination, len(dests)),
	}
	for i, d := range dests {
		cmd.Destinations[i] = cluster.CommandDestination{
			Kind: string(d.Kind),
			Name: d.Name,
		}
	}
	data, err := cluster.EncodeBrokerCommand(&cluster.BrokerCommand{
		Type:    cluster.BrokerCmdAppend,
		Payload: mustMarshal(cmd),
	})
	if err != nil {
		return nil, err
	}
	if err := s.proposeAndCommit(ctx, data); err != nil {
		return nil, err
	}
	// After Raft commit, the leader's local store has applied the mutation.
	// Query it for the actual stored messages to return to the caller.
	localMsg, ok, err := s.Store.Get(ctx, msg.ID)
	if err != nil {
		return nil, err
	}
	if ok {
		return []Message{localMsg}, nil
	}
	// Fallback: return the original message if local lookup fails
	return []Message{msg}, nil
}

func (s *ReplicatedStore) AppendAtomic(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	if s.partitionRouter != nil {
		if stored, routed, err := s.partitionRouter.appendAtomicToAssignedPartition(ctx, entries); routed || err != nil {
			return stored, err
		}
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if s.raftCtrl == nil || !s.raftCtrl.IsLeader() {
		return nil, cluster.ErrNotLeader
	}
	preparer, ok := s.Store.(interface {
		prepareAppendAtomic(context.Context, []AppendEntry) ([]Message, error)
	})
	if !ok {
		return nil, fmt.Errorf("%w: backing store cannot prepare a replicated atomic append", ErrUnsupportedMode)
	}
	prepared, err := preparer.prepareAppendAtomic(ctx, entries)
	if err != nil {
		return nil, err
	}
	persisted := storedAppendEntries(prepared)
	if err := s.submitUnlocked(ctx, cluster.BrokerCmdAppendAtomic, persisted); err != nil {
		return nil, err
	}
	return prepared, nil
}

func (s *ReplicatedStore) Lease(ctx context.Context, req LeaseRequest) ([]Message, error) {
	if s.partitionRouter != nil && req.Partitioned {
		return s.partitionRouter.LeasePartitionQueue(ctx, req)
	}
	if s.partitionRouter != nil && req.Queue != "" {
		assigned, err := s.partitionRouter.hasResourcePartitions(ctx, cluster.PartitionResourceQueue, req.Queue)
		if err != nil {
			return nil, err
		}
		if assigned {
			return nil, fmt.Errorf("%w: queue %q requires a partition-specific lease", ErrUnsupportedMode, req.Queue)
		}
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if s.raftCtrl == nil || !s.raftCtrl.IsLeader() {
		return nil, cluster.ErrNotLeader
	}
	preparer, ok := s.Store.(interface {
		prepareLease(context.Context, LeaseRequest) ([]Message, error)
	})
	if !ok {
		return nil, fmt.Errorf("%w: backing store cannot prepare a replicated lease", ErrUnsupportedMode)
	}
	messages, err := preparer.prepareLease(ctx, req)
	if err != nil || len(messages) == 0 {
		return messages, err
	}
	if err := s.submitUnlocked(ctx, cluster.BrokerCmdLease, replicatedLease{Request: req, Messages: messages}); err != nil {
		return nil, err
	}
	return messages, nil
}

func (s *ReplicatedStore) ExtendLease(ctx context.Context, req LeaseRequest) error {
	if s.partitionRouter != nil && req.Partitioned {
		return s.partitionRouter.ExtendPartitionQueueLease(ctx, req)
	}
	if s.partitionRouter != nil && req.Queue != "" {
		assigned, err := s.partitionRouter.hasResourcePartitions(ctx, cluster.PartitionResourceQueue, req.Queue)
		if err != nil {
			return err
		}
		if assigned {
			return fmt.Errorf("%w: queue %q requires a partition-specific lease extension", ErrUnsupportedMode, req.Queue)
		}
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if s.raftCtrl == nil || !s.raftCtrl.IsLeader() {
		return cluster.ErrNotLeader
	}
	validator, ok := s.Store.(interface {
		validateExtendLease(context.Context, LeaseRequest) (Message, error)
	})
	if !ok {
		return fmt.Errorf("%w: backing store cannot prepare a replicated lease extension", ErrUnsupportedMode)
	}
	message, err := validator.validateExtendLease(ctx, req)
	if err != nil {
		return err
	}
	if req.VisibilityTimeout <= 0 {
		req.VisibilityTimeout = 30 * time.Second
	}
	message.LeaseUntil = time.Now().Add(req.VisibilityTimeout)
	return s.submitUnlocked(ctx, cluster.BrokerCmdUpdate, message)
}

func (s *ReplicatedStore) Update(ctx context.Context, message Message) error {
	if s.partitionRouter != nil && message.Queue != "" {
		assigned, err := s.partitionRouter.hasResourcePartitions(ctx, cluster.PartitionResourceQueue, message.Queue)
		if err != nil {
			return err
		}
		if assigned {
			return fmt.Errorf("%w: updates on partitioned queue %q require a partition-specific operation", ErrUnsupportedMode, message.Queue)
		}
	}
	return s.submit(ctx, cluster.BrokerCmdUpdate, message)
}

func (s *ReplicatedStore) UpdateBatch(ctx context.Context, messages []Message) []error {
	errs := make([]error, len(messages))
	if len(messages) == 0 {
		return errs
	}
	if err := s.submit(ctx, cluster.BrokerCmdUpdateBatch, messages); err != nil {
		for i := range errs {
			errs[i] = err
		}
	}
	return errs
}

func (s *ReplicatedStore) SweepLeases(ctx context.Context, now time.Time) (int, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if s.raftCtrl == nil || !s.raftCtrl.IsLeader() {
		return 0, nil
	}
	preparer, ok := s.Store.(interface {
		prepareSweepLeases(context.Context, time.Time) ([]Message, error)
	})
	if !ok {
		return 0, fmt.Errorf("%w: backing store cannot prepare replicated lease expiry", ErrUnsupportedMode)
	}
	messages, err := preparer.prepareSweepLeases(ctx, now)
	if err != nil || len(messages) == 0 {
		return 0, err
	}
	if err := s.submitUnlocked(ctx, cluster.BrokerCmdUpdateBatch, messages); err != nil {
		return 0, err
	}
	return len(messages), nil
}

func (s *ReplicatedStore) Move(ctx context.Context, req MoveRequest) (MoveResult, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if s.raftCtrl == nil || !s.raftCtrl.IsLeader() {
		return MoveResult{}, cluster.ErrNotLeader
	}
	preparer, ok := s.Store.(interface {
		prepareMove(context.Context, MoveRequest) (map[string]string, MoveResult, error)
	})
	if !ok {
		return MoveResult{}, fmt.Errorf("%w: backing store cannot prepare a deterministic replicated move", ErrUnsupportedMode)
	}
	planned, result, err := preparer.prepareMove(ctx, req)
	if err != nil || req.DryRun || len(result.MessageIDs) == 0 {
		return result, err
	}
	if err := s.submitUnlocked(ctx, cluster.BrokerCmdMove, replicatedMove{OperationID: newIDFromTime(), Request: req, PlannedIDs: planned}); err != nil {
		return MoveResult{}, err
	}
	return result, nil
}

func (s *ReplicatedStore) CommitOffset(ctx context.Context, commit OffsetCommit) error {
	if s.partitionRouter != nil && commit.Partitioned {
		return s.partitionRouter.commitPartitionStreamOffset(ctx, commit)
	}
	persisted, err := durableOffsetCommit(ctx, commit)
	if err != nil {
		return err
	}
	return s.submit(ctx, cluster.BrokerCmdCommitOffset, replicatedOffsetCommit{Version: 2, Commit: persisted})
}

func (s *ReplicatedStore) CommitOffsetAndAppend(ctx context.Context, commit OffsetCommit, entries []AppendEntry) ([]Message, error) {
	if s.partitionRouter != nil && commit.Partitioned {
		return s.partitionRouter.commitPartitionStreamOffsetAndAppend(ctx, commit, entries)
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if s.raftCtrl == nil || !s.raftCtrl.IsLeader() {
		return nil, cluster.ErrNotLeader
	}
	commit, entries, fingerprint, err := prepareOffsetAppendRequest(ctx, commit, entries)
	if err != nil {
		return nil, err
	}
	if _, err := s.Store.ConsumerOffset(ctx, commit); err != nil {
		return nil, err
	}
	if commit.TransactionID != "" {
		lookup, ok := s.Store.(interface {
			lookupOffsetAppendTransaction(OffsetCommit, string) (OffsetAppendTransaction, bool, error)
		})
		if !ok {
			return nil, fmt.Errorf("%w: backing store cannot check offset append transaction identity", ErrUnsupportedMode)
		}
		if transaction, found, err := lookup.lookupOffsetAppendTransaction(commit, fingerprint); err != nil {
			return nil, err
		} else if found {
			return transaction.Messages, nil
		}
	}
	persistedOffset, err := durableOffsetCommit(ctx, commit)
	if err != nil {
		return nil, err
	}
	preparer, ok := s.Store.(interface {
		prepareAppendAtomic(context.Context, []AppendEntry) ([]Message, error)
	})
	if !ok {
		return nil, fmt.Errorf("%w: backing store cannot prepare a replicated offset append", ErrUnsupportedMode)
	}
	prepared, err := preparer.prepareAppendAtomic(ctx, entries)
	if err != nil {
		return nil, err
	}
	transaction := OffsetAppendTransaction{}
	if fingerprint != "" {
		transaction = OffsetAppendTransaction{Fingerprint: fingerprint, Messages: compactOffsetAppendMessages(prepared), CreatedAt: time.Now()}
	}
	payload := replicatedOffsetAppend{Offset: persistedOffset, Entries: storedAppendEntries(prepared), Transaction: transaction}
	if err := s.submitUnlocked(ctx, cluster.BrokerCmdCommitOffsetAppend, payload); err != nil {
		return nil, err
	}
	return prepared, nil
}

type replicatedOffsetCommit struct {
	Version int          `json:"version"`
	Commit  OffsetCommit `json:"commit"`
}

type replicatedOffsetAppend struct {
	Offset      OffsetCommit            `json:"offset"`
	Entries     []AppendEntry           `json:"entries"`
	Transaction OffsetAppendTransaction `json:"transaction,omitempty"`
	// CoordinatorID binds a partition checkpoint participant to its durable
	// distributed transaction while Offset.TransactionID remains the caller's
	// idempotency key for retry lookup.
	CoordinatorID string `json:"coordinator_id,omitempty"`
}

func (s *ReplicatedStore) Ack(ctx context.Context, req AckRequest) error {
	if s.partitionRouter != nil && req.Partitioned {
		return s.partitionRouter.AckPartitionQueue(ctx, req)
	}
	if s.partitionRouter != nil && req.Queue != "" {
		assigned, err := s.partitionRouter.hasResourcePartitions(ctx, cluster.PartitionResourceQueue, req.Queue)
		if err != nil {
			return err
		}
		if assigned {
			return fmt.Errorf("%w: queue %q requires a partition-specific acknowledgement", ErrUnsupportedMode, req.Queue)
		}
	}
	cmd := cluster.AckCommand{
		MessageID:  req.MessageID,
		Queue:      req.Queue,
		LeaseToken: req.LeaseToken,
		ConsumerID: req.ConsumerID,
	}
	return s.submit(ctx, cluster.BrokerCmdAck, cmd)
}

func (s *ReplicatedStore) Nack(ctx context.Context, req NackRequest) error {
	if s.partitionRouter != nil && req.Partitioned {
		return s.partitionRouter.NackPartitionQueue(ctx, req)
	}
	if s.partitionRouter != nil && req.Queue != "" {
		assigned, err := s.partitionRouter.hasResourcePartitions(ctx, cluster.PartitionResourceQueue, req.Queue)
		if err != nil {
			return err
		}
		if assigned {
			return fmt.Errorf("%w: queue %q requires a partition-specific negative acknowledgement", ErrUnsupportedMode, req.Queue)
		}
	}
	cmd := cluster.NackCommand{
		MessageID:        req.MessageID,
		Queue:            req.Queue,
		LeaseToken:       req.LeaseToken,
		ConsumerID:       req.ConsumerID,
		Action:           string(req.Action),
		Reason:           req.Reason,
		Error:            req.Error,
		Delay:            req.Delay,
		DLQ:              req.DLQ,
		ResetLease:       req.ResetLease,
		IncrementAttempt: req.IncrementAttempt,
	}
	return s.submit(ctx, cluster.BrokerCmdNack, cmd)
}

func (s *ReplicatedStore) PauseQueue(ctx context.Context, name string) error {
	return s.submit(ctx, cluster.BrokerCmdPauseQueue, name)
}

func (s *ReplicatedStore) ResumeQueue(ctx context.Context, name string) error {
	return s.submit(ctx, cluster.BrokerCmdResumeQueue, name)
}

func (s *ReplicatedStore) PurgeQueue(ctx context.Context, name string) (int, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	before, listErr := s.Store.ListQueue(ctx, name, 0)
	if listErr != nil {
		return 0, listErr
	}
	cmd, err := cluster.MakeBrokerCommand(cluster.BrokerCmdPurgeQueue, name)
	if err != nil {
		return 0, err
	}
	data, err := cluster.EncodeBrokerCommand(cmd)
	if err != nil {
		return 0, err
	}
	if err := s.proposeAndCommit(ctx, data); err != nil {
		return 0, err
	}
	return len(before), nil
}

type brokerStoreAdapter struct {
	store     Store
	txMu      sync.Mutex
	txStaged  map[string]stagedBrokerTransaction
	txDone    map[string]string
	txResults map[string][]Message
	txPurged  map[string]int
}

const brokerConsensusSnapshotFormat = "broker-consensus-v3"
const brokerConsensusSnapshotPreviousFormat = "broker-consensus-v2"

type stagedBrokerTransaction struct {
	TransactionID      string                    `json:"transaction_id"`
	ParticipantID      string                    `json:"participant_id"`
	Operation          cluster.BrokerCommandType `json:"operation"`
	RequestFingerprint string                    `json:"request_fingerprint,omitempty"`
	Rejection          string                    `json:"rejection,omitempty"`
	Payload            json.RawMessage           `json:"payload"`
}

type brokerTransactionCommand struct {
	TransactionID      string                    `json:"transaction_id"`
	ParticipantID      string                    `json:"participant_id"`
	Operation          cluster.BrokerCommandType `json:"operation,omitempty"`
	RequestFingerprint string                    `json:"request_fingerprint,omitempty"`
	Payload            json.RawMessage           `json:"payload,omitempty"`
	Decision           string                    `json:"decision,omitempty"`
	ResultMessages     []Message                 `json:"result_messages,omitempty"`
}

type brokerConsensusSnapshot struct {
	Format   string                             `json:"format"`
	Store    MemoryStoreSnapshot                `json:"store"`
	Staged   map[string]stagedBrokerTransaction `json:"staged_transactions"`
	Resolved map[string]string                  `json:"resolved_transactions"`
	Results  map[string][]Message               `json:"transaction_results,omitempty"`
	Purged   map[string]int                     `json:"transaction_purge_results,omitempty"`
}

func brokerTransactionKey(txID, participantID string) string {
	return fmt.Sprintf("%d:%s%s", len(txID), txID, participantID)
}

func transactionPayloadFingerprint(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func validTransactionPayloadFingerprint(fingerprint string) bool {
	if len(fingerprint) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(fingerprint)
	return err == nil
}

func (a *brokerStoreAdapter) lookupStagedTransaction(txID, participantID string) (stagedBrokerTransaction, bool) {
	key := brokerTransactionKey(txID, participantID)
	a.txMu.Lock()
	defer a.txMu.Unlock()
	staged, ok := a.txStaged[key]
	if ok {
		staged.Payload = append(json.RawMessage(nil), staged.Payload...)
	}
	return staged, ok
}

func (a *brokerStoreAdapter) lookupTransactionResult(txID, participantID string) ([]Message, bool) {
	key := brokerTransactionKey(txID, participantID)
	a.txMu.Lock()
	defer a.txMu.Unlock()
	if a.txDone[key] != cluster.TransactionCommit {
		return nil, false
	}
	messages, ok := a.txResults[key]
	if !ok {
		return nil, false
	}
	return cloneTransactionMessages(messages), true
}

func (a *brokerStoreAdapter) lookupTransactionPurgeResult(txID, participantID string) (int, bool) {
	key := brokerTransactionKey(txID, participantID)
	a.txMu.Lock()
	defer a.txMu.Unlock()
	if a.txDone[key] != cluster.TransactionCommit {
		return 0, false
	}
	count, ok := a.txPurged[key]
	return count, ok
}

func (db *DistributedBroker) lookupPartitionStagedTransaction(ctx context.Context, id cluster.PartitionGroupID, txID, participantID string) (stagedBrokerTransaction, bool, error) {
	id = id.Canonical()
	route, err := db.PartitionResourceRoute(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil {
		return stagedBrokerTransaction{}, false, err
	}
	var staged stagedBrokerTransaction
	var found bool
	if !route.LocalLeader {
		staged, found, err = db.forwardPartitionStagedLookup(ctx, id, txID, participantID, route.LeaderClientAddress)
	} else {
		staged, found, err = db.lookupLocalPartitionStagedTransaction(ctx, id, txID, participantID)
	}
	if err != nil || !found {
		return staged, found, err
	}
	if err := validateStagedTransactionLookup(id, txID, participantID, staged); err != nil {
		return stagedBrokerTransaction{}, false, err
	}
	return staged, true, nil
}

func validateStagedTransactionLookup(id cluster.PartitionGroupID, txID, participantID string, staged stagedBrokerTransaction) error {
	id = id.Canonical()
	if !validBrokerTransactionIdentity(staged.TransactionID, staged.ParticipantID) || staged.TransactionID != txID || staged.ParticipantID != participantID ||
		!json.Valid(staged.Payload) || len(staged.Payload) == 0 || len(staged.Payload) > 64<<20 ||
		(staged.RequestFingerprint != "" && !validTransactionPayloadFingerprint(staged.RequestFingerprint)) {
		return cluster.ErrInvalidTransaction
	}
	switch staged.Operation {
	case cluster.BrokerCmdAppendAtomic:
		if participantID != PartitionTransactionParticipantID(id) {
			return cluster.ErrInvalidTransaction
		}
	case cluster.BrokerCmdCommitOffset:
		if participantID != PartitionOffsetTransactionParticipantID(id) || id.Kind != cluster.PartitionResourceStream {
			return cluster.ErrInvalidTransaction
		}
	case cluster.BrokerCmdCommitOffsetAppend:
		if participantID != PartitionOffsetTransactionParticipantID(id) || id.Kind != cluster.PartitionResourceStream {
			return cluster.ErrInvalidTransaction
		}
	case cluster.BrokerCmdPauseQueue, cluster.BrokerCmdResumeQueue:
		if participantID != PartitionTransactionParticipantID(id) || id.Kind != cluster.PartitionResourceQueue {
			return cluster.ErrInvalidTransaction
		}
	case cluster.BrokerCmdPurgeQueue:
		if participantID != PartitionTransactionParticipantID(id) || id.Kind != cluster.PartitionResourceQueue {
			return cluster.ErrInvalidTransaction
		}
	default:
		return cluster.ErrInvalidTransaction
	}
	return nil
}

func (db *DistributedBroker) lookupLocalPartitionStagedTransaction(ctx context.Context, id cluster.PartitionGroupID, txID, participantID string) (stagedBrokerTransaction, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	groupID := "partition/" + id.Canonical().String()
	controller, ok := db.RaftGroup(groupID)
	if !ok || !controller.Running() || controller.FatalError() != nil {
		return stagedBrokerTransaction{}, false, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	if err := controller.ReadIndex(ctx); err != nil {
		if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
			return stagedBrokerTransaction{}, false, db.partitionGroupNotLeaderError(ctx, controller)
		}
		return stagedBrokerTransaction{}, false, err
	}
	db.startMu.Lock()
	adapter := db.partitionAdapters[groupID]
	db.startMu.Unlock()
	if adapter == nil {
		return stagedBrokerTransaction{}, false, fmt.Errorf("%w: partition %s has no managed transaction adapter", ErrBrokerUnavailable, id)
	}
	staged, ok := adapter.lookupStagedTransaction(txID, participantID)
	if ok {
		if err := validateStagedTransactionLookup(id, txID, participantID, staged); err != nil {
			return stagedBrokerTransaction{}, false, err
		}
	}
	return staged, ok, nil
}

func (db *DistributedBroker) lookupPartitionTransactionResult(ctx context.Context, id cluster.PartitionGroupID, txID, participantID string) ([]Message, bool, error) {
	id = id.Canonical()
	route, err := db.PartitionResourceRoute(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil {
		return nil, false, err
	}
	if !route.LocalLeader {
		return db.forwardPartitionResultLookup(ctx, id, txID, participantID, route.LeaderClientAddress)
	}
	return db.lookupLocalPartitionTransactionResult(ctx, id, txID, participantID)
}

func (db *DistributedBroker) lookupLocalPartitionTransactionResult(ctx context.Context, id cluster.PartitionGroupID, txID, participantID string) ([]Message, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	groupID := "partition/" + id.Canonical().String()
	controller, ok := db.RaftGroup(groupID)
	if !ok || !controller.Running() || controller.FatalError() != nil {
		return nil, false, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	if err := controller.ReadIndex(ctx); err != nil {
		if errors.Is(err, cluster.ErrNotLeader) || errors.Is(err, cluster.ErrLeaderLost) {
			return nil, false, db.partitionGroupNotLeaderError(ctx, controller)
		}
		return nil, false, err
	}
	db.startMu.Lock()
	adapter := db.partitionAdapters[groupID]
	db.startMu.Unlock()
	if adapter == nil {
		return nil, false, fmt.Errorf("%w: partition %s has no managed transaction adapter", ErrBrokerUnavailable, id)
	}
	messages, found := adapter.lookupTransactionResult(txID, participantID)
	return messages, found, nil
}

func (db *DistributedBroker) lookupPartitionTransactionPurgeResult(ctx context.Context, id cluster.PartitionGroupID, txID, participantID string) (int, bool, error) {
	id = id.Canonical()
	route, err := db.PartitionResourceRoute(ctx, id.Kind, id.Topic, id.Partition)
	if err != nil {
		return 0, false, err
	}
	if !route.LocalLeader {
		return db.forwardPartitionPurgeResultLookup(ctx, id, txID, participantID, route.LeaderClientAddress)
	}
	return db.lookupLocalPartitionTransactionPurgeResult(ctx, id, txID, participantID)
}

func (db *DistributedBroker) lookupLocalPartitionTransactionPurgeResult(ctx context.Context, id cluster.PartitionGroupID, txID, participantID string) (int, bool, error) {
	id = id.Canonical()
	groupID := "partition/" + id.String()
	controller, ok := db.RaftGroup(groupID)
	if !ok || !controller.Running() || controller.FatalError() != nil {
		return 0, false, fmt.Errorf("%w: local partition group %q is not ready", ErrBrokerUnavailable, groupID)
	}
	if err := controller.ReadIndex(ctx); err != nil {
		return 0, false, err
	}
	db.startMu.Lock()
	adapter := db.partitionAdapters[groupID]
	db.startMu.Unlock()
	if adapter == nil {
		return 0, false, fmt.Errorf("%w: partition %s has no managed transaction adapter", ErrBrokerUnavailable, id)
	}
	count, found := adapter.lookupTransactionPurgeResult(txID, participantID)
	return count, found, nil
}

func (a *brokerStoreAdapter) ReplicatePrepareTransaction(ctx context.Context, payload json.RawMessage) error {
	var command brokerTransactionCommand
	if err := json.Unmarshal(payload, &command); err != nil {
		return err
	}
	if !validBrokerTransactionIdentity(command.TransactionID, command.ParticipantID) || !json.Valid(command.Payload) || len(command.Payload) > 64<<20 ||
		(command.RequestFingerprint != "" && !validTransactionPayloadFingerprint(command.RequestFingerprint)) {
		return fmt.Errorf("invalid transaction prepare command")
	}
	switch command.Operation {
	case cluster.BrokerCmdAppendAtomic, cluster.BrokerCmdCommitOffset, cluster.BrokerCmdCommitOffsetAppend,
		cluster.BrokerCmdPauseQueue, cluster.BrokerCmdResumeQueue, cluster.BrokerCmdPurgeQueue:
	default:
		return fmt.Errorf("unsupported transaction participant operation %q", command.Operation)
	}
	key := brokerTransactionKey(command.TransactionID, command.ParticipantID)
	staged := stagedBrokerTransaction{
		TransactionID: command.TransactionID, ParticipantID: command.ParticipantID,
		Operation: command.Operation, RequestFingerprint: command.RequestFingerprint,
		Payload: append(json.RawMessage(nil), command.Payload...),
	}
	a.txMu.Lock()
	defer a.txMu.Unlock()
	if decision := a.txDone[key]; decision != "" {
		return fmt.Errorf("transaction participant %q is already resolved as %s", command.ParticipantID, decision)
	}
	if prior, ok := a.txStaged[key]; ok {
		fingerprintMismatch := prior.RequestFingerprint != staged.RequestFingerprint && (prior.RequestFingerprint != "" || staged.RequestFingerprint != "")
		legacyPayloadMismatch := prior.RequestFingerprint == "" && staged.RequestFingerprint == "" && string(prior.Payload) != string(staged.Payload)
		if prior.Operation != staged.Operation || fingerprintMismatch || legacyPayloadMismatch {
			return cluster.ErrTransactionConflict
		}
		return nil
	}
	if staged.Operation == cluster.BrokerCmdAppendAtomic {
		var entries []AppendEntry
		if err := json.Unmarshal(staged.Payload, &entries); err != nil {
			return err
		}
		if hasStreamAppendDestination(entries) {
			if _, ok := a.store.(interface {
				appendReservedAtomicCollect(string, []AppendEntry) ([]Message, error)
			}); !ok {
				staged.Rejection = "partition store cannot retain committed stream transaction results"
			}
		}
		if staged.Rejection == "" {
			if reserver, ok := a.store.(interface {
				reserveQueueCapacity(string, []AppendEntry) error
			}); ok {
				if err := reserver.reserveQueueCapacity(key, entries); err != nil {
					// Persist the refusal as a participant outcome. Returning it as an
					// apply error would make replicas disagree about committed history.
					staged.Rejection = err.Error()
				}
			} else {
				lookup, hasLookup := a.store.(QueueConfigLookup)
				for _, entry := range entries {
					destinations := entry.Destinations
					if len(destinations) == 0 && entry.Message.Topic != "" {
						destinations = []Destination{{Kind: DestinationTopic, Name: entry.Message.Topic}}
					}
					for _, destination := range destinations {
						if destination.Kind != DestinationQueue && destination.Kind != DestinationDLQ {
							continue
						}
						if !hasLookup {
							staged.Rejection = "partition store cannot reserve queue capacity"
							break
						}
						config, found, err := lookup.LookupQueueConfig(ctx, destination.Name)
						if err != nil {
							staged.Rejection = err.Error()
							break
						}
						if !found || config.MaxDepth > 0 {
							staged.Rejection = "partition store cannot reserve bounded queue capacity"
							break
						}
					}
					if staged.Rejection != "" {
						break
					}
				}
			}
		}
	}
	if a.txStaged == nil {
		a.txStaged = make(map[string]stagedBrokerTransaction)
	}
	a.txStaged[key] = staged
	return nil
}

func (a *brokerStoreAdapter) ReplicateResolveTransaction(ctx context.Context, payload json.RawMessage) error {
	var command brokerTransactionCommand
	if err := json.Unmarshal(payload, &command); err != nil {
		return err
	}
	if !validBrokerTransactionIdentity(command.TransactionID, command.ParticipantID) || (command.Decision != cluster.TransactionCommit && command.Decision != cluster.TransactionAbort) {
		return fmt.Errorf("invalid transaction resolution command")
	}
	key := brokerTransactionKey(command.TransactionID, command.ParticipantID)
	a.txMu.Lock()
	defer a.txMu.Unlock()
	if prior := a.txDone[key]; prior != "" {
		if prior != command.Decision {
			return cluster.ErrTransactionConflict
		}
		return nil
	}
	staged, ok := a.txStaged[key]
	if !ok {
		if command.Decision == cluster.TransactionAbort {
			if a.txDone == nil {
				a.txDone = make(map[string]string)
			}
			a.txDone[key] = command.Decision
			return nil
		}
		return fmt.Errorf("transaction participant %q has no staged work", command.ParticipantID)
	}
	if command.Decision == cluster.TransactionCommit {
		if staged.Rejection != "" {
			return fmt.Errorf("cannot commit rejected transaction prepare: %s", staged.Rejection)
		}
		var combined replicatedOffsetAppend
		if staged.Operation == cluster.BrokerCmdCommitOffsetAppend {
			if err := json.Unmarshal(staged.Payload, &combined); err != nil {
				return err
			}
			if command.ResultMessages == nil {
				for _, output := range combined.Transaction.Outputs {
					if output.Kind == DestinationStream {
						return cluster.ErrInvalidTransaction
					}
				}
			}
		}
		if command.ResultMessages != nil {
			if staged.Operation != cluster.BrokerCmdCommitOffsetAppend || !strings.HasPrefix(command.ParticipantID, partitionOffsetTransactionParticipantPrefix) {
				return cluster.ErrInvalidTransaction
			}
			if validateOffsetAppendResult(combined.Transaction.Messages, command.ResultMessages) != nil || len(combined.Transaction.Outputs) != len(command.ResultMessages) {
				return cluster.ErrInvalidTransaction
			}
			for i, output := range combined.Transaction.Outputs {
				actual := command.ResultMessages[i]
				if output.MessageID != actual.ID || output.Name == "" || output.Partition < 0 ||
					(output.Kind != DestinationTopic && output.Kind != DestinationStream && output.Kind != DestinationQueue) {
					return cluster.ErrInvalidTransaction
				}
				if output.Kind == DestinationStream && (actual.Stream != output.Name || actual.Partition != output.Partition) {
					return cluster.ErrInvalidTransaction
				}
			}
		}
		var committedMessages []Message
		if staged.Operation == cluster.BrokerCmdPurgeQueue {
			var queue string
			if err := json.Unmarshal(staged.Payload, &queue); err != nil || queue == "" {
				return cluster.ErrInvalidTransaction
			}
			purged, err := a.store.PurgeQueue(ctx, queue)
			if err != nil {
				return err
			}
			if a.txPurged == nil {
				a.txPurged = make(map[string]int)
			}
			a.txPurged[key] = purged
		} else {
			var err error
			committedMessages, err = a.applyStagedTransaction(ctx, staged)
			if err != nil {
				return err
			}
		}
		if command.ResultMessages != nil {
			updater, ok := a.store.(interface {
				updateOffsetAppendTransactionResult(OffsetCommit, string, []Message) error
			})
			if !ok {
				return fmt.Errorf("%w: backing store cannot update replicated offset-append results", ErrUnsupportedMode)
			}
			if err := updater.updateOffsetAppendTransactionResult(combined.Offset, combined.Transaction.Fingerprint, command.ResultMessages); err != nil {
				return err
			}
		}
		if staged.Operation == cluster.BrokerCmdAppendAtomic && len(committedMessages) > 0 {
			var entries []AppendEntry
			if err := json.Unmarshal(staged.Payload, &entries); err != nil {
				return err
			}
			if hasStreamAppendDestination(entries) {
				if len(committedMessages) != len(entries) {
					return cluster.ErrInvalidTransaction
				}
				if a.txResults == nil {
					a.txResults = make(map[string][]Message)
				}
				a.txResults[key] = compactOffsetAppendMessages(committedMessages)
			}
		}
	} else if releaser, ok := a.store.(interface{ releaseQueueCapacity(string) }); ok {
		releaser.releaseQueueCapacity(key)
	}
	delete(a.txStaged, key)
	if a.txDone == nil {
		a.txDone = make(map[string]string)
	}
	a.txDone[key] = command.Decision
	return nil
}

func (a *brokerStoreAdapter) ReplicateForgetTransaction(ctx context.Context, payload json.RawMessage) error {
	var command brokerTransactionCommand
	if err := json.Unmarshal(payload, &command); err != nil {
		return err
	}
	if !validBrokerTransactionIdentity(command.TransactionID, command.ParticipantID) {
		return fmt.Errorf("invalid transaction cleanup command")
	}
	key := brokerTransactionKey(command.TransactionID, command.ParticipantID)
	a.txMu.Lock()
	defer a.txMu.Unlock()
	if a.txDone[key] == "" {
		if _, staged := a.txStaged[key]; staged {
			return fmt.Errorf("transaction participant %q still has staged work", command.ParticipantID)
		}
		return nil
	}
	delete(a.txDone, key)
	delete(a.txResults, key)
	delete(a.txPurged, key)
	return nil
}

func validBrokerTransactionIdentity(txID, participantID string) bool {
	return txID != "" && len(txID) <= cluster.MaxTransactionIDBytes && utf8.ValidString(txID) &&
		participantID != "" && len(participantID) <= cluster.MaxTransactionParticipantIDBytes && utf8.ValidString(participantID)
}

func (a *brokerStoreAdapter) applyStagedTransaction(ctx context.Context, staged stagedBrokerTransaction) ([]Message, error) {
	switch staged.Operation {
	case cluster.BrokerCmdAppendAtomic:
		var entries []AppendEntry
		if err := json.Unmarshal(staged.Payload, &entries); err != nil {
			return nil, err
		}
		if applier, ok := a.store.(interface {
			appendReservedAtomicCollect(string, []AppendEntry) ([]Message, error)
		}); ok {
			return applier.appendReservedAtomicCollect(brokerTransactionKey(staged.TransactionID, staged.ParticipantID), entries)
		}
		return nil, a.ReplicateAppendAtomic(ctx, staged.Payload)
	case cluster.BrokerCmdCommitOffset:
		return nil, a.ReplicateCommitOffset(ctx, staged.Payload)
	case cluster.BrokerCmdCommitOffsetAppend:
		return nil, a.ReplicateCommitOffsetAndAppend(ctx, staged.Payload)
	case cluster.BrokerCmdPauseQueue:
		var queue string
		if err := json.Unmarshal(staged.Payload, &queue); err != nil {
			return nil, err
		}
		return nil, a.ReplicatePauseQueue(ctx, queue)
	case cluster.BrokerCmdPurgeQueue:
		return nil, fmt.Errorf("purge queue transaction must be applied by the resolution handler")
	case cluster.BrokerCmdResumeQueue:
		var queue string
		if err := json.Unmarshal(staged.Payload, &queue); err != nil {
			return nil, err
		}
		return nil, a.ReplicateResumeQueue(ctx, queue)
	default:
		return nil, fmt.Errorf("unsupported staged transaction operation %q", staged.Operation)
	}
}

func hasStreamAppendDestination(entries []AppendEntry) bool {
	for _, entry := range entries {
		if len(entry.Destinations) == 0 {
			if entry.Message.Stream != "" {
				return true
			}
			continue
		}
		for _, destination := range entry.Destinations {
			if destination.Kind == DestinationStream {
				return true
			}
		}
	}
	return false
}

func (a *brokerStoreAdapter) ReplicateDeclareQueue(ctx context.Context, cmd cluster.DeclareQueueCommand) error {
	return a.store.DeclareQueue(ctx, QueueConfig{
		Name:              cmd.Name,
		Durable:           cmd.Durable,
		Workers:           cmd.Workers,
		MaxInFlight:       cmd.MaxInFlight,
		MaxDepth:          cmd.MaxDepth,
		VisibilityTimeout: cmd.VisibilityTimeout,
		AckMode:           AckMode(cmd.AckMode),
		RetryPolicy: RetryPolicy{
			MaxAttempts: cmd.RetryMaxAttempts,
			Initial:     cmd.RetryInitial,
			Max:         cmd.RetryMax,
			Factor:      cmd.RetryFactor,
		},
		DLQ:          cmd.DLQ,
		Ordering:     OrderMode(cmd.Ordering),
		Backpressure: BackpressurePolicy(cmd.Backpressure),
		Strategy:     StrategyType(cmd.Strategy),
	})
}

func (a *brokerStoreAdapter) ReplicateDeclareTopic(ctx context.Context, cmd cluster.DeclareTopicCommand) error {
	return a.store.DeclareTopic(ctx, TopicConfig{
		Name:       cmd.Name,
		Partitions: cmd.Partitions,
		Retention:  cmd.Retention,
		Paused:     cmd.Paused,
	})
}

func (a *brokerStoreAdapter) ReplicateDeclareStream(ctx context.Context, cmd cluster.DeclareStreamCommand) error {
	return a.store.DeclareStream(ctx, StreamConfig{
		Name:       cmd.Name,
		Partitions: cmd.Partitions,
		Retention:  cmd.Retention,
		Compact:    cmd.Compact,
	})
}

func (a *brokerStoreAdapter) ReplicateAppend(ctx context.Context, cmd cluster.AppendCommand) error {
	// Committed entries are replayed after a controller restart. If the backing
	// store recovered the same command from its own WAL, applying it again must
	// be a no-op rather than a duplicate-producer failure or duplicate queue ID.
	if _, exists, err := a.store.Get(ctx, cmd.ID); err != nil {
		return err
	} else if exists {
		return nil
	}
	dests := make([]Destination, len(cmd.Destinations))
	for i, d := range cmd.Destinations {
		dests[i] = Destination{
			Kind: DestinationKind(d.Kind),
			Name: d.Name,
		}
	}
	_, err := a.store.Append(ctx, Message{
		ID:               cmd.ID,
		ParentID:         cmd.ParentID,
		Topic:            cmd.Topic,
		Queue:            cmd.Queue,
		Stream:           cmd.Stream,
		Partition:        cmd.Partition,
		Offset:           cmd.Offset,
		Key:              cmd.Key,
		RoutingKey:       cmd.RoutingKey,
		Type:             cmd.Type,
		ContentType:      cmd.ContentType,
		Schema:           cmd.Schema,
		SchemaVersion:    cmd.SchemaVersion,
		Payload:          cmd.Payload,
		Headers:          cmd.Headers,
		Metadata:         cmd.Metadata,
		TraceID:          cmd.TraceID,
		SpanID:           cmd.SpanID,
		CorrelationID:    cmd.CorrelationID,
		CausationID:      cmd.CausationID,
		RequestID:        cmd.RequestID,
		ReplyTo:          cmd.ReplyTo,
		ReplyGroup:       cmd.ReplyGroup,
		IdempotencyKey:   cmd.IdempotencyKey,
		DeduplicationID:  cmd.DeduplicationID,
		Checksum:         cmd.Checksum,
		Priority:         cmd.Priority,
		CreatedAt:        cmd.CreatedAt,
		AvailableAt:      cmd.AvailableAt,
		ExpiresAt:        cmd.ExpiresAt,
		Attempts:         cmd.Attempts,
		MaxAttempts:      cmd.MaxAttempts,
		Redeliveries:     cmd.Redeliveries,
		TenantID:         cmd.TenantID,
		ProducerID:       cmd.ProducerID,
		ProducerEpoch:    cmd.ProducerEpoch,
		ProducerSequence: cmd.ProducerSequence,
		ConsumerID:       cmd.ConsumerID,
		ConsumerGroup:    cmd.ConsumerGroup,
		LeaseToken:       cmd.LeaseToken,
		LeaseUntil:       cmd.LeaseUntil,
		OriginalTopic:    cmd.OriginalTopic,
		OriginalQueue:    cmd.OriginalQueue,
		OriginalStream:   cmd.OriginalStream,
		OriginalOffset:   cmd.OriginalOffset,
		Status:           MessageStatus(cmd.Status),
		LastError:        cmd.LastError,
		DeadLetterReason: cmd.DeadLetterReason,
		TaskID:           cmd.TaskID,
		TaskName:         cmd.TaskName,
	}, dests)
	return err
}

func (a *brokerStoreAdapter) ReplicateAppendAtomic(ctx context.Context, payload json.RawMessage) error {
	var entries []AppendEntry
	if err := json.Unmarshal(payload, &entries); err != nil {
		return err
	}
	store, ok := a.store.(interface {
		appendRecoveredAtomic([]AppendEntry) error
	})
	if !ok {
		return fmt.Errorf("%w: backing store cannot apply atomic append", ErrUnsupportedMode)
	}
	return store.appendRecoveredAtomic(entries)
}

func (a *brokerStoreAdapter) ReplicateAck(ctx context.Context, cmd cluster.AckCommand) error {
	return a.store.Ack(ctx, AckRequest{
		MessageID:  cmd.MessageID,
		Queue:      cmd.Queue,
		LeaseToken: cmd.LeaseToken,
		ConsumerID: cmd.ConsumerID,
	})
}

func (a *brokerStoreAdapter) ReplicateNack(ctx context.Context, cmd cluster.NackCommand) error {
	return a.store.Nack(ctx, NackRequest{
		MessageID:        cmd.MessageID,
		Queue:            cmd.Queue,
		LeaseToken:       cmd.LeaseToken,
		ConsumerID:       cmd.ConsumerID,
		Action:           NackAction(cmd.Action),
		Reason:           cmd.Reason,
		Error:            cmd.Error,
		Delay:            cmd.Delay,
		DLQ:              cmd.DLQ,
		ResetLease:       cmd.ResetLease,
		IncrementAttempt: cmd.IncrementAttempt,
	})
}

func (a *brokerStoreAdapter) ReplicateLease(ctx context.Context, payload json.RawMessage) error {
	var lease replicatedLease
	if err := json.Unmarshal(payload, &lease); err != nil {
		return err
	}
	applier, ok := a.store.(interface {
		applyLease(context.Context, []Message, LeaseRequest) error
	})
	if !ok {
		return fmt.Errorf("%w: backing store cannot apply replicated leases", ErrUnsupportedMode)
	}
	return applier.applyLease(ctx, lease.Messages, lease.Request)
}

func (a *brokerStoreAdapter) ReplicateUpdate(ctx context.Context, payload json.RawMessage) error {
	var message Message
	if err := json.Unmarshal(payload, &message); err != nil {
		return err
	}
	return a.store.Update(ctx, message)
}

func (a *brokerStoreAdapter) ReplicateUpdateBatch(ctx context.Context, payload json.RawMessage) error {
	var messages []Message
	if err := json.Unmarshal(payload, &messages); err != nil {
		return err
	}
	batch, ok := a.store.(BatchUpdateStore)
	if !ok {
		for i := range messages {
			if err := a.store.Update(ctx, messages[i]); err != nil {
				return err
			}
		}
		return nil
	}
	errs := batch.UpdateBatch(ctx, messages)
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *brokerStoreAdapter) ReplicateMove(ctx context.Context, payload json.RawMessage) error {
	var move replicatedMove
	if err := json.Unmarshal(payload, &move); err != nil {
		return err
	}
	applier, ok := a.store.(interface {
		applyMovePlan(context.Context, string, MoveRequest, map[string]string) (MoveResult, error)
	})
	if !ok {
		return fmt.Errorf("%w: backing store cannot apply a deterministic replicated move", ErrUnsupportedMode)
	}
	_, err := applier.applyMovePlan(ctx, move.OperationID, move.Request, move.PlannedIDs)
	return err
}

func (a *brokerStoreAdapter) ReplicateCommitOffset(ctx context.Context, payload json.RawMessage) error {
	var versioned replicatedOffsetCommit
	if err := json.Unmarshal(payload, &versioned); err != nil {
		return err
	}
	if versioned.Version == 2 {
		return a.store.CommitOffset(ctx, versioned.Commit)
	}
	var commit OffsetCommit
	if err := json.Unmarshal(payload, &commit); err != nil {
		return err
	}
	return a.store.CommitOffset(ctx, legacyOffsetCommit(commit))
}

func (a *brokerStoreAdapter) ReplicateCommitOffsetAndAppend(ctx context.Context, payload json.RawMessage) error {
	var command replicatedOffsetAppend
	if err := json.Unmarshal(payload, &command); err != nil {
		return err
	}
	if applier, ok := a.store.(interface {
		appendRecoveredOffsetAtomic([]AppendEntry, OffsetCommit, OffsetAppendTransaction) error
	}); ok {
		return applier.appendRecoveredOffsetAtomic(command.Entries, command.Offset, command.Transaction)
	}
	if command.Offset.TransactionID == "" {
		if applier, ok := a.store.(OffsetAppendStore); ok {
			_, err := applier.CommitOffsetAndAppend(ctx, command.Offset, command.Entries)
			return err
		}
	}
	return fmt.Errorf("%w: backing store cannot apply a replicated offset append", ErrUnsupportedMode)
}

func (a *brokerStoreAdapter) ReplicateSnapshot() ([]byte, error) {
	store, ok := a.store.(interface {
		Snapshot() MemoryStoreSnapshot
	})
	if !ok {
		return nil, fmt.Errorf("%w: backing store cannot create a consensus snapshot", ErrUnsupportedMode)
	}
	a.txMu.Lock()
	defer a.txMu.Unlock()
	staged := make(map[string]stagedBrokerTransaction, len(a.txStaged))
	for key, value := range a.txStaged {
		value.Payload = append(json.RawMessage(nil), value.Payload...)
		staged[key] = value
	}
	resolved := make(map[string]string, len(a.txDone))
	for key, value := range a.txDone {
		resolved[key] = value
	}
	results := make(map[string][]Message, len(a.txResults))
	for key, value := range a.txResults {
		results[key] = compactOffsetAppendMessages(value)
	}
	purged := make(map[string]int, len(a.txPurged))
	for key, value := range a.txPurged {
		purged[key] = value
	}
	return json.Marshal(brokerConsensusSnapshot{Format: brokerConsensusSnapshotFormat, Store: store.Snapshot(), Staged: staged, Resolved: resolved, Results: results, Purged: purged})
}

func (a *brokerStoreAdapter) ReplicateRestore(data []byte) error {
	var envelope brokerConsensusSnapshot
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode broker consensus snapshot: %w", err)
	}
	snapshot := envelope.Store
	switch envelope.Format {
	case brokerConsensusSnapshotFormat:
	case brokerConsensusSnapshotPreviousFormat:
		envelope.Results = nil
		envelope.Purged = nil
	case "":
		// Prior snapshots contained the MemoryStoreSnapshot directly.
		if err := json.Unmarshal(data, &snapshot); err != nil {
			return fmt.Errorf("decode broker consensus snapshot: %w", err)
		}
		envelope.Staged = nil
		envelope.Resolved = nil
		envelope.Results = nil
		envelope.Purged = nil
	default:
		return fmt.Errorf("unsupported broker consensus snapshot format %q", envelope.Format)
	}
	for key, staged := range envelope.Staged {
		if key != brokerTransactionKey(staged.TransactionID, staged.ParticipantID) ||
			!validBrokerTransactionIdentity(staged.TransactionID, staged.ParticipantID) ||
			!json.Valid(staged.Payload) || len(staged.Payload) > 64<<20 ||
			(staged.RequestFingerprint != "" && !validTransactionPayloadFingerprint(staged.RequestFingerprint)) ||
			(staged.Operation != cluster.BrokerCmdAppendAtomic && staged.Operation != cluster.BrokerCmdCommitOffset && staged.Operation != cluster.BrokerCmdCommitOffsetAppend &&
				staged.Operation != cluster.BrokerCmdPauseQueue && staged.Operation != cluster.BrokerCmdResumeQueue && staged.Operation != cluster.BrokerCmdPurgeQueue) {
			return fmt.Errorf("invalid staged transaction in broker consensus snapshot")
		}
	}
	for key, decision := range envelope.Resolved {
		// The key already contains a length-prefixed transaction ID; validate the
		// decision value here and trust the originating committed state-machine log.
		if key == "" || (decision != cluster.TransactionCommit && decision != cluster.TransactionAbort) {
			return fmt.Errorf("invalid resolved transaction in broker consensus snapshot")
		}
	}
	for key, messages := range envelope.Results {
		if key == "" || envelope.Resolved[key] != cluster.TransactionCommit || len(messages) == 0 {
			return fmt.Errorf("invalid committed transaction result in broker consensus snapshot")
		}
		for _, message := range messages {
			if message.ID == "" || message.Stream == "" || message.Partition < 0 {
				return fmt.Errorf("invalid committed stream result in broker consensus snapshot")
			}
		}
	}
	for key, count := range envelope.Purged {
		if key == "" || envelope.Resolved[key] != cluster.TransactionCommit || count < 0 {
			return fmt.Errorf("invalid committed queue purge result in broker consensus snapshot")
		}
	}
	store, ok := a.store.(interface {
		RestoreExact(MemoryStoreSnapshot)
	})
	if !ok {
		return fmt.Errorf("%w: backing store cannot restore a consensus snapshot", ErrUnsupportedMode)
	}
	store.RestoreExact(snapshot)
	reservations := make(map[string][]AppendEntry)
	for key, staged := range envelope.Staged {
		if staged.Operation != cluster.BrokerCmdAppendAtomic || staged.Rejection != "" {
			continue
		}
		var entries []AppendEntry
		if err := json.Unmarshal(staged.Payload, &entries); err != nil {
			return fmt.Errorf("decode staged append in broker consensus snapshot: %w", err)
		}
		reservations[key] = entries
	}
	if reserver, ok := a.store.(interface {
		restoreQueueCapacityReservations(map[string][]AppendEntry) error
	}); ok {
		if err := reserver.restoreQueueCapacityReservations(reservations); err != nil {
			return fmt.Errorf("restore staged queue capacity reservations: %w", err)
		}
	}
	a.txMu.Lock()
	a.txStaged = envelope.Staged
	a.txDone = envelope.Resolved
	a.txResults = envelope.Results
	a.txPurged = envelope.Purged
	a.txMu.Unlock()
	return nil
}

func (a *brokerStoreAdapter) ReplicatePauseQueue(ctx context.Context, name string) error {
	return a.store.PauseQueue(ctx, name)
}

func (a *brokerStoreAdapter) ReplicateResumeQueue(ctx context.Context, name string) error {
	return a.store.ResumeQueue(ctx, name)
}

func (a *brokerStoreAdapter) ReplicatePurgeQueue(ctx context.Context, name string) (int, error) {
	return a.store.PurgeQueue(ctx, name)
}

func mustMarshal(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return json.RawMessage(data)
}
