package broker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/broker/cluster"
)

var (
	ErrCircuitOpen = errors.New("cluster: circuit breaker open, write rejected")
)

type DistributedConfig struct {
	Peers      []string
	ListenAddr string
	WALDir     string
	TLSConfig  *tls.Config
	Cluster    *cluster.Cluster
	// AllowExperimentalCustomRaft explicitly enables the legacy custom
	// consensus path. It is unsafe for production and will be removed when the
	// etcd/raft controller and partition groups replace it.
	AllowExperimentalCustomRaft bool

	// UseEtcdRaft enables the audited production etcd/raft consensus engine.
	UseEtcdRaft            bool
	RaftNodeID             uint64
	RaftPeers              map[uint64]string
	ReplicationFactor      int
	ControllerAddrs        []string
	SnapshotEntries        uint64
	SnapshotCatchUpEntries uint64
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
	distCfg      DistributedConfig
	raft         *cluster.Raft
	raftCtrl     *cluster.RaftController
	wal          *cluster.RaftWAL
	transport    *cluster.TCPTransport
	startMu      sync.Mutex
	started      bool
	cb           *circuitBreaker
	lastSubmitAt atomic.Value // time.Time
	pending      atomic.Int64
	startTime    time.Time
}

func NewDistributed(cfg Config, distCfg DistributedConfig) (*DistributedBroker, error) {
	if !distCfg.UseEtcdRaft && !distCfg.AllowExperimentalCustomRaft {
		return nil, fmt.Errorf("%w: distributed mode requires either UseEtcdRaft: true (production etcd/raft consensus) or AllowExperimentalCustomRaft: true", ErrUnsupportedMode)
	}
	if distCfg.UseEtcdRaft && distCfg.WALDir == "" {
		return nil, fmt.Errorf("%w: etcd/raft distributed mode requires WALDir for durable consensus state", ErrUnsupportedMode)
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
	sm := cluster.NewBrokerStateMachine(storeAdapter)

	var (
		raft     *cluster.Raft
		raftCtrl *cluster.RaftController
	)

	if distCfg.UseEtcdRaft {
		nodeID := distCfg.RaftNodeID
		if nodeID == 0 {
			nodeID = 1
		}
		peers := distCfg.RaftPeers
		if len(peers) == 0 {
			peers = map[uint64]string{nodeID: cfg.NodeID}
		}
		ctrl, err := cluster.NewRaftController(cluster.RaftControllerConfig{
			ID:                     nodeID,
			NodeID:                 cfg.NodeID,
			Peers:                  peers,
			ElectionTick:           10,
			HeartbeatTick:          1,
			TickInterval:           50 * time.Millisecond,
			Transport:              transport,
			StateMachine:           sm,
			StorageDir:             distCfg.WALDir,
			SkipBootstrap:          distCfg.JoinExisting,
			SnapshotEntries:        distCfg.SnapshotEntries,
			SnapshotCatchUpEntries: distCfg.SnapshotCatchUpEntries,
		})
		if err != nil {
			return nil, fmt.Errorf("etcd raft controller: %w", err)
		}
		raftCtrl = ctrl
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
		Broker:    b,
		distCfg:   distCfg,
		raft:      raft,
		raftCtrl:  raftCtrl,
		wal:       wal,
		transport: transport,
		cb:        newCircuitBreaker(5, 10*time.Second),
	}

	db.lastSubmitAt.Store(time.Time{})

	_ = sm
	return db, nil
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
	if db.started {
		return fmt.Errorf("already started")
	}

	db.startTime = time.Now()

	// Accept peer traffic before starting elections so a node cannot campaign
	// while its transport is still unreachable.
	if db.distCfg.ListenAddr != "" {
		if err := db.transport.Listen(db.distCfg.ListenAddr, db.handleMessage); err != nil {
			return fmt.Errorf("transport: %w", err)
		}
	}

	if db.raftCtrl != nil {
		if err := db.raftCtrl.Start(); err != nil {
			_ = db.transport.Close()
			return fmt.Errorf("etcd raft: %w", err)
		}
		if len(db.distCfg.RaftPeers) <= 1 {
			_ = db.raftCtrl.Campaign()
		}
	} else if db.raft != nil {
		if err := db.raft.Start(); err != nil {
			_ = db.transport.Close()
			return fmt.Errorf("raft: %w", err)
		}
	}

	if err := db.Broker.Start(ctx); err != nil {
		if db.raftCtrl != nil {
			db.raftCtrl.Stop()
		}
		if db.raft != nil {
			db.raft.Stop()
		}
		return fmt.Errorf("broker: %w", err)
	}

	db.started = true
	return nil
}

func (db *DistributedBroker) Stop(ctx context.Context) error {
	db.startMu.Lock()
	defer db.startMu.Unlock()
	if !db.started {
		return nil
	}

	var stopErr error
	// Stop consensus before closing its transport. Otherwise the active Raft
	// loop can race transport shutdown by dialing a peer while Close is waiting
	// for connection goroutines to exit.
	if db.raftCtrl != nil {
		stopErr = errors.Join(stopErr, db.raftCtrl.Stop())
	}
	if db.raft != nil {
		db.raft.Stop()
	}
	if db.transport != nil {
		stopErr = errors.Join(stopErr, db.transport.Close())
	}
	stopErr = errors.Join(stopErr, db.Broker.Stop(ctx))
	if db.wal != nil {
		stopErr = errors.Join(stopErr, db.wal.Close())
	}

	db.started = false
	return stopErr
}

func (db *DistributedBroker) handleMessage(from string, msg *cluster.RaftMessage) {
	if db.raftCtrl != nil {
		_ = db.raftCtrl.HandleMessage(from, msg)
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
	return fmt.Errorf("%w; leader is at %s", cluster.ErrNotLeader, leader)
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
		ctx, cancel := context.WithTimeout(context.Background(), db.distCfg.SubmitTimeout)
		defer cancel()
		_, err := db.AddRaftNode(ctx, addr)
		return err
	}
	if db.raft != nil {
		return db.raft.AddPeer(addr)
	}
	return fmt.Errorf("%w: no Raft engine is running", ErrClosed)
}

// AddRaftNode commits a voting-member addition and returns the ID that the
// joining process must use with JoinExisting enabled.
func (db *DistributedBroker) AddRaftNode(ctx context.Context, addr string) (uint64, error) {
	if db.raftCtrl == nil {
		return 0, fmt.Errorf("%w: etcd/raft controller is not enabled", ErrUnsupportedMode)
	}
	return db.raftCtrl.AddPeer(ctx, addr)
}

func (db *DistributedBroker) RemoveNode(id string) error {
	if db.raftCtrl != nil {
		peerID, ok := db.raftCtrl.PeerID(id)
		if !ok {
			return cluster.ErrNodeNotFound
		}
		ctx, cancel := context.WithTimeout(context.Background(), db.distCfg.SubmitTimeout)
		defer cancel()
		return db.raftCtrl.RemovePeer(ctx, peerID)
	}
	if db.raft != nil {
		return db.raft.RemovePeer(id)
	}
	return fmt.Errorf("%w: no Raft engine is running", ErrClosed)
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

type ReplicatedStore struct {
	Store
	raft            *cluster.Raft
	raftCtrl        *cluster.RaftController
	commitTimeout   time.Duration
	forwardTimeout  time.Duration
	cb              *circuitBreaker
	mutationMu      sync.Mutex
	allowStaleReads bool
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
	if err := s.checkReadable(); err != nil {
		return nil, err
	}
	return s.Store.ListQueue(ctx, name, limit)
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
	if err := s.checkReadable(); err != nil {
		return 0, err
	}
	return s.Store.ConsumerOffset(ctx, commit)
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
				return fmt.Errorf("%w; leader is node %d", cluster.ErrNotLeader, leadID)
			}
			return cluster.ErrNotLeader
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
			return fmt.Errorf("%w; leader is at %s", cluster.ErrNotLeader, leader)
		}
		return cluster.ErrNotLeader
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
	persisted, err := durableOffsetCommit(ctx, commit)
	if err != nil {
		return err
	}
	return s.submit(ctx, cluster.BrokerCmdCommitOffset, persisted)
}

func (s *ReplicatedStore) Ack(ctx context.Context, req AckRequest) error {
	cmd := cluster.AckCommand{
		MessageID:  req.MessageID,
		Queue:      req.Queue,
		LeaseToken: req.LeaseToken,
		ConsumerID: req.ConsumerID,
	}
	return s.submit(ctx, cluster.BrokerCmdAck, cmd)
}

func (s *ReplicatedStore) Nack(ctx context.Context, req NackRequest) error {
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
	store Store
}

func (a *brokerStoreAdapter) ReplicateDeclareQueue(ctx context.Context, cmd cluster.DeclareQueueCommand) error {
	return a.store.DeclareQueue(ctx, QueueConfig{
		Name:              cmd.Name,
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
	var commit OffsetCommit
	if err := json.Unmarshal(payload, &commit); err != nil {
		return err
	}
	return a.store.CommitOffset(ctx, commit)
}

func (a *brokerStoreAdapter) ReplicateSnapshot() ([]byte, error) {
	store, ok := a.store.(interface {
		Snapshot() MemoryStoreSnapshot
	})
	if !ok {
		return nil, fmt.Errorf("%w: backing store cannot create a consensus snapshot", ErrUnsupportedMode)
	}
	return json.Marshal(store.Snapshot())
}

func (a *brokerStoreAdapter) ReplicateRestore(data []byte) error {
	var snapshot MemoryStoreSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode broker consensus snapshot: %w", err)
	}
	store, ok := a.store.(interface {
		RestoreExact(MemoryStoreSnapshot)
	})
	if !ok {
		return fmt.Errorf("%w: backing store cannot restore a consensus snapshot", ErrUnsupportedMode)
	}
	store.RestoreExact(snapshot)
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
