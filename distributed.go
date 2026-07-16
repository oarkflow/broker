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

	SubmitTimeout  time.Duration
	ForwardTimeout time.Duration

	OnLeaderChange func(leader string)
	OnStateChange  func(state string)
	OnNodeJoin     func(nodeID string)
	OnNodeLeave    func(nodeID string)
}

type circuitBreaker struct {
	mu                 sync.Mutex
	consecutiveFails   int
	state              string // "closed", "open", "half-open"
	trippedAt          time.Time
	threshold          int
	tripDuration       time.Duration
	cooldownExpiry     func() bool
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
	b, err := New(cfg)
	if err != nil {
		return nil, err
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
	if distCfg.WALDir != "" {
		wal, err = cluster.NewRaftWAL(cluster.WALConfig{Directory: distCfg.WALDir})
		if err != nil {
			return nil, fmt.Errorf("raft wal: %w", err)
		}
	}

	storeAdapter := &brokerStoreAdapter{store: b.store}
	sm := cluster.NewBrokerStateMachine(storeAdapter)

	raftCfg := cluster.RaftConfig{
		ID:               cfg.NodeID,
		Peers:            distCfg.Peers,
		Transport:        transport,
		StateMachine:     sm,
		WAL:              wal,
		OnLeaderChange:   distCfg.OnLeaderChange,
		OnStateChange:    wrapStateCallback(distCfg.OnStateChange),
		OnConfigChange:   nil,
	}
	raft := cluster.NewRaft(raftCfg)

	origStore := b.store
	b.store = &ReplicatedStore{
		Store:         origStore,
		raft:          raft,
		commitTimeout: distCfg.SubmitTimeout,
		forwardTimeout: distCfg.ForwardTimeout,
		cb:            newCircuitBreaker(5, 10*time.Second),
	}

	db := &DistributedBroker{
		Broker:   b,
		distCfg:  distCfg,
		raft:     raft,
		wal:      wal,
		transport: transport,
		cb:       newCircuitBreaker(5, 10*time.Second),
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

	if err := db.raft.Start(); err != nil {
		return fmt.Errorf("raft: %w", err)
	}

	if err := db.transport.Listen(db.distCfg.ListenAddr, db.handleMessage); err != nil {
		db.raft.Stop()
		return fmt.Errorf("transport: %w", err)
	}

	if err := db.Broker.Start(ctx); err != nil {
		db.raft.Stop()
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

	db.Broker.Stop(ctx)

	if db.transport != nil {
		db.transport.Close()
	}
	if db.wal != nil {
		db.wal.Close()
	}

	db.started = false
	return nil
}

func (db *DistributedBroker) handleMessage(from string, msg *cluster.RaftMessage) {
	db.raft.HandleMessage(from, msg)
}

func (db *DistributedBroker) forwardToLeader() error {
	leader := db.raft.Leader()
	if leader == "" {
		return fmt.Errorf("cluster: no leader known; cannot forward request")
	}
	if db.raft.IsLeader() {
		return nil
	}
	return fmt.Errorf("%w; leader is at %s", cluster.ErrNotLeader, leader)
}

func (db *DistributedBroker) DegradedMode() bool {
	return !db.raft.IsLeader() && db.raft.Leader() != "" && db.CanServeReads()
}

func (db *DistributedBroker) CanServeReads() bool {
	state := db.raft.State()
	return state == cluster.StateFollower || state == cluster.StateLeader
}

func (db *DistributedBroker) SubmitToRaft(command []byte) (uint64, error) {
	if !db.cb.allow() {
		return 0, ErrCircuitOpen
	}

	db.pending.Add(1)
	db.lastSubmitAt.Store(time.Now())

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

func (db *DistributedBroker) IsLeader() bool {
	return db.raft.IsLeader()
}

func (db *DistributedBroker) Leader() string {
	return db.raft.Leader()
}

func (db *DistributedBroker) RaftState() string {
	return db.raft.State().String()
}

func (db *DistributedBroker) AddNode(addr string) error {
	return db.raft.AddPeer(addr)
}

func (db *DistributedBroker) RemoveNode(id string) error {
	return db.raft.RemovePeer(id)
}

func (db *DistributedBroker) ClusterStats() map[string]any {
	lastSubmit, _ := db.lastSubmitAt.Load().(time.Time)

	stats := map[string]any{
		"raft_state":           db.RaftState(),
		"is_leader":            db.IsLeader(),
		"leader":               db.Leader(),
		"term":                 db.raft.CurrentTerm(),
		"commit_index":         db.raft.CommitIndex(),
		"last_applied":         db.raft.LastApplied(),
		"pending_submissions":  db.pending.Load(),
		"circuit_state":        db.cb.getState(),
		"consecutive_failures": db.cb.getConsecutiveFailures(),
		"last_submit_time":     lastSubmit,
		"uptime":               time.Since(db.startTime).String(),
	}
	if db.wal != nil {
		for k, v := range db.wal.Stats() {
			stats["wal_"+k] = v
		}
	}
	return stats
}

func (db *DistributedBroker) HealthCheck() map[string]any {
	return map[string]any{
		"leader":    db.Leader(),
		"state":     db.RaftState(),
		"is_leader": db.IsLeader(),
		"started":   db.started,
	}
}

type ReplicatedStore struct {
	Store
	raft           *cluster.Raft
	commitTimeout  time.Duration
	forwardTimeout time.Duration
	cb             *circuitBreaker
}

func (s *ReplicatedStore) submit(ctx context.Context, cmdType cluster.BrokerCommandType, payload any) error {
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
	cmd, err := cluster.MakeBrokerCommand(cmdType, payload)
	if err != nil {
		return err
	}
	data, err := cluster.EncodeBrokerCommand(cmd)
	if err != nil {
		return err
	}
	idx, err := s.raft.Submit(data)
	if err != nil {
		s.cb.recordFailure()
		return err
	}
	s.cb.recordSuccess()
	return s.waitForCommit(ctx, idx)
}

func (s *ReplicatedStore) waitForCommit(ctx context.Context, index uint64) error {
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
	if !s.raft.IsLeader() {
		leader := s.raft.Leader()
		if leader != "" {
			return nil, fmt.Errorf("%w; leader is at %s", cluster.ErrNotLeader, leader)
		}
		return nil, cluster.ErrNotLeader
	}
	if !s.cb.allow() {
		return nil, ErrCircuitOpen
	}
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
	idx, err := s.raft.Submit(data)
	if err != nil {
		s.cb.recordFailure()
		return nil, err
	}
	s.cb.recordSuccess()
	if err := s.waitForCommit(ctx, idx); err != nil {
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
	if !s.raft.IsLeader() {
		leader := s.raft.Leader()
		if leader != "" {
			return 0, fmt.Errorf("%w; leader is at %s", cluster.ErrNotLeader, leader)
		}
		return 0, cluster.ErrNotLeader
	}
	if !s.cb.allow() {
		return 0, ErrCircuitOpen
	}
	cmd, err := cluster.MakeBrokerCommand(cluster.BrokerCmdPurgeQueue, name)
	if err != nil {
		return 0, err
	}
	data, err := cluster.EncodeBrokerCommand(cmd)
	if err != nil {
		return 0, err
	}
	idx, err := s.raft.Submit(data)
	if err != nil {
		s.cb.recordFailure()
		return 0, err
	}
	s.cb.recordSuccess()
	if err := s.waitForCommit(ctx, idx); err != nil {
		return 0, err
	}
	// Query local store for the actual purge count
	n, err := s.Store.PurgeQueue(ctx, name)
	if err != nil {
		// If local purge fails (already purged by Raft), return 0
		return 0, nil
	}
	return n, nil
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
