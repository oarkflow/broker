package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	fw "github.com/oarkflow/broker/pkg/pool"
)

type Broker struct {
	cfg                   Config
	store                 Store
	stateStore            StateStore
	router                *Router
	codec                 Codec
	frameCodec            FrameCodec
	pool                  *fw.Pool
	ownPool               bool
	ctx                   context.Context
	cancel                context.CancelFunc
	state                 atomic.Value
	seq                   atomic.Uint64
	subsMu                sync.RWMutex
	subs                  map[string]*Subscription
	waitMu                sync.Mutex
	waiters               map[string]chan Response
	taskMu                sync.RWMutex
	tasks                 map[string]*TaskInfo
	taskWait              map[string][]chan TaskResult
	notify                *NotificationBus
	cluster               *Cluster
	consumers             *ConsumerRegistry
	groupRebalancer       *GroupRebalancer
	sagaEngine            *SagaEngine
	wg                    sync.WaitGroup
	startedAt             time.Time
	promMetrics           *PrometheusMetrics
	tracer                *Tracer
	auditLogger           *AuditLogger
	rateLimiter           *QueueRateLimiter
	wsServer              *WebSocketServer
	federationMgr         *FederationManager
	shovelMgr             *ShovelManager
	tenantMgr             *TenantManager
	outbox                *TransactionalOutbox
	offsetMgr             *ConsumerOffsetManager
	messageReplay         *MessageReplay
	logCompaction         *StreamLogCompaction
	partitionRouter       *PartitionRouter
	partitionOrdering     *PartitionOrdering
	partitionReassignment *PartitionReassignment
	mqttServer            *MQTTServer
	grpcServer            *GRPCServer
	healthMonitor         *StoreHealthMonitor
	producerRegistry      *ProducerRegistry
}

func New(cfg Config) (*Broker, error) {
	profile, err := DefaultsForProfile(cfg.PerformanceProfile)
	if err != nil {
		return nil, err
	}
	cfg.PerformanceProfile = profile.Profile
	if cfg.Name == "" {
		cfg.Name = "fastworker-broker"
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeEmbedded
	}
	if cfg.DefaultQueue == "" {
		cfg.DefaultQueue = "default"
	}
	if cfg.DefaultDLQ == "" {
		cfg.DefaultDLQ = cfg.DefaultQueue + ".dlq"
	}
	if cfg.DefaultVisibility <= 0 {
		cfg.DefaultVisibility = 30 * time.Second
	}
	if cfg.DefaultRetry.MaxAttempts <= 0 {
		cfg.DefaultRetry = DefaultRetryPolicy()
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = profile.PollInterval
	}
	if cfg.LeaseSweepInterval <= 0 {
		cfg.LeaseSweepInterval = time.Second
	}
	if cfg.RemoteConsumerTimeout <= 0 {
		cfg.RemoteConsumerTimeout = 3 * cfg.DefaultVisibility
		if cfg.RemoteConsumerTimeout < 30*time.Second {
			cfg.RemoteConsumerTimeout = 30 * time.Second
		}
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 15 * time.Second
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 64 << 20
	}
	if cfg.Codec == nil {
		cfg.Codec = JSONCodec{}
	}
	if cfg.FrameCodec == nil {
		cfg.FrameCodec = NewFrameCodec(FrameCodecConfig{
			Type:          cfg.FrameCodecType,
			MaxFrameSize:  cfg.MaxMessageSize + (1 << 20),
			SigningKey:    cfg.SigningKey,
			EncryptionKey: cfg.EncryptionKey,
		})
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.StateStore == nil {
		cfg.StateStore = NewMemoryStateStore()
	}
	pool := cfg.Pool
	own := cfg.OwnPool
	if pool == nil {
		pool = fw.MustNew(fw.Config{MinWorkers: 4, MaxWorkers: 128, QueueSize: 65536, EnableAutoScale: true, RecoverPanics: true, DefaultMaxAttempts: 1})
		own = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{cfg: cfg, store: cfg.Store, stateStore: cfg.StateStore, router: NewRouter(), codec: cfg.Codec, frameCodec: cfg.FrameCodec, pool: pool, ownPool: own, ctx: ctx, cancel: cancel, subs: map[string]*Subscription{}, waiters: map[string]chan Response{}, tasks: map[string]*TaskInfo{}, taskWait: map[string][]chan TaskResult{}, notify: NewNotificationBus(cfg.NotificationHistoryCapacity), consumers: NewConsumerRegistry(512), cluster: NewCluster(firstNonEmpty(cfg.NodeID, "node-1"))}
	if cfg.EnablePrometheus {
		b.promMetrics = GlobalPrometheusMetrics()
		if cfg.Hooks.OnStarted == nil {
			cfg.Hooks = mergeHooks(cfg.Hooks, b.promMetrics.HookMetrics())
		}
	}
	if cfg.EnableTracing || cfg.TracerConfig != nil {
		tcfg := TracerConfig{Enabled: true, ServiceName: cfg.Name}
		if cfg.TracerConfig != nil {
			tcfg = *cfg.TracerConfig
			tcfg.Enabled = true
		}
		b.tracer = NewTracer(tcfg)
		b.tracer.Start()
		if cfg.Hooks.OnStarted == nil {
			cfg.Hooks = mergeHooks(cfg.Hooks, b.tracer.TracingHooks())
		}
	}
	if cfg.AuditEnabled {
		b.auditLogger = NewAuditLogger(10000)
		b.auditLogger.AddSink(ConsoleAuditSink{})
		if b.notify != nil {
			b.auditLogger.AddSink(NotificationAuditSink{Bus: b.notify})
		}
	}
	if cfg.RateLimit != nil {
		b.rateLimiter = NewQueueRateLimiter(*cfg.RateLimit)
	}
	if cfg.Tenancy != nil {
		b.tenantMgr = NewTenantManager(b, *cfg.Tenancy)
	}
	if cfg.Partition != nil {
		b.partitionRouter = NewPartitionRouter(*cfg.Partition)
	} else {
		b.partitionRouter = NewPartitionRouter(PartitionConfig{})
	}
	if cfg.WebSocket != nil {
		b.wsServer = NewWebSocketServer(b, *cfg.WebSocket)
	} else {
		b.wsServer = NewWebSocketServer(b, WebSocketConfig{})
	}
	b.sagaEngine = NewSagaEngine(b)
	b.federationMgr = NewFederationManager(b)
	b.shovelMgr = NewShovelManager(b)
	b.outbox = NewTransactionalOutbox(b)
	b.offsetMgr = NewConsumerOffsetManager(b)
	b.messageReplay = NewMessageReplay(b)
	b.logCompaction = NewStreamLogCompaction(b)
	b.partitionOrdering = NewPartitionOrdering(b)
	b.partitionReassignment = NewPartitionReassignment(b)
	b.healthMonitor = NewStoreHealthMonitor(b, b.store, 250*time.Millisecond)
	b.producerRegistry = NewProducerRegistry()
	b.cfg = cfg
	b.state.Store(BrokerConfigured)
	if err := b.store.DeclareQueue(context.Background(), QueueConfig{Name: cfg.DefaultQueue, VisibilityTimeout: cfg.DefaultVisibility, RetryPolicy: cfg.DefaultRetry, DLQ: cfg.DefaultDLQ}); err != nil {
		return nil, fmt.Errorf("declare default queue: %w", err)
	}
	if err := b.store.DeclareQueue(context.Background(), QueueConfig{Name: cfg.DefaultDLQ, VisibilityTimeout: cfg.DefaultVisibility, RetryPolicy: cfg.DefaultRetry}); err != nil {
		return nil, fmt.Errorf("declare default DLQ: %w", err)
	}
	return b, nil
}

func (b *Broker) FrameCodec() FrameCodec {
	if b == nil || b.frameCodec == nil {
		return NewBinaryMessageCodec()
	}
	return b.frameCodec
}
func MustNew(cfg Config) *Broker {
	b, err := New(cfg)
	if err != nil {
		panic(err)
	}
	return b
}

func (b *Broker) Start(ctx context.Context) error {
	b.startedAt = time.Now()
	b.setState(ctx, BrokerStarting, nil)
	if b.ownPool {
		if err := b.pool.Start(); err != nil {
			b.setState(ctx, BrokerFailed, err)
			return err
		}
	}
	b.setState(ctx, BrokerReady, nil)
	b.setState(ctx, BrokerRunning, nil)
	b.wg.Add(1)
	go b.leaseSweeper()
	b.wg.Add(1)
	go b.consumerSweeper()
	b.startGroupRebalancer()
	if b.healthMonitor != nil {
		b.healthMonitor.Start(b.ctx)
	}
	return nil
}

func (b *Broker) Stop(ctx context.Context) error {
	b.setState(ctx, BrokerStopping, nil)
	b.cancel()
	if b.healthMonitor != nil {
		b.healthMonitor.Stop()
	}
	b.subsMu.RLock()
	subs := make([]*Subscription, 0, len(b.subs))
	for _, s := range b.subs {
		subs = append(subs, s)
	}
	b.subsMu.RUnlock()
	for _, s := range subs {
		s.Stop()
	}
	// Use drain timeout to prevent infinite hang on stuck handlers.
	drainTimeout := b.cfg.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 15 * time.Second
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
	defer drainCancel()
	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-drainCtx.Done():
		// Drain timeout exceeded; force shutdown.
	case <-ctx.Done():
		return ctx.Err()
	}
	// Clean up new components.
	if b.tracer != nil {
		b.tracer.Stop()
	}
	if b.rateLimiter != nil {
		b.rateLimiter.Stop()
	}
	if b.tenantMgr != nil {
		b.tenantMgr.Stop()
	}
	if b.federationMgr != nil {
		b.federationMgr.Stop()
	}
	if b.shovelMgr != nil {
		b.shovelMgr.Stop()
	}
	if b.wsServer != nil {
		b.wsServer.Stop()
	}
	// Always close all resources regardless of individual errors.
	var firstErr error
	if b.ownPool {
		if err := b.pool.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := b.store.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if b.stateStore != nil {
		if err := b.stateStore.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	b.notify.Close()
	b.setState(ctx, BrokerStopped, nil)
	return firstErr
}

func (b *Broker) Pause(ctx context.Context) error {
	b.setState(ctx, BrokerPaused, nil)
	return b.pool.Pause()
}
func (b *Broker) Resume(ctx context.Context) error {
	b.setState(ctx, BrokerRunning, nil)
	return b.pool.Resume()
}
func (b *Broker) State() BrokerState {
	if v, ok := b.state.Load().(BrokerState); ok {
		return v
	}
	return BrokerConfigured
}
func (b *Broker) Store() Store                          { return b.store }
func (b *Broker) StateStore() StateStore                { return b.stateStore }
func (b *Broker) Router() *Router                       { return b.router }
func (b *Broker) Notifications() *NotificationBus       { return b.notify }
func (b *Broker) Cluster() *Cluster                     { return b.cluster }
func (b *Broker) Publisher() *Publisher                 { return &Publisher{b: b} }
func (b *Broker) AuditLogger() *AuditLogger             { return b.auditLogger }
func (b *Broker) PrometheusMetrics() *PrometheusMetrics { return b.promMetrics }
func (b *Broker) Tracer() *Tracer                       { return b.tracer }
func (b *Broker) HealthMonitor() *StoreHealthMonitor    { return b.healthMonitor }
func (b *Broker) IsStoreFenced() bool {
	return b != nil && b.healthMonitor != nil && b.healthMonitor.IsFenced()
}

func (b *Broker) ListTasks(ctx context.Context) []TaskInfo {
	b.taskMu.RLock()
	defer b.taskMu.RUnlock()
	out := make([]TaskInfo, 0, len(b.tasks))
	for _, t := range b.tasks {
		if t == nil {
			continue
		}
		if tenantAllowed(ctx, t.TenantID) {
			out = append(out, *t)
		}
	}
	return out
}

func (b *Broker) setState(ctx context.Context, st BrokerState, err error) {
	b.state.Store(st)
	e := BrokerEvent{Broker: b.cfg.Name, State: st, Error: err, Time: time.Now()}
	switch st {
	case BrokerStarting:
		if b.cfg.Hooks.OnStarted != nil {
			b.cfg.Hooks.OnStarted(ctx, e)
		}
	case BrokerReady:
		if b.cfg.Hooks.OnReady != nil {
			b.cfg.Hooks.OnReady(ctx, e)
		}
	case BrokerPaused:
		if b.cfg.Hooks.OnPaused != nil {
			b.cfg.Hooks.OnPaused(ctx, e)
		}
	case BrokerRunning:
		if b.cfg.Hooks.OnResumed != nil {
			b.cfg.Hooks.OnResumed(ctx, e)
		}
	case BrokerStopping:
		if b.cfg.Hooks.OnDraining != nil {
			b.cfg.Hooks.OnDraining(ctx, e)
		}
	case BrokerStopped:
		if b.cfg.Hooks.OnStopped != nil {
			b.cfg.Hooks.OnStopped(ctx, e)
		}
	case BrokerFailed:
		if b.cfg.Hooks.OnFailed != nil {
			b.cfg.Hooks.OnFailed(ctx, e)
		}
	}
	if b.stateStore != nil {
		_ = b.stateStore.PutBrokerState(ctx, BrokerStateRecord{Name: b.cfg.Name, NodeID: b.cfg.NodeID, Mode: b.cfg.Mode, State: st, UpdatedAt: time.Now()})
	}
	b.notify.Publish(Notification{Type: "broker." + string(st), Source: b.cfg.Name, Severity: SeverityInfo, Timestamp: time.Now()})
}

func (b *Broker) consumerSweeper() {
	defer b.wg.Done()
	interval := b.cfg.RemoteConsumerTimeout / 3
	if interval <= 0 || interval > 10*time.Second {
		interval = 10 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			b.sweepRemoteConsumers(time.Now())
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *Broker) sweepRemoteConsumers(now time.Time) {
	if b.consumers == nil || b.cfg.RemoteConsumerTimeout <= 0 {
		return
	}
	for _, c := range b.Consumers().List() {
		if c.State == ConsumerStopped || c.State == ConsumerDisconnected || c.State == ConsumerDraining || c.State == ConsumerPaused || c.State == ConsumerBanned {
			continue
		}
		if !c.LastHeartbeat.IsZero() && now.Sub(c.LastHeartbeat) > b.cfg.RemoteConsumerTimeout {
			info, ok := b.Consumers().SetState(c.ConsumerID, ConsumerDisconnected, "heartbeat timeout")
			if !ok {
				continue
			}
			b.persistConsumer(b.ctx, info)
			if b.cfg.Hooks.OnConsumerStopped != nil {
				b.cfg.Hooks.OnConsumerStopped(b.ctx, ConsumerEvent{Consumer: c.ConsumerID, Group: c.Group, State: ConsumerDisconnected, Time: now})
			}
			b.emitConsumerNotification(c.ConsumerID, Notification{Type: "consumer.disconnected", Source: b.cfg.Name, Severity: SeverityWarn, Consumer: c.ConsumerID, ConsumerGroup: c.Group, Timestamp: now, Data: map[string]any{"last_heartbeat": info.LastHeartbeat, "reason": "heartbeat timeout"}})
		}
	}
}

func (b *Broker) leaseSweeper() {
	defer b.wg.Done()
	t := time.NewTicker(b.cfg.LeaseSweepInterval)
	defer t.Stop()
	// Run cleanup every 60 seconds to prevent memory leaks.
	cleanupTicker := time.NewTicker(60 * time.Second)
	defer cleanupTicker.Stop()
	for {
		select {
		case <-t.C:
			_, _ = b.store.SweepLeases(b.ctx, time.Now())
		case <-cleanupTicker.C:
			if ms, ok := b.store.(*FileWALStore); ok {
				ms.MemoryStore.Cleanup(100)
			} else if ms, ok := b.store.(*MemoryStore); ok {
				ms.Cleanup(100)
			}
			// Cleanup stale sagas (older than 1 hour)
			if b.sagaEngine != nil {
				b.sagaEngine.CleanupSagas(time.Hour)
			}
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *Broker) DeclareQueue(ctx context.Context, cfg QueueConfig) error {
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = b.cfg.DefaultVisibility
	}
	if cfg.RetryPolicy.MaxAttempts <= 0 {
		cfg.RetryPolicy = b.cfg.DefaultRetry
	}
	if cfg.DLQ == "" {
		cfg.DLQ = cfg.Name + ".dlq"
	}
	err := b.store.DeclareQueue(ctx, cfg)
	if err == nil && cfg.DLQ != "" {
		_ = b.store.DeclareQueue(ctx, QueueConfig{Name: cfg.DLQ, VisibilityTimeout: cfg.VisibilityTimeout, RetryPolicy: cfg.RetryPolicy})
	}
	return err
}
func (b *Broker) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	return b.store.DeclareTopic(ctx, cfg)
}
func (b *Broker) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	return b.store.DeclareStream(ctx, cfg)
}

func (b *Broker) PauseQueue(ctx context.Context, name string) error {
	err := b.store.PauseQueue(ctx, name)
	if err == nil {
		b.notify.Publish(Notification{Type: "queue.paused", Source: b.cfg.Name, Severity: SeverityInfo, Queue: name, Timestamp: time.Now()})
	}
	return err
}

func (b *Broker) ResumeQueue(ctx context.Context, name string) error {
	err := b.store.ResumeQueue(ctx, name)
	if err == nil {
		b.notify.Publish(Notification{Type: "queue.resumed", Source: b.cfg.Name, Severity: SeverityInfo, Queue: name, Timestamp: time.Now()})
	}
	return err
}

func (b *Broker) PurgeQueue(ctx context.Context, name string) (int, error) {
	n, err := b.store.PurgeQueue(ctx, name)
	if err == nil {
		b.notify.Publish(Notification{Type: "queue.purged", Source: b.cfg.Name, Severity: SeverityWarn, Queue: name, Timestamp: time.Now(), Data: map[string]any{"purged": n}})
	}
	return n, err
}

func (b *Broker) ReadStream(ctx context.Context, req StreamReadRequest) ([]Message, error) {
	return b.store.ReadStream(ctx, req)
}

func (b *Broker) CommitOffset(ctx context.Context, commit OffsetCommit) error {
	tenant, err := resolveTenant(ctx, commit.TenantID)
	if err != nil {
		return err
	}
	commit.TenantID = tenant
	err = b.store.CommitOffset(ctx, commit)
	if err == nil {
		b.notify.Publish(Notification{Type: "stream.offset_committed", Source: b.cfg.Name, Severity: SeverityInfo, Stream: commit.Stream, TenantID: tenant, Timestamp: time.Now(), Data: map[string]any{"group": commit.ConsumerGroup, "offset": commit.Offset}})
	}
	return err
}

func (b *Broker) ConsumerOffset(ctx context.Context, commit OffsetCommit) (uint64, error) {
	return b.store.ConsumerOffset(ctx, commit)
}

// StreamLag returns the consumer lag (messages behind) for a given stream and consumer group.
func (b *Broker) StreamLag(ctx context.Context, stream, consumerGroup string) (uint64, error) {
	if store, ok := b.store.(streamLagStore); ok {
		return store.StreamLagContext(ctx, stream, consumerGroup, "")
	}
	return 0, ErrStreamNotFound
}

// StreamLagAll returns consumer lag for all consumer groups on a stream.
func (b *Broker) StreamLagAll(ctx context.Context, stream string) (map[string]uint64, error) {
	if store, ok := b.store.(streamLagStore); ok {
		return store.StreamLagAllContext(ctx, stream, "")
	}
	return nil, ErrStreamNotFound
}

// StreamHeadOffset returns the latest offset for a stream.
func (b *Broker) StreamHeadOffset(ctx context.Context, stream string) (uint64, error) {
	if store, ok := b.store.(streamLagStore); ok {
		return store.StreamHeadOffsetContext(ctx, stream, "")
	}
	return 0, ErrStreamNotFound
}

type streamLagStore interface {
	StreamLagContext(context.Context, string, string, string) (uint64, error)
	StreamLagAllContext(context.Context, string, string) (map[string]uint64, error)
	StreamHeadOffsetContext(context.Context, string, string) (uint64, error)
}

// StreamConsumerLag is a result type for stream lag queries.
type StreamConsumerLag struct {
	Stream      string            `json:"stream"`
	HeadOffset  uint64            `json:"head_offset"`
	ConsumerLag map[string]uint64 `json:"consumer_lag"`
}

// StreamLagReport returns a full lag report for a stream.
func (b *Broker) StreamLagReport(ctx context.Context, stream string) (StreamConsumerLag, error) {
	head, err := b.StreamHeadOffset(ctx, stream)
	if err != nil {
		return StreamConsumerLag{}, err
	}
	lags, err := b.StreamLagAll(ctx, stream)
	if err != nil {
		return StreamConsumerLag{}, err
	}
	return StreamConsumerLag{
		Stream:      stream,
		HeadOffset:  head,
		ConsumerLag: lags,
	}, nil
}

func (b *Broker) SagaEngine() *SagaEngine             { return b.sagaEngine }
func (b *Broker) TaskRouter() *TaskRouter             { return b.router.TaskRouter() }
func (b *Broker) ConfigLoader() *RoutingConfigLoader  { return b.router.ConfigLoader() }
func (b *Broker) AddRoute(route Route)                { b.router.Add(route) }
func (b *Broker) AddRoutingPlan(plan RoutingPlan)     { b.router.AddPlan(plan) }
func (b *Broker) RemoveRoutingPlan(name string)       { b.router.RemovePlan(name) }
func (b *Broker) LoadRoutingConfig(data []byte) error { return b.router.LoadRoutingConfig(data) }

// SelectConsumers selects consumers for a message using the configured routing strategy.
func (b *Broker) SelectConsumers(ctx context.Context, msg Message, candidates []ConsumerCandidate) ([]ConsumerCandidate, error) {
	result, _, err := b.router.RouteWithStrategy(ctx, msg, candidates)
	return result, err
}

// DefaultStrategy returns the default routing strategy type.
func (b *Broker) DefaultStrategy() StrategyType {
	return b.router.ConfigLoader().GetConfig().DefaultStrategy
}

// SetDefaultStrategy sets the default routing strategy type.
func (b *Broker) SetDefaultStrategy(st StrategyType) {
	loader := b.router.ConfigLoader()
	cfg := loader.GetConfig()
	cfg.DefaultStrategy = st
	_ = loader.LoadFromMap(map[string]any{
		"default_strategy": string(st),
	})
}

func (b *Broker) nextID(prefix string) string {
	return fmt.Sprintf("%s-%d-%s", prefix, b.seq.Add(1), randomHex(6))
}
func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

// mergeHooks merges two Hooks structs, with the override taking precedence.
func mergeHooks(base, override Hooks) Hooks {
	merged := base
	if override.BeforePublish != nil {
		merged.BeforePublish = override.BeforePublish
	}
	if override.AfterPublish != nil {
		merged.AfterPublish = override.AfterPublish
	}
	if override.OnPublishRejected != nil {
		merged.OnPublishRejected = override.OnPublishRejected
	}
	if override.OnPublishConfirmed != nil {
		merged.OnPublishConfirmed = override.OnPublishConfirmed
	}
	if override.OnPublishFailed != nil {
		merged.OnPublishFailed = override.OnPublishFailed
	}
	if override.BeforeRoute != nil {
		merged.BeforeRoute = override.BeforeRoute
	}
	if override.AfterRoute != nil {
		merged.AfterRoute = override.AfterRoute
	}
	if override.OnRouteMiss != nil {
		merged.OnRouteMiss = override.OnRouteMiss
	}
	if override.OnRouteConflict != nil {
		merged.OnRouteConflict = override.OnRouteConflict
	}
	if override.OnMessageMoved != nil {
		merged.OnMessageMoved = override.OnMessageMoved
	}
	if override.OnMessageCopied != nil {
		merged.OnMessageCopied = override.OnMessageCopied
	}
	if override.OnMessageRerouted != nil {
		merged.OnMessageRerouted = override.OnMessageRerouted
	}
	if override.OnConsumerRegistered != nil {
		merged.OnConsumerRegistered = override.OnConsumerRegistered
	}
	if override.OnConsumerAssigned != nil {
		merged.OnConsumerAssigned = override.OnConsumerAssigned
	}
	if override.OnConsumerRevoked != nil {
		merged.OnConsumerRevoked = override.OnConsumerRevoked
	}
	if override.BeforeHandle != nil {
		merged.BeforeHandle = override.BeforeHandle
	}
	if override.AfterHandle != nil {
		merged.AfterHandle = override.AfterHandle
	}
	if override.OnHandlerError != nil {
		merged.OnHandlerError = override.OnHandlerError
	}
	if override.OnPanic != nil {
		merged.OnPanic = override.OnPanic
	}
	if override.OnAck != nil {
		merged.OnAck = override.OnAck
	}
	if override.OnNack != nil {
		merged.OnNack = override.OnNack
	}
	if override.OnLeaseExtended != nil {
		merged.OnLeaseExtended = override.OnLeaseExtended
	}
	if override.OnConsumerPaused != nil {
		merged.OnConsumerPaused = override.OnConsumerPaused
	}
	if override.OnConsumerResumed != nil {
		merged.OnConsumerResumed = override.OnConsumerResumed
	}
	if override.OnConsumerDraining != nil {
		merged.OnConsumerDraining = override.OnConsumerDraining
	}
	if override.OnConsumerBanned != nil {
		merged.OnConsumerBanned = override.OnConsumerBanned
	}
	if override.OnConsumerUnbanned != nil {
		merged.OnConsumerUnbanned = override.OnConsumerUnbanned
	}
	if override.OnConsumerStopped != nil {
		merged.OnConsumerStopped = override.OnConsumerStopped
	}
	if override.OnTaskCreated != nil {
		merged.OnTaskCreated = override.OnTaskCreated
	}
	if override.OnTaskScheduled != nil {
		merged.OnTaskScheduled = override.OnTaskScheduled
	}
	if override.OnTaskQueued != nil {
		merged.OnTaskQueued = override.OnTaskQueued
	}
	if override.OnTaskStarted != nil {
		merged.OnTaskStarted = override.OnTaskStarted
	}
	if override.OnTaskProgress != nil {
		merged.OnTaskProgress = override.OnTaskProgress
	}
	if override.OnTaskSucceeded != nil {
		merged.OnTaskSucceeded = override.OnTaskSucceeded
	}
	if override.OnTaskFailed != nil {
		merged.OnTaskFailed = override.OnTaskFailed
	}
	if override.OnTaskRetrying != nil {
		merged.OnTaskRetrying = override.OnTaskRetrying
	}
	if override.OnTaskCancelled != nil {
		merged.OnTaskCancelled = override.OnTaskCancelled
	}
	if override.OnTaskTimedOut != nil {
		merged.OnTaskTimedOut = override.OnTaskTimedOut
	}
	if override.OnTaskCompensating != nil {
		merged.OnTaskCompensating = override.OnTaskCompensating
	}
	if override.OnTaskCompensated != nil {
		merged.OnTaskCompensated = override.OnTaskCompensated
	}
	if override.OnTaskArchived != nil {
		merged.OnTaskArchived = override.OnTaskArchived
	}
	if override.BeforeAppend != nil {
		merged.BeforeAppend = override.BeforeAppend
	}
	if override.AfterAppend != nil {
		merged.AfterAppend = override.AfterAppend
	}
	if override.OnCommit != nil {
		merged.OnCommit = override.OnCommit
	}
	if override.OnRollback != nil {
		merged.OnRollback = override.OnRollback
	}
	if override.OnFsyncSlow != nil {
		merged.OnFsyncSlow = override.OnFsyncSlow
	}
	if override.OnCorruption != nil {
		merged.OnCorruption = override.OnCorruption
	}
	if override.OnRecoveryStart != nil {
		merged.OnRecoveryStart = override.OnRecoveryStart
	}
	if override.OnRecoveryDone != nil {
		merged.OnRecoveryDone = override.OnRecoveryDone
	}
	if override.OnRetryDecision != nil {
		merged.OnRetryDecision = override.OnRetryDecision
	}
	if override.OnRetryScheduled != nil {
		merged.OnRetryScheduled = override.OnRetryScheduled
	}
	if override.OnRetryExhausted != nil {
		merged.OnRetryExhausted = override.OnRetryExhausted
	}
	if override.OnDeadLettered != nil {
		merged.OnDeadLettered = override.OnDeadLettered
	}
	if override.OnDLQReplay != nil {
		merged.OnDLQReplay = override.OnDLQReplay
	}
	if override.OnDLQPurge != nil {
		merged.OnDLQPurge = override.OnDLQPurge
	}
	if override.OnDLQArchive != nil {
		merged.OnDLQArchive = override.OnDLQArchive
	}
	if override.OnMaxRedeliveryExceeded != nil {
		merged.OnMaxRedeliveryExceeded = override.OnMaxRedeliveryExceeded
	}
	if override.OnRateLimited != nil {
		merged.OnRateLimited = override.OnRateLimited
	}
	if override.OnCircuitOpen != nil {
		merged.OnCircuitOpen = override.OnCircuitOpen
	}
	if override.OnCircuitClosed != nil {
		merged.OnCircuitClosed = override.OnCircuitClosed
	}
	return merged
}
