package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/broker/cluster"
	fw "github.com/oarkflow/broker/pkg/pool"
)

type Broker struct {
	cfg                 Config
	lifecycleMu         sync.Mutex
	lifecycleStarting   bool
	lifecycleStarted    bool
	lifecycleStopping   bool
	lifecycleStopped    bool
	store               Store
	stateStore          StateStore
	router              *Router
	codec               Codec
	frameCodec          FrameCodec
	pool                *fw.Pool
	ownPool             bool
	ctx                 context.Context
	cancel              context.CancelFunc
	state               atomic.Value
	seq                 atomic.Uint64
	queueMaxDepth       sync.Map
	remoteRoutesMu      sync.Mutex
	remoteRoutes        map[string][]string
	subsMu              sync.RWMutex
	subs                map[string]*Subscription
	dispatchMu          sync.Mutex
	consumerStrategies  map[consumerStrategyKey]ConsumerStrategy
	dispatchAssignments map[string]dispatchAssignment
	waitMu              sync.Mutex
	waiters             map[string]chan Response
	taskMu              sync.RWMutex
	tasks               map[string]*TaskInfo
	taskWait            map[string][]chan TaskResult
	// taskCancels holds the cancel function of each task handler that is currently
	// executing. RegisterTask used to hand the subscription's context to the
	// handler, so there was nothing per-task to cancel: Cancel set the status and
	// the work carried on regardless.
	taskCancels           map[string]context.CancelFunc
	notify                *NotificationBus
	cluster               *Cluster
	consumers             *ConsumerRegistry
	groupRebalancer       *GroupRebalancer
	groupRebalancerOnce   sync.Once
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
	distributedPartitions *DistributedBroker
	partitionOrdering     *PartitionOrdering
	partitionReassignment *PartitionReassignment
	mqttServer            *MQTTServer
	grpcServer            *GRPCServer
	healthMonitor         *StoreHealthMonitor
	producerRegistry      *ProducerRegistry
	acl                   *ACL
	otlpExporter          *OTLPExporter
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
	switch cfg.Mode {
	case ModeEmbedded, ModeStandalone:
	case ModeDistributed:
		return nil, fmt.Errorf("%w: use NewDistributed for distributed mode", ErrUnsupportedMode)
	default:
		return nil, fmt.Errorf("%w: unknown broker mode %q", ErrUnsupportedMode, cfg.Mode)
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
	// Message history is written on every state transition, so it is a hot-path
	// cost: measured on the lease and ack path it is about a third of all
	// allocations and roughly a 1.4x throughput difference. MemoryStore defaults
	// to 32 entries per message, while the daemon defaults to 0 and its flag help
	// calls 0 out as disabling hot-path history. Without this an in-process
	// embedder had no way to reach the daemon's setting short of type asserting
	// the store after construction, so the embedded path was structurally slower
	// than the same broker run standalone. Left nil, the store default applies.
	if cfg.MessageHistory != nil {
		if configurable, ok := cfg.Store.(interface{ SetHistoryLimit(int) }); ok {
			configurable.SetHistoryLimit(*cfg.MessageHistory)
		}
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
	b := &Broker{cfg: cfg, store: cfg.Store, stateStore: cfg.StateStore, router: NewRouter(), codec: cfg.Codec, frameCodec: cfg.FrameCodec, pool: pool, ownPool: own, ctx: ctx, cancel: cancel, subs: map[string]*Subscription{}, consumerStrategies: map[consumerStrategyKey]ConsumerStrategy{}, dispatchAssignments: map[string]dispatchAssignment{}, waiters: map[string]chan Response{}, tasks: map[string]*TaskInfo{}, taskWait: map[string][]chan TaskResult{}, taskCancels: map[string]context.CancelFunc{}, notify: NewNotificationBus(cfg.NotificationHistoryCapacity), consumers: NewConsumerRegistry(512), cluster: NewCluster(firstNonEmpty(cfg.NodeID, "node-1"))}
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
	b.acl = cfg.ACL
	if cfg.OTLPEndpoint != "" {
		opts := []OTLPOption{
			WithOTLPServiceName(cfg.Name),
			WithOTLPHeaders(cfg.OTLPHeaders),
		}
		exp, err := NewOTLPExporter(cfg.OTLPEndpoint, cfg.OTLPInterval, b.promMetrics, opts...)
		if err == nil {
			b.otlpExporter = exp
		}
	}
	b.cfg = cfg
	b.state.Store(BrokerConfigured)
	if err := b.store.DeclareQueue(context.Background(), QueueConfig{Name: cfg.DefaultQueue, VisibilityTimeout: cfg.DefaultVisibility, RetryPolicy: cfg.DefaultRetry, DLQ: cfg.DefaultDLQ}); err != nil {
		return nil, fmt.Errorf("declare default queue: %w", err)
	}
	if err := b.store.DeclareQueue(context.Background(), QueueConfig{Name: cfg.DefaultDLQ, VisibilityTimeout: cfg.DefaultVisibility, RetryPolicy: cfg.DefaultRetry}); err != nil {
		return nil, fmt.Errorf("declare default DLQ: %w", err)
	}
	b.noteQueueDepthLimit(QueueConfig{Name: cfg.DefaultQueue, DLQ: cfg.DefaultDLQ})
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
	alreadyStarted, err := b.beginStart()
	if err != nil {
		return err
	}
	if alreadyStarted {
		return nil
	}
	startSucceeded := false
	defer func() { b.endStart(startSucceeded) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	b.startedAt = time.Now()
	b.setState(ctx, BrokerStarting, nil)
	if err := b.restorePersistedConsumers(ctx); err != nil {
		b.setState(ctx, BrokerFailed, err)
		return err
	}
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
	if b.otlpExporter != nil {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.otlpExporter.Run(b.ctx)
		}()
	}
	startSucceeded = true
	return nil
}

func (b *Broker) beginStart() (alreadyStarted bool, err error) {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
	if b.lifecycleStopped || b.lifecycleStopping || b.State() == BrokerStopping || b.State() == BrokerStopped {
		return false, ErrStopped
	}
	if b.lifecycleStarted {
		return true, nil
	}
	if b.lifecycleStarting {
		return false, ErrClosed
	}
	b.lifecycleStarting = true
	return false, nil
}

func (b *Broker) endStart(succeeded bool) {
	b.lifecycleMu.Lock()
	b.lifecycleStarting = false
	if succeeded {
		b.lifecycleStarted = true
	}
	b.lifecycleMu.Unlock()
}

func (b *Broker) beginStop() (alreadyStopped bool, err error) {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
	if b.lifecycleStopped || b.State() == BrokerStopped {
		return true, nil
	}
	if b.lifecycleStarting || b.lifecycleStopping {
		return false, ErrClosed
	}
	b.lifecycleStopping = true
	return false, nil
}

func (b *Broker) endStop(succeeded bool) {
	b.lifecycleMu.Lock()
	b.lifecycleStopping = false
	if succeeded {
		b.lifecycleStopped = true
	}
	b.lifecycleMu.Unlock()
}

func (b *Broker) restorePersistedConsumers(ctx context.Context) error {
	if b.stateStore == nil {
		return nil
	}
	consumers, err := b.stateStore.ListConsumers(ctx)
	if err != nil {
		return fmt.Errorf("restore persisted consumers: %w", err)
	}
	for _, stored := range consumers {
		if err := b.Consumers().RestoreContext(ctx, stored); err != nil {
			return fmt.Errorf("restore consumer %q: %w", stored.ConsumerID, err)
		}
		if _, err := b.Consumers().SetStateContextWithPersist(ctx, stored.ConsumerID, ConsumerDisconnected, "broker restarted", func(info ConsumerInfo) error {
			return b.persistConsumer(ctx, info)
		}); err != nil {
			return fmt.Errorf("mark restored consumer %q disconnected: %w", stored.ConsumerID, err)
		}
	}
	return nil
}

func (b *Broker) Stop(ctx context.Context) error {
	alreadyStopped, err := b.beginStop()
	if err != nil {
		return err
	}
	if alreadyStopped {
		return nil
	}
	stopSucceeded := false
	defer func() { b.endStop(stopSucceeded) }()
	b.setState(ctx, BrokerStopping, nil)
	b.cancel()
	if b.healthMonitor != nil {
		b.healthMonitor.Stop()
	}
	if b.otlpExporter != nil {
		b.otlpExporter.Stop()
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
	stopSucceeded = true
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
func (b *Broker) MaxMessageSize() int                   { return b.cfg.MaxMessageSize }
func (b *Broker) Router() *Router                       { return b.router }
func (b *Broker) Notifications() *NotificationBus       { return b.notify }
func (b *Broker) Cluster() *Cluster                     { return b.cluster }
func (b *Broker) Publisher() *Publisher                 { return &Publisher{b: b} }
func (b *Broker) AuditLogger() *AuditLogger             { return b.auditLogger }
func (b *Broker) PrometheusMetrics() *PrometheusMetrics { return b.promMetrics }
func (b *Broker) OTLPExporter() *OTLPExporter           { return b.otlpExporter }
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
			info, err := b.Consumers().SetStateContextWithPersist(b.ctx, c.ConsumerID, ConsumerDisconnected, "heartbeat timeout", func(info ConsumerInfo) error {
				return b.persistConsumer(b.ctx, info)
			})
			if err != nil {
				continue
			}
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
		if dlqErr := b.store.DeclareQueue(ctx, QueueConfig{Name: cfg.DLQ, VisibilityTimeout: cfg.VisibilityTimeout, RetryPolicy: cfg.RetryPolicy}); dlqErr != nil {
			return fmt.Errorf("declare queue dead-letter queue %q: %w", cfg.DLQ, dlqErr)
		}
	}
	if err == nil {
		b.noteQueueDepthLimit(cfg)
	}
	return err
}

// noteQueueDepthLimit records a queue's depth limit and declaration in the
// broker. Backpressure needs the limit for queues about to receive publishes,
// and tenant-scoped subscriptions need to know whether a queue already exists.
// The limit lives in each store's queue config, which is not reachable for
// every Store implementation, so this index works with all configured stores.
func (b *Broker) noteQueueDepthLimit(cfg QueueConfig) {
	if cfg.Name == "" {
		return
	}
	b.queueMaxDepth.Store(cfg.Name, cfg.MaxDepth)
	if cfg.DLQ != "" && cfg.DLQ != cfg.Name {
		b.queueMaxDepth.Store(cfg.DLQ, cfg.MaxDepth)
	}
}

func (b *Broker) queueDeclared(name string) bool {
	_, ok := b.queueMaxDepth.Load(name)
	return ok
}

// QueueMaxDepth reports the configured depth limit for a queue, or 0 when the
// queue is unlimited or undeclared.
func (b *Broker) QueueMaxDepth(name string) int {
	if v, ok := b.queueMaxDepth.Load(name); ok {
		if n, ok := v.(int); ok {
			return n
		}
	}
	return 0
}

// ListTopic returns the retained messages of a topic, oldest first.
//
// Topics had no read path before this: a message published to a topic could not be
// read back, replayed or backfilled, so topic retention grew with nothing able to
// consume it.
func (b *Broker) ListTopic(ctx context.Context, name string, limit int) ([]Message, error) {
	if b.distributedPartitions != nil {
		if messages, handled, err := b.distributedPartitions.listPartitionedTopic(ctx, name, limit); handled || err != nil {
			return messages, err
		}
	}
	return b.store.ListTopic(ctx, name, limit)
}

// ReadTopic returns retained topic messages published after afterID, so a reader can
// resume from where it stopped instead of restarting from the oldest message.
func (b *Broker) ReadTopic(ctx context.Context, name, afterID string, limit int) ([]Message, error) {
	if b.distributedPartitions != nil {
		if messages, handled, err := b.distributedPartitions.readPartitionedTopic(ctx, name, afterID, limit); handled || err != nil {
			return messages, err
		}
	}
	return b.store.ReadTopic(ctx, name, afterID, limit)
}

func (b *Broker) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	return b.store.DeclareTopic(ctx, cfg)
}
func (b *Broker) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	return b.store.DeclareStream(ctx, cfg)
}

func (b *Broker) PauseQueue(ctx context.Context, name string) error {
	if err := b.rejectPartitionedQueueOperation(ctx, name, "pause"); err != nil {
		return err
	}
	err := b.store.PauseQueue(ctx, name)
	if err == nil {
		b.notify.Publish(Notification{Type: "queue.paused", Source: b.cfg.Name, Severity: SeverityInfo, Queue: name, Timestamp: time.Now()})
	}
	return err
}

func (b *Broker) ResumeQueue(ctx context.Context, name string) error {
	if err := b.rejectPartitionedQueueOperation(ctx, name, "resume"); err != nil {
		return err
	}
	err := b.store.ResumeQueue(ctx, name)
	if err == nil {
		b.notify.Publish(Notification{Type: "queue.resumed", Source: b.cfg.Name, Severity: SeverityInfo, Queue: name, Timestamp: time.Now()})
	}
	return err
}

func (b *Broker) PurgeQueue(ctx context.Context, name string) (int, error) {
	if err := b.rejectPartitionedQueueOperation(ctx, name, "purge"); err != nil {
		return 0, err
	}
	n, err := b.store.PurgeQueue(ctx, name)
	if err == nil {
		b.notify.Publish(Notification{Type: "queue.purged", Source: b.cfg.Name, Severity: SeverityWarn, Queue: name, Timestamp: time.Now(), Data: map[string]any{"purged": n}})
	}
	return n, err
}

func (b *Broker) rejectPartitionedQueueOperation(ctx context.Context, queue, operation string) error {
	if b.distributedPartitions == nil {
		return nil
	}
	assigned, err := b.distributedPartitions.hasResourcePartitions(ctx, cluster.PartitionResourceQueue, queue)
	if err != nil {
		return err
	}
	if assigned {
		return fmt.Errorf("%w: %s on partitioned queue %q requires a partition-specific operation", ErrUnsupportedMode, operation, queue)
	}
	return nil
}

func (b *Broker) ListPartitionQueue(ctx context.Context, queue string, partition, limit int) ([]Message, error) {
	if b.distributedPartitions == nil {
		return nil, ErrUnsupportedMode
	}
	return b.distributedPartitions.ListPartitionQueue(ctx, queue, partition, limit)
}

// ListAssignedQueue returns a bounded best-effort sample across assigned
// partitions. Per-partition successes and failures are included in the result.
func (b *Broker) ListAssignedQueue(ctx context.Context, queue string, limit int) (*AssignedQueueMessages, error) {
	if b.distributedPartitions == nil {
		return nil, ErrUnsupportedMode
	}
	return b.distributedPartitions.ListAssignedQueue(ctx, queue, limit)
}

// ApplyAssignedQueueControl applies pause, resume, and purge through the
// durable multi-partition transaction coordinator.
func (b *Broker) ApplyAssignedQueueControl(ctx context.Context, queue, action string) (*QueuePartitionOperationResult, error) {
	if b.distributedPartitions == nil {
		return nil, ErrUnsupportedMode
	}
	return b.distributedPartitions.ApplyAssignedQueueControl(ctx, queue, action)
}

func (b *Broker) PausePartitionQueue(ctx context.Context, queue string, partition int) error {
	if b.distributedPartitions == nil {
		return ErrUnsupportedMode
	}
	return b.distributedPartitions.PausePartitionQueue(ctx, queue, partition)
}

func (b *Broker) ResumePartitionQueue(ctx context.Context, queue string, partition int) error {
	if b.distributedPartitions == nil {
		return ErrUnsupportedMode
	}
	return b.distributedPartitions.ResumePartitionQueue(ctx, queue, partition)
}

func (b *Broker) PurgePartitionQueue(ctx context.Context, queue string, partition int) (int, error) {
	if b.distributedPartitions == nil {
		return 0, ErrUnsupportedMode
	}
	return b.distributedPartitions.PurgePartitionQueue(ctx, queue, partition)
}

func (b *Broker) ReadStream(ctx context.Context, req StreamReadRequest) ([]Message, error) {
	if b.distributedPartitions != nil {
		if messages, handled, err := b.distributedPartitions.readPartitionedStream(ctx, req); handled || err != nil {
			return messages, err
		}
	}
	return b.store.ReadStream(ctx, req)
}

func (b *Broker) rejectPartitionedStreamOffsetOperation(ctx context.Context, stream, operation string) error {
	if b.distributedPartitions == nil {
		return nil
	}
	assigned, err := b.distributedPartitions.hasResourcePartitions(ctx, cluster.PartitionResourceStream, stream)
	if err != nil {
		return err
	}
	if assigned {
		return fmt.Errorf("%w: %s on partitioned stream %q requires per-partition offset support", ErrUnsupportedMode, operation, stream)
	}
	return nil
}

func (b *Broker) CommitOffset(ctx context.Context, commit OffsetCommit) error {
	if commit.Partitioned && commit.Partition < 0 {
		return errf(ErrInvalidMessage, "stream partition must be non-negative")
	}
	if commit.Partitioned && b.distributedPartitions == nil {
		return fmt.Errorf("%w: partition-scoped offsets require distributed stream partitions", ErrUnsupportedMode)
	}
	if !commit.Partitioned {
		if err := b.rejectPartitionedStreamOffsetOperation(ctx, commit.Stream, "offset commit"); err != nil {
			return err
		}
	}
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

// CommitOffsetAndAppend atomically checkpoints a stream consumer group and
// appends the supplied output messages. The configured Store must implement
// OffsetAppendStore; the method never falls back to two separate mutations.
// Set OffsetCommit.TransactionID to deduplicate retries. Reusing an ID with
// different input returns ErrTransactionConflict. Stores retain the most recent
// 4096 transaction outcomes; older IDs may be evicted. On ReplicatedStore, a
// caller timeout can leave the commit result unknown, so use a stable ID. A
// retry resolves full messages while they remain in the store; after message
// cleanup, it returns the retained message metadata.
func (b *Broker) CommitOffsetAndAppend(ctx context.Context, commit OffsetCommit, entries []AppendEntry) ([]Message, error) {
	if b.State() == BrokerPaused {
		return nil, ErrPaused
	}
	if commit.Partitioned && commit.Partition < 0 {
		return nil, errf(ErrInvalidMessage, "stream partition must be non-negative")
	}
	if commit.Partitioned && b.distributedPartitions == nil {
		return nil, fmt.Errorf("%w: partition-scoped offsets require distributed stream partitions", ErrUnsupportedMode)
	}
	if !commit.Partitioned {
		if err := b.rejectPartitionedStreamOffsetOperation(ctx, commit.Stream, "offset-plus-append transaction"); err != nil {
			return nil, err
		}
	}
	var distributedPlan distributedOffsetAppendPlan
	tenant, err := resolveTenant(ctx, commit.TenantID)
	if err != nil {
		return nil, err
	}
	commit.TenantID = tenant
	preparedEntries := make([]AppendEntry, len(entries))
	for i, entry := range entries {
		if entry.Message.TenantID == "" {
			entry.Message.TenantID = tenant
		}
		entryTenant, tenantErr := resolveTenant(ctx, entry.Message.TenantID)
		if tenantErr != nil {
			return nil, tenantErr
		}
		entry.Message.TenantID = entryTenant
		destinations := entry.Destinations
		if len(destinations) == 0 {
			switch {
			case entry.Message.Queue != "":
				destinations = []Destination{{Kind: DestinationQueue, Name: entry.Message.Queue}}
			case entry.Message.Stream != "":
				destinations = []Destination{{Kind: DestinationStream, Name: entry.Message.Stream}}
			case entry.Message.Topic != "":
				destinations = []Destination{{Kind: DestinationTopic, Name: entry.Message.Topic}}
			}
		}
		entry.Destinations = destinations
		preparedEntries[i] = entry
	}
	var requestFingerprint string
	commit, preparedEntries, requestFingerprint, err = prepareOffsetAppendRequest(ctx, commit, preparedEntries)
	if err != nil {
		return nil, err
	}
	if b.distributedPartitions != nil && commit.Partitioned && commit.TransactionID != "" {
		cached, found, err := b.distributedPartitions.lookupPartitionOffsetAppendOutcome(ctx, commit, requestFingerprint)
		if err != nil {
			return nil, err
		}
		if found {
			b.notify.Publish(Notification{Type: "stream.offset_committed", Source: b.cfg.Name, Severity: SeverityInfo, Stream: commit.Stream, TenantID: tenant, Timestamp: time.Now(), Data: map[string]any{"group": commit.ConsumerGroup, "offset": commit.Offset}})
			return cached.Messages, nil
		}
	}
	if b.distributedPartitions != nil {
		distributedPlan, err = b.distributedPartitions.planOffsetAppendTransaction(ctx, commit, preparedEntries)
		if err != nil {
			return nil, err
		}
		if distributedPlan.multiPartition {
			if !commit.Partitioned || commit.TransactionID == "" {
				return nil, fmt.Errorf("%w: multi-partition offset-plus-append requires a partitioned checkpoint and transaction ID", ErrInvalidMessage)
			}
		} else {
			for _, entry := range preparedEntries {
				for _, destination := range entry.Destinations {
					var kind cluster.PartitionResourceKind
					switch destination.Kind {
					case DestinationTopic:
						kind = cluster.PartitionResourceTopic
					case DestinationStream, DestinationArchive:
						kind = cluster.PartitionResourceStream
					case DestinationQueue, DestinationDLQ:
						kind = cluster.PartitionResourceQueue
					default:
						continue
					}
					assigned, err := b.distributedPartitions.hasResourcePartitions(ctx, kind, destination.Name)
					if err != nil {
						return nil, err
					}
					if assigned && !(commit.Partitioned && kind == cluster.PartitionResourceStream && destination.Name == commit.Stream) {
						return nil, fmt.Errorf("%w: offset-plus-append transaction spans an assigned partition resource", ErrUnsupportedMode)
					}
				}
			}
		}
	}
	store, ok := b.store.(OffsetAppendStore)
	if !ok {
		return nil, ErrUnsupportedMode
	}
	publisher := &Publisher{b: b}
	tenantCounts := make(map[string]int)
	tenantDestinations := make(map[string][]Destination)
	for _, entry := range preparedEntries {
		if len(entry.Message.Payload) > b.cfg.MaxMessageSize {
			return nil, ErrMessageTooLarge
		}
		if b.cfg.SchemaRegistry != nil {
			if err := b.cfg.SchemaRegistry.Validate(ctx, entry.Message); err != nil {
				return nil, err
			}
		}
		if err := publisher.checkBackpressure(ctx, preparedPublish{msg: entry.Message, dests: entry.Destinations}); err != nil {
			return nil, err
		}
		tenantCounts[entry.Message.TenantID]++
		tenantDestinations[entry.Message.TenantID] = append(tenantDestinations[entry.Message.TenantID], entry.Destinations...)
	}
	if b.tenantMgr != nil {
		for entryTenant, count := range tenantCounts {
			if err := b.tenantMgr.CheckPublishBatchQuota(ctx, entryTenant, tenantDestinations[entryTenant], count); err != nil {
				return nil, err
			}
		}
	}
	if distributedPlan.multiPartition {
		stored, err := b.distributedPartitions.commitDistributedOffsetAppend(ctx, commit, preparedEntries, distributedPlan)
		if err != nil {
			publisher.recordBackpressureError(err, uint64(len(entries)))
			return nil, err
		}
		b.notify.Publish(Notification{Type: "stream.offset_committed", Source: b.cfg.Name, Severity: SeverityInfo, Stream: commit.Stream, TenantID: tenant, Timestamp: time.Now(), Data: map[string]any{"group": commit.ConsumerGroup, "offset": commit.Offset}})
		return stored, nil
	}
	stored, err := store.CommitOffsetAndAppend(ctx, commit, preparedEntries)
	if err != nil {
		publisher.recordBackpressureError(err, uint64(len(entries)))
		return nil, err
	}
	b.notify.Publish(Notification{Type: "stream.offset_committed", Source: b.cfg.Name, Severity: SeverityInfo, Stream: commit.Stream, TenantID: tenant, Timestamp: time.Now(), Data: map[string]any{"group": commit.ConsumerGroup, "offset": commit.Offset}})
	return stored, nil
}

func (b *Broker) ConsumerOffset(ctx context.Context, commit OffsetCommit) (uint64, error) {
	if commit.Partitioned && commit.Partition < 0 {
		return 0, errf(ErrInvalidMessage, "stream partition must be non-negative")
	}
	if commit.Partitioned && b.distributedPartitions == nil {
		return 0, fmt.Errorf("%w: partition-scoped offsets require distributed stream partitions", ErrUnsupportedMode)
	}
	if !commit.Partitioned {
		if err := b.rejectPartitionedStreamOffsetOperation(ctx, commit.Stream, "consumer offset lookup"); err != nil {
			return 0, err
		}
	}
	return b.store.ConsumerOffset(ctx, commit)
}

// StreamLag returns the consumer lag (messages behind) for a given stream and consumer group.
func (b *Broker) StreamLag(ctx context.Context, stream, consumerGroup string) (uint64, error) {
	if err := b.rejectPartitionedStreamOffsetOperation(ctx, stream, "stream lag lookup"); err != nil {
		return 0, err
	}
	if store, ok := b.store.(streamLagStore); ok {
		return store.StreamLagContext(ctx, stream, consumerGroup, "")
	}
	return 0, ErrStreamNotFound
}

// StreamLagAll returns consumer lag for all consumer groups on a stream.
func (b *Broker) StreamLagAll(ctx context.Context, stream string) (map[string]uint64, error) {
	if err := b.rejectPartitionedStreamOffsetOperation(ctx, stream, "stream lag lookup"); err != nil {
		return nil, err
	}
	if store, ok := b.store.(streamLagStore); ok {
		return store.StreamLagAllContext(ctx, stream, "")
	}
	return nil, ErrStreamNotFound
}

// StreamHeadOffset returns the latest offset for a stream.
func (b *Broker) StreamHeadOffset(ctx context.Context, stream string) (uint64, error) {
	if err := b.rejectPartitionedStreamOffsetOperation(ctx, stream, "stream head offset lookup"); err != nil {
		return 0, err
	}
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

type PartitionStreamLag struct {
	Stream          string `json:"stream"`
	Partition       int    `json:"partition"`
	ConsumerGroup   string `json:"consumer_group"`
	HeadOffset      uint64 `json:"head_offset"`
	CommittedOffset uint64 `json:"committed_offset"`
	Lag             uint64 `json:"lag"`
}

func (b *Broker) PartitionStreamLag(ctx context.Context, stream string, partition int, consumerGroup string) (PartitionStreamLag, error) {
	if b.distributedPartitions == nil {
		return PartitionStreamLag{}, ErrUnsupportedMode
	}
	return b.distributedPartitions.PartitionStreamLag(ctx, stream, partition, consumerGroup)
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

func (b *Broker) SagaEngine() *SagaEngine            { return b.sagaEngine }
func (b *Broker) TaskRouter() *TaskRouter            { return b.router.TaskRouter() }
func (b *Broker) ConfigLoader() *RoutingConfigLoader { return b.router.ConfigLoader() }
func (b *Broker) AddRoute(route Route)               { b.router.Add(route) }

// RemoveRoute deletes a route by name, reporting whether one was found.
func (b *Broker) RemoveRoute(name string) bool        { return b.router.Remove(name) }
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
	// The sequence number already guarantees uniqueness; the random suffix only
	// keeps ids unguessable across restarts. math/rand/v2's global source is
	// lock-free, unlike a crypto/rand syscall per id.
	var buf [64]byte
	out := append(buf[:0], prefix...)
	out = append(out, '-')
	out = strconv.AppendUint(out, b.seq.Add(1), 10)
	out = append(out, '-')
	r := mrand.Uint64()
	const hexdigits = "0123456789abcdef"
	for i := 0; i < 12; i++ {
		out = append(out, hexdigits[r&0xf])
		r >>= 4
	}
	return string(out)
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

// ACL returns the configured ACL manager for the broker.
func (b *Broker) ACL() *ACL {
	return b.acl
}

// SetACL sets or updates the ACL manager for the broker.
func (b *Broker) SetACL(a *ACL) {
	b.acl = a
}
