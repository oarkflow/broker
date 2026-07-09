package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	fastworker "github.com/oarkflow/fastworker"
)

type Broker struct {
	cfg        Config
	store      Store
	stateStore StateStore
	router     *Router
	codec      Codec
	frameCodec *BinaryMessageCodec
	pool       *fastworker.Pool
	ownPool    bool
	ctx        context.Context
	cancel     context.CancelFunc
	state      atomic.Value
	seq        atomic.Uint64
	subsMu     sync.RWMutex
	subs       map[string]*Subscription
	waitMu     sync.Mutex
	waiters    map[string]chan Response
	taskMu     sync.RWMutex
	tasks      map[string]*TaskInfo
	taskWait   map[string][]chan TaskResult
	notify     *NotificationBus
	cluster    *Cluster
	consumers  *ConsumerRegistry
	wg         sync.WaitGroup
}

func New(cfg Config) (*Broker, error) {
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
		cfg.PollInterval = 5 * time.Millisecond
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
		cfg.FrameCodec = NewBinaryMessageCodec(WithFrameMaxSize(cfg.MaxMessageSize + (1 << 20)))
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
		pool = fastworker.MustNew(fastworker.Config{MinWorkers: 4, MaxWorkers: 128, QueueSize: 65536, EnableAutoScale: true, RecoverPanics: true, DefaultMaxAttempts: 1})
		own = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{cfg: cfg, store: cfg.Store, stateStore: cfg.StateStore, router: NewRouter(), codec: cfg.Codec, frameCodec: cfg.FrameCodec, pool: pool, ownPool: own, ctx: ctx, cancel: cancel, subs: map[string]*Subscription{}, waiters: map[string]chan Response{}, tasks: map[string]*TaskInfo{}, taskWait: map[string][]chan TaskResult{}, notify: NewNotificationBus(1024), consumers: NewConsumerRegistry(512), cluster: NewCluster(firstNonEmpty(cfg.NodeID, "node-1"))}
	b.state.Store(BrokerConfigured)
	_ = b.store.DeclareQueue(context.Background(), QueueConfig{Name: cfg.DefaultQueue, VisibilityTimeout: cfg.DefaultVisibility, RetryPolicy: cfg.DefaultRetry, DLQ: cfg.DefaultDLQ})
	_ = b.store.DeclareQueue(context.Background(), QueueConfig{Name: cfg.DefaultDLQ, VisibilityTimeout: cfg.DefaultVisibility, RetryPolicy: cfg.DefaultRetry})
	return b, nil
}

func (b *Broker) FrameCodec() *BinaryMessageCodec {
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
	return nil
}

func (b *Broker) Stop(ctx context.Context) error {
	b.setState(ctx, BrokerStopping, nil)
	b.cancel()
	b.subsMu.RLock()
	subs := make([]*Subscription, 0, len(b.subs))
	for _, s := range b.subs {
		subs = append(subs, s)
	}
	b.subsMu.RUnlock()
	for _, s := range subs {
		s.Stop()
	}
	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if b.ownPool {
		if err := b.pool.Shutdown(ctx); err != nil {
			return err
		}
	}
	if err := b.store.Close(); err != nil {
		return err
	}
	if b.stateStore != nil {
		if err := b.stateStore.Close(); err != nil {
			return err
		}
	}
	b.notify.Close()
	b.setState(ctx, BrokerStopped, nil)
	return nil
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
func (b *Broker) Store() Store                    { return b.store }
func (b *Broker) StateStore() StateStore          { return b.stateStore }
func (b *Broker) Router() *Router                 { return b.router }
func (b *Broker) Notifications() *NotificationBus { return b.notify }
func (b *Broker) Cluster() *Cluster               { return b.cluster }
func (b *Broker) Publisher() *Publisher           { return &Publisher{b: b} }

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
		if c.State == ConsumerStopped || c.State == ConsumerDisconnected || c.State == ConsumerDraining || c.State == ConsumerPaused {
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
	for {
		select {
		case <-t.C:
			_, _ = b.store.SweepLeases(b.ctx, time.Now())
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
	err := b.store.CommitOffset(ctx, commit)
	if err == nil {
		b.notify.Publish(Notification{Type: "stream.offset_committed", Source: b.cfg.Name, Severity: SeverityInfo, Stream: commit.Stream, Timestamp: time.Now(), Data: map[string]any{"group": commit.ConsumerGroup, "offset": commit.Offset}})
	}
	return err
}

func (b *Broker) ConsumerOffset(ctx context.Context, commit OffsetCommit) (uint64, error) {
	return b.store.ConsumerOffset(ctx, commit)
}

func (b *Broker) AddRoute(route Route) { b.router.Add(route) }

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
