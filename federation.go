package broker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// FederationConfig configures federation between broker instances.
type FederationConfig struct {
	// Upstream is the URL of the upstream broker to pull messages from.
	Upstream string `json:"upstream"`
	// Downstream is the URL to push messages to (for shovel).
	Downstream string `json:"downstream,omitempty"`
	// Queues is the list of queue names to federate.
	Queues []string `json:"queues,omitempty"`
	// Topics is the list of topic patterns to federate.
	Topics []string `json:"topics,omitempty"`
	// PollInterval is how often to poll upstream. Default: 1s.
	PollInterval time.Duration `json:"poll_interval,omitempty"`
	// BatchSize is the number of messages to pull per poll. Default: 100.
	BatchSize int `json:"batch_size,omitempty"`
	// MaxInFlight limits concurrent unacked federated messages. Default: 1000.
	MaxInFlight int `json:"max_inflight,omitempty"`
	// ReconnectDelay is the delay between reconnection attempts. Default: 5s.
	ReconnectDelay time.Duration `json:"reconnect_delay,omitempty"`
	// Filter is an optional message filter.
	Filter *FederationFilter `json:"filter,omitempty"`
	// Transform is an optional message transform.
	Transform *FederationTransform `json:"transform,omitempty"`
}

// FederationFilter filters messages during federation.
type FederationFilter struct {
	Types   []string          `json:"types,omitempty"`
	Tenants []string          `json:"tenants,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	MinPriority int           `json:"min_priority,omitempty"`
}

// FederationTransform transforms messages during federation.
type FederationTransform struct {
	// AddHeaders adds headers to federated messages.
	AddHeaders map[string]string `json:"add_headers,omitempty"`
	// RemoveHeaders removes headers from federated messages.
	RemoveHeaders []string `json:"remove_headers,omitempty"`
	// PrefixTopic adds a prefix to the topic.
	PrefixTopic string `json:"prefix_topic,omitempty"`
	// RewriteQueue rewrites the target queue.
	RewriteQueue string `json:"rewrite_queue,omitempty"`
}

// FederationLink represents a single federation link.
type FederationLink struct {
	ID        string           `json:"id"`
	Config    FederationConfig `json:"config"`
	State     string           `json:"state"`
	Messages  uint64           `json:"messages"`
	Errors    uint64           `json:"errors"`
	StartedAt time.Time        `json:"started_at"`
	stopCh    chan struct{}
}

// FederationManager manages federation links.
type FederationManager struct {
	broker *Broker
	links  map[string]*FederationLink
	mu     sync.RWMutex
	wg     sync.WaitGroup
}

// NewFederationManager creates a new federation manager.
func NewFederationManager(b *Broker) *FederationManager {
	return &FederationManager{
		broker: b,
		links:  make(map[string]*FederationLink),
	}
}

// AddLink adds a federation link.
func (fm *FederationManager) AddLink(cfg FederationConfig) *FederationLink {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 1000
	}
	if cfg.ReconnectDelay <= 0 {
		cfg.ReconnectDelay = 5 * time.Second
	}
	id := fmt.Sprintf("federation-%d", time.Now().UnixNano())
	link := &FederationLink{
		ID:        id,
		Config:    cfg,
		State:     "starting",
		StartedAt: time.Now(),
		stopCh:    make(chan struct{}),
	}
	fm.mu.Lock()
	fm.links[id] = link
	fm.mu.Unlock()
	fm.wg.Add(1)
	go fm.runLink(link)
	return link
}

// RemoveLink stops and removes a federation link.
func (fm *FederationManager) RemoveLink(id string) {
	fm.mu.Lock()
	link, ok := fm.links[id]
	if ok {
		delete(fm.links, id)
	}
	fm.mu.Unlock()
	if ok && link.stopCh != nil {
		close(link.stopCh)
	}
}

// ListLinks returns all federation links.
func (fm *FederationManager) ListLinks() []*FederationLink {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	out := make([]*FederationLink, 0, len(fm.links))
	for _, l := range fm.links {
		out = append(out, l)
	}
	return out
}

// Stop halts all federation links.
func (fm *FederationManager) Stop() {
	fm.mu.Lock()
	for id, l := range fm.links {
		if l.stopCh != nil {
			close(l.stopCh)
		}
		delete(fm.links, id)
	}
	fm.mu.Unlock()
	fm.wg.Wait()
}

func (fm *FederationManager) runLink(link *FederationLink) {
	defer fm.wg.Done()
	link.State = "running"
	for {
		select {
		case <-link.stopCh:
			link.State = "stopped"
			return
		default:
		}
		if err := fm.federateOnce(link); err != nil {
			link.Errors++
			link.State = "error"
			time.Sleep(link.Config.ReconnectDelay)
			link.State = "running"
			continue
		}
		time.Sleep(link.Config.PollInterval)
	}
}

func (fm *FederationManager) federateOnce(link *FederationLink) error {
	ctx := context.Background()
	for _, queue := range link.Config.Queues {
		msgs, err := fm.broker.Store().Lease(ctx, LeaseRequest{
			Queue:      queue,
			ConsumerID: "federation-" + link.ID,
			Limit:      link.Config.BatchSize,
		})
		if err != nil {
			return err
		}
		for _, msg := range msgs {
			if !fm.matchesFilter(msg, link.Config.Filter) {
				_ = fm.broker.Store().Ack(ctx, AckRequest{MessageID: msg.ID, Queue: queue})
				continue
			}
			transformed := fm.applyTransform(msg, link.Config.Transform)
			_, err := fm.broker.Publisher().Publish(ctx, transformed.Topic, transformed.Payload,
				WithQueue(firstNonEmpty(transformed.Queue, queue)),
				WithHeaders(transformed.Headers),
			)
			if err != nil {
				_ = fm.broker.Store().Nack(ctx, NackRequest{
					MessageID:  msg.ID,
					Queue:      queue,
					Action:     NackRetry,
					IncrementAttempt: true,
				})
				link.Errors++
				continue
			}
			_ = fm.broker.Store().Ack(ctx, AckRequest{MessageID: msg.ID, Queue: queue})
			link.Messages++
		}
	}
	return nil
}

func (fm *FederationManager) matchesFilter(msg Message, filter *FederationFilter) bool {
	if filter == nil {
		return true
	}
	if len(filter.Types) > 0 {
		found := false
		for _, t := range filter.Types {
			if msg.Type == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if len(filter.Tenants) > 0 {
		found := false
		for _, t := range filter.Tenants {
			if msg.TenantID == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for k, v := range filter.Headers {
		if msg.Headers[k] != v {
			return false
		}
	}
	if filter.MinPriority > 0 && msg.Priority < filter.MinPriority {
		return false
	}
	return true
}

func (fm *FederationManager) applyTransform(msg Message, transform *FederationTransform) Message {
	if transform == nil {
		return msg
	}
	m := cloneMessage(msg)
	if transform.AddHeaders != nil {
		if m.Headers == nil {
			m.Headers = map[string]string{}
		}
		for k, v := range transform.AddHeaders {
			m.Headers[k] = v
		}
	}
	for _, h := range transform.RemoveHeaders {
		delete(m.Headers, h)
	}
	if transform.PrefixTopic != "" {
		m.Topic = transform.PrefixTopic + m.Topic
	}
	if transform.RewriteQueue != "" {
		m.Queue = transform.RewriteQueue
	}
	return m
}

// ShovelConfig configures a shovel (one-directional message forwarding).
type ShovelConfig struct {
	// Source is the source queue to read from.
	Source string `json:"source"`
	// Destination is the destination queue to write to.
	Destination string `json:"destination"`
	// BatchSize is messages per batch. Default: 100.
	BatchSize int `json:"batch_size,omitempty"`
	// PollInterval is polling frequency. Default: 100ms.
	PollInterval time.Duration `json:"poll_interval,omitempty"`
}

// ShovelManager manages shovels.
type ShovelManager struct {
	broker   *Broker
	shovels  map[string]*shovelState
	mu       sync.RWMutex
	wg       sync.WaitGroup
}

type shovelState struct {
	id     string
	cfg    ShovelConfig
	stopCh chan struct{}
}

// NewShovelManager creates a new shovel manager.
func NewShovelManager(b *Broker) *ShovelManager {
	return &ShovelManager{broker: b, shovels: make(map[string]*shovelState)}
}

// StartShovel starts a shovel between two queues.
func (sm *ShovelManager) StartShovel(cfg ShovelConfig) string {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	id := fmt.Sprintf("shovel-%s-%s", cfg.Source, cfg.Destination)
	s := &shovelState{id: id, cfg: cfg, stopCh: make(chan struct{})}
	sm.mu.Lock()
	sm.shovels[id] = s
	sm.mu.Unlock()
	sm.wg.Add(1)
	go sm.runShovel(s)
	return id
}

// StopShovel stops a shovel.
func (sm *ShovelManager) StopShovel(id string) {
	sm.mu.Lock()
	s, ok := sm.shovels[id]
	if ok {
		delete(sm.shovels, id)
	}
	sm.mu.Unlock()
	if ok && s.stopCh != nil {
		close(s.stopCh)
	}
}

// Stop halts all shovels.
func (sm *ShovelManager) Stop() {
	sm.mu.Lock()
	for id, s := range sm.shovels {
		if s.stopCh != nil {
			close(s.stopCh)
		}
		delete(sm.shovels, id)
	}
	sm.mu.Unlock()
	sm.wg.Wait()
}

func (sm *ShovelManager) runShovel(s *shovelState) {
	defer sm.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		ctx := context.Background()
		msgs, err := sm.broker.Store().Lease(ctx, LeaseRequest{
			Queue:      s.cfg.Source,
			ConsumerID: s.id,
			Limit:      s.cfg.BatchSize,
		})
		if err != nil || len(msgs) == 0 {
			time.Sleep(s.cfg.PollInterval)
			continue
		}
		for _, msg := range msgs {
			_, err := sm.broker.Publisher().Publish(ctx, msg.Topic, msg.Payload,
				WithQueue(s.cfg.Destination),
				WithHeaders(msg.Headers),
				WithType(msg.Type),
			)
			if err != nil {
				_ = sm.broker.Store().Nack(ctx, NackRequest{
					MessageID:  msg.ID,
					Queue:      s.cfg.Source,
					Action:     NackRetry,
					IncrementAttempt: true,
				})
				continue
			}
			_ = sm.broker.Store().Ack(ctx, AckRequest{MessageID: msg.ID, Queue: s.cfg.Source})
		}
	}
}

// FederationManager accessor on Broker.
func (b *Broker) FederationManager() *FederationManager {
	return b.federationMgr
}

// ShovelManager accessor on Broker.
func (b *Broker) ShovelManager() *ShovelManager {
	return b.shovelMgr
}
