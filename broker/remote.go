package broker

import (
	"context"
	"sort"
	"sync"
	"time"
)

const (
	PermissionConsumerRegister = "consumer:register"
	PermissionConsumerRead     = "consumer:read"
	PermissionConsumerControl  = "consumer:control"
	PermissionConsumerPoll     = "consumer:poll"
	PermissionTaskRead         = "task:read"
	PermissionTaskWrite        = "task:write"
)

type ConsumerRegistration struct {
	ConsumerID        string            `json:"consumer_id"`
	Group             string            `json:"group"`
	Name              string            `json:"name,omitempty"`
	Hostname          string            `json:"hostname,omitempty"`
	Version           string            `json:"version,omitempty"`
	Capabilities      []string          `json:"capabilities,omitempty"`
	Topics            []string          `json:"topics,omitempty"`
	Queues            []string          `json:"queues,omitempty"`
	Patterns          []string          `json:"patterns,omitempty"`
	TaskTypes         []string          `json:"task_types,omitempty"`
	CommandTypes      []string          `json:"command_types,omitempty"`
	RequestSubjects   []string          `json:"request_subjects,omitempty"`
	MaxInFlight       int               `json:"max_inflight,omitempty"`
	Concurrency       int               `json:"concurrency,omitempty"`
	AckMode           AckMode           `json:"ack_mode,omitempty"`
	HeartbeatEveryNS  int64             `json:"heartbeat_every_ns,omitempty"`
	VisibilityTimeout int64             `json:"visibility_timeout_ns,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
}

type RemoteSubscription struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	Queue      string `json:"queue,omitempty"`
	Pattern    string `json:"pattern,omitempty"`
	AutoCreate bool   `json:"auto_create"`
}

type ConsumerInfo struct {
	ConsumerRegistration
	State         ConsumerState        `json:"state"`
	RegisteredAt  time.Time            `json:"registered_at"`
	LastHeartbeat time.Time            `json:"last_heartbeat"`
	Paused        bool                 `json:"paused"`
	Draining      bool                 `json:"draining"`
	InFlight      int                  `json:"inflight"`
	Leased        int64                `json:"leased"`
	Acked         int64                `json:"acked"`
	Nacked        int64                `json:"nacked"`
	Failed        int64                `json:"failed"`
	LastError     string               `json:"last_error,omitempty"`
	Subscriptions []RemoteSubscription `json:"subscriptions,omitempty"`
}

type ConsumerRegistry struct {
	mu        sync.RWMutex
	consumers map[string]*ConsumerInfo
	events    map[string][]Notification
	cap       int
}

func NewConsumerRegistry(capacity int) *ConsumerRegistry {
	if capacity <= 0 {
		capacity = 256
	}
	return &ConsumerRegistry{consumers: map[string]*ConsumerInfo{}, events: map[string][]Notification{}, cap: capacity}
}
func (r *ConsumerRegistry) Register(reg ConsumerRegistration) ConsumerInfo {
	now := time.Now()
	if reg.ConsumerID == "" {
		reg.ConsumerID = "consumer-" + newIDFromTime()
	}
	if reg.Group == "" {
		reg.Group = reg.ConsumerID
	}
	if reg.Name == "" {
		reg.Name = reg.ConsumerID
	}
	if reg.Concurrency <= 0 {
		reg.Concurrency = 1
	}
	if reg.MaxInFlight <= 0 {
		reg.MaxInFlight = reg.Concurrency
	}
	if reg.AckMode == "" {
		reg.AckMode = AckManual
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cur := r.consumers[reg.ConsumerID]
	if cur == nil {
		cur = &ConsumerInfo{RegisteredAt: now}
		r.consumers[reg.ConsumerID] = cur
	}
	subs := cur.Subscriptions
	*cur = ConsumerInfo{ConsumerRegistration: reg, State: ConsumerRegistered, RegisteredAt: cur.RegisteredAt, LastHeartbeat: now, Subscriptions: subs}
	if cur.RegisteredAt.IsZero() {
		cur.RegisteredAt = now
	}
	return cloneConsumerInfo(*cur)
}
func (r *ConsumerRegistry) Heartbeat(id string) (ConsumerInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.consumers[id]
	if c == nil {
		return ConsumerInfo{}, false
	}
	c.LastHeartbeat = time.Now()
	if c.State == ConsumerDisconnected || c.State == ConsumerFailed || c.State == ConsumerRegistered {
		c.State = ConsumerReady
	}
	return cloneConsumerInfo(*c), true
}
func (r *ConsumerRegistry) Subscribe(id string, sub RemoteSubscription) (ConsumerInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.consumers[id]
	if c == nil {
		return ConsumerInfo{}, false
	}
	for _, s := range c.Subscriptions {
		if s.Type == sub.Type && s.Name == sub.Name && s.Queue == sub.Queue && s.Pattern == sub.Pattern {
			return cloneConsumerInfo(*c), true
		}
	}
	c.Subscriptions = append(c.Subscriptions, sub)
	c.State = ConsumerReady
	return cloneConsumerInfo(*c), true
}
func (r *ConsumerRegistry) SetState(id string, st ConsumerState, reason string) (ConsumerInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.consumers[id]
	if c == nil {
		return ConsumerInfo{}, false
	}
	c.State = st
	if st == ConsumerPaused {
		c.Paused = true
	}
	if st == ConsumerReady {
		c.Paused = false
		c.Draining = false
	}
	if st == ConsumerDraining {
		c.Draining = true
	}
	if reason != "" {
		c.LastError = reason
	}
	return cloneConsumerInfo(*c), true
}
func (r *ConsumerRegistry) AddStats(id string, leased, acked, nacked, failed int64, lastErr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.consumers[id]
	if c == nil {
		return
	}
	c.Leased += leased
	c.Acked += acked
	c.Nacked += nacked
	c.Failed += failed
	c.InFlight += int(leased - acked - nacked - failed)
	if c.InFlight < 0 {
		c.InFlight = 0
	}
	if lastErr != "" {
		c.LastError = lastErr
	}
}
func (r *ConsumerRegistry) Unregister(id string) (ConsumerInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.consumers[id]
	if c == nil {
		return ConsumerInfo{}, false
	}
	c.State = ConsumerStopped
	out := cloneConsumerInfo(*c)
	delete(r.consumers, id)
	return out, true
}
func (r *ConsumerRegistry) Get(id string) (ConsumerInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c := r.consumers[id]
	if c == nil {
		return ConsumerInfo{}, false
	}
	return cloneConsumerInfo(*c), true
}
func (r *ConsumerRegistry) List() []ConsumerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ConsumerInfo, 0, len(r.consumers))
	for _, c := range r.consumers {
		out = append(out, cloneConsumerInfo(*c))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConsumerID < out[j].ConsumerID })
	return out
}
func (r *ConsumerRegistry) AddEvent(id string, n Notification) {
	if id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	evs := append(r.events[id], n)
	if len(evs) > r.cap {
		evs = evs[len(evs)-r.cap:]
	}
	r.events[id] = evs
}
func (r *ConsumerRegistry) Events(id, after string, limit int) []Notification {
	r.mu.RLock()
	defer r.mu.RUnlock()
	evs := r.events[id]
	if limit <= 0 || limit > len(evs) {
		limit = len(evs)
	}
	start := 0
	if after != "" {
		for i, n := range evs {
			if n.ID == after {
				start = i + 1
				break
			}
		}
	} else if limit < len(evs) {
		start = len(evs) - limit
	}
	out := append([]Notification(nil), evs[start:]...)
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}
func cloneConsumerInfo(in ConsumerInfo) ConsumerInfo {
	in.Capabilities = append([]string(nil), in.Capabilities...)
	in.Topics = append([]string(nil), in.Topics...)
	in.Queues = append([]string(nil), in.Queues...)
	in.Patterns = append([]string(nil), in.Patterns...)
	in.TaskTypes = append([]string(nil), in.TaskTypes...)
	in.CommandTypes = append([]string(nil), in.CommandTypes...)
	in.RequestSubjects = append([]string(nil), in.RequestSubjects...)
	in.Subscriptions = append([]RemoteSubscription(nil), in.Subscriptions...)
	in.Metadata = cloneMap(in.Metadata)
	return in
}
func (b *Broker) Consumers() *ConsumerRegistry {
	if b == nil {
		return nil
	}
	if b.consumers == nil {
		b.consumers = NewConsumerRegistry(512)
	}
	return b.consumers
}
func (b *Broker) RegisterRemoteConsumer(ctx context.Context, reg ConsumerRegistration) (ConsumerInfo, error) {
	info := b.Consumers().Register(reg)
	b.persistConsumer(ctx, info)
	if b.cfg.Hooks.OnConsumerRegistered != nil {
		b.cfg.Hooks.OnConsumerRegistered(ctx, ConsumerEvent{Consumer: info.ConsumerID, Group: info.Group, State: ConsumerRegistered, Time: time.Now()})
	}
	b.emitConsumerNotification(info.ConsumerID, Notification{Type: "consumer.registered", Source: b.cfg.Name, Severity: SeverityInfo, Consumer: info.ConsumerID, ConsumerGroup: info.Group, Timestamp: time.Now()})
	return info, nil
}
func (b *Broker) RemoteConsumerHeartbeat(ctx context.Context, id string) (ConsumerInfo, error) {
	info, ok := b.Consumers().Heartbeat(id)
	if !ok {
		return ConsumerInfo{}, ErrMessageNotFound
	}
	b.persistConsumer(ctx, info)
	b.emitConsumerNotification(id, Notification{Type: "consumer.heartbeat", Source: b.cfg.Name, Severity: SeverityDebug, Consumer: id, ConsumerGroup: info.Group, Timestamp: time.Now()})
	return info, nil
}
func (b *Broker) RemoteConsumerSubscribe(ctx context.Context, id string, sub RemoteSubscription) (ConsumerInfo, error) {
	if sub.Type == "" {
		sub.Type = "queue"
	}
	switch sub.Type {
	case "queue":
		if sub.Name == "" {
			sub.Name = sub.Queue
		}
		if sub.Name == "" {
			return ConsumerInfo{}, errf(ErrInvalidMessage, "queue subscription requires name")
		}
		if sub.AutoCreate {
			_ = b.DeclareQueue(ctx, QueueConfig{Name: sub.Name, VisibilityTimeout: b.cfg.DefaultVisibility, RetryPolicy: b.cfg.DefaultRetry})
		}
	case "topic", "pattern":
		pattern := firstNonEmpty(sub.Pattern, sub.Name)
		if pattern == "" {
			return ConsumerInfo{}, errf(ErrInvalidMessage, "topic/pattern subscription requires name")
		}
		if sub.Queue == "" {
			sub.Queue = "sub." + id + "." + sanitize(pattern)
		}
		if sub.AutoCreate {
			_ = b.DeclareQueue(ctx, QueueConfig{Name: sub.Queue, VisibilityTimeout: b.cfg.DefaultVisibility, RetryPolicy: b.cfg.DefaultRetry})
		}
		b.AddRoute(Route{Name: "remote-sub." + id + "." + sanitize(pattern), Mode: RouteTopic, Match: pattern, To: []Destination{{Kind: DestinationQueue, Name: sub.Queue}}, Enabled: true})
		sub.Name = pattern
		sub.Pattern = pattern
	case "task":
		if sub.Name == "" {
			return ConsumerInfo{}, errf(ErrInvalidMessage, "task subscription requires name")
		}
		if sub.Queue == "" {
			sub.Queue = "tasks." + sub.Name
		}
		if sub.AutoCreate {
			_ = b.DeclareQueue(ctx, QueueConfig{Name: sub.Queue, VisibilityTimeout: b.cfg.DefaultVisibility, RetryPolicy: b.cfg.DefaultRetry})
		}
	case "command", "request":
		if sub.Name == "" {
			return ConsumerInfo{}, errf(ErrInvalidMessage, "subscription requires name")
		}
		if sub.Queue == "" {
			sub.Queue = sub.Type + "." + sanitize(sub.Name)
		}
		if sub.AutoCreate {
			_ = b.DeclareQueue(ctx, QueueConfig{Name: sub.Queue, VisibilityTimeout: b.cfg.DefaultVisibility, RetryPolicy: b.cfg.DefaultRetry})
		}
		b.AddRoute(Route{Name: "remote-" + sub.Type + "." + id + "." + sanitize(sub.Name), Mode: RouteTopic, Match: sub.Name, To: []Destination{{Kind: DestinationQueue, Name: sub.Queue}}, Enabled: true})
	}
	info, ok := b.Consumers().Subscribe(id, sub)
	if !ok {
		return ConsumerInfo{}, ErrMessageNotFound
	}
	b.persistConsumer(ctx, info)
	if b.cfg.Hooks.OnConsumerAssigned != nil {
		b.cfg.Hooks.OnConsumerAssigned(ctx, ConsumerEvent{Consumer: id, Group: info.Group, Queue: firstNonEmpty(sub.Queue, sub.Name), State: ConsumerAssigned, Time: time.Now()})
	}
	b.emitConsumerNotification(id, Notification{Type: "consumer.subscribed", Source: b.cfg.Name, Severity: SeverityInfo, Consumer: id, ConsumerGroup: info.Group, Queue: firstNonEmpty(sub.Queue, sub.Name), Timestamp: time.Now(), Data: map[string]any{"subscription": sub}})
	return info, nil
}
func (b *Broker) SetRemoteConsumerState(ctx context.Context, id string, st ConsumerState, reason string) (ConsumerInfo, error) {
	info, ok := b.Consumers().SetState(id, st, reason)
	if !ok {
		return ConsumerInfo{}, ErrMessageNotFound
	}
	b.persistConsumer(ctx, info)
	ce := ConsumerEvent{Consumer: id, Group: info.Group, State: st, Time: time.Now()}
	switch st {
	case ConsumerPaused:
		if b.cfg.Hooks.OnConsumerPaused != nil {
			b.cfg.Hooks.OnConsumerPaused(ctx, ce)
		}
	case ConsumerReady:
		if b.cfg.Hooks.OnConsumerResumed != nil {
			b.cfg.Hooks.OnConsumerResumed(ctx, ce)
		}
	case ConsumerDraining:
		if b.cfg.Hooks.OnConsumerDraining != nil {
			b.cfg.Hooks.OnConsumerDraining(ctx, ce)
		}
	case ConsumerStopped:
		if b.cfg.Hooks.OnConsumerStopped != nil {
			b.cfg.Hooks.OnConsumerStopped(ctx, ce)
		}
	}
	b.emitConsumerNotification(id, Notification{Type: "consumer." + string(st), Source: b.cfg.Name, Severity: SeverityInfo, Consumer: id, ConsumerGroup: info.Group, Timestamp: time.Now(), Data: map[string]any{"reason": reason}})
	return info, nil
}
func (b *Broker) UnregisterRemoteConsumer(ctx context.Context, id string) error {
	info, ok := b.Consumers().Unregister(id)
	if !ok {
		return ErrMessageNotFound
	}
	if b.stateStore != nil {
		_ = b.stateStore.DeleteConsumer(ctx, id)
	}
	if b.cfg.Hooks.OnConsumerStopped != nil {
		b.cfg.Hooks.OnConsumerStopped(ctx, ConsumerEvent{Consumer: id, Group: info.Group, State: ConsumerStopped, Time: time.Now()})
	}
	b.emitConsumerNotification(id, Notification{Type: "consumer.unregistered", Source: b.cfg.Name, Severity: SeverityInfo, Consumer: id, ConsumerGroup: info.Group, Timestamp: time.Now()})
	return nil
}
func (b *Broker) emitConsumerNotification(id string, n Notification) {
	b.notify.Publish(n)
	if b.consumers != nil {
		b.consumers.AddEvent(id, n)
	}
	if b.stateStore != nil {
		_ = b.stateStore.AddConsumerEvent(context.Background(), id, n)
	}
}

func (b *Broker) persistConsumer(ctx context.Context, info ConsumerInfo) {
	if b != nil && b.stateStore != nil && info.ConsumerID != "" {
		_ = b.stateStore.PutConsumer(ctx, info)
	}
}
