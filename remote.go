package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	TenantID          string            `json:"tenant_id,omitempty"`
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
	Banned        bool                 `json:"banned"`
	BannedReason  string               `json:"banned_reason,omitempty"`
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

// RestoreContext hydrates a persisted consumer without replacing a live entry.
// It is used before a reconnect refresh so durable subscriptions survive broker
// restarts while tenant ownership is checked before the record becomes visible.
func (r *ConsumerRegistry) RestoreContext(ctx context.Context, info ConsumerInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if info.ConsumerID == "" {
		return ErrInvalidMessage
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if current := r.consumers[info.ConsumerID]; current != nil {
		if !tenantAllowed(ctx, current.TenantID) {
			return ErrUnauthorized
		}
		return nil
	}
	if !tenantAllowed(ctx, info.TenantID) {
		return ErrUnauthorized
	}
	copy := cloneConsumerInfo(info)
	r.consumers[info.ConsumerID] = &copy
	return nil
}

func (r *ConsumerRegistry) Register(reg ConsumerRegistration) ConsumerInfo {
	info, _ := r.RegisterContext(context.Background(), reg)
	return info
}

// RegisterContext registers or refreshes a consumer while atomically checking
// that a tenant-scoped caller owns any existing consumer ID.
func (r *ConsumerRegistry) RegisterContext(ctx context.Context, reg ConsumerRegistration) (ConsumerInfo, error) {
	return r.RegisterContextWithPersist(ctx, reg, nil)
}

// RegisterContextWithPersist persists the proposed registration before it is
// made visible in the registry. The callback runs while the registry mutation
// is serialized, so concurrent refreshes cannot overwrite a newer durable
// record with stale consumer state.
func (r *ConsumerRegistry) RegisterContextWithPersist(ctx context.Context, reg ConsumerRegistration, persist func(ConsumerInfo) error) (ConsumerInfo, error) {
	tenant, err := resolveTenant(ctx, reg.TenantID)
	if err != nil {
		return ConsumerInfo{}, err
	}
	reg.TenantID = tenant
	reg = cloneConsumerRegistration(reg)
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
	if err := ctx.Err(); err != nil {
		return ConsumerInfo{}, err
	}
	cur := r.consumers[reg.ConsumerID]
	if cur != nil && !tenantAllowed(ctx, cur.TenantID) {
		return ConsumerInfo{}, ErrUnauthorized
	}
	registeredAt := now
	var subs []RemoteSubscription
	if cur != nil {
		registeredAt = cur.RegisteredAt
		subs = append([]RemoteSubscription(nil), cur.Subscriptions...)
	}
	candidate := ConsumerInfo{ConsumerRegistration: reg, State: ConsumerRegistered, RegisteredAt: registeredAt, LastHeartbeat: now, Subscriptions: subs}
	if candidate.RegisteredAt.IsZero() {
		candidate.RegisteredAt = now
	}
	out := cloneConsumerInfo(candidate)
	if persist != nil {
		if err := persist(out); err != nil {
			return ConsumerInfo{}, err
		}
	}
	r.consumers[reg.ConsumerID] = &candidate
	return out, nil
}
func (r *ConsumerRegistry) Heartbeat(id string) (ConsumerInfo, bool) {
	info, err := r.HeartbeatContext(context.Background(), id)
	return info, err == nil
}

func (r *ConsumerRegistry) HeartbeatContext(ctx context.Context, id string) (ConsumerInfo, error) {
	return r.HeartbeatContextWithPersist(ctx, id, nil)
}

func (r *ConsumerRegistry) HeartbeatContextWithPersist(ctx context.Context, id string, persist func(ConsumerInfo) error) (ConsumerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ConsumerInfo{}, err
	}
	c := r.consumers[id]
	if c == nil {
		return ConsumerInfo{}, ErrMessageNotFound
	}
	if !tenantAllowed(ctx, c.TenantID) {
		return ConsumerInfo{}, ErrUnauthorized
	}
	candidate := cloneConsumerInfo(*c)
	candidate.LastHeartbeat = time.Now()
	if candidate.State == ConsumerDisconnected || candidate.State == ConsumerFailed || candidate.State == ConsumerRegistered {
		candidate.State = ConsumerReady
	}
	out := cloneConsumerInfo(candidate)
	if persist != nil {
		if err := persist(out); err != nil {
			return ConsumerInfo{}, err
		}
	}
	r.consumers[id] = &candidate
	return out, nil
}
func (r *ConsumerRegistry) Subscribe(id string, sub RemoteSubscription) (ConsumerInfo, bool) {
	info, err := r.SubscribeContext(context.Background(), id, sub)
	return info, err == nil
}

func (r *ConsumerRegistry) SubscribeContext(ctx context.Context, id string, sub RemoteSubscription) (ConsumerInfo, error) {
	return r.SubscribeContextWithPersist(ctx, id, sub, nil)
}

func (r *ConsumerRegistry) SubscribeContextWithPersist(ctx context.Context, id string, sub RemoteSubscription, persist func(ConsumerInfo) error) (ConsumerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ConsumerInfo{}, err
	}
	c := r.consumers[id]
	if c == nil {
		return ConsumerInfo{}, ErrMessageNotFound
	}
	if !tenantAllowed(ctx, c.TenantID) {
		return ConsumerInfo{}, ErrUnauthorized
	}
	for _, s := range c.Subscriptions {
		if s.Type == sub.Type && s.Name == sub.Name && s.Queue == sub.Queue && s.Pattern == sub.Pattern {
			return cloneConsumerInfo(*c), nil
		}
	}
	candidate := cloneConsumerInfo(*c)
	candidate.Subscriptions = append(candidate.Subscriptions, sub)
	candidate.State = ConsumerReady
	out := cloneConsumerInfo(candidate)
	if persist != nil {
		if err := persist(out); err != nil {
			return ConsumerInfo{}, err
		}
	}
	r.consumers[id] = &candidate
	return out, nil
}
func (r *ConsumerRegistry) SetState(id string, st ConsumerState, reason string) (ConsumerInfo, bool) {
	info, err := r.SetStateContext(context.Background(), id, st, reason)
	return info, err == nil
}

func (r *ConsumerRegistry) SetStateContext(ctx context.Context, id string, st ConsumerState, reason string) (ConsumerInfo, error) {
	return r.SetStateContextWithPersist(ctx, id, st, reason, nil)
}

func (r *ConsumerRegistry) SetStateContextWithPersist(ctx context.Context, id string, st ConsumerState, reason string, persist func(ConsumerInfo) error) (ConsumerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ConsumerInfo{}, err
	}
	c := r.consumers[id]
	if c == nil {
		return ConsumerInfo{}, ErrMessageNotFound
	}
	if !tenantAllowed(ctx, c.TenantID) {
		return ConsumerInfo{}, ErrUnauthorized
	}
	candidate := cloneConsumerInfo(*c)
	candidate.State = st
	if st == ConsumerPaused {
		candidate.Paused = true
	}
	if st == ConsumerReady {
		candidate.Paused = false
		candidate.Draining = false
		candidate.Banned = false
		candidate.BannedReason = ""
	}
	if st == ConsumerDraining {
		candidate.Draining = true
	}
	if st == ConsumerBanned {
		candidate.Banned = true
		candidate.BannedReason = reason
		candidate.Paused = false
		candidate.Draining = false
	}
	if reason != "" {
		candidate.LastError = reason
	}
	out := cloneConsumerInfo(candidate)
	if persist != nil {
		if err := persist(out); err != nil {
			return ConsumerInfo{}, err
		}
	}
	r.consumers[id] = &candidate
	return out, nil
}

func (r *ConsumerRegistry) Ban(id string, reason string) (ConsumerInfo, bool) {
	return r.SetState(id, ConsumerBanned, reason)
}

func (r *ConsumerRegistry) Unban(id string) (ConsumerInfo, bool) {
	info, err := r.UnbanContext(context.Background(), id)
	return info, err == nil
}

func (r *ConsumerRegistry) UnbanContext(ctx context.Context, id string) (ConsumerInfo, error) {
	return r.UnbanContextWithPersist(ctx, id, nil)
}

func (r *ConsumerRegistry) UnbanContextWithPersist(ctx context.Context, id string, persist func(ConsumerInfo) error) (ConsumerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ConsumerInfo{}, err
	}
	c := r.consumers[id]
	if c == nil {
		return ConsumerInfo{}, ErrMessageNotFound
	}
	if !tenantAllowed(ctx, c.TenantID) {
		return ConsumerInfo{}, ErrUnauthorized
	}
	candidate := cloneConsumerInfo(*c)
	candidate.State = ConsumerReady
	candidate.Banned = false
	candidate.BannedReason = ""
	candidate.Paused = false
	candidate.Draining = false
	out := cloneConsumerInfo(candidate)
	if persist != nil {
		if err := persist(out); err != nil {
			return ConsumerInfo{}, err
		}
	}
	r.consumers[id] = &candidate
	return out, nil
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
	// Calculate in-flight safely using int64 to avoid overflow on 32-bit systems
	delta := leased - acked - nacked - failed
	newInFlight := int64(c.InFlight) + delta
	if newInFlight < 0 {
		newInFlight = 0
	}
	c.InFlight = int(newInFlight)
	if lastErr != "" {
		c.LastError = lastErr
	}
}
func (r *ConsumerRegistry) Unregister(id string) (ConsumerInfo, bool) {
	info, err := r.UnregisterContext(context.Background(), id)
	return info, err == nil
}

func (r *ConsumerRegistry) UnregisterContext(ctx context.Context, id string) (ConsumerInfo, error) {
	return r.UnregisterContextWithPersist(ctx, id, nil)
}

func (r *ConsumerRegistry) UnregisterContextWithPersist(ctx context.Context, id string, remove func(ConsumerInfo) error) (ConsumerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ConsumerInfo{}, err
	}
	c := r.consumers[id]
	if c == nil {
		return ConsumerInfo{}, ErrMessageNotFound
	}
	if !tenantAllowed(ctx, c.TenantID) {
		return ConsumerInfo{}, ErrUnauthorized
	}
	candidate := cloneConsumerInfo(*c)
	candidate.State = ConsumerStopped
	out := cloneConsumerInfo(candidate)
	if remove != nil {
		if err := remove(out); err != nil {
			return ConsumerInfo{}, err
		}
	}
	delete(r.consumers, id)
	return out, nil
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
	return r.ListContext(context.Background())
}

func (r *ConsumerRegistry) ListContext(ctx context.Context) []ConsumerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ConsumerInfo, 0, len(r.consumers))
	for _, c := range r.consumers {
		if tenantAllowed(ctx, c.TenantID) {
			out = append(out, cloneConsumerInfo(*c))
		}
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
	evs := append(r.events[id], cloneNotification(n))
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
	out := make([]Notification, len(evs)-start)
	for i, event := range evs[start:] {
		out[i] = cloneNotification(event)
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}
func cloneConsumerInfo(in ConsumerInfo) ConsumerInfo {
	in.ConsumerRegistration = cloneConsumerRegistration(in.ConsumerRegistration)
	in.Subscriptions = append([]RemoteSubscription(nil), in.Subscriptions...)
	return in
}

func cloneConsumerRegistration(reg ConsumerRegistration) ConsumerRegistration {
	reg.Capabilities = append([]string(nil), reg.Capabilities...)
	reg.Topics = append([]string(nil), reg.Topics...)
	reg.Queues = append([]string(nil), reg.Queues...)
	reg.Patterns = append([]string(nil), reg.Patterns...)
	reg.TaskTypes = append([]string(nil), reg.TaskTypes...)
	reg.CommandTypes = append([]string(nil), reg.CommandTypes...)
	reg.RequestSubjects = append([]string(nil), reg.RequestSubjects...)
	reg.Metadata = cloneMap(reg.Metadata)
	return reg
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
	if reg.ConsumerID != "" && b.stateStore != nil {
		stored, ok, err := b.stateStore.GetConsumer(ctx, reg.ConsumerID)
		if err != nil {
			return ConsumerInfo{}, err
		}
		if ok {
			if err := b.Consumers().RestoreContext(ctx, stored); err != nil {
				return ConsumerInfo{}, err
			}
		}
	}
	info, err := b.Consumers().RegisterContextWithPersist(ctx, reg, func(info ConsumerInfo) error {
		return b.persistConsumer(ctx, info)
	})
	if err != nil {
		return ConsumerInfo{}, err
	}
	if info.Group != "" {
		b.ConsumerJoinGroup(ctx, info.ConsumerID, info.Group)
	}
	if b.cfg.Hooks.OnConsumerRegistered != nil {
		b.cfg.Hooks.OnConsumerRegistered(ctx, ConsumerEvent{Consumer: info.ConsumerID, Group: info.Group, State: ConsumerRegistered, Time: time.Now()})
	}
	b.emitConsumerNotification(info.ConsumerID, Notification{Type: "consumer.registered", Source: b.cfg.Name, Severity: SeverityInfo, Consumer: info.ConsumerID, ConsumerGroup: info.Group, TenantID: info.TenantID, Timestamp: time.Now()})
	return info, nil
}
func (b *Broker) RemoteConsumerHeartbeat(ctx context.Context, id string) (ConsumerInfo, error) {
	info, err := b.Consumers().HeartbeatContextWithPersist(ctx, id, func(info ConsumerInfo) error {
		return b.persistConsumer(ctx, info)
	})
	if err != nil {
		return ConsumerInfo{}, err
	}
	b.emitConsumerNotification(id, Notification{Type: "consumer.heartbeat", Source: b.cfg.Name, Severity: SeverityDebug, Consumer: id, ConsumerGroup: info.Group, TenantID: info.TenantID, Timestamp: time.Now()})
	return info, nil
}
func (b *Broker) RemoteConsumerSubscribe(ctx context.Context, id string, sub RemoteSubscription) (ConsumerInfo, error) {
	curr, ok := b.Consumers().Get(id)
	if !ok {
		return ConsumerInfo{}, ErrMessageNotFound
	}
	if !tenantAllowed(ctx, curr.TenantID) {
		return ConsumerInfo{}, ErrUnauthorized
	}
	if sub.Type == "" {
		sub.Type = "queue"
	}
	var route *Route
	switch sub.Type {
	case "queue":
		if sub.Name == "" {
			sub.Name = sub.Queue
		}
		if sub.Name == "" {
			return ConsumerInfo{}, errf(ErrInvalidMessage, "queue subscription requires name")
		}
		if sub.Queue == "" {
			sub.Queue = sub.Name
		}
		if sub.AutoCreate {
			if err := b.DeclareQueue(ctx, QueueConfig{Name: sub.Queue, VisibilityTimeout: b.cfg.DefaultVisibility, RetryPolicy: b.cfg.DefaultRetry}); err != nil {
				return ConsumerInfo{}, err
			}
		}
	case "topic", "pattern":
		pattern := firstNonEmpty(sub.Pattern, sub.Name)
		if pattern == "" {
			return ConsumerInfo{}, errf(ErrInvalidMessage, "topic/pattern subscription requires name")
		}
		if sub.Queue == "" {
			// Reuse a persisted pre-hash queue name when an older consumer record
			// already has this subscription. This keeps reconnects stable across
			// upgrades that changed generated queue names to avoid sanitize clashes.
			for _, existing := range curr.Subscriptions {
				if (existing.Type == "topic" || existing.Type == "pattern") &&
					(existing.Name == pattern || existing.Pattern == pattern) && existing.Queue != "" {
					sub.Queue = existing.Queue
					break
				}
			}
			if sub.Queue == "" {
				sub.Queue = "sub." + id + "." + remoteSubscriptionKey(pattern)
			}
		}
		if sub.AutoCreate {
			if err := b.DeclareQueue(ctx, QueueConfig{Name: sub.Queue, VisibilityTimeout: b.cfg.DefaultVisibility, RetryPolicy: b.cfg.DefaultRetry}); err != nil {
				return ConsumerInfo{}, err
			}
		}
		routeName := "remote-sub." + id + "." + remoteSubscriptionKey(pattern+"\x00"+sub.Queue)
		route = &Route{Name: routeName, Mode: RouteTopic, Match: pattern, To: []Destination{{Kind: DestinationQueue, Name: sub.Queue}}, Enabled: true}
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
			if err := b.DeclareQueue(ctx, QueueConfig{Name: sub.Queue, VisibilityTimeout: b.cfg.DefaultVisibility, RetryPolicy: b.cfg.DefaultRetry}); err != nil {
				return ConsumerInfo{}, err
			}
		}
	case "command", "request":
		if sub.Name == "" {
			return ConsumerInfo{}, errf(ErrInvalidMessage, "subscription requires name")
		}
		if sub.Queue == "" {
			sub.Queue = sub.Type + "." + sanitize(sub.Name)
		}
		if sub.AutoCreate {
			if err := b.DeclareQueue(ctx, QueueConfig{Name: sub.Queue, VisibilityTimeout: b.cfg.DefaultVisibility, RetryPolicy: b.cfg.DefaultRetry}); err != nil {
				return ConsumerInfo{}, err
			}
		}
		routeName := "remote-" + sub.Type + "." + id + "." + remoteSubscriptionKey(sub.Name+"\x00"+sub.Queue)
		route = &Route{Name: routeName, Mode: RouteTopic, Match: sub.Name, To: []Destination{{Kind: DestinationQueue, Name: sub.Queue}}, Enabled: true}
	}
	info, err := b.Consumers().SubscribeContextWithPersist(ctx, id, sub, func(info ConsumerInfo) error {
		return b.persistConsumer(ctx, info)
	})
	if err != nil {
		return ConsumerInfo{}, err
	}
	if route != nil {
		// A subscription route is owned by its consumer tenant. Without this
		// fence, messages from other tenants could be copied into the consumer's
		// private subscription queue and create cross-tenant backpressure.
		route.TenantID = info.TenantID
		// Re-subscribing an existing filter is idempotent in the registry. Keep
		// the router table idempotent as well instead of appending duplicate
		// copies of the same route on every reconnect.
		b.RemoveRoute(route.Name)
		b.AddRoute(*route)
		b.trackRemoteRoute(id, route.Name)
	}
	if b.cfg.Hooks.OnConsumerAssigned != nil {
		b.cfg.Hooks.OnConsumerAssigned(ctx, ConsumerEvent{Consumer: id, Group: info.Group, Queue: firstNonEmpty(sub.Queue, sub.Name), State: ConsumerAssigned, Time: time.Now()})
	}
	b.emitConsumerNotification(id, Notification{Type: "consumer.subscribed", Source: b.cfg.Name, Severity: SeverityInfo, Consumer: id, ConsumerGroup: info.Group, Queue: firstNonEmpty(sub.Queue, sub.Name), TenantID: info.TenantID, Timestamp: time.Now(), Data: map[string]any{"subscription": sub}})
	return info, nil
}

func remoteSubscriptionKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:12])
}

func (b *Broker) SetRemoteConsumerState(ctx context.Context, id string, st ConsumerState, reason string) (ConsumerInfo, error) {
	info, err := b.Consumers().SetStateContextWithPersist(ctx, id, st, reason, func(info ConsumerInfo) error {
		return b.persistConsumer(ctx, info)
	})
	if err != nil {
		return ConsumerInfo{}, err
	}
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
	case ConsumerBanned:
		if b.cfg.Hooks.OnConsumerBanned != nil {
			b.cfg.Hooks.OnConsumerBanned(ctx, ce)
		}
	}
	eventType := "consumer." + string(st)
	if st == ConsumerReady && info.BannedReason == "" && info.Banned {
		eventType = "consumer.unbanned"
	}
	b.emitConsumerNotification(id, Notification{Type: eventType, Source: b.cfg.Name, Severity: SeverityInfo, Consumer: id, ConsumerGroup: info.Group, Timestamp: time.Now(), Data: map[string]any{"reason": reason}})
	return info, nil
}

func (b *Broker) BanRemoteConsumer(ctx context.Context, id string, reason string) (ConsumerInfo, error) {
	info, err := b.Consumers().SetStateContextWithPersist(ctx, id, ConsumerBanned, reason, func(info ConsumerInfo) error {
		return b.persistConsumer(ctx, info)
	})
	if err != nil {
		return ConsumerInfo{}, err
	}
	ce := ConsumerEvent{Consumer: id, Group: info.Group, State: ConsumerBanned, Error: errf(ErrPermanent, "%s", reason), Time: time.Now()}
	if b.cfg.Hooks.OnConsumerBanned != nil {
		b.cfg.Hooks.OnConsumerBanned(ctx, ce)
	}
	b.emitConsumerNotification(id, Notification{Type: "consumer.banned", Source: b.cfg.Name, Severity: SeverityWarn, Consumer: id, ConsumerGroup: info.Group, Timestamp: time.Now(), Data: map[string]any{"reason": reason}})
	return info, nil
}

func (b *Broker) UnbanRemoteConsumer(ctx context.Context, id string) (ConsumerInfo, error) {
	info, err := b.Consumers().UnbanContextWithPersist(ctx, id, func(info ConsumerInfo) error {
		return b.persistConsumer(ctx, info)
	})
	if err != nil {
		return ConsumerInfo{}, err
	}
	ce := ConsumerEvent{Consumer: id, Group: info.Group, State: ConsumerReady, Time: time.Now()}
	if b.cfg.Hooks.OnConsumerUnbanned != nil {
		b.cfg.Hooks.OnConsumerUnbanned(ctx, ce)
	}
	b.emitConsumerNotification(id, Notification{Type: "consumer.unbanned", Source: b.cfg.Name, Severity: SeverityInfo, Consumer: id, ConsumerGroup: info.Group, Timestamp: time.Now()})
	return info, nil
}
func (b *Broker) UnregisterRemoteConsumer(ctx context.Context, id string) error {
	info, err := b.Consumers().UnregisterContextWithPersist(ctx, id, func(info ConsumerInfo) error {
		if b.stateStore == nil {
			return nil
		}
		return b.stateStore.DeleteConsumer(ctx, id)
	})
	if err != nil {
		return err
	}
	// A remote subscription writes into the broker's global routing table. Those
	// routes outlived the consumer, because the Router had no way to remove a
	// route, so every transient worker permanently widened the routing table.
	b.dropRemoteRoutes(id)
	if info.Group != "" {
		b.ConsumerLeaveGroup(ctx, id, info.Group)
	}
	if b.cfg.Hooks.OnConsumerStopped != nil {
		b.cfg.Hooks.OnConsumerStopped(ctx, ConsumerEvent{Consumer: id, Group: info.Group, State: ConsumerStopped, Time: time.Now()})
	}
	b.emitConsumerNotification(id, Notification{Type: "consumer.unregistered", Source: b.cfg.Name, Severity: SeverityInfo, Consumer: id, ConsumerGroup: info.Group, Timestamp: time.Now()})
	return nil
}

// trackRemoteRoute records a route added on behalf of a remote consumer so it can
// be removed when that consumer goes away.
func (b *Broker) trackRemoteRoute(consumerID, routeName string) {
	b.remoteRoutesMu.Lock()
	if b.remoteRoutes == nil {
		b.remoteRoutes = map[string][]string{}
	}
	for _, existing := range b.remoteRoutes[consumerID] {
		if existing == routeName {
			b.remoteRoutesMu.Unlock()
			return
		}
	}
	b.remoteRoutes[consumerID] = append(b.remoteRoutes[consumerID], routeName)
	b.remoteRoutesMu.Unlock()
}

// dropRemoteRoutes removes every route added for a remote consumer.
func (b *Broker) dropRemoteRoutes(consumerID string) {
	b.remoteRoutesMu.Lock()
	prefix := "remote-"
	routes := b.remoteRoutes[consumerID]
	delete(b.remoteRoutes, consumerID)
	b.remoteRoutesMu.Unlock()
	for _, name := range routes {
		b.router.Remove(name)
	}
	// Also sweep by prefix, in case a route was added for this consumer by an older
	// path that did not record ownership.
	if b.router != nil {
		b.router.RemovePrefix(prefix + "sub." + consumerID + ".")
	}
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

func (b *Broker) persistConsumer(ctx context.Context, info ConsumerInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b != nil && b.stateStore != nil && info.ConsumerID != "" {
		return b.stateStore.PutConsumer(ctx, info)
	}
	return nil
}
