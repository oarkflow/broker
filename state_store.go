package broker

import (
	"context"
	"sort"
	"sync"
	"time"
)

// BrokerStateRecord is durable broker/runtime state independent from the hot
// message queue store. It is intentionally compact so it can be persisted by
// memory, SQLite, Postgres, etc.
type BrokerStateRecord struct {
	Name      string            `json:"name"`
	NodeID    string            `json:"node_id,omitempty"`
	Mode      Mode              `json:"mode"`
	State     BrokerState       `json:"state"`
	UpdatedAt time.Time         `json:"updated_at"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// PublisherRecord tracks publish/request/task operations for audit,
// troubleshooting, and external persistence.
type PublisherRecord struct {
	ID            string            `json:"id"`
	Operation     string            `json:"operation"`
	Topic         string            `json:"topic,omitempty"`
	Queue         string            `json:"queue,omitempty"`
	Stream        string            `json:"stream,omitempty"`
	Subject       string            `json:"subject,omitempty"`
	MessageID     string            `json:"message_id,omitempty"`
	TaskID        string            `json:"task_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	Status        string            `json:"status"`
	Error         string            `json:"error,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

type BrokerStateStore interface {
	PutBrokerState(context.Context, BrokerStateRecord) error
	GetBrokerState(context.Context, string) (BrokerStateRecord, bool, error)
}

type TaskStateStore interface {
	PutTask(context.Context, TaskInfo) error
	GetTask(context.Context, string) (TaskInfo, bool, error)
	ListTasks(context.Context, TaskStatus, int) ([]TaskInfo, error)
	DeleteTask(context.Context, string) error
}

type ConsumerStateStore interface {
	PutConsumer(context.Context, ConsumerInfo) error
	GetConsumer(context.Context, string) (ConsumerInfo, bool, error)
	ListConsumers(context.Context) ([]ConsumerInfo, error)
	DeleteConsumer(context.Context, string) error
	AddConsumerEvent(context.Context, string, Notification) error
	ConsumerEvents(context.Context, string, string, int) ([]Notification, error)
}

type PublisherStateStore interface {
	PutPublisherRecord(context.Context, PublisherRecord) error
	GetPublisherRecord(context.Context, string) (PublisherRecord, bool, error)
	ListPublisherRecords(context.Context, int) ([]PublisherRecord, error)
}

// OutboxStateStore persists dispatch state for DurableOutbox. Implementations
// backed by durable storage allow pending entries to survive process restarts.
// It is optional so existing StateStore implementations remain compatible.
type OutboxStateStore interface {
	PutOutboxEntry(context.Context, DurableOutboxEntry) error
	GetOutboxEntry(context.Context, string) (DurableOutboxEntry, bool, error)
	ListOutboxEntries(context.Context, int) ([]DurableOutboxEntry, error)
	ListDispatchableOutboxEntries(context.Context, int) ([]DurableOutboxEntry, error)
}

// OutboxClaimStore atomically leases dispatchable entries to one dispatcher.
// Claim leases allow another dispatcher to recover work after a worker dies.
type OutboxClaimStore interface {
	ClaimOutboxEntries(context.Context, string, int, time.Duration) ([]DurableOutboxEntry, error)
	UpdateClaimedOutboxEntry(context.Context, string, DurableOutboxEntry) error
}

// OutboxRetentionStore removes terminal outbox records older than a cutoff.
type OutboxRetentionStore interface {
	PruneCompletedOutboxEntries(context.Context, time.Time, int) ([]string, error)
}

// MQTTSessionStore persists MQTT session subscriptions independently from the
// message data plane. It is optional so existing StateStore implementations
// remain source compatible.
type MQTTSessionStore interface {
	PutMQTTSession(context.Context, MQTTSession) error
	GetMQTTSession(context.Context, string) (MQTTSession, bool, error)
	DeleteMQTTSession(context.Context, string) error
}

// MQTTRetainedMessage is one MQTT retained publication, scoped by tenant and
// exact topic name. A zero-length payload is represented by deleting the row.
type MQTTRetainedMessage struct {
	TenantID  string    `json:"tenant_id,omitempty"`
	Topic     string    `json:"topic"`
	Payload   []byte    `json:"payload,omitempty"`
	QoS       byte      `json:"qos"`
	UpdatedAt time.Time `json:"updated_at"`
}

// MQTTRetainedStore persists MQTT retained messages independently from the
// message data plane. It is optional to keep existing StateStore adapters
// source-compatible.
type MQTTRetainedStore interface {
	PutMQTTRetained(context.Context, MQTTRetainedMessage) error
	GetMQTTRetained(context.Context, string, string) (MQTTRetainedMessage, bool, error)
	ListMQTTRetained(context.Context, string) ([]MQTTRetainedMessage, error)
	DeleteMQTTRetained(context.Context, string, string) error
}

// MQTTRetainedPagedStore lists retained messages in tenant/topic order without
// requiring replay callers to load every retained payload at once.
type MQTTRetainedPagedStore interface {
	ListMQTTRetainedPage(context.Context, string, string, string, int) ([]MQTTRetainedMessage, error)
}

// OutboxEntryTransaction inserts an outbox entry into a caller-owned
// transaction. Implementations must not commit or roll back the transaction.
// Use this to write business data and its outbox record atomically in the same
// database transaction. InsertOutboxEntry must reject an ID that already exists.
type OutboxEntryTransaction interface {
	InsertOutboxEntry(context.Context, DurableOutboxEntry) error
}

// StateStore persists broker/task/consumer/publisher and optional protocol
// session control-plane state. The existing Store interface remains the
// high-performance message data-plane.
type StateStore interface {
	BrokerStateStore
	TaskStateStore
	ConsumerStateStore
	PublisherStateStore
	Close() error
}

// MemoryStateStore is the default state store. It is concurrency-safe and has
// no external dependencies. Use contrib/sqlitestore for persistent state.
type MemoryStateStore struct {
	mu            sync.RWMutex
	brokers       map[string]BrokerStateRecord
	tasks         map[string]TaskInfo
	consumers     map[string]ConsumerInfo
	events        map[string][]Notification
	publishers    map[string]PublisherRecord
	outbox        map[string]DurableOutboxEntry
	mqtt          map[string]MQTTSession
	retained      map[string]MQTTRetainedMessage
	retainedOrder []mqttRetainedOrderKey
	eventCap      int
	closed        bool
}

type mqttRetainedOrderKey struct {
	tenant string
	topic  string
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{brokers: map[string]BrokerStateRecord{}, tasks: map[string]TaskInfo{}, consumers: map[string]ConsumerInfo{}, events: map[string][]Notification{}, publishers: map[string]PublisherRecord{}, outbox: map[string]DurableOutboxEntry{}, mqtt: map[string]MQTTSession{}, retained: map[string]MQTTRetainedMessage{}, eventCap: 512}
}

func mqttRetainedKey(tenantID, topic string) string { return tenantScopedKey(tenantID, topic) }

func cloneMQTTRetained(message MQTTRetainedMessage) MQTTRetainedMessage {
	message.Payload = append([]byte(nil), message.Payload...)
	return message
}

func (s *MemoryStateStore) PutMQTTRetained(ctx context.Context, message MQTTRetainedMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if message.Topic == "" || message.QoS > 2 {
		return ErrInvalidMessage
	}
	tenant, err := resolveTenant(ctx, message.TenantID)
	if err != nil {
		return err
	}
	if len(message.Payload) == 0 {
		return s.DeleteMQTTRetained(ctx, tenant, message.Topic)
	}
	message.TenantID = tenant
	if message.UpdatedAt.IsZero() {
		message.UpdatedAt = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.retained == nil {
		s.retained = make(map[string]MQTTRetainedMessage)
	}
	key := mqttRetainedKey(tenant, message.Topic)
	if _, exists := s.retained[key]; !exists {
		orderKey := mqttRetainedOrderKey{tenant: tenant, topic: message.Topic}
		index := sort.Search(len(s.retainedOrder), func(i int) bool {
			current := s.retainedOrder[i]
			return current.tenant > tenant || (current.tenant == tenant && current.topic >= message.Topic)
		})
		s.retainedOrder = append(s.retainedOrder, mqttRetainedOrderKey{})
		copy(s.retainedOrder[index+1:], s.retainedOrder[index:])
		s.retainedOrder[index] = orderKey
	}
	s.retained[key] = cloneMQTTRetained(message)
	return nil
}

func (s *MemoryStateStore) GetMQTTRetained(ctx context.Context, tenant, topic string) (MQTTRetainedMessage, bool, error) {
	resolved, err := resolveTenant(ctx, tenant)
	if err != nil {
		return MQTTRetainedMessage{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return MQTTRetainedMessage{}, false, ErrClosed
	}
	message, ok := s.retained[mqttRetainedKey(resolved, topic)]
	if ok && !tenantAllowed(ctx, message.TenantID) {
		return MQTTRetainedMessage{}, false, ErrUnauthorized
	}
	return cloneMQTTRetained(message), ok, nil
}

func (s *MemoryStateStore) ListMQTTRetained(ctx context.Context, tenant string) ([]MQTTRetainedMessage, error) {
	resolved, err := resolveTenant(ctx, tenant)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	out := make([]MQTTRetainedMessage, 0, len(s.retained))
	for _, orderKey := range s.retainedOrder {
		message := s.retained[mqttRetainedKey(orderKey.tenant, orderKey.topic)]
		if tenant != "" && message.TenantID != resolved {
			continue
		}
		if tenantAllowed(ctx, message.TenantID) {
			out = append(out, cloneMQTTRetained(message))
		}
	}
	return out, nil
}

func (s *MemoryStateStore) ListMQTTRetainedPage(ctx context.Context, tenant, afterTenant, afterTopic string, limit int) ([]MQTTRetainedMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, ErrInvalidMessage
	}
	resolved, err := resolveTenant(ctx, tenant)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	start := 0
	if afterTenant != "" || afterTopic != "" {
		start = sort.Search(len(s.retainedOrder), func(i int) bool {
			current := s.retainedOrder[i]
			return current.tenant > afterTenant || (current.tenant == afterTenant && current.topic > afterTopic)
		})
	}
	out := make([]MQTTRetainedMessage, 0, limit)
	for i := start; i < len(s.retainedOrder) && len(out) < limit; i++ {
		orderKey := s.retainedOrder[i]
		if tenant != "" && orderKey.tenant != resolved {
			if orderKey.tenant > resolved {
				break
			}
			continue
		}
		message := s.retained[mqttRetainedKey(orderKey.tenant, orderKey.topic)]
		if tenantAllowed(ctx, message.TenantID) {
			out = append(out, cloneMQTTRetained(message))
		}
	}
	return out, nil
}

func (s *MemoryStateStore) DeleteMQTTRetained(ctx context.Context, tenant, topic string) error {
	resolved, err := resolveTenant(ctx, tenant)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	key := mqttRetainedKey(resolved, topic)
	if message, ok := s.retained[key]; ok && !tenantAllowed(ctx, message.TenantID) {
		return ErrUnauthorized
	}
	if _, exists := s.retained[key]; exists {
		index := sort.Search(len(s.retainedOrder), func(i int) bool {
			current := s.retainedOrder[i]
			return current.tenant > resolved || (current.tenant == resolved && current.topic >= topic)
		})
		if index < len(s.retainedOrder) && s.retainedOrder[index] == (mqttRetainedOrderKey{tenant: resolved, topic: topic}) {
			copy(s.retainedOrder[index:], s.retainedOrder[index+1:])
			s.retainedOrder = s.retainedOrder[:len(s.retainedOrder)-1]
		}
	}
	delete(s.retained, key)
	return nil
}

func (s *MemoryStateStore) PutMQTTSession(ctx context.Context, session MQTTSession) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if session.Key == "" || session.ClientID == "" {
		return ErrInvalidMessage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.mqtt[session.Key] = cloneMQTTSession(session)
	return nil
}

func (s *MemoryStateStore) GetMQTTSession(ctx context.Context, key string) (MQTTSession, bool, error) {
	if err := ctx.Err(); err != nil {
		return MQTTSession{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return MQTTSession{}, false, ErrClosed
	}
	session, ok := s.mqtt[key]
	return cloneMQTTSession(session), ok, nil
}

func (s *MemoryStateStore) DeleteMQTTSession(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	delete(s.mqtt, key)
	return nil
}

func cloneMQTTSession(session MQTTSession) MQTTSession {
	if session.Subscriptions != nil {
		subscriptions := make(map[string]uint8, len(session.Subscriptions))
		for filter, qos := range session.Subscriptions {
			subscriptions[filter] = qos
		}
		session.Subscriptions = subscriptions
	}
	if session.QoS2InFlight != nil {
		inFlight := make(map[uint16]MQTTQoS2Pending, len(session.QoS2InFlight))
		for packetID, pending := range session.QoS2InFlight {
			pending.Payload = append([]byte(nil), pending.Payload...)
			inFlight[packetID] = pending
		}
		session.QoS2InFlight = inFlight
	}
	if session.QoS1InFlight != nil {
		inFlight := make(map[uint16]MQTTQoS1Pending, len(session.QoS1InFlight))
		for packetID, pending := range session.QoS1InFlight {
			pending.Payload = append([]byte(nil), pending.Payload...)
			inFlight[packetID] = pending
		}
		session.QoS1InFlight = inFlight
	}
	if session.QoS2OutInFlight != nil {
		inFlight := make(map[uint16]MQTTQoS2OutboundPending, len(session.QoS2OutInFlight))
		for packetID, pending := range session.QoS2OutInFlight {
			pending.Payload = append([]byte(nil), pending.Payload...)
			inFlight[packetID] = pending
		}
		session.QoS2OutInFlight = inFlight
	}
	return session
}

func (s *MemoryStateStore) PutOutboxEntry(ctx context.Context, entry DurableOutboxEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry.ID == "" {
		return ErrInvalidMessage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.outbox[entry.ID] = cloneOutboxEntry(entry)
	return nil
}

func (s *MemoryStateStore) GetOutboxEntry(ctx context.Context, id string) (DurableOutboxEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return DurableOutboxEntry{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.outbox[id]
	return cloneOutboxEntry(entry), ok, nil
}

func (s *MemoryStateStore) ListOutboxEntries(ctx context.Context, limit int) ([]DurableOutboxEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DurableOutboxEntry, 0, len(s.outbox))
	for _, entry := range s.outbox {
		out = append(out, cloneOutboxEntry(entry))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *MemoryStateStore) ListDispatchableOutboxEntries(ctx context.Context, limit int) ([]DurableOutboxEntry, error) {
	entries, err := s.ListOutboxEntries(ctx, 0)
	if err != nil {
		return nil, err
	}
	out := make([]DurableOutboxEntry, 0, len(entries))
	now := time.Now()
	for _, entry := range entries {
		if outboxEntryDue(entry, now) {
			out = append(out, entry)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *MemoryStateStore) ClaimOutboxEntries(ctx context.Context, token string, limit int, lease time.Duration) ([]DurableOutboxEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if token == "" || lease <= 0 {
		return nil, ErrInvalidMessage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	now := time.Now()
	ready := make([]DurableOutboxEntry, 0)
	for _, entry := range s.outbox {
		if outboxEntryDue(entry, now) ||
			(entry.Status == OutboxInFlight && !entry.ClaimUntil.After(now) && dueAt(entry.NextAttemptAt, now)) {
			ready = append(ready, cloneOutboxEntry(entry))
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].CreatedAt.Equal(ready[j].CreatedAt) {
			return ready[i].ID < ready[j].ID
		}
		return ready[i].CreatedAt.Before(ready[j].CreatedAt)
	})
	if limit > 0 && len(ready) > limit {
		ready = ready[:limit]
	}
	claimed := make([]DurableOutboxEntry, 0, len(ready))
	for _, entry := range ready {
		entry.Status = OutboxInFlight
		entry.ClaimToken = token
		entry.ClaimUntil = now.Add(lease)
		entry.UpdatedAt = now
		s.outbox[entry.ID] = cloneOutboxEntry(entry)
		claimed = append(claimed, entry)
	}
	return claimed, nil
}

func outboxEntryDue(entry DurableOutboxEntry, now time.Time) bool {
	return entry.Status == OutboxPending ||
		(entry.Status == OutboxFailed && dueAt(entry.NextAttemptAt, now))
}

func dueAt(at, now time.Time) bool { return at.IsZero() || !at.After(now) }

func (s *MemoryStateStore) UpdateClaimedOutboxEntry(ctx context.Context, token string, entry DurableOutboxEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	current, ok := s.outbox[entry.ID]
	if !ok || current.Status != OutboxInFlight || current.ClaimToken != token {
		return ErrOutboxClaimLost
	}
	s.outbox[entry.ID] = cloneOutboxEntry(entry)
	return nil
}

func (s *MemoryStateStore) PruneCompletedOutboxEntries(ctx context.Context, before time.Time, limit int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if before.IsZero() || limit <= 0 {
		return nil, ErrInvalidMessage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	type completed struct {
		id        string
		updatedAt time.Time
	}
	var candidates []completed
	for id, entry := range s.outbox {
		if (entry.Status == OutboxPublished || entry.Status == OutboxDeadLetter) && entry.UpdatedAt.Before(before) {
			candidates = append(candidates, completed{id: id, updatedAt: entry.UpdatedAt})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].updatedAt.Equal(candidates[j].updatedAt) {
			return candidates[i].id < candidates[j].id
		}
		return candidates[i].updatedAt.Before(candidates[j].updatedAt)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		delete(s.outbox, candidate.id)
		ids = append(ids, candidate.id)
	}
	return ids, nil
}

func (s *MemoryStateStore) PutBrokerState(ctx context.Context, rec BrokerStateRecord) error {
	if rec.Name == "" {
		rec.Name = "broker"
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	rec.Metadata = cloneMap(rec.Metadata)
	s.brokers[rec.Name] = rec
	return nil
}
func (s *MemoryStateStore) GetBrokerState(ctx context.Context, name string) (BrokerStateRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.brokers[name]
	rec.Metadata = cloneMap(rec.Metadata)
	return rec, ok, nil
}
func (s *MemoryStateStore) PutTask(ctx context.Context, t TaskInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.tasks[t.ID] = cloneTask(t)
	return nil
}
func (s *MemoryStateStore) GetTask(ctx context.Context, id string) (TaskInfo, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	return cloneTask(t), ok, nil
}
func (s *MemoryStateStore) ListTasks(ctx context.Context, st TaskStatus, limit int) ([]TaskInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TaskInfo, 0, len(s.tasks))
	for _, t := range s.tasks {
		if st == "" || t.Status == st {
			out = append(out, cloneTask(t))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (s *MemoryStateStore) DeleteTask(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tasks, id)
	return nil
}
func (s *MemoryStateStore) PutConsumer(ctx context.Context, c ConsumerInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.consumers[c.ConsumerID] = cloneConsumerInfo(c)
	return nil
}
func (s *MemoryStateStore) GetConsumer(ctx context.Context, id string) (ConsumerInfo, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.consumers[id]
	return cloneConsumerInfo(c), ok, nil
}
func (s *MemoryStateStore) ListConsumers(ctx context.Context) ([]ConsumerInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ConsumerInfo, 0, len(s.consumers))
	for _, c := range s.consumers {
		out = append(out, cloneConsumerInfo(c))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConsumerID < out[j].ConsumerID })
	return out, nil
}
func (s *MemoryStateStore) DeleteConsumer(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.consumers, id)
	delete(s.events, id)
	return nil
}
func (s *MemoryStateStore) AddConsumerEvent(ctx context.Context, id string, n Notification) error {
	if id == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	evs := append(s.events[id], n)
	if s.eventCap > 0 && len(evs) > s.eventCap {
		evs = evs[len(evs)-s.eventCap:]
	}
	s.events[id] = evs
	return nil
}
func (s *MemoryStateStore) ConsumerEvents(ctx context.Context, id, after string, limit int) ([]Notification, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	evs := s.events[id]
	start := 0
	if after != "" {
		for i, n := range evs {
			if n.ID == after {
				start = i + 1
				break
			}
		}
	} else if limit > 0 && limit < len(evs) {
		start = len(evs) - limit
	}
	out := append([]Notification(nil), evs[start:]...)
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, nil
}
func (s *MemoryStateStore) PutPublisherRecord(ctx context.Context, rec PublisherRecord) error {
	if rec.ID == "" {
		rec.ID = newIDFromTime()
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now()
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = rec.CreatedAt
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	rec.Metadata = cloneMap(rec.Metadata)
	s.publishers[rec.ID] = rec
	return nil
}
func (s *MemoryStateStore) GetPublisherRecord(ctx context.Context, id string) (PublisherRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.publishers[id]
	rec.Metadata = cloneMap(rec.Metadata)
	return rec, ok, nil
}
func (s *MemoryStateStore) ListPublisherRecords(ctx context.Context, limit int) ([]PublisherRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]PublisherRecord, 0, len(s.publishers))
	for _, rec := range s.publishers {
		rec.Metadata = cloneMap(rec.Metadata)
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (s *MemoryStateStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
