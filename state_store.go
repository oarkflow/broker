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
	// AddConsumerEvent ignores an empty consumer ID and fills a missing timestamp.
	AddConsumerEvent(context.Context, string, Notification) error
	// ConsumerEvents returns the most recent page when after is empty or unknown.
	// For a known cursor, it returns the next page in event order.
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
// Claims require a nonempty token, positive limit, and positive lease duration.
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

// MQTTWillRecord is the durable work needed to publish one accepted MQTT
// CONNECT Will after an unexpected disconnect or server restart. Principal
// fields contain authorization scope and quota metadata but never credentials.
type MQTTWillRecord struct {
	ID                string    `json:"id"`
	SessionKey        string    `json:"session_key"`
	ClientID          string    `json:"client_id"`
	PrincipalName     string    `json:"principal_name,omitempty"`
	Tenants           []string  `json:"tenants,omitempty"`
	RequestsPerSecond int       `json:"requests_per_second,omitempty"`
	Burst             int       `json:"burst,omitempty"`
	MaxConcurrent     int       `json:"max_concurrent,omitempty"`
	Topic             string    `json:"topic"`
	Payload           []byte    `json:"payload"`
	QoS               byte      `json:"qos"`
	Retain            bool      `json:"retain,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}

// MQTTWillStore persists CONNECT Wills independently of optional session
// persistence. List pages are ordered by ID and return the next ID cursor.
type MQTTWillStore interface {
	PutMQTTWill(context.Context, MQTTWillRecord) error
	DeleteMQTTWill(context.Context, string) error
	ListMQTTWills(context.Context, string, int) ([]MQTTWillRecord, string, error)
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

// AMQPExchangeRecord stores the properties of a durable AMQP exchange.
type AMQPExchangeRecord struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Durable bool   `json:"durable"`
}

// AMQPExchangeBindingRecord stores a durable exchange-to-exchange binding.
type AMQPExchangeBindingRecord struct {
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Destination     string          `json:"destination"`
	DestinationKind DestinationKind `json:"destination_kind"`
	RoutingKey      string          `json:"routing_key,omitempty"`
	TenantID        string          `json:"tenant_id,omitempty"`
	HeadersJSON     string          `json:"headers_json,omitempty"`
	HeadersMatchAll bool            `json:"headers_match_all,omitempty"`
}

type AMQPHeaderMatch struct {
	Name     string `json:"name"`
	Value    string `json:"value,omitempty"`
	HasValue bool   `json:"has_value,omitempty"`
}

// AMQPExchangeStore persists durable AMQP exchanges. It is optional so
// existing StateStore implementations remain source-compatible.
type AMQPExchangeStore interface {
	PutAMQPExchange(context.Context, AMQPExchangeRecord) error
	GetAMQPExchange(context.Context, string) (AMQPExchangeRecord, bool, error)
	ListAMQPExchanges(context.Context) ([]AMQPExchangeRecord, error)
	DeleteAMQPExchange(context.Context, string) error
}

// AMQPExchangeBindingStore persists bindings from durable AMQP exchanges to
// durable exchanges or queues.
type AMQPExchangeBindingStore interface {
	PutAMQPExchangeBinding(context.Context, AMQPExchangeBindingRecord) error
	ListAMQPExchangeBindings(context.Context) ([]AMQPExchangeBindingRecord, error)
	DeleteAMQPExchangeBinding(context.Context, string) error
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
	mqttWills     map[string]MQTTWillRecord
	retained      map[string]MQTTRetainedMessage
	amqpExchanges map[string]AMQPExchangeRecord
	amqpBindings  map[string]AMQPExchangeBindingRecord
	retainedOrder []mqttRetainedOrderKey
	eventCap      int
	closed        bool
}

type mqttRetainedOrderKey struct {
	tenant string
	topic  string
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{brokers: map[string]BrokerStateRecord{}, tasks: map[string]TaskInfo{}, consumers: map[string]ConsumerInfo{}, events: map[string][]Notification{}, publishers: map[string]PublisherRecord{}, outbox: map[string]DurableOutboxEntry{}, mqtt: map[string]MQTTSession{}, mqttWills: map[string]MQTTWillRecord{}, retained: map[string]MQTTRetainedMessage{}, amqpExchanges: map[string]AMQPExchangeRecord{}, amqpBindings: map[string]AMQPExchangeBindingRecord{}, eventCap: 512}
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
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if err := ctx.Err(); err != nil {
		return MQTTRetainedMessage{}, false, err
	}
	resolved, err := resolveTenant(ctx, tenant)
	if err != nil {
		return MQTTRetainedMessage{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return MQTTRetainedMessage{}, false, err
	}
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resolved, err := resolveTenant(ctx, tenant)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	resolved, err := resolveTenant(ctx, tenant)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if err := ctx.Err(); err != nil {
		return MQTTSession{}, false, err
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	delete(s.mqtt, key)
	return nil
}

func (s *MemoryStateStore) PutMQTTWill(ctx context.Context, record MQTTWillRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateMQTTWillRecord(record); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	if s.mqttWills == nil {
		s.mqttWills = make(map[string]MQTTWillRecord)
	}
	s.mqttWills[record.ID] = cloneMQTTWillRecord(record)
	return nil
}

func (s *MemoryStateStore) DeleteMQTTWill(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	delete(s.mqttWills, id)
	return nil
}

func (s *MemoryStateStore) ListMQTTWills(ctx context.Context, after string, limit int) ([]MQTTWillRecord, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if limit <= 0 || limit > 1000 {
		return nil, "", ErrInvalidMessage
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if s.closed {
		return nil, "", ErrClosed
	}
	ids := make([]string, 0, len(s.mqttWills))
	for id := range s.mqttWills {
		if id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		next := ids[limit-1]
		ids = ids[:limit]
		out := make([]MQTTWillRecord, len(ids))
		for i, id := range ids {
			out[i] = cloneMQTTWillRecord(s.mqttWills[id])
		}
		return out, next, nil
	}
	out := make([]MQTTWillRecord, len(ids))
	for i, id := range ids {
		out[i] = cloneMQTTWillRecord(s.mqttWills[id])
	}
	return out, "", nil
}

func cloneMQTTWillRecord(record MQTTWillRecord) MQTTWillRecord {
	record.Tenants = append([]string(nil), record.Tenants...)
	record.Payload = append([]byte(nil), record.Payload...)
	return record
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
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if err := ctx.Err(); err != nil {
		return DurableOutboxEntry{}, false, err
	}
	if s.closed {
		return DurableOutboxEntry{}, false, ErrClosed
	}
	entry, ok := s.outbox[id]
	return cloneOutboxEntry(entry), ok, nil
}

func (s *MemoryStateStore) ListOutboxEntries(ctx context.Context, limit int) ([]DurableOutboxEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrClosed
	}
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
	if token == "" || limit <= 0 || lease <= 0 {
		return nil, ErrInvalidMessage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if rec.Name == "" {
		rec.Name = "broker"
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	rec.Metadata = cloneMap(rec.Metadata)
	s.brokers[rec.Name] = rec
	return nil
}
func (s *MemoryStateStore) GetBrokerState(ctx context.Context, name string) (BrokerStateRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return BrokerStateRecord{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return BrokerStateRecord{}, false, err
	}
	if s.closed {
		return BrokerStateRecord{}, false, ErrClosed
	}
	rec, ok := s.brokers[name]
	rec.Metadata = cloneMap(rec.Metadata)
	return rec, ok, nil
}

func (s *MemoryStateStore) PutAMQPExchange(ctx context.Context, exchange AMQPExchangeRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if exchange.Name == "" || exchange.Type == "" || !exchange.Durable {
		return ErrInvalidMessage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	if existing, ok := s.amqpExchanges[exchange.Name]; ok && existing != exchange {
		return ErrInvalidMessage
	}
	s.amqpExchanges[exchange.Name] = exchange
	return nil
}

func (s *MemoryStateStore) GetAMQPExchange(ctx context.Context, name string) (AMQPExchangeRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return AMQPExchangeRecord{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return AMQPExchangeRecord{}, false, err
	}
	if s.closed {
		return AMQPExchangeRecord{}, false, ErrClosed
	}
	exchange, ok := s.amqpExchanges[name]
	return exchange, ok, nil
}

func (s *MemoryStateStore) ListAMQPExchanges(ctx context.Context) ([]AMQPExchangeRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrClosed
	}
	exchanges := make([]AMQPExchangeRecord, 0, len(s.amqpExchanges))
	for _, exchange := range s.amqpExchanges {
		exchanges = append(exchanges, exchange)
	}
	sort.Slice(exchanges, func(i, j int) bool { return exchanges[i].Name < exchanges[j].Name })
	return exchanges, nil
}

func (s *MemoryStateStore) DeleteAMQPExchange(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	delete(s.amqpExchanges, name)
	for id, binding := range s.amqpBindings {
		if binding.Source == name || (binding.DestinationKind == DestinationExchange && binding.Destination == name) {
			delete(s.amqpBindings, id)
		}
	}
	return nil
}

func (s *MemoryStateStore) PutAMQPExchangeBinding(ctx context.Context, binding AMQPExchangeBindingRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if binding.ID == "" || binding.Source == "" || binding.Destination == "" {
		return ErrInvalidMessage
	}
	if binding.DestinationKind == "" {
		binding.DestinationKind = DestinationExchange
	}
	if binding.HeadersJSON == "" {
		binding.HeadersJSON = "[]"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	if existing, ok := s.amqpBindings[binding.ID]; ok && existing != binding {
		return ErrInvalidMessage
	}
	s.amqpBindings[binding.ID] = binding
	return nil
}

func (s *MemoryStateStore) ListAMQPExchangeBindings(ctx context.Context) ([]AMQPExchangeBindingRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrClosed
	}
	bindings := make([]AMQPExchangeBindingRecord, 0, len(s.amqpBindings))
	for _, binding := range s.amqpBindings {
		bindings = append(bindings, binding)
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].ID < bindings[j].ID })
	return bindings, nil
}

func (s *MemoryStateStore) DeleteAMQPExchangeBinding(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	delete(s.amqpBindings, id)
	return nil
}

func (s *MemoryStateStore) PutTask(ctx context.Context, t TaskInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	s.tasks[t.ID] = cloneTask(t)
	return nil
}
func (s *MemoryStateStore) GetTask(ctx context.Context, id string) (TaskInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return TaskInfo{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return TaskInfo{}, false, err
	}
	if s.closed {
		return TaskInfo{}, false, ErrClosed
	}
	t, ok := s.tasks[id]
	return cloneTask(t), ok, nil
}
func (s *MemoryStateStore) ListTasks(ctx context.Context, st TaskStatus, limit int) ([]TaskInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrClosed
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	delete(s.tasks, id)
	return nil
}
func (s *MemoryStateStore) PutConsumer(ctx context.Context, c ConsumerInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	s.consumers[c.ConsumerID] = cloneConsumerInfo(c)
	return nil
}
func (s *MemoryStateStore) GetConsumer(ctx context.Context, id string) (ConsumerInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return ConsumerInfo{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return ConsumerInfo{}, false, err
	}
	if s.closed {
		return ConsumerInfo{}, false, ErrClosed
	}
	c, ok := s.consumers[id]
	return cloneConsumerInfo(c), ok, nil
}
func (s *MemoryStateStore) ListConsumers(ctx context.Context) ([]ConsumerInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrClosed
	}
	out := make([]ConsumerInfo, 0, len(s.consumers))
	for _, c := range s.consumers {
		out = append(out, cloneConsumerInfo(c))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConsumerID < out[j].ConsumerID })
	return out, nil
}
func (s *MemoryStateStore) DeleteConsumer(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	delete(s.consumers, id)
	delete(s.events, id)
	return nil
}
func (s *MemoryStateStore) AddConsumerEvent(ctx context.Context, id string, n Notification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" {
		return nil
	}
	if n.Timestamp.IsZero() {
		n.Timestamp = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	evs := append(s.events[id], cloneNotification(n))
	if s.eventCap > 0 && len(evs) > s.eventCap {
		evs = evs[len(evs)-s.eventCap:]
	}
	s.events[id] = evs
	return nil
}
func (s *MemoryStateStore) ConsumerEvents(ctx context.Context, id, after string, limit int) ([]Notification, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrClosed
	}
	evs := s.events[id]
	start := 0
	foundCursor := false
	if after != "" {
		for i, n := range evs {
			if n.ID == after {
				start = i + 1
				foundCursor = true
				break
			}
		}
	}
	if after == "" && limit > 0 && limit < len(evs) {
		start = len(evs) - limit
	}
	out := make([]Notification, len(evs)-start)
	for i, event := range evs[start:] {
		out[i] = cloneNotification(event)
	}
	if limit > 0 && len(out) > limit {
		if foundCursor {
			out = out[:limit]
		} else {
			out = out[len(out)-limit:]
		}
	}
	return out, nil
}
func (s *MemoryStateStore) PutPublisherRecord(ctx context.Context, rec PublisherRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	rec.Metadata = cloneMap(rec.Metadata)
	s.publishers[rec.ID] = rec
	return nil
}
func (s *MemoryStateStore) GetPublisherRecord(ctx context.Context, id string) (PublisherRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return PublisherRecord{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return PublisherRecord{}, false, err
	}
	if s.closed {
		return PublisherRecord{}, false, ErrClosed
	}
	rec, ok := s.publishers[id]
	rec.Metadata = cloneMap(rec.Metadata)
	return rec, ok, nil
}
func (s *MemoryStateStore) ListPublisherRecords(ctx context.Context, limit int) ([]PublisherRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrClosed
	}
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
