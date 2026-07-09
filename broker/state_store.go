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

// StateStore persists broker/task/consumer/publisher control-plane state. The
// existing Store interface remains the high-performance message data-plane.
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
	mu         sync.RWMutex
	brokers    map[string]BrokerStateRecord
	tasks      map[string]TaskInfo
	consumers  map[string]ConsumerInfo
	events     map[string][]Notification
	publishers map[string]PublisherRecord
	eventCap   int
	closed     bool
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{brokers: map[string]BrokerStateRecord{}, tasks: map[string]TaskInfo{}, consumers: map[string]ConsumerInfo{}, events: map[string][]Notification{}, publishers: map[string]PublisherRecord{}, eventCap: 512}
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
