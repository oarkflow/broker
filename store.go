package broker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Store interface {
	DeclareQueue(context.Context, QueueConfig) error
	DeclareTopic(context.Context, TopicConfig) error
	DeclareStream(context.Context, StreamConfig) error
	Append(context.Context, Message, []Destination) ([]Message, error)
	Lease(context.Context, LeaseRequest) ([]Message, error)
	Ack(context.Context, AckRequest) error
	Nack(context.Context, NackRequest) error
	ExtendLease(context.Context, LeaseRequest) error
	Update(context.Context, Message) error
	Get(context.Context, string) (Message, bool, error)
	History(context.Context, string) ([]MessageHistory, error)
	ListQueue(context.Context, string, int) ([]Message, error)
	ListDLQ(context.Context, string, int) ([]Message, error)
	Move(context.Context, MoveRequest) (MoveResult, error)
	PauseQueue(context.Context, string) error
	ResumeQueue(context.Context, string) error
	PurgeQueue(context.Context, string) (int, error)
	ReadStream(context.Context, StreamReadRequest) ([]Message, error)
	CommitOffset(context.Context, OffsetCommit) error
	ConsumerOffset(context.Context, OffsetCommit) (uint64, error)
	Stats(context.Context) (StoreStats, error)
	SweepLeases(context.Context, time.Time) (int, error)
	Close() error
}

type AppendEntry struct {
	Message      Message       `json:"message"`
	Destinations []Destination `json:"destinations"`
}

type AtomicStore interface {
	AppendAtomic(context.Context, []AppendEntry) ([]Message, error)
}

type MinimalAtomicStore interface {
	AppendAtomicMinimal(context.Context, []AppendEntry) error
}

// OwnedAtomicStore is the zero-copy batch append path. Ownership of message
// payload/header/metadata slices and maps transfers to the store; callers must
// not mutate entries after the method returns.
type OwnedAtomicStore interface {
	AppendAtomicOwned(context.Context, []AppendEntry) error
}

type BatchAckStore interface {
	AckBatch(context.Context, []AckRequest) []error
}

type BatchUpdateStore interface {
	UpdateBatch(context.Context, []Message) []error
}

type LeaseRequest struct {
	Queue             string
	ConsumerID        string
	ConsumerGroup     string
	Limit             int
	VisibilityTimeout time.Duration
	LeaseToken        string
	MessageID         string
	TenantID          string `json:"tenant_id,omitempty"`
}

type AckRequest struct {
	MessageID  string `json:"message_id,omitempty"`
	Queue      string `json:"queue,omitempty"`
	LeaseToken string `json:"lease_token,omitempty"`
	ConsumerID string `json:"consumer_id,omitempty"`
}

type NackAction string

const (
	NackRequeue NackAction = "requeue"
	NackRetry   NackAction = "retry"
	NackDLQ     NackAction = "dlq"
	NackReject  NackAction = "reject"
)

type NackRequest struct {
	MessageID        string        `json:"message_id,omitempty"`
	Queue            string        `json:"queue,omitempty"`
	LeaseToken       string        `json:"lease_token,omitempty"`
	ConsumerID       string        `json:"consumer_id,omitempty"`
	Action           NackAction    `json:"action,omitempty"`
	Reason           string        `json:"reason,omitempty"`
	Error            string        `json:"error,omitempty"`
	Delay            time.Duration `json:"delay,omitempty"`
	DLQ              string        `json:"dlq,omitempty"`
	ResetLease       bool          `json:"reset_lease,omitempty"`
	IncrementAttempt bool          `json:"increment_attempt,omitempty"`
}

type MoveMode string

const (
	MoveRemoveSource MoveMode = "move"
	MoveCopy         MoveMode = "copy"
	MoveReplay       MoveMode = "replay"
	MoveReroute      MoveMode = "reroute"
	MoveQuarantine   MoveMode = "quarantine"
	MovePark         MoveMode = "park"
	MoveArchive      MoveMode = "archive"
)

type StreamReadRequest struct {
	Stream        string `json:"stream"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
	AfterOffset   uint64 `json:"after_offset,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	TenantID      string `json:"tenant_id,omitempty"`
}

type OffsetCommit struct {
	Stream        string `json:"stream"`
	ConsumerGroup string `json:"consumer_group"`
	Offset        uint64 `json:"offset"`
	TenantID      string `json:"tenant_id,omitempty"`
}

type MoveRequest struct {
	SourceType      DestinationKind `json:"source_type"`
	Source          string          `json:"source"`
	DestinationType DestinationKind `json:"destination_type"`
	Destination     string          `json:"destination"`
	MessageIDs      []string        `json:"message_ids,omitempty"`
	Filter          MessageFilter   `json:"filter,omitempty"`
	Mode            MoveMode        `json:"mode"`
	PreserveID      bool            `json:"preserve_id"`
	PreserveOffset  bool            `json:"preserve_offset"`
	ResetAttempts   bool            `json:"reset_attempts"`
	ResetLease      bool            `json:"reset_lease"`
	ResetError      bool            `json:"reset_error"`
	Reason          string          `json:"reason,omitempty"`
	Actor           string          `json:"actor,omitempty"`
	DryRun          bool            `json:"dry_run"`
}

type MessageFilter struct {
	Status   MessageStatus     `json:"status,omitempty"`
	Type     string            `json:"type,omitempty"`
	TenantID string            `json:"tenant_id,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
}

type MoveResult struct {
	Moved       int      `json:"moved"`
	Copied      int      `json:"copied"`
	Skipped     int      `json:"skipped"`
	MessageIDs  []string `json:"message_ids,omitempty"`
	Destination string   `json:"destination"`
	DryRun      bool     `json:"dry_run"`
}

type StoreStats struct {
	Queues     map[string]int               `json:"queues"`
	Topics     map[string]int               `json:"topics"`
	Streams    map[string]int               `json:"streams"`
	Offsets    map[string]map[string]uint64 `json:"offsets,omitempty"`
	DLQ        map[string]int               `json:"dlq"`
	Messages   int                          `json:"messages"`
	Leased     int                          `json:"leased"`
	DeadLetter int                          `json:"dead_letter"`
}

type memoryQueue struct {
	cfg         QueueConfig
	ids         []string
	head        int
	depth       int
	hasPriority bool
	state       QueueState
}

type memoryTopic struct {
	cfg   TopicConfig
	ids   []string
	state QueueState
}

type memoryStream struct {
	cfg    StreamConfig
	ids    []string
	offset uint64
}

type MemoryStore struct {
	mu              sync.RWMutex
	queues          map[string]*memoryQueue
	topics          map[string]*memoryTopic
	streams         map[string]*memoryStream
	messages        map[string]*Message
	history         map[string][]MessageHistory
	offsets         map[string]map[string]uint64
	dedup           map[string]time.Time
	idemp           map[string]string
	rand            *mathrand.Rand
	historyLimit    int
	retainCompleted bool
	completed       map[string]struct{}
	completedOrder  []string
	completedNext   int
	completedLimit  int
	closed          bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{queues: map[string]*memoryQueue{}, topics: map[string]*memoryTopic{}, streams: map[string]*memoryStream{}, messages: map[string]*Message{}, history: map[string][]MessageHistory{}, offsets: map[string]map[string]uint64{}, dedup: map[string]time.Time{}, idemp: map[string]string{}, rand: mathrand.New(mathrand.NewSource(time.Now().UnixNano())), historyLimit: 32, retainCompleted: true, completed: map[string]struct{}{}, completedLimit: 65536}
}

func (*MemoryStore) ConfirmationLevel() ConfirmLevel { return ConfirmEnqueued }

func (s *MemoryStore) addCompletedTombstoneLocked(id string) {
	if id == "" || s.completedLimit <= 0 {
		return
	}
	if _, exists := s.completed[id]; exists {
		return
	}
	if len(s.completedOrder) < s.completedLimit {
		s.completedOrder = append(s.completedOrder, id)
	} else {
		old := s.completedOrder[s.completedNext]
		delete(s.completed, old)
		s.completedOrder[s.completedNext] = id
		s.completedNext++
		if s.completedNext == len(s.completedOrder) {
			s.completedNext = 0
		}
	}
	s.completed[id] = struct{}{}
}

func (s *MemoryStore) wasCompleted(id string) bool {
	s.mu.RLock()
	_, ok := s.completed[id]
	s.mu.RUnlock()
	return ok
}

func (s *MemoryStore) SetRetainCompleted(retain bool) {
	s.mu.Lock()
	s.retainCompleted = retain
	if !retain {
		for id, m := range s.messages {
			if m != nil && (m.Status == MessageCompleted || m.Status == MessagePurged) {
				if m.Status == MessageCompleted {
					s.addCompletedTombstoneLocked(id)
				}
				delete(s.messages, id)
				delete(s.history, id)
			}
		}
		for _, q := range s.queues {
			s.advanceQueueHeadLocked(q)
		}
	}
	s.mu.Unlock()
}

func (s *MemoryStore) SetHistoryLimit(limit int) {
	if limit < 0 {
		limit = 0
	}
	s.mu.Lock()
	s.historyLimit = limit
	if limit == 0 {
		s.history = map[string][]MessageHistory{}
	}
	s.mu.Unlock()
}

func (s *MemoryStore) DeclareQueue(ctx context.Context, cfg QueueConfig) error {
	if hasRestrictedTenantScope(ctx) {
		return errf(ErrUnauthorized, "tenant-scoped credentials cannot alter shared queue configuration")
	}
	if cfg.Name == "" {
		return errf(ErrInvalidMessage, "queue name required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.queues[cfg.Name]; ok {
		q.cfg = mergeQueueConfig(q.cfg, cfg)
		if q.state == "" {
			q.state = QueueActive
		}
		return nil
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = 30 * time.Second
	}
	if cfg.RetryPolicy.MaxAttempts <= 0 {
		cfg.RetryPolicy = DefaultRetryPolicy()
	}
	s.queues[cfg.Name] = &memoryQueue{cfg: cfg, state: QueueActive}
	return nil
}

func (s *MemoryStore) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	if hasRestrictedTenantScope(ctx) {
		return errf(ErrUnauthorized, "tenant-scoped credentials cannot alter shared topic configuration")
	}
	if cfg.Name == "" {
		return errf(ErrInvalidMessage, "topic name required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.topics[cfg.Name]; ok {
		t.cfg = cfg
		return nil
	}
	s.topics[cfg.Name] = &memoryTopic{cfg: cfg, state: QueueActive}
	return nil
}

func (s *MemoryStore) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	if hasRestrictedTenantScope(ctx) {
		return errf(ErrUnauthorized, "tenant-scoped credentials cannot alter shared stream configuration")
	}
	if cfg.Name == "" {
		return errf(ErrInvalidMessage, "stream name required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.streams[cfg.Name]; ok {
		st.cfg = cfg
		return nil
	}
	s.streams[cfg.Name] = &memoryStream{cfg: cfg}
	return nil
}

func mergeQueueConfig(old, new QueueConfig) QueueConfig {
	if new.Name != "" {
		old.Name = new.Name
	}
	if new.Workers > 0 {
		old.Workers = new.Workers
	}
	if new.MaxInFlight > 0 {
		old.MaxInFlight = new.MaxInFlight
	}
	if new.MaxDepth > 0 {
		old.MaxDepth = new.MaxDepth
	}
	if new.VisibilityTimeout > 0 {
		old.VisibilityTimeout = new.VisibilityTimeout
	}
	if new.AckMode != "" {
		old.AckMode = new.AckMode
	}
	if new.RetryPolicy.MaxAttempts > 0 {
		old.RetryPolicy = new.RetryPolicy
	}
	if new.DLQ != "" {
		old.DLQ = new.DLQ
	}
	if new.Ordering != "" {
		old.Ordering = new.Ordering
	}
	if new.Backpressure != "" {
		old.Backpressure = new.Backpressure
	}
	return old
}

func (s *MemoryStore) Append(ctx context.Context, msg Message, dests []Destination) ([]Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	tenant, err := resolveTenant(ctx, msg.TenantID)
	if err != nil {
		return nil, err
	}
	msg.TenantID = tenant
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLockedCollect(msg, dests, true, false, false)
}

func (s *MemoryStore) appendLocked(msg Message, dests []Destination) ([]Message, error) {
	return s.appendLockedCollect(msg, dests, true, false, false)
}

func (s *MemoryStore) appendLockedCollect(msg Message, dests []Destination, collect, owned, replay bool) ([]Message, error) {
	if s.closed {
		return nil, ErrClosed
	}
	if msg.ID == "" {
		return nil, errf(ErrInvalidMessage, "message id required")
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	if msg.AvailableAt.IsZero() {
		msg.AvailableAt = msg.CreatedAt
	}
	if msg.Status == "" {
		msg.Status = MessageAccepted
	}
	if !replay && msg.DeduplicationID != "" {
		if until, ok := s.dedup[msg.DeduplicationID]; ok && until.After(time.Now()) {
			return nil, ErrDuplicateMessage
		}
	}
	if !replay && msg.IdempotencyKey != "" {
		if existing, ok := s.idemp[msg.IdempotencyKey]; ok && existing != "" {
			return nil, ErrDuplicateMessage
		}
	}
	if len(dests) == 0 {
		dests = []Destination{{Kind: DestinationTopic, Name: msg.Topic}}
	}
	var out []Message
	if collect {
		out = make([]Message, 0, len(dests))
	}
	copyNo := 0
	now := time.Now()
	for _, d := range dests {
		if d.Name == "" {
			continue
		}
		var m Message
		if owned && copyNo == 0 {
			m = msg
		} else {
			m = cloneMessage(msg)
		}
		if copyNo > 0 {
			m.ParentID = msg.ID
			m.ID = msg.ID + ":copy:" + itoa(copyNo)
		}
		copyNo++
		switch d.Kind {
		case DestinationTopic:
			if _, ok := s.topics[d.Name]; !ok {
				s.topics[d.Name] = &memoryTopic{cfg: TopicConfig{Name: d.Name}, state: QueueActive}
			}
			m.Topic = d.Name
			m.Status = MessagePersisted
			s.messages[m.ID] = &m
			s.topics[d.Name].ids = append(s.topics[d.Name].ids, m.ID)
			s.addHistoryLocked(m.ID, MessagePersisted, "topic_append", "", d.Name, "", "", "")
		case DestinationQueue, DestinationDLQ:
			if _, ok := s.queues[d.Name]; !ok {
				s.queues[d.Name] = &memoryQueue{cfg: QueueConfig{Name: d.Name, VisibilityTimeout: 30 * time.Second, RetryPolicy: DefaultRetryPolicy()}, state: QueueActive}
			}
			q := s.queues[d.Name]
			if q.cfg.MaxDepth > 0 && q.depth >= q.cfg.MaxDepth {
				if handled, err := s.applyBackpressureLocked(q, d.Name, &m); handled || err != nil {
					if err != nil {
						return out, err
					}
					if collect {
						out = append(out, cloneMessage(m))
					}
					continue
				}
			}
			m.Queue = d.Name
			if d.Kind == DestinationDLQ {
				m.Status = MessageDeadLettered
			} else if m.AvailableAt.After(now) {
				m.Status = MessageScheduled
			} else {
				m.Status = MessageQueued
			}
			m.LeaseToken = ""
			m.LeaseUntil = time.Time{}
			s.messages[m.ID] = &m
			q.ids = append(q.ids, m.ID)
			q.depth++
			if m.Priority != 0 {
				q.hasPriority = true
			}
			s.addHistoryLocked(m.ID, m.Status, "queue_append", "", d.Name, "", "", "")
		case DestinationStream, DestinationArchive:
			if _, ok := s.streams[d.Name]; !ok {
				s.streams[d.Name] = &memoryStream{cfg: StreamConfig{Name: d.Name}}
			}
			st := s.streams[d.Name]
			st.offset++
			m.Stream = d.Name
			m.Offset = st.offset
			m.Status = MessagePersisted
			s.messages[m.ID] = &m
			st.ids = append(st.ids, m.ID)
			s.addHistoryLocked(m.ID, MessagePersisted, "stream_append", "", d.Name, "", "", "")
		}
		if collect {
			out = append(out, cloneMessage(m))
		}
	}
	if msg.DeduplicationID != "" {
		s.dedup[msg.DeduplicationID] = time.Now().Add(dedupWindow(msg))
	}
	if msg.IdempotencyKey != "" {
		s.idemp[msg.IdempotencyKey] = msg.ID
	}
	return out, nil
}

func (s *MemoryStore) AppendAtomic(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	return s.appendAtomic(ctx, entries, true, false)
}

func (s *MemoryStore) AppendAtomicMinimal(ctx context.Context, entries []AppendEntry) error {
	_, err := s.appendAtomic(ctx, entries, false, false)
	return err
}

func (s *MemoryStore) AppendAtomicOwned(ctx context.Context, entries []AppendEntry) error {
	_, err := s.appendAtomic(ctx, entries, false, true)
	return err
}

// storedAppendEntries captures the exact IDs, timestamps, offsets and copy
// metadata assigned by the in-memory index before the durable acknowledgement.
// Replaying caller inputs would lose those generated values.
func storedAppendEntries(messages []Message) []AppendEntry {
	entries := make([]AppendEntry, 0, len(messages))
	for _, message := range messages {
		// Drop-newest is intentionally not durable state: the incoming record was
		// rejected by the queue's capacity policy and is never deliverable.
		if message.Status == MessagePurged && message.Queue != "" {
			continue
		}
		var destination Destination
		switch {
		case message.Queue != "":
			destination = Destination{Kind: DestinationQueue, Name: message.Queue}
			if message.Status == MessageDeadLettered {
				destination.Kind = DestinationDLQ
			}
		case message.Stream != "":
			destination = Destination{Kind: DestinationStream, Name: message.Stream}
		case message.Topic != "":
			destination = Destination{Kind: DestinationTopic, Name: message.Topic}
		default:
			continue
		}
		entries = append(entries, AppendEntry{Message: message, Destinations: []Destination{destination}})
	}
	return entries
}

// prepareAppendAtomic validates an append and assigns its exact stored IDs,
// timestamps, destinations and stream offsets without changing visible state.
// Durable stores persist this prepared form before installing it in memory, so
// a failed fsync cannot expose an unacknowledged record.
func (s *MemoryStore) prepareAppendAtomic(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	entries = append([]AppendEntry(nil), entries...)
	for i := range entries {
		tenant, err := resolveTenant(ctx, entries[i].Message.TenantID)
		if err != nil {
			return nil, err
		}
		entries[i].Message.TenantID = tenant
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.validateAtomicLocked(entries); err != nil {
		return nil, err
	}
	now := time.Now()
	streamOffsets := make(map[string]uint64)
	queueDepths := make(map[string]int)
	out := make([]Message, 0, len(entries))
	for _, entry := range entries {
		msg := cloneMessage(entry.Message)
		if msg.CreatedAt.IsZero() {
			msg.CreatedAt = now
		}
		if msg.AvailableAt.IsZero() {
			msg.AvailableAt = msg.CreatedAt
		}
		if msg.Status == "" {
			msg.Status = MessageAccepted
		}
		dests := entry.Destinations
		if len(dests) == 0 {
			dests = []Destination{{Kind: DestinationTopic, Name: msg.Topic}}
		}
		copyNo := 0
		for _, dest := range dests {
			if dest.Name == "" {
				continue
			}
			m := cloneMessage(msg)
			if copyNo > 0 {
				m.ParentID = msg.ID
				m.ID = msg.ID + ":copy:" + itoa(copyNo)
			}
			copyNo++
			switch dest.Kind {
			case DestinationTopic:
				m.Topic = dest.Name
				m.Status = MessagePersisted
			case DestinationQueue, DestinationDLQ:
				m.Queue = dest.Name
				m.LeaseToken = ""
				m.LeaseUntil = time.Time{}
				if dest.Kind == DestinationDLQ {
					m.Status = MessageDeadLettered
				} else if m.AvailableAt.After(now) {
					m.Status = MessageScheduled
				} else {
					m.Status = MessageQueued
				}
				q := s.queues[dest.Name]
				depth, seen := queueDepths[dest.Name]
				if !seen && q != nil {
					depth = q.depth
				}
				if q != nil && q.cfg.MaxDepth > 0 && depth >= q.cfg.MaxDepth {
					switch q.cfg.Backpressure {
					case BackpressureDropNewest:
						m.Status = MessagePurged
					case BackpressureDLQ:
						m.OriginalQueue = firstNonEmpty(m.OriginalQueue, dest.Name)
						m.Queue = firstNonEmpty(q.cfg.DLQ, dest.Name+".dlq")
						m.Status = MessageDeadLettered
						m.DeadLetterReason = "backpressure"
					default:
						// Drop-oldest keeps the queue at the configured depth;
						// rejecting/blocking policies were rejected by validation.
					}
				} else {
					queueDepths[dest.Name] = depth + 1
				}
			case DestinationStream, DestinationArchive:
				offset, seen := streamOffsets[dest.Name]
				if !seen {
					if stream := s.streams[dest.Name]; stream != nil {
						offset = stream.offset
					}
				}
				offset++
				streamOffsets[dest.Name] = offset
				m.Stream = dest.Name
				m.Offset = offset
				m.Status = MessagePersisted
			}
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *MemoryStore) appendAtomic(ctx context.Context, entries []AppendEntry, collect, owned bool) ([]Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	entries = append([]AppendEntry(nil), entries...)
	for i := range entries {
		tenant, err := resolveTenant(ctx, entries[i].Message.TenantID)
		if err != nil {
			return nil, err
		}
		entries[i].Message.TenantID = tenant
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateAtomicLocked(entries); err != nil {
		return nil, err
	}
	var out []Message
	if collect {
		out = make([]Message, 0, len(entries))
	}
	for _, entry := range entries {
		stored, err := s.appendLockedCollect(entry.Message, entry.Destinations, collect, owned, false)
		if err != nil {
			return nil, err
		}
		if collect {
			out = append(out, stored...)
		}
	}
	return out, nil
}

func (s *MemoryStore) appendRecoveredAtomic(entries []AppendEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range entries {
		if _, err := s.appendLockedCollect(entry.Message, entry.Destinations, false, false, true); err != nil {
			return err
		}
	}
	return nil
}

func (s *MemoryStore) validateAtomicLocked(entries []AppendEntry) error {
	if s.closed {
		return ErrClosed
	}
	var seenDedup, seenIdemp map[string]struct{}
	var pendingDepth map[string]int
	now := time.Now()
	for _, entry := range entries {
		if entry.Message.ID == "" {
			return errf(ErrInvalidMessage, "message id required")
		}
		if id := entry.Message.DeduplicationID; id != "" {
			if seenDedup == nil {
				seenDedup = make(map[string]struct{})
			}
			if _, ok := seenDedup[id]; ok {
				return ErrDuplicateMessage
			}
			seenDedup[id] = struct{}{}
			if until, ok := s.dedup[id]; ok && until.After(now) {
				return ErrDuplicateMessage
			}
		}
		if key := entry.Message.IdempotencyKey; key != "" {
			if seenIdemp == nil {
				seenIdemp = make(map[string]struct{})
			}
			if _, ok := seenIdemp[key]; ok {
				return ErrDuplicateMessage
			}
			seenIdemp[key] = struct{}{}
			if existing := s.idemp[key]; existing != "" {
				return ErrDuplicateMessage
			}
		}
		for _, d := range entry.Destinations {
			if d.Kind != DestinationQueue && d.Kind != DestinationDLQ {
				continue
			}
			q := s.queues[d.Name]
			if q == nil || q.cfg.MaxDepth <= 0 {
				continue
			}
			if pendingDepth == nil {
				pendingDepth = make(map[string]int)
			}
			pendingDepth[d.Name]++
			if q.depth+pendingDepth[d.Name] > q.cfg.MaxDepth && q.cfg.Backpressure != BackpressureDropOldest && q.cfg.Backpressure != BackpressureDLQ && q.cfg.Backpressure != BackpressureDropNewest {
				return ErrBackpressure
			}
		}
	}
	return nil
}

func (s *MemoryStore) Lease(ctx context.Context, req LeaseRequest) ([]Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if req.Limit <= 0 {
		req.Limit = 1
	}
	if req.VisibilityTimeout <= 0 {
		req.VisibilityTimeout = 30 * time.Second
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[req.Queue]
	if !ok {
		return nil, ErrQueueNotFound
	}
	if q.state == QueuePaused {
		return nil, ErrPaused
	}
	out := make([]Message, 0, minInt(req.Limit, q.depth))

	if !q.hasPriority {
		for i := q.head; i < len(q.ids) && len(out) < req.Limit; i++ {
			m := s.messages[q.ids[i]]
			if m == nil || m.Queue != req.Queue || isTerminalQueueStatus(m.Status) {
				continue
			}
			if req.TenantID != "" && m.TenantID != req.TenantID {
				continue
			}
			if !m.ExpiresAt.IsZero() && now.After(m.ExpiresAt) {
				m.Status = MessageExpired
				m.LeaseToken = ""
				m.LeaseUntil = time.Time{}
				if q.depth > 0 {
					q.depth--
				}
				s.addHistoryLocked(m.ID, MessageExpired, "expired", req.Queue, "", "ttl expired", req.ConsumerID, "")
				continue
			}
			if !isLeaseableQueueStatus(m.Status) || (!m.AvailableAt.IsZero() && m.AvailableAt.After(now)) {
				continue
			}
			s.leaseMessageLocked(m, req, now)
			out = append(out, cloneMessage(*m))
		}
		s.advanceQueueHeadLocked(q)
		return out, nil
	}

	type candidate struct{ idx, priority int }
	candidates := make([]candidate, 0, minInt(req.Limit, q.depth))
	for i := q.head; i < len(q.ids); i++ {
		m := s.messages[q.ids[i]]
		if m == nil || m.Queue != req.Queue || isTerminalQueueStatus(m.Status) {
			continue
		}
		if req.TenantID != "" && m.TenantID != req.TenantID {
			continue
		}
		if !m.ExpiresAt.IsZero() && now.After(m.ExpiresAt) {
			m.Status = MessageExpired
			m.LeaseToken = ""
			m.LeaseUntil = time.Time{}
			if q.depth > 0 {
				q.depth--
			}
			s.addHistoryLocked(m.ID, MessageExpired, "expired", req.Queue, "", "ttl expired", req.ConsumerID, "")
			continue
		}
		if !isLeaseableQueueStatus(m.Status) || (!m.AvailableAt.IsZero() && m.AvailableAt.After(now)) {
			continue
		}
		if len(candidates) < req.Limit {
			candidates = append(candidates, candidate{i, m.Priority})
			for k := len(candidates) - 1; k > 0 && candidates[k].priority > candidates[k-1].priority; k-- {
				candidates[k], candidates[k-1] = candidates[k-1], candidates[k]
			}
		} else if m.Priority > candidates[len(candidates)-1].priority {
			candidates[len(candidates)-1] = candidate{i, m.Priority}
			for k := len(candidates) - 1; k > 0 && candidates[k].priority > candidates[k-1].priority; k-- {
				candidates[k], candidates[k-1] = candidates[k-1], candidates[k]
			}
		}
	}
	for _, c := range candidates {
		m := s.messages[q.ids[c.idx]]
		if m == nil {
			continue
		}
		s.leaseMessageLocked(m, req, now)
		out = append(out, cloneMessage(*m))
	}
	s.advanceQueueHeadLocked(q)
	return out, nil
}

func isLeaseableQueueStatus(status MessageStatus) bool {
	switch status {
	case MessageQueued, MessageScheduled, MessageAvailable, MessageRetryScheduled, MessageDeadLettered:
		return true
	default:
		return false
	}
}

func isTerminalQueueStatus(status MessageStatus) bool {
	switch status {
	case MessageCompleted, MessagePurged, MessageExpired, MessageMoved, MessageCancelled:
		return true
	default:
		return false
	}
}

func (s *MemoryStore) leaseMessageLocked(m *Message, req LeaseRequest, now time.Time) {
	m.Status = MessageLeased
	m.ConsumerID = req.ConsumerID
	m.ConsumerGroup = req.ConsumerGroup
	m.LeaseToken = newIDLocked(s, "lease")
	m.LeaseUntil = now.Add(req.VisibilityTimeout)
	s.addHistoryLocked(m.ID, MessageLeased, "leased", req.Queue, req.ConsumerID, "", req.ConsumerID, "")
}

func (s *MemoryStore) Ack(ctx context.Context, req AckRequest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if m := s.messages[req.MessageID]; m != nil && !tenantAllowed(ctx, m.TenantID) {
		return ErrUnauthorized
	}
	return s.ackLocked(req)
}

func (s *MemoryStore) ackLocked(req AckRequest) error {
	m, ok := s.messages[req.MessageID]
	if !ok {
		if _, completed := s.completed[req.MessageID]; completed {
			return nil
		}
		return ErrMessageNotFound
	}
	if m.Status == MessageCompleted {
		return nil
	}
	if m.LeaseToken != "" && (req.LeaseToken == "" || m.LeaseToken != req.LeaseToken) {
		return ErrLeaseLost
	}
	m.Status = MessageCompleted
	m.LeaseToken = ""
	m.LeaseUntil = time.Time{}
	s.removeFromQueueLocked(m.Queue, m.ID)
	s.addHistoryLocked(m.ID, MessageCompleted, "acked", req.Queue, "", "", req.ConsumerID, "")
	if !s.retainCompleted {
		s.addCompletedTombstoneLocked(m.ID)
		delete(s.messages, m.ID)
		delete(s.history, m.ID)
	}
	return nil
}

func (s *MemoryStore) AckBatch(ctx context.Context, reqs []AckRequest) []error {
	errs := make([]error, len(reqs))
	select {
	case <-ctx.Done():
		for i := range errs {
			errs[i] = ctx.Err()
		}
		return errs
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range reqs {
		if m := s.messages[reqs[i].MessageID]; m != nil && !tenantAllowed(ctx, m.TenantID) {
			errs[i] = ErrUnauthorized
		} else {
			errs[i] = s.ackLocked(reqs[i])
		}
	}
	return errs
}

func (s *MemoryStore) validateAckLease(ctx context.Context, req AckRequest) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.messages[req.MessageID]
	if !ok {
		if _, completed := s.completed[req.MessageID]; completed {
			return nil
		}
		return ErrMessageNotFound
	}
	if !tenantAllowed(ctx, m.TenantID) {
		return ErrUnauthorized
	}
	if m.LeaseToken != "" {
		if req.LeaseToken == "" || m.LeaseToken != req.LeaseToken {
			return ErrLeaseLost
		}
	}
	return nil
}

func (s *MemoryStore) validateNackLease(ctx context.Context, req NackRequest) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.messages[req.MessageID]
	if !ok {
		return ErrMessageNotFound
	}
	if !tenantAllowed(ctx, m.TenantID) {
		return ErrUnauthorized
	}
	if m.LeaseToken != "" {
		if req.LeaseToken == "" || m.LeaseToken != req.LeaseToken {
			return ErrLeaseLost
		}
	}
	return nil
}

func (s *MemoryStore) Nack(ctx context.Context, req NackRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.messages[req.MessageID]
	if !ok {
		return ErrMessageNotFound
	}
	if !tenantAllowed(ctx, m.TenantID) {
		return ErrUnauthorized
	}
	if m.LeaseToken != "" {
		if req.LeaseToken == "" || m.LeaseToken != req.LeaseToken {
			return ErrLeaseLost
		}
	}
	if req.IncrementAttempt {
		m.Attempts++
	}
	m.LastError = req.Error
	m.LeaseToken = ""
	m.LeaseUntil = time.Time{}
	switch req.Action {
	case NackDLQ, NackReject:
		dlq := req.DLQ
		if dlq == "" {
			dlq = m.Queue + ".dlq"
		}
		sourceQueue := m.Queue
		s.removeFromQueueLocked(sourceQueue, m.ID)
		m.OriginalQueue = firstNonEmpty(m.OriginalQueue, sourceQueue)
		m.Queue = dlq
		m.Status = MessageDeadLettered
		m.DeadLetterReason = req.Reason
		if _, ok := s.queues[dlq]; !ok {
			s.queues[dlq] = &memoryQueue{cfg: QueueConfig{Name: dlq}, state: QueueActive}
		}
		dlqQueue := s.queues[dlq]
		dlqQueue.ids = append(dlqQueue.ids, m.ID)
		dlqQueue.depth++
		if source := s.queues[sourceQueue]; source != nil {
			s.advanceQueueHeadLocked(source)
		}
		s.addHistoryLocked(m.ID, MessageDeadLettered, "dead_lettered", req.Queue, dlq, req.Reason, req.ConsumerID, req.Error)
	case NackRetry:
		m.Status = MessageRetryScheduled
		m.AvailableAt = time.Now().Add(req.Delay)
		s.addHistoryLocked(m.ID, MessageRetryScheduled, "retry_scheduled", req.Queue, req.Queue, req.Reason, req.ConsumerID, req.Error)
	default:
		m.Status = MessageQueued
		m.AvailableAt = time.Now().Add(req.Delay)
		s.addHistoryLocked(m.ID, MessageQueued, "nacked", req.Queue, req.Queue, req.Reason, req.ConsumerID, req.Error)
	}
	return nil
}

func (s *MemoryStore) ExtendLease(ctx context.Context, req LeaseRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.messages[req.MessageID]
	if !ok {
		return ErrMessageNotFound
	}
	if !tenantAllowed(ctx, m.TenantID) {
		return ErrUnauthorized
	}
	if m.LeaseToken != req.LeaseToken {
		return ErrLeaseLost
	}
	if req.VisibilityTimeout <= 0 {
		req.VisibilityTimeout = 30 * time.Second
	}
	m.LeaseUntil = time.Now().Add(req.VisibilityTimeout)
	s.addHistoryLocked(m.ID, MessageLeased, "lease_extended", m.Queue, req.ConsumerID, "", req.ConsumerID, "")
	return nil
}

func (s *MemoryStore) Update(ctx context.Context, msg Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.messages[msg.ID]
	if !ok {
		return ErrMessageNotFound
	}
	if !tenantAllowed(ctx, existing.TenantID) {
		return ErrUnauthorized
	}
	tenant, err := resolveTenant(ctx, msg.TenantID)
	if err != nil || tenant != existing.TenantID {
		return ErrUnauthorized
	}
	msg.TenantID = tenant
	m := cloneMessage(msg)
	s.messages[msg.ID] = &m
	s.addHistoryLocked(m.ID, m.Status, "updated", m.Queue, "", "", m.ConsumerID, m.LastError)
	return nil
}

func (s *MemoryStore) UpdateBatch(ctx context.Context, messages []Message) []error {
	errs := make([]error, len(messages))
	select {
	case <-ctx.Done():
		for i := range errs {
			errs[i] = ctx.Err()
		}
		return errs
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range messages {
		msg := messages[i]
		existing, ok := s.messages[msg.ID]
		if !ok {
			errs[i] = ErrMessageNotFound
			continue
		}
		if !tenantAllowed(ctx, existing.TenantID) {
			errs[i] = ErrUnauthorized
			continue
		}
		tenant, err := resolveTenant(ctx, msg.TenantID)
		if err != nil || tenant != existing.TenantID {
			errs[i] = ErrUnauthorized
			continue
		}
		msg.TenantID = tenant
		m := cloneMessage(msg)
		s.messages[msg.ID] = &m
		s.addHistoryLocked(m.ID, m.Status, "updated", m.Queue, "", "", m.ConsumerID, m.LastError)
	}
	return errs
}

func (s *MemoryStore) Get(ctx context.Context, id string) (Message, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.messages[id]
	if !ok {
		return Message{}, false, nil
	}
	if !tenantAllowed(ctx, m.TenantID) {
		return Message{}, false, ErrUnauthorized
	}
	return cloneMessage(*m), true, nil
}

func (s *MemoryStore) History(ctx context.Context, id string) ([]MessageHistory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if m := s.messages[id]; m != nil && !tenantAllowed(ctx, m.TenantID) {
		return nil, ErrUnauthorized
	}
	h := s.history[id]
	out := append([]MessageHistory(nil), h...)
	return out, nil
}

func (s *MemoryStore) ListQueue(ctx context.Context, name string, limit int) ([]Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q, ok := s.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	messages := s.listQueueIDsLocked(q, name, 0)
	out := messages[:0]
	for _, message := range messages {
		if tenantAllowed(ctx, message.TenantID) {
			out = append(out, message)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *MemoryStore) ListDLQ(ctx context.Context, name string, limit int) ([]Message, error) {
	return s.ListQueue(ctx, name, limit)
}

func (s *MemoryStore) Move(ctx context.Context, req MoveRequest) (MoveResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := MoveResult{Destination: req.Destination, DryRun: req.DryRun}
	ids := s.resolveMoveIDsLocked(req)
	for _, id := range ids {
		m, ok := s.messages[id]
		if !ok {
			res.Skipped++
			continue
		}
		if !tenantAllowed(ctx, m.TenantID) {
			res.Skipped++
			continue
		}
		if !filterMessage(*m, req.Filter) {
			res.Skipped++
			continue
		}
		res.MessageIDs = append(res.MessageIDs, id)
		if req.DryRun {
			res.Moved++
			continue
		}
		newID := id
		if req.Mode == MoveCopy || req.Mode == MoveReplay || !req.PreserveID {
			newID = id + ":" + string(req.Mode) + ":" + newIDFromTime()
		}
		clone := cloneMessage(*m)
		clone.ParentID = firstNonEmpty(clone.ParentID, m.ID)
		clone.ID = newID
		clone.LeaseToken = ""
		clone.LeaseUntil = time.Time{}
		if req.ResetAttempts {
			clone.Attempts = 0
			clone.Redeliveries = 0
		}
		if req.ResetError {
			clone.LastError = ""
			clone.DeadLetterReason = ""
		}
		if !req.PreserveOffset {
			clone.Offset = 0
		}
		d := Destination{Kind: req.DestinationType, Name: req.Destination}
		s.appendCloneLocked(clone, d, req, &res)
		if req.Mode != MoveCopy && req.Mode != MoveReplay {
			s.removeFromSourceLocked(req.SourceType, req.Source, id)
			m.Status = MessageMoved
			s.addHistoryLocked(id, MessageMoved, "moved", req.Source, req.Destination, req.Reason, req.Actor, "")
		}
	}
	return res, nil
}

func (s *MemoryStore) applyBackpressureLocked(q *memoryQueue, queue string, incoming *Message) (bool, error) {
	policy := q.cfg.Backpressure
	if policy == "" {
		policy = BackpressureReject
	}
	switch policy {
	case BackpressureDropOldest:
		for i := q.head; i < len(q.ids); i++ {
			id := q.ids[i]
			old := s.messages[id]
			if old == nil || isTerminalQueueStatus(old.Status) {
				continue
			}
			if old.Status == MessageLeased || old.Status == MessageProcessing || old.Status == MessageProgressing {
				continue
			}
			old.Status = MessagePurged
			old.LeaseToken = ""
			old.LeaseUntil = time.Time{}
			if q.depth > 0 {
				q.depth--
			}
			s.addHistoryLocked(id, MessagePurged, "backpressure_drop_oldest", queue, "", "queue full", "", "")
			s.advanceQueueHeadLocked(q)
			return false, nil
		}
		return false, ErrBackpressure
	case BackpressureDropNewest:
		incoming.Queue = queue
		incoming.Status = MessagePurged
		s.messages[incoming.ID] = incoming
		s.addHistoryLocked(incoming.ID, MessagePurged, "backpressure_drop_newest", queue, "", "queue full", "", "")
		return true, nil
	case BackpressureDLQ:
		dlq := q.cfg.DLQ
		if dlq == "" {
			dlq = queue + ".dlq"
		}
		if _, ok := s.queues[dlq]; !ok {
			s.queues[dlq] = &memoryQueue{cfg: QueueConfig{Name: dlq, VisibilityTimeout: q.cfg.VisibilityTimeout, RetryPolicy: q.cfg.RetryPolicy}, state: QueueActive}
		}
		incoming.OriginalQueue = firstNonEmpty(incoming.OriginalQueue, queue)
		incoming.Queue = dlq
		incoming.Status = MessageDeadLettered
		incoming.DeadLetterReason = "backpressure"
		s.messages[incoming.ID] = incoming
		dlqQueue := s.queues[dlq]
		dlqQueue.ids = append(dlqQueue.ids, incoming.ID)
		dlqQueue.depth++
		s.addHistoryLocked(incoming.ID, MessageDeadLettered, "backpressure_dlq", queue, dlq, "queue full", "", "")
		return true, nil
	case BackpressureBlock, BackpressureSpillDisk, BackpressureReject:
		fallthrough
	default:
		return false, ErrBackpressure
	}
}

func (s *MemoryStore) appendCloneLocked(m Message, d Destination, req MoveRequest, res *MoveResult) {
	switch d.Kind {
	case DestinationQueue, DestinationDLQ:
		if _, ok := s.queues[d.Name]; !ok {
			s.queues[d.Name] = &memoryQueue{cfg: QueueConfig{Name: d.Name}, state: QueueActive}
		}
		m.Queue = d.Name
		m.Status = MessageQueued
		if d.Kind == DestinationDLQ {
			m.Status = MessageDeadLettered
		}
		s.messages[m.ID] = &m
		q := s.queues[d.Name]
		q.ids = append(q.ids, m.ID)
		q.depth++
		if m.Priority != 0 {
			q.hasPriority = true
		}
	case DestinationTopic:
		if _, ok := s.topics[d.Name]; !ok {
			s.topics[d.Name] = &memoryTopic{cfg: TopicConfig{Name: d.Name}, state: QueueActive}
		}
		m.Topic = d.Name
		m.Status = MessagePersisted
		s.messages[m.ID] = &m
		s.topics[d.Name].ids = append(s.topics[d.Name].ids, m.ID)
	case DestinationStream, DestinationArchive:
		if _, ok := s.streams[d.Name]; !ok {
			s.streams[d.Name] = &memoryStream{cfg: StreamConfig{Name: d.Name}}
		}
		st := s.streams[d.Name]
		st.offset++
		m.Stream = d.Name
		m.Offset = st.offset
		m.Status = MessagePersisted
		s.messages[m.ID] = &m
		st.ids = append(st.ids, m.ID)
	}
	if req.Mode == MoveCopy || req.Mode == MoveReplay {
		res.Copied++
	} else {
		res.Moved++
	}
	s.addHistoryLocked(m.ID, m.Status, string(req.Mode), req.Source, req.Destination, req.Reason, req.Actor, "")
}

func (s *MemoryStore) PauseQueue(ctx context.Context, name string) error {
	if hasRestrictedTenantScope(ctx) {
		return errf(ErrUnauthorized, "tenant-scoped credentials cannot pause a shared queue")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[name]
	if !ok {
		return ErrQueueNotFound
	}
	q.state = QueuePaused
	return nil
}

func (s *MemoryStore) ResumeQueue(ctx context.Context, name string) error {
	if hasRestrictedTenantScope(ctx) {
		return errf(ErrUnauthorized, "tenant-scoped credentials cannot resume a shared queue")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[name]
	if !ok {
		return ErrQueueNotFound
	}
	q.state = QueueActive
	return nil
}

func (s *MemoryStore) PurgeQueue(ctx context.Context, name string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[name]
	if !ok {
		return 0, ErrQueueNotFound
	}
	n := 0
	for _, id := range q.ids[q.head:] {
		if m := s.messages[id]; m != nil && m.Queue == name && !isTerminalQueueStatus(m.Status) {
			if !tenantAllowed(ctx, m.TenantID) {
				continue
			}
			m.Status = MessagePurged
			m.LeaseToken = ""
			m.LeaseUntil = time.Time{}
			s.addHistoryLocked(id, MessagePurged, "purged", name, "", "queue purge", "", "")
			if !s.retainCompleted {
				delete(s.messages, id)
				delete(s.history, id)
			}
			n++
		}
	}
	if !hasRestrictedTenantScope(ctx) {
		q.head = len(q.ids)
		q.depth = 0
		q.hasPriority = false
		q.state = QueuePurged
	} else {
		q.depth -= n
		if q.depth < 0 {
			q.depth = 0
		}
		s.advanceQueueHeadLocked(q)
	}
	return n, nil
}

func (s *MemoryStore) ReadStream(ctx context.Context, req StreamReadRequest) ([]Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	tenant, err := resolveTenant(ctx, req.TenantID)
	if err != nil {
		return nil, err
	}
	req.TenantID = tenant
	s.mu.RLock()
	defer s.mu.RUnlock()
	stream, ok := s.streams[req.Stream]
	if !ok {
		return nil, ErrStreamNotFound
	}
	after := req.AfterOffset
	if after == 0 && req.ConsumerGroup != "" {
		if groups := s.offsets[req.Stream]; groups != nil {
			after = groups[offsetGroupKey(req.TenantID, req.ConsumerGroup)]
		}
	}
	out := make([]Message, 0, minInt(req.Limit, len(stream.ids)))
	for _, id := range stream.ids {
		m := s.messages[id]
		if m == nil {
			continue
		}
		if !tenantAllowed(ctx, m.TenantID) || (req.TenantID != "" && m.TenantID != req.TenantID) {
			continue
		}
		if m.Offset <= after {
			continue
		}
		out = append(out, cloneMessage(*m))
		if len(out) >= req.Limit {
			break
		}
	}
	return out, nil
}

func (s *MemoryStore) CommitOffset(ctx context.Context, commit OffsetCommit) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if commit.Stream == "" || commit.ConsumerGroup == "" {
		return errf(ErrInvalidMessage, "stream and consumer group required")
	}
	tenant, err := resolveTenant(ctx, commit.TenantID)
	if err != nil {
		return err
	}
	commit.TenantID = tenant
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.streams[commit.Stream]; !ok {
		return ErrStreamNotFound
	}
	if s.offsets[commit.Stream] == nil {
		s.offsets[commit.Stream] = map[string]uint64{}
	}
	group := offsetGroupKey(commit.TenantID, commit.ConsumerGroup)
	if commit.Offset > s.offsets[commit.Stream][group] {
		s.offsets[commit.Stream][group] = commit.Offset
	}
	return nil
}

func (s *MemoryStore) ConsumerOffset(ctx context.Context, commit OffsetCommit) (uint64, error) {
	tenant, err := resolveTenant(ctx, commit.TenantID)
	if err != nil {
		return 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.streams[commit.Stream]; !ok {
		return 0, ErrStreamNotFound
	}
	if s.offsets[commit.Stream] == nil {
		return 0, nil
	}
	return s.offsets[commit.Stream][offsetGroupKey(tenant, commit.ConsumerGroup)], nil
}

func offsetGroupKey(tenant, group string) string {
	if tenant == "" {
		return group
	}
	return tenant + "\x00" + group
}

// durableOffsetCommit folds the tenant into the persisted consumer-group key.
// This preserves tenant isolation without changing the read-only BWL1 format.
// BWL2 will carry tenant_id as an explicit field.
func durableOffsetCommit(ctx context.Context, commit OffsetCommit) (OffsetCommit, error) {
	tenant, err := resolveTenant(ctx, commit.TenantID)
	if err != nil {
		return OffsetCommit{}, err
	}
	commit.ConsumerGroup = offsetGroupKey(tenant, commit.ConsumerGroup)
	commit.TenantID = ""
	return commit, nil
}

func (s *MemoryStore) Stats(ctx context.Context) (StoreStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := StoreStats{Queues: map[string]int{}, Topics: map[string]int{}, Streams: map[string]int{}, Offsets: map[string]map[string]uint64{}, DLQ: map[string]int{}}
	for _, m := range s.messages {
		if !tenantAllowed(ctx, m.TenantID) {
			continue
		}
		st.Messages++
		if m.Status == MessageLeased {
			st.Leased++
		}
		if m.Status == MessageDeadLettered {
			st.DeadLetter++
		}
	}
	for name, q := range s.queues {
		count := 0
		for _, id := range q.ids[q.head:] {
			if m := s.messages[id]; m != nil && tenantAllowed(ctx, m.TenantID) {
				count++
			}
		}
		if count > 0 || !hasRestrictedTenantScope(ctx) {
			st.Queues[name] = count
			if strings.Contains(name, ".dlq") || strings.HasSuffix(name, "dlq") {
				st.DLQ[name] = count
			}
		}
	}
	for name, t := range s.topics {
		count := s.visibleMessageCountLocked(ctx, t.ids, 0, "")
		if count > 0 || !hasRestrictedTenantScope(ctx) {
			st.Topics[name] = count
		}
	}
	for name, sm := range s.streams {
		count := s.visibleMessageCountLocked(ctx, sm.ids, 0, "")
		if count > 0 || !hasRestrictedTenantScope(ctx) {
			st.Streams[name] = count
		}
	}
	for stream, groups := range s.offsets {
		for group, offset := range groups {
			visible, ok := visibleOffsetGroup(ctx, group)
			if !ok {
				continue
			}
			if st.Offsets[stream] == nil {
				st.Offsets[stream] = map[string]uint64{}
			}
			st.Offsets[stream][visible] = offset
		}
	}
	return st, nil
}

func (s *MemoryStore) SweepLeases(ctx context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.messages {
		if m.Status == MessageLeased && !m.LeaseUntil.IsZero() && now.After(m.LeaseUntil) {
			m.Status = MessageQueued
			m.LeaseToken = ""
			m.LeaseUntil = time.Time{}
			m.Redeliveries++
			s.addHistoryLocked(m.ID, MessageQueued, "lease_expired", m.Queue, "", "visibility timeout", m.ConsumerID, "")
			n++
		}
	}
	return n, nil
}

func (s *MemoryStore) Close() error { s.mu.Lock(); s.closed = true; s.mu.Unlock(); return nil }

// Cleanup removes expired dedup entries, stale idempotency keys, and truncates
// history slices to prevent unbounded memory growth. Should be called periodically.
func (s *MemoryStore) Cleanup(maxHistoryPerMessage int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	// Purge expired dedup entries.
	for k, until := range s.dedup {
		if now.After(until) {
			delete(s.dedup, k)
		}
	}
	// Purge idempotency keys for messages that no longer exist.
	for k, msgID := range s.idemp {
		if _, ok := s.messages[msgID]; !ok {
			delete(s.idemp, k)
		}
	}
	// Truncate history slices to prevent unbounded growth.
	if maxHistoryPerMessage <= 0 {
		maxHistoryPerMessage = 100
	}
	for id, hist := range s.history {
		if len(hist) > maxHistoryPerMessage {
			s.history[id] = hist[len(hist)-maxHistoryPerMessage:]
		}
	}
}

// StreamLag returns the consumer lag (messages behind) for a given stream and consumer group.
func (s *MemoryStore) StreamLag(stream, consumerGroup string) (uint64, error) {
	return s.StreamLagContext(context.Background(), stream, consumerGroup, "")
}

// StreamLagContext returns the number of messages visible to the caller after
// its committed offset. Counting visible records avoids including traffic from
// another tenant in the reported lag.
func (s *MemoryStore) StreamLagContext(ctx context.Context, stream, consumerGroup, tenantID string) (uint64, error) {
	tenant, err := resolveTenant(ctx, tenantID)
	if err != nil {
		return 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.streams[stream]
	if !ok {
		return 0, ErrStreamNotFound
	}
	committed := uint64(0)
	if groups := s.offsets[stream]; groups != nil {
		committed = groups[offsetGroupKey(tenant, consumerGroup)]
	}
	return uint64(s.visibleMessageCountLocked(ctx, st.ids, committed, tenant)), nil
}

// StreamLagAll returns the consumer lag for all consumer groups on a stream.
func (s *MemoryStore) StreamLagAll(stream string) (map[string]uint64, error) {
	return s.StreamLagAllContext(context.Background(), stream, "")
}

func (s *MemoryStore) StreamLagAllContext(ctx context.Context, stream, tenantID string) (map[string]uint64, error) {
	tenant, err := resolveTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.streams[stream]
	if !ok {
		return nil, ErrStreamNotFound
	}
	result := make(map[string]uint64)
	if groups := s.offsets[stream]; groups != nil {
		for stored, committed := range groups {
			group, visible := visibleOffsetGroup(ctx, stored)
			if !visible {
				continue
			}
			if tenant != "" && stored != offsetGroupKey(tenant, group) {
				continue
			}
			result[group] = uint64(s.visibleMessageCountLocked(ctx, st.ids, committed, tenant))
		}
	}
	return result, nil
}

// StreamHeadOffset returns the latest offset for a stream.
func (s *MemoryStore) StreamHeadOffset(stream string) (uint64, error) {
	return s.StreamHeadOffsetContext(context.Background(), stream, "")
}

func (s *MemoryStore) StreamHeadOffsetContext(ctx context.Context, stream, tenantID string) (uint64, error) {
	tenant, err := resolveTenant(ctx, tenantID)
	if err != nil {
		return 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.streams[stream]
	if !ok {
		return 0, ErrStreamNotFound
	}
	if tenant == "" && !hasRestrictedTenantScope(ctx) {
		return st.offset, nil
	}
	var head uint64
	for _, id := range st.ids {
		m := s.messages[id]
		if m != nil && tenantAllowed(ctx, m.TenantID) && (tenant == "" || m.TenantID == tenant) && m.Offset > head {
			head = m.Offset
		}
	}
	return head, nil
}

func (s *MemoryStore) visibleMessageCountLocked(ctx context.Context, ids []string, after uint64, tenant string) int {
	count := 0
	for _, id := range ids {
		m := s.messages[id]
		if m != nil && m.Offset > after && tenantAllowed(ctx, m.TenantID) && (tenant == "" || m.TenantID == tenant) {
			count++
		}
	}
	return count
}

func (s *MemoryStore) listIDsLocked(ids []string, limit int, dlqOnly bool) []Message {
	if limit <= 0 || limit > len(ids) {
		limit = len(ids)
	}
	out := make([]Message, 0, limit)
	for _, id := range ids {
		if len(out) >= limit {
			break
		}
		if m := s.messages[id]; m != nil {
			if isTerminalQueueStatus(m.Status) {
				continue
			}
			if dlqOnly && m.Status != MessageDeadLettered {
				continue
			}
			out = append(out, cloneMessage(*m))
		}
	}
	return out
}

func (s *MemoryStore) resolveMoveIDsLocked(req MoveRequest) []string {
	if len(req.MessageIDs) > 0 {
		return append([]string(nil), req.MessageIDs...)
	}
	switch req.SourceType {
	case DestinationQueue, DestinationDLQ:
		if q := s.queues[req.Source]; q != nil {
			return append([]string(nil), q.ids[q.head:]...)
		}
	case DestinationTopic:
		if t := s.topics[req.Source]; t != nil {
			return append([]string(nil), t.ids...)
		}
	case DestinationStream, DestinationArchive:
		if st := s.streams[req.Source]; st != nil {
			return append([]string(nil), st.ids...)
		}
	}
	return nil
}

func (s *MemoryStore) removeFromSourceLocked(kind DestinationKind, source, id string) {
	switch kind {
	case DestinationQueue, DestinationDLQ:
		s.removeFromQueueLocked(source, id)
	case DestinationTopic:
		if t := s.topics[source]; t != nil {
			t.ids = removeID(t.ids, id)
		}
	case DestinationStream, DestinationArchive:
		if st := s.streams[source]; st != nil {
			st.ids = removeID(st.ids, id)
		}
	}
}

func (s *MemoryStore) removeFromQueueLocked(queue, id string) {
	if q := s.queues[queue]; q != nil {
		if q.depth > 0 {
			q.depth--
		}
		s.advanceQueueHeadLocked(q)
	}
}

func (s *MemoryStore) advanceQueueHeadLocked(q *memoryQueue) {
	for q.head < len(q.ids) {
		m := s.messages[q.ids[q.head]]
		if m != nil && m.Queue == q.cfg.Name && !isTerminalQueueStatus(m.Status) {
			break
		}
		q.head++
	}
	if q.head > 4096 && q.head*2 >= len(q.ids) {
		copy(q.ids, q.ids[q.head:])
		q.ids = q.ids[:len(q.ids)-q.head]
		q.head = 0
	}
}

func (s *MemoryStore) listQueueIDsLocked(q *memoryQueue, queue string, limit int) []Message {
	if limit <= 0 || limit > q.depth {
		limit = q.depth
	}
	out := make([]Message, 0, limit)
	for _, id := range q.ids[q.head:] {
		if len(out) >= limit {
			break
		}
		m := s.messages[id]
		if m == nil || m.Queue != queue || isTerminalQueueStatus(m.Status) {
			continue
		}
		out = append(out, cloneMessage(*m))
	}
	return out
}

func removeID(ids []string, id string) []string {
	for i, v := range ids {
		if v == id {
			copy(ids[i:], ids[i+1:])
			return ids[:len(ids)-1]
		}
	}
	return ids
}

func (s *MemoryStore) addHistoryLocked(id string, status MessageStatus, event, source, target, reason, actor, errText string) {
	if s.historyLimit <= 0 {
		return
	}
	history := append(s.history[id], MessageHistory{MessageID: id, Status: status, Event: event, Source: source, Target: target, Reason: reason, Actor: actor, Error: errText, Time: time.Now()})
	if len(history) > s.historyLimit {
		copy(history, history[len(history)-s.historyLimit:])
		history = history[:s.historyLimit]
	}
	s.history[id] = history
}

func cloneMessage(in Message) Message {
	if len(in.Headers) > 0 {
		in.Headers = cloneMap(in.Headers)
	}
	if len(in.Metadata) > 0 {
		in.Metadata = cloneMap(in.Metadata)
	}
	if len(in.Payload) > 0 {
		in.Payload = append([]byte(nil), in.Payload...)
	}
	return in
}

func filterMessage(m Message, f MessageFilter) bool {
	if f.Status != "" && m.Status != f.Status {
		return false
	}
	if f.Type != "" && m.Type != f.Type {
		return false
	}
	if f.TenantID != "" && m.TenantID != f.TenantID {
		return false
	}
	for k, v := range f.Headers {
		if m.Headers[k] != v {
			return false
		}
	}
	return true
}

func dedupWindow(msg Message) time.Duration {
	if msg.Metadata != nil {
		if raw := msg.Metadata["_dedup_window_ns"]; raw != "" {
			if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
				return time.Duration(n)
			}
		}
	}
	return 10 * time.Minute
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func itoa(i int) string { return strconvFormatInt(int64(i)) }
func newIDLocked(s *MemoryStore, prefix string) string {
	return prefix + "-" + strconvFormatInt(time.Now().UnixNano()) + "-" + strconvFormatInt(int64(s.rand.Int31()))
}
func newIDFromTime() string { return strconvFormatInt(time.Now().UnixNano()) }

func strconvFormatInt(i int64) string { return strconv.FormatInt(i, 36) }

// FileWALStore is a durable local store that appends every mutation to JSONL and
// rebuilds the in-memory indexes on startup. It is intended for embedded and
// single-node standalone deployments. Encryption is optional — set EncryptionKey
// in the config to enable AES-GCM encryption of all WAL records at rest.
type FileWALStore struct {
	*MemoryStore
	path    string
	file    *os.File
	gcm     cipher.AEAD
	keyName string
	gc      *groupCommitWriter
	writeMu sync.Mutex
}

func (*FileWALStore) ConfirmationLevel() ConfirmLevel { return ConfirmPersisted }

func (s *FileWALStore) StoreHealth() error {
	if s == nil || s.gc == nil {
		return ErrClosed
	}
	return s.gc.healthError()
}

// FileWALStoreConfig configures the FileWALStore.
type FileWALStoreConfig struct {
	Dir           string
	EncryptionKey []byte
	KeyName       string
}

type walRecord struct {
	Op        string        `json:"op"`
	Message   Message       `json:"message,omitempty"`
	Dests     []Destination `json:"dests,omitempty"`
	Entries   []AppendEntry `json:"entries,omitempty"`
	Queue     QueueConfig   `json:"queue,omitempty"`
	Topic     TopicConfig   `json:"topic,omitempty"`
	Stream    StreamConfig  `json:"stream,omitempty"`
	Lease     LeaseRequest  `json:"lease,omitempty"`
	Messages  []Message     `json:"messages,omitempty"`
	Ack       AckRequest    `json:"ack,omitempty"`
	Acks      []AckRequest  `json:"acks,omitempty"`
	Nack      NackRequest   `json:"nack,omitempty"`
	Move      MoveRequest   `json:"move,omitempty"`
	QueueName string        `json:"queue_name,omitempty"`
	Offset    OffsetCommit  `json:"offset,omitempty"`
	At        time.Time     `json:"at"`
	Checksum  string        `json:"checksum,omitempty"`
}

func NewFileWALStore(dir string) (*FileWALStore, error) {
	return NewFileWALStoreWithConfig(FileWALStoreConfig{Dir: dir})
}

// NewFileWALStoreWithConfig creates a FileWALStore with optional encryption.
// If EncryptionKey is nil or empty, WAL records are stored in plaintext.
// If EncryptionKey is provided (16, 24, or 32 bytes), all records are AES-GCM encrypted.
func NewFileWALStoreWithConfig(cfg FileWALStoreConfig) (*FileWALStore, error) {
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(cfg.Dir, "broker.wal.jsonl")
	st := &FileWALStore{MemoryStore: NewMemoryStore(), path: path, keyName: cfg.KeyName}
	// Set up optional encryption
	if len(cfg.EncryptionKey) > 0 {
		block, err := aes.NewCipher(cfg.EncryptionKey)
		if err != nil {
			return nil, fmt.Errorf("wal: invalid encryption key: %w", err)
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("wal: init GCM: %w", err)
		}
		st.gcm = gcm
	}
	if err := st.recover(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	st.file = f
	st.gc = newGroupCommitWriter(f)
	return st, nil
}

func (s *FileWALStore) recover() error {
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var offset int64
	var recoveryErrors []error
	for {
		line, readErr := r.ReadBytes('\n')
		if len(line) == 0 && errors.Is(readErr, io.EOF) {
			s.normalizeRecoveredInflight()
			if len(recoveryErrors) > 0 {
				return fmt.Errorf("WAL recovery completed with %d errors: %w", len(recoveryErrors), errors.Join(recoveryErrors...))
			}
			return nil
		}
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 {
			// Handle encrypted records
			data := trimmed
			if bytes.HasPrefix(data, []byte("ENC:")) {
				if s.gcm == nil {
					recoveryErrors = append(recoveryErrors, fmt.Errorf("encrypted WAL record at offset %d but no encryption key configured", offset))
					offset += int64(len(line))
					continue
				}
				encoded := data[4:] // strip "ENC:" prefix
				encrypted, err := base64.StdEncoding.DecodeString(string(encoded))
				if err != nil {
					recoveryErrors = append(recoveryErrors, fmt.Errorf("base64 decode WAL record at offset %d: %w", offset, err))
					offset += int64(len(line))
					continue
				}
				if len(encrypted) < s.gcm.NonceSize() {
					recoveryErrors = append(recoveryErrors, fmt.Errorf("encrypted WAL record too short at offset %d", offset))
					offset += int64(len(line))
					continue
				}
				nonce := encrypted[:s.gcm.NonceSize()]
				ciphertext := encrypted[s.gcm.NonceSize():]
				plaintext, err := s.gcm.Open(nil, nonce, ciphertext, nil)
				if err != nil {
					recoveryErrors = append(recoveryErrors, fmt.Errorf("decrypt WAL record at offset %d: %w", offset, err))
					offset += int64(len(line))
					continue
				}
				data = plaintext
			}
			var rec walRecord
			if err := json.Unmarshal(data, &rec); err != nil {
				if errors.Is(readErr, io.EOF) {
					_ = os.Truncate(s.path, offset)
					s.normalizeRecoveredInflight()
					return nil
				}
				// Skip corrupted record but continue recovery.
				recoveryErrors = append(recoveryErrors, fmt.Errorf("corrupted WAL record at offset %d: %w", offset, err))
				offset += int64(len(line))
				continue
			}
			if err := verifyWALRecord(rec); err != nil {
				if errors.Is(readErr, io.EOF) {
					_ = os.Truncate(s.path, offset)
					s.normalizeRecoveredInflight()
					return nil
				}
				// Skip invalid record but continue recovery.
				recoveryErrors = append(recoveryErrors, fmt.Errorf("invalid WAL record at offset %d: %w", offset, err))
				offset += int64(len(line))
				continue
			}
			if err := s.applyRecovered(rec); err != nil {
				// Log but continue recovery; non-fatal errors (e.g., queue not found) are expected.
				recoveryErrors = append(recoveryErrors, fmt.Errorf("replay %s at offset %d: %w", rec.Op, offset, err))
			}
		}
		offset += int64(len(line))
		if errors.Is(readErr, io.EOF) {
			s.normalizeRecoveredInflight()
			if len(recoveryErrors) > 0 {
				return fmt.Errorf("WAL recovery completed with %d errors: %w", len(recoveryErrors), errors.Join(recoveryErrors...))
			}
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (s *FileWALStore) normalizeRecoveredInflight() {
	s.MemoryStore.mu.Lock()
	defer s.MemoryStore.mu.Unlock()
	for _, m := range s.MemoryStore.messages {
		switch m.Status {
		case MessageLeased, MessageDelivered, MessageProcessing, MessageProgressing:
			m.Status = MessageQueued
			m.LeaseToken = ""
			m.LeaseUntil = time.Time{}
			m.Redeliveries++
			s.MemoryStore.addHistoryLocked(m.ID, MessageQueued, "recovered_redelivery", m.Queue, "", "broker restart", m.ConsumerID, "")
		}
	}
}

func (s *FileWALStore) applyRecovered(r walRecord) error {
	ctx := context.Background()
	switch r.Op {
	case "declare_queue":
		return s.MemoryStore.DeclareQueue(ctx, r.Queue)
	case "declare_topic":
		return s.MemoryStore.DeclareTopic(ctx, r.Topic)
	case "declare_stream":
		return s.MemoryStore.DeclareStream(ctx, r.Stream)
	case "append":
		_, err := s.MemoryStore.Append(ctx, r.Message, r.Dests)
		return err
	case "append_atomic":
		return s.MemoryStore.appendRecoveredAtomic(r.Entries)
	case "lease":
		return s.MemoryStore.Update(ctx, r.Message)
	case "lease_batch", "update_batch":
		return errors.Join(s.MemoryStore.UpdateBatch(ctx, r.Messages)...)
	case "ack":
		return s.MemoryStore.Ack(ctx, r.Ack)
	case "ack_batch":
		return errors.Join(s.MemoryStore.AckBatch(ctx, r.Acks)...)
	case "nack":
		return s.MemoryStore.Nack(ctx, r.Nack)
	case "move":
		_, err := s.MemoryStore.Move(ctx, r.Move)
		return err
	case "pause_queue":
		return s.MemoryStore.PauseQueue(ctx, r.QueueName)
	case "resume_queue":
		return s.MemoryStore.ResumeQueue(ctx, r.QueueName)
	case "purge_queue":
		_, err := s.MemoryStore.PurgeQueue(ctx, r.QueueName)
		return err
	case "update":
		return s.MemoryStore.Update(ctx, r.Message)
	case "commit_offset":
		return s.MemoryStore.CommitOffset(ctx, r.Offset)
	}
	return nil
}

func (s *FileWALStore) write(r walRecord) error {
	if s.file == nil {
		return ErrClosed
	}
	r.At = time.Now()
	r.Checksum = ""
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	r.Checksum = hex.EncodeToString(sum[:])
	line, err := json.Marshal(r)
	if err != nil {
		return err
	}
	// Optional AES-GCM encryption at rest
	if s.gcm != nil {
		nonce := make([]byte, s.gcm.NonceSize())
		if _, err := io.ReadFull(cryptorand.Reader, nonce); err != nil {
			return fmt.Errorf("wal: generate nonce: %w", err)
		}
		encrypted := s.gcm.Seal(nonce, nonce, line, nil)
		// Base64-encode to avoid newlines in binary data breaking line-based reading
		encoded := make([]byte, base64.StdEncoding.EncodedLen(len(encrypted)))
		base64.StdEncoding.Encode(encoded, encrypted)
		line = append([]byte("ENC:"), encoded...)
	}
	line = append(line, '\n')
	return s.gc.write(line)
}

func verifyWALRecord(r walRecord) error {
	if r.Checksum == "" {
		// Legacy WAL records from older versions are accepted so existing data can
		// be upgraded in place. New writes always include a checksum.
		return nil
	}
	want := r.Checksum
	r.Checksum = ""
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	got := hex.EncodeToString(sum[:])
	if !constantTimeEqual(got, want) {
		return errf(ErrInvalidMessage, "wal checksum mismatch")
	}
	return nil
}

func (s *FileWALStore) DeclareQueue(ctx context.Context, cfg QueueConfig) error {
	if err := validateSharedDeclaration(ctx, cfg.Name, "queue"); err != nil {
		return err
	}
	if err := s.write(walRecord{Op: "declare_queue", Queue: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareQueue(ctx, cfg)
}
func (s *FileWALStore) DeclareTopic(ctx context.Context, cfg TopicConfig) error {
	if err := validateSharedDeclaration(ctx, cfg.Name, "topic"); err != nil {
		return err
	}
	if err := s.write(walRecord{Op: "declare_topic", Topic: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareTopic(ctx, cfg)
}
func (s *FileWALStore) DeclareStream(ctx context.Context, cfg StreamConfig) error {
	if err := validateSharedDeclaration(ctx, cfg.Name, "stream"); err != nil {
		return err
	}
	if err := s.write(walRecord{Op: "declare_stream", Stream: cfg}); err != nil {
		return err
	}
	return s.MemoryStore.DeclareStream(ctx, cfg)
}

func validateSharedDeclaration(ctx context.Context, name, kind string) error {
	if hasRestrictedTenantScope(ctx) {
		return errf(ErrUnauthorized, "tenant-scoped credentials cannot alter shared %s configuration", kind)
	}
	if name == "" {
		return errf(ErrInvalidMessage, "%s name required", kind)
	}
	return nil
}
func (s *FileWALStore) Append(ctx context.Context, msg Message, dests []Destination) ([]Message, error) {
	return s.durableAppend(ctx, []AppendEntry{{Message: msg, Destinations: dests}})
}
func (s *FileWALStore) AppendAtomic(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	return s.durableAppend(ctx, entries)
}
func (s *FileWALStore) AppendAtomicMinimal(ctx context.Context, entries []AppendEntry) error {
	_, err := s.durableAppend(ctx, entries)
	return err
}
func (s *FileWALStore) AppendAtomicOwned(ctx context.Context, entries []AppendEntry) error {
	_, err := s.durableAppend(ctx, entries)
	return err
}
func (s *FileWALStore) durableAppend(ctx context.Context, entries []AppendEntry) ([]Message, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	out, err := s.MemoryStore.prepareAppendAtomic(ctx, entries)
	if err != nil {
		return nil, err
	}
	persisted := storedAppendEntries(out)
	if err := s.write(walRecord{Op: "append_atomic", Entries: persisted}); err != nil {
		return nil, err
	}
	if err := s.MemoryStore.appendRecoveredAtomic(persisted); err != nil {
		return nil, fmt.Errorf("apply durable append: %w", err)
	}
	return out, nil
}
func (s *FileWALStore) Lease(ctx context.Context, req LeaseRequest) ([]Message, error) {
	msgs, err := s.MemoryStore.Lease(ctx, req)
	if err != nil || len(msgs) == 0 {
		return msgs, err
	}
	if err := s.write(walRecord{Op: "lease_batch", Messages: msgs, Lease: req}); err != nil {
		return msgs, err
	}
	return msgs, nil
}
func (s *FileWALStore) Ack(ctx context.Context, req AckRequest) error {
	if err := s.MemoryStore.validateAckLease(ctx, req); err != nil {
		return err
	}
	if err := s.MemoryStore.Ack(ctx, req); err != nil {
		return err
	}
	return s.write(walRecord{Op: "ack", Ack: req})
}
func (s *FileWALStore) AckBatch(ctx context.Context, reqs []AckRequest) []error {
	errs := s.MemoryStore.AckBatch(ctx, reqs)
	successful := make([]AckRequest, 0, len(reqs))
	indices := make([]int, 0, len(reqs))
	for i, err := range errs {
		if err == nil {
			successful = append(successful, reqs[i])
			indices = append(indices, i)
		}
	}
	if len(successful) == 0 {
		return errs
	}
	if err := s.write(walRecord{Op: "ack_batch", Acks: successful}); err != nil {
		for _, i := range indices {
			errs[i] = err
		}
	}
	return errs
}
func (s *FileWALStore) Nack(ctx context.Context, req NackRequest) error {
	if err := s.MemoryStore.validateNackLease(ctx, req); err != nil {
		return err
	}
	if err := s.MemoryStore.Nack(ctx, req); err != nil {
		return err
	}
	return s.write(walRecord{Op: "nack", Nack: req})
}
func (s *FileWALStore) ExtendLease(ctx context.Context, req LeaseRequest) error {
	if err := s.MemoryStore.ExtendLease(ctx, req); err != nil {
		return err
	}
	m, ok, err := s.MemoryStore.Get(ctx, req.MessageID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrMessageNotFound
	}
	return s.write(walRecord{Op: "lease", Message: m, Lease: req})
}
func (s *FileWALStore) Update(ctx context.Context, msg Message) error {
	if err := s.MemoryStore.Update(ctx, msg); err != nil {
		return err
	}
	return s.write(walRecord{Op: "update", Message: msg})
}
func (s *FileWALStore) UpdateBatch(ctx context.Context, messages []Message) []error {
	errs := s.MemoryStore.UpdateBatch(ctx, messages)
	successful := make([]Message, 0, len(messages))
	indices := make([]int, 0, len(messages))
	for i, err := range errs {
		if err == nil {
			successful = append(successful, messages[i])
			indices = append(indices, i)
		}
	}
	if len(successful) == 0 {
		return errs
	}
	if err := s.write(walRecord{Op: "update_batch", Messages: successful}); err != nil {
		for _, i := range indices {
			errs[i] = err
		}
	}
	return errs
}
func (s *FileWALStore) Move(ctx context.Context, req MoveRequest) (MoveResult, error) {
	res, err := s.MemoryStore.Move(ctx, req)
	if err != nil {
		return MoveResult{}, err
	}
	if err := s.write(walRecord{Op: "move", Move: req}); err != nil {
		return res, err
	}
	return res, nil
}
func (s *FileWALStore) PauseQueue(ctx context.Context, name string) error {
	if err := s.MemoryStore.PauseQueue(ctx, name); err != nil {
		return err
	}
	return s.write(walRecord{Op: "pause_queue", QueueName: name})
}
func (s *FileWALStore) ResumeQueue(ctx context.Context, name string) error {
	if err := s.MemoryStore.ResumeQueue(ctx, name); err != nil {
		return err
	}
	return s.write(walRecord{Op: "resume_queue", QueueName: name})
}
func (s *FileWALStore) PurgeQueue(ctx context.Context, name string) (int, error) {
	n, err := s.MemoryStore.PurgeQueue(ctx, name)
	if err != nil {
		return 0, err
	}
	if err := s.write(walRecord{Op: "purge_queue", QueueName: name}); err != nil {
		return n, err
	}
	return n, nil
}
func (s *FileWALStore) CommitOffset(ctx context.Context, commit OffsetCommit) error {
	persisted, err := durableOffsetCommit(ctx, commit)
	if err != nil {
		return err
	}
	if err := s.MemoryStore.CommitOffset(context.Background(), persisted); err != nil {
		return err
	}
	return s.write(walRecord{Op: "commit_offset", Offset: persisted})
}

func (s *FileWALStore) Close() error {
	var joined error
	if s.gc != nil {
		joined = errors.Join(joined, s.gc.close())
	}
	joined = errors.Join(joined, s.MemoryStore.Close())
	if s.file != nil {
		joined = errors.Join(joined, s.file.Close())
	}
	return joined
}
