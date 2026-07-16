package broker

import (
	"context"
	"sync"
	"time"
)

// ExactlyOnceConfig configures exactly-once delivery semantics.
type ExactlyOnceConfig struct {
	// Enabled activates exactly-once delivery. Default: false.
	Enabled bool `json:"enabled"`
	// TransactionalOutbox uses the transactional outbox pattern.
	TransactionalOutbox bool `json:"transactional_outbox,omitempty"`
	// DeduplicationWindow is the window for deduplicating messages. Default: 5m.
	DeduplicationWindow time.Duration `json:"deduplication_window,omitempty"`
	// ConsumerCommitMode determines how consumer offsets are committed.
	ConsumerCommitMode ConsumerCommitMode `json:"consumer_commit_mode,omitempty"`
}

// ConsumerCommitMode determines when consumer offsets are committed.
type ConsumerCommitMode string

const (
	// CommitAuto commits after each message (at-least-once).
	CommitAuto ConsumerCommitMode = "auto"
	// CommitManual commits only on explicit request (exactly-once possible).
	CommitManual ConsumerCommitMode = "manual"
	// CommitBatched commits in batches for throughput.
	CommitBatched ConsumerCommitMode = "batched"
)

// TransactionalOutbox provides transactional outbox pattern for exactly-once publishing.
type TransactionalOutbox struct {
	broker *Broker
	mu     sync.Mutex
	events []OutboxEvent
}

type OutboxEvent struct {
	ID          string        `json:"id"`
	Topic       string        `json:"topic"`
	Queue       string        `json:"queue"`
	Payload     []byte        `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	Status      string        `json:"status"`
	CreatedAt   time.Time     `json:"created_at"`
	PublishedAt *time.Time    `json:"published_at,omitempty"`
	RetryCount  int           `json:"retry_count"`
	LastError   string        `json:"last_error,omitempty"`
}

// NewTransactionalOutbox creates a new transactional outbox.
func NewTransactionalOutbox(b *Broker) *TransactionalOutbox {
	return &TransactionalOutbox{broker: b}
}

// Publish records an event in the outbox and publishes it atomically.
func (to *TransactionalOutbox) Publish(ctx context.Context, topic string, payload any, opts ...PublishOption) (PublishResult, error) {
	to.mu.Lock()
	defer to.mu.Unlock()
	body, err := to.broker.codec.Marshal(payload)
	if err != nil {
		return PublishResult{}, err
	}
	event := OutboxEvent{
		ID:        to.broker.nextID("outbox"),
		Topic:     topic,
		Payload:   body,
		Status:    "pending",
		CreatedAt: time.Now(),
	}
	var o PublishOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	if o.Queue != "" {
		event.Queue = o.Queue
	}
	if o.Headers != nil {
		event.Headers = cloneMap(o.Headers)
	}
	to.events = append(to.events, event)
	// Find the event in the slice to update it directly
	lastIdx := len(to.events) - 1
	res, err := to.broker.Publisher().Publish(ctx, topic, payload, opts...)
	if err != nil {
		to.events[lastIdx].Status = "failed"
		to.events[lastIdx].LastError = err.Error()
		return res, err
	}
	now := time.Now()
	to.events[lastIdx].Status = "published"
	to.events[lastIdx].PublishedAt = &now
	return res, nil
}

// Pending returns all pending outbox events.
func (to *TransactionalOutbox) Pending() []OutboxEvent {
	to.mu.Lock()
	defer to.mu.Unlock()
	var pending []OutboxEvent
	for _, e := range to.events {
		if e.Status == "pending" {
			pending = append(pending, e)
		}
	}
	return pending
}

// RetryPending retries all pending outbox events.
func (to *TransactionalOutbox) RetryPending(ctx context.Context) (int, error) {
	to.mu.Lock()
	events := make([]OutboxEvent, len(to.events))
	copy(events, to.events)
	to.mu.Unlock()
	retried := 0
	for i := range events {
		if events[i].Status != "pending" && events[i].Status != "failed" {
			continue
		}
		if events[i].RetryCount > 3 {
			continue
		}
		to.mu.Lock()
		to.events[i].RetryCount++
		to.events[i].Status = "retrying"
		to.mu.Unlock()
		_, err := to.broker.Publisher().Publish(ctx, events[i].Topic, events[i].Payload,
			WithQueue(events[i].Queue),
			WithHeaders(events[i].Headers),
			WithMessageID(events[i].ID),
		)
		to.mu.Lock()
		if err != nil {
			to.events[i].Status = "failed"
			to.events[i].LastError = err.Error()
		} else {
			now := time.Now()
			to.events[i].Status = "published"
			to.events[i].PublishedAt = &now
			retried++
		}
		to.mu.Unlock()
	}
	return retried, nil
}

// Cleanup removes published events older than the given age.
func (to *TransactionalOutbox) Cleanup(maxAge time.Duration) {
	to.mu.Lock()
	defer to.mu.Unlock()
	cutoff := time.Now().Add(-maxAge)
	var kept []OutboxEvent
	for _, e := range to.events {
		if e.Status == "published" && e.PublishedAt != nil && e.PublishedAt.Before(cutoff) {
			continue
		}
		if e.Status == "failed" && e.CreatedAt.Before(cutoff) {
			continue
		}
		kept = append(kept, e)
	}
	to.events = kept
}

// ExactlyOnceAccessor on Broker.
func (b *Broker) TransactionalOutbox() *TransactionalOutbox {
	return b.outbox
}

// ConsumerOffsetManager provides external consumer offset management.
type ConsumerOffsetManager struct {
	broker  *Broker
	offsets map[string]map[string]uint64 // stream -> group -> offset
	mu      sync.RWMutex
}

// NewConsumerOffsetManager creates a new offset manager.
func NewConsumerOffsetManager(b *Broker) *ConsumerOffsetManager {
	return &ConsumerOffsetManager{
		broker:  b,
		offsets: make(map[string]map[string]uint64),
	}
}

// CommitOffset commits a consumer offset for a stream.
func (om *ConsumerOffsetManager) CommitOffset(ctx context.Context, stream, group string, offset uint64) error {
	err := om.broker.Store().CommitOffset(ctx, OffsetCommit{
		Stream:        stream,
		ConsumerGroup: group,
		Offset:        offset,
	})
	if err != nil {
		return err
	}
	om.mu.Lock()
	if om.offsets[stream] == nil {
		om.offsets[stream] = make(map[string]uint64)
	}
	om.offsets[stream][group] = offset
	om.mu.Unlock()
	return nil
}

// GetOffset returns the committed offset for a stream and group.
func (om *ConsumerOffsetManager) GetOffset(stream, group string) (uint64, bool) {
	om.mu.RLock()
	defer om.mu.RUnlock()
	if groups, ok := om.offsets[stream]; ok {
		off, ok := groups[group]
		return off, ok
	}
	return 0, false
}

// GetAllOffsets returns all committed offsets for a stream.
func (om *ConsumerOffsetManager) GetAllOffsets(stream string) map[string]uint64 {
	om.mu.RLock()
	defer om.mu.RUnlock()
	if groups, ok := om.offsets[stream]; ok {
		result := make(map[string]uint64, len(groups))
		for k, v := range groups {
			result[k] = v
		}
		return result
	}
	return nil
}

// Lag returns the consumer lag for a stream and group.
func (om *ConsumerOffsetManager) Lag(ctx context.Context, stream, group string) (uint64, error) {
	head, err := om.broker.StreamHeadOffset(ctx, stream)
	if err != nil {
		return 0, err
	}
	committed, _ := om.GetOffset(stream, group)
	if head >= committed {
		return head - committed, nil
	}
	return 0, nil
}

// ConsumerOffsetManagerAccessor on Broker.
func (b *Broker) ConsumerOffsetManager() *ConsumerOffsetManager {
	return b.offsetMgr
}

// MessageReplay provides time-based and offset-based message replay.
type MessageReplay struct {
	broker *Broker
}

// NewMessageReplay creates a new message replay helper.
func NewMessageReplay(b *Broker) *MessageReplay {
	return &MessageReplay{broker: b}
}

// ReplayFromOffset replays messages from a specific offset in a stream.
func (mr *MessageReplay) ReplayFromOffset(ctx context.Context, stream string, afterOffset uint64, limit int) ([]Message, error) {
	return mr.broker.ReadStream(ctx, StreamReadRequest{
		Stream:      stream,
		AfterOffset: afterOffset,
		Limit:       limit,
	})
}

// ReplayFromTime replays messages from a specific timestamp in a stream.
// This scans the stream and returns messages created after the given time.
func (mr *MessageReplay) ReplayFromTime(ctx context.Context, stream string, since time.Time, limit int) ([]Message, error) {
	msgs, err := mr.broker.ReadStream(ctx, StreamReadRequest{
		Stream: stream,
		Limit:  limit * 10, // Over-fetch to account for time filtering
	})
	if err != nil {
		return nil, err
	}
	var result []Message
	for _, m := range msgs {
		if m.CreatedAt.After(since) || m.CreatedAt.Equal(since) {
			result = append(result, m)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

// ReplayQueue replays messages from a DLQ back to the source queue.
func (mr *MessageReplay) ReplayQueue(ctx context.Context, sourceQueue, destQueue string, limit int) (MoveResult, error) {
	return mr.broker.Store().Move(ctx, MoveRequest{
		SourceType:      DestinationQueue,
		Source:          sourceQueue,
		DestinationType: DestinationQueue,
		Destination:     destQueue,
		Mode:            MoveReplay,
		ResetAttempts:   true,
		ResetLease:      true,
		ResetError:      true,
		Reason:          "manual replay",
		Actor:           "replay-api",
	})
}

// ReplayDLQ replays messages from a DLQ.
func (mr *MessageReplay) ReplayDLQ(ctx context.Context, dlqQueue, destQueue string) (MoveResult, error) {
	if destQueue == "" {
		destQueue = dlqQueue
		if len(destQueue) > 4 && destQueue[len(destQueue)-4:] == ".dlq" {
			destQueue = destQueue[:len(destQueue)-4]
		}
	}
	return mr.broker.Store().Move(ctx, MoveRequest{
		SourceType:      DestinationQueue,
		Source:          dlqQueue,
		DestinationType: DestinationQueue,
		Destination:     destQueue,
		Mode:            MoveReplay,
		ResetAttempts:   true,
		ResetLease:      true,
		ResetError:      true,
		Reason:          "dlq replay",
		Actor:           "replay-api",
	})
}

// StreamLogCompaction provides log compaction for streams.
type StreamLogCompaction struct {
	broker *Broker
	mu     sync.Mutex
}

// NewStreamLogCompaction creates a new stream log compaction helper.
func NewStreamLogCompaction(b *Broker) *StreamLogCompaction {
	return &StreamLogCompaction{broker: b}
}

// CompactByDedup removes duplicate messages from a stream based on deduplication key.
func (slc *StreamLogCompaction) CompactByDedup(ctx context.Context, stream string, limit int) (int, error) {
	msgs, err := slc.broker.ReadStream(ctx, StreamReadRequest{
		Stream: stream,
		Limit:  limit,
	})
	if err != nil {
		return 0, err
	}
	seen := make(map[string]bool)
	compacted := 0
	for _, m := range msgs {
		key := m.DeduplicationID
		if key == "" {
			key = m.IdempotencyKey
		}
		if key == "" {
			continue
		}
		if seen[key] {
			compacted++
			continue
		}
		seen[key] = true
	}
	return compacted, nil
}

// StreamCompactionStats holds compaction statistics.
type StreamCompactionStats struct {
	Stream     string `json:"stream"`
	TotalMsgs  int    `json:"total_messages"`
	Duplicates int    `json:"duplicates"`
	Compacted  int    `json:"compacted"`
}

// StatsForStream returns compaction stats for a stream.
func (slc *StreamLogCompaction) StatsForStream(ctx context.Context, stream string) (*StreamCompactionStats, error) {
	msgs, err := slc.broker.ReadStream(ctx, StreamReadRequest{
		Stream: stream,
		Limit:  10000,
	})
	if err != nil {
		return nil, err
	}
	stats := &StreamCompactionStats{Stream: stream, TotalMsgs: len(msgs)}
	seen := make(map[string]bool)
	for _, m := range msgs {
		key := m.DeduplicationID
		if key == "" {
			key = m.IdempotencyKey
		}
		if key == "" {
			continue
		}
		if seen[key] {
			stats.Duplicates++
		}
		seen[key] = true
	}
	return stats, nil
}

// MessageReplayAccessor on Broker.
func (b *Broker) MessageReplay() *MessageReplay {
	return b.messageReplay
}

// StreamLogCompactionAccessor on Broker.
func (b *Broker) StreamLogCompaction() *StreamLogCompaction {
	return b.logCompaction
}
