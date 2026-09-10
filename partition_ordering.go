package broker

import (
	"context"
	"sync"
	"time"
)

// PartitionOrdering provides message ordering guarantees within a partition.
type PartitionOrdering struct {
	broker *Broker
	mu     sync.RWMutex
	queues map[string]*partitionQueue
}

type partitionQueue struct {
	messages  []Message
	committed uint64
	mu        sync.Mutex
}

// NewPartitionOrdering creates a new partition ordering manager.
func NewPartitionOrdering(b *Broker) *PartitionOrdering {
	return &PartitionOrdering{
		broker: b,
		queues: make(map[string]*partitionQueue),
	}
}

// OrderedPublish publishes a message with ordering guarantees within a partition.
// Messages with the same key are guaranteed to be delivered in order.
func (po *PartitionOrdering) OrderedPublish(ctx context.Context, topic string, key string, payload any, opts ...PublishOption) (PublishResult, error) {
	opts = append(opts, WithKey(key))
	return po.broker.Publisher().Publish(ctx, topic, payload, opts...)
}

// OrderedSubscribe subscribes with ordering guarantees.
// Messages with the same key are delivered sequentially to the same consumer.
func (po *PartitionOrdering) OrderedSubscribe(ctx context.Context, cfg ConsumerConfig, handler Handler) (*Subscription, error) {
	cfg.Ordered = true
	cfg.OrderBy = OrderKey
	return po.broker.Subscribe(ctx, cfg, handler)
}

// StreamOrderedPublish publishes to a stream with partition ordering.
func (po *PartitionOrdering) StreamOrderedPublish(ctx context.Context, stream string, partition int, key string, payload any, opts ...PublishOption) (PublishResult, error) {
	opts = append(opts, WithKey(key), WithStream(stream))
	return po.broker.Publisher().Publish(ctx, stream, payload, opts...)
}

// StreamOrderedRead reads from a stream partition in order.
func (po *PartitionOrdering) StreamOrderedRead(ctx context.Context, stream string, consumerGroup string, limit int) ([]Message, error) {
	return po.broker.ReadStream(ctx, StreamReadRequest{
		Stream:        stream,
		ConsumerGroup: consumerGroup,
		Limit:         limit,
	})
}

// PartitionReassignment handles dynamic partition reassignment.
type PartitionReassignment struct {
	broker *Broker
	mu     sync.RWMutex
	rules  []ReassignmentRule
}

type ReassignmentRule struct {
	// SourceTopic is the topic to reassign from.
	SourceTopic string `json:"source_topic"`
	// DestTopic is the topic to reassign to.
	DestTopic string `json:"dest_topic"`
	// Filter is an optional message filter.
	Filter *MessageFilter `json:"filter,omitempty"`
	// PreserveOffset maintains the original offset.
	PreserveOffset bool `json:"preserve_offset,omitempty"`
	// BatchSize is messages per reassignment batch. Default: 100.
	BatchSize int `json:"batch_size,omitempty"`
	// Enabled controls whether this rule is active.
	Enabled bool `json:"enabled"`
}

// NewPartitionReassignment creates a new partition reassignment manager.
func NewPartitionReassignment(b *Broker) *PartitionReassignment {
	return &PartitionReassignment{broker: b}
}

// AddRule adds a reassignment rule.
func (pr *PartitionReassignment) AddRule(rule ReassignmentRule) {
	if rule.BatchSize <= 0 {
		rule.BatchSize = 100
	}
	rule.Enabled = true
	pr.mu.Lock()
	pr.rules = append(pr.rules, rule)
	pr.mu.Unlock()
}

// RemoveRule removes a reassignment rule by source topic.
func (pr *PartitionReassignment) RemoveRule(sourceTopic string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	var kept []ReassignmentRule
	for _, r := range pr.rules {
		if r.SourceTopic != sourceTopic {
			kept = append(kept, r)
		}
	}
	pr.rules = kept
}

// ListRules returns all reassignment rules.
func (pr *PartitionReassignment) ListRules() []ReassignmentRule {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	out := make([]ReassignmentRule, len(pr.rules))
	copy(out, pr.rules)
	return out
}

// Execute performs a single reassignment pass for all active rules.
func (pr *PartitionReassignment) Execute(ctx context.Context) (int, error) {
	pr.mu.RLock()
	rules := make([]ReassignmentRule, 0)
	for _, r := range pr.rules {
		if r.Enabled {
			rules = append(rules, r)
		}
	}
	pr.mu.RUnlock()
	totalReassigned := 0
	for _, rule := range rules {
		n, err := pr.reassignOne(ctx, rule)
		if err != nil {
			return totalReassigned, err
		}
		totalReassigned += n
	}
	return totalReassigned, nil
}

func (pr *PartitionReassignment) reassignOne(ctx context.Context, rule ReassignmentRule) (int, error) {
	srcStats, err := pr.broker.Store().Stats(ctx)
	if err != nil {
		return 0, err
	}
	srcDepth, ok := srcStats.Queues[rule.SourceTopic]
	if !ok || srcDepth == 0 {
		return 0, nil
	}
	limit := rule.BatchSize
	if limit > srcDepth {
		limit = srcDepth
	}
	msgs, err := pr.broker.Store().Lease(ctx, LeaseRequest{
		Queue:      rule.SourceTopic,
		ConsumerID: "reassignment-" + rule.SourceTopic,
		Limit:      limit,
	})
	if err != nil {
		return 0, err
	}
	reassigned := 0
	for _, msg := range msgs {
		if rule.Filter != nil && !filterMessage(msg, *rule.Filter) {
			_ = pr.broker.Store().Ack(ctx, AckRequest{MessageID: msg.ID, Queue: rule.SourceTopic})
			continue
		}
		_, err := pr.broker.Publisher().Publish(ctx, msg.Topic, msg.Payload,
			WithQueue(rule.DestTopic),
			WithHeaders(msg.Headers),
			WithKey(msg.Key),
			WithType(msg.Type),
			WithMessageID(msg.ID),
		)
		if err != nil {
			_ = pr.broker.Store().Nack(ctx, NackRequest{
				MessageID:        msg.ID,
				Queue:            rule.SourceTopic,
				Action:           NackRetry,
				IncrementAttempt: true,
			})
			continue
		}
		_ = pr.broker.Store().Ack(ctx, AckRequest{MessageID: msg.ID, Queue: rule.SourceTopic})
		reassigned++
	}
	return reassigned, nil
}

// StartReassignmentLoop starts periodic partition reassignment.
func (pr *PartitionReassignment) StartReassignmentLoop(ctx context.Context, interval time.Duration) context.CancelFunc {
	if interval <= 0 {
		interval = time.Minute
	}
	loopCtx, cancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_, _ = pr.Execute(loopCtx)
			case <-loopCtx.Done():
				return
			}
		}
	}()
	return cancel
}

// PartitionOrderingAccessor on Broker.
func (b *Broker) PartitionOrdering() *PartitionOrdering {
	return b.partitionOrdering
}

// PartitionReassignmentAccessor on Broker.
func (b *Broker) PartitionReassignment() *PartitionReassignment {
	return b.partitionReassignment
}

// TopicPartitionStats returns statistics for a topic's partitions.
type TopicPartitionStats struct {
	Topic      string          `json:"topic"`
	Partitions []PartitionInfo `json:"partitions"`
	TotalMsgs  int             `json:"total_messages"`
}

type PartitionInfo struct {
	Partition int    `json:"partition"`
	Name      string `json:"name"`
	Depth     int    `json:"depth"`
	Offset    uint64 `json:"offset"`
}

// TopicPartitionStats returns partition statistics for a topic.
func (b *Broker) TopicPartitionStats(ctx context.Context, topic string, partitions int) (*TopicPartitionStats, error) {
	if partitions <= 0 {
		partitions = 1
	}
	stats, err := b.Store().Stats(ctx)
	if err != nil {
		return nil, err
	}
	result := &TopicPartitionStats{Topic: topic, Partitions: make([]PartitionInfo, partitions)}
	for i := 0; i < partitions; i++ {
		name := PartitionedTopicName(topic, i)
		depth := stats.Queues[name]
		result.Partitions[i] = PartitionInfo{
			Partition: i,
			Name:      name,
			Depth:     depth,
		}
		result.TotalMsgs += depth
	}
	return result, nil
}
