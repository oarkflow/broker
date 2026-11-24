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
	// SourceTopic is the source queue to reassign from.
	SourceTopic string `json:"source_topic"`
	// DestTopic is the destination queue to reassign to.
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
	rule = cloneReassignmentRule(rule)
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
	for i, rule := range pr.rules {
		out[i] = cloneReassignmentRule(rule)
	}
	return out
}

func cloneReassignmentRule(rule ReassignmentRule) ReassignmentRule {
	if rule.Filter != nil {
		filter := *rule.Filter
		filter.Headers = cloneMap(rule.Filter.Headers)
		rule.Filter = &filter
	}
	return rule
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
	if rule.SourceTopic == "" || rule.DestTopic == "" || rule.SourceTopic == rule.DestTopic {
		return 0, errf(ErrInvalidMessage, "partition reassignment requires distinct source and destination queues")
	}
	msgs, err := pr.broker.Store().ListQueue(ctx, rule.SourceTopic, rule.BatchSize)
	if err != nil {
		return 0, err
	}
	ids := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		if msg.Status != MessageQueued {
			continue
		}
		if rule.Filter != nil && !filterMessage(msg, *rule.Filter) {
			continue
		}
		ids = append(ids, msg.ID)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	moveFilter := MessageFilter{Status: MessageQueued}
	if rule.Filter != nil {
		if rule.Filter.Status != "" && rule.Filter.Status != MessageQueued {
			return 0, nil
		}
		moveFilter.Type = rule.Filter.Type
		moveFilter.TenantID = rule.Filter.TenantID
		moveFilter.Headers = rule.Filter.Headers
	}
	result, err := pr.broker.Store().Move(ctx, MoveRequest{
		SourceType:      DestinationQueue,
		Source:          rule.SourceTopic,
		DestinationType: DestinationQueue,
		Destination:     rule.DestTopic,
		MessageIDs:      ids,
		Filter:          moveFilter,
		Mode:            MoveRemoveSource,
		PreserveID:      true,
		PreserveOffset:  rule.PreserveOffset,
		Reason:          "partition reassignment",
	})
	if err != nil {
		return 0, err
	}
	return result.Moved, nil
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
	Topic             string          `json:"topic"`
	Partitions        []PartitionInfo `json:"partitions"`
	TotalMsgs         int             `json:"total_messages"`
	AverageDepth      float64         `json:"average_depth"`
	MaxPartitionDepth int             `json:"max_partition_depth"`
	HotPartitions     []int           `json:"hot_partitions,omitempty"`
}

type PartitionInfo struct {
	Partition int     `json:"partition"`
	Name      string  `json:"name"`
	Depth     int     `json:"depth"`
	Offset    uint64  `json:"offset"`
	SkewRatio float64 `json:"skew_ratio"`
	Hot       bool    `json:"hot"`
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
	depths := make([]int, partitions)
	for i := 0; i < partitions; i++ {
		name := PartitionedTopicName(topic, i)
		depth := stats.Queues[name]
		depths[i] = depth
		result.Partitions[i] = PartitionInfo{
			Partition: i,
			Name:      name,
			Depth:     depth,
		}
		result.TotalMsgs += depth
		if depth > result.MaxPartitionDepth {
			result.MaxPartitionDepth = depth
		}
	}
	result.AverageDepth = float64(result.TotalMsgs) / float64(partitions)
	threshold := 2.0
	if b.partitionRouter != nil && b.partitionRouter.config.HotPartitionThreshold > 1 {
		threshold = b.partitionRouter.config.HotPartitionThreshold
	}
	if result.AverageDepth > 0 {
		for i, depth := range depths {
			ratio := float64(depth) / result.AverageDepth
			result.Partitions[i].SkewRatio = ratio
			if ratio >= threshold && float64(depth) > result.AverageDepth {
				result.Partitions[i].Hot = true
				result.HotPartitions = append(result.HotPartitions, i)
			}
		}
	}
	return result, nil
}

// SelectPartition chooses a partition using the broker's configured strategy.
// PartitionLeastDepth sends unkeyed messages to a currently shallowest
// partition, rotating ties. Messages with a key or routing key remain hash
// assigned so repeated keys continue to map to the same partition.
func (b *Broker) SelectPartition(ctx context.Context, topic string, msg Message) (int, error) {
	if b == nil || b.partitionRouter == nil {
		return 0, ErrClosed
	}
	router := b.partitionRouter
	if router.config.Strategy != PartitionLeastDepth || msg.Key != "" || msg.RoutingKey != "" {
		return router.SelectPartition(msg), nil
	}
	depths := make([]int, router.config.Partitions)
	if reporter, ok := b.store.(QueueDepthReporter); ok {
		for i := range depths {
			depths[i] = reporter.queueDepth(PartitionedTopicName(topic, i))
		}
	} else {
		stats, err := b.store.Stats(ctx)
		if err != nil {
			return 0, err
		}
		for i := range depths {
			depths[i] = stats.Queues[PartitionedTopicName(topic, i)]
		}
	}
	return router.selectLeastDepth(depths), nil
}

// PublishPartitioned publishes to a queue named <topic>.p<partition>, using
// the broker's configured partition count and strategy.
func (b *Broker) PublishPartitioned(ctx context.Context, topic string, payload any, opts ...PublishOption) (PublishResult, error) {
	if b == nil || b.partitionRouter == nil {
		return PublishResult{}, ErrClosed
	}
	var options PublishOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&options)
		}
	}
	partition, err := b.SelectPartition(ctx, topic, Message{ID: options.MessageID, Key: options.Key, RoutingKey: options.RoutingKey})
	if err != nil {
		return PublishResult{}, err
	}
	opts = append(opts, WithQueue(PartitionedTopicName(topic, partition)))
	return b.Publisher().Publish(ctx, topic, payload, opts...)
}
