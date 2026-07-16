package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// TenancyConfig configures multi-tenancy isolation.
type TenancyConfig struct {
	// Enabled activates tenancy isolation. Default: false.
	Enabled bool `json:"enabled"`
	// DefaultTenant is the tenant for unauthenticated requests. Default: "default".
	DefaultTenant string `json:"default_tenant,omitempty"`
	// IsolationLevel determines how tenants are isolated. Default: "queue_prefix".
	IsolationLevel IsolationLevel `json:"isolation_level,omitempty"`
	// QueuePrefix is the prefix added to queue names per tenant. Default: "t.<tenant>.".
	QueuePrefix string `json:"queue_prefix,omitempty"`
	// MaxQueuesPerTenant limits queues per tenant. 0 = unlimited.
	MaxQueuesPerTenant int `json:"max_queues_per_tenant,omitempty"`
	// MaxMessagesPerTenant limits total messages per tenant. 0 = unlimited.
	MaxMessagesPerTenant int `json:"max_messages_per_tenant,omitempty"`
	// RateLimitPerTenant limits messages per second per tenant. 0 = unlimited.
	RateLimitPerTenant int `json:"rate_limit_per_tenant,omitempty"`
}

// IsolationLevel determines tenant isolation strategy.
type IsolationLevel string

const (
	// IsolationQueuePrefix isolates tenants by prefixing queue names.
	IsolationQueuePrefix IsolationLevel = "queue_prefix"
	// IsolationVHost isolates tenants like AMQP vhosts (logical namespace).
	IsolationVHost IsolationLevel = "vhost"
	// IsolationShared uses shared queues but filters by tenant header.
	IsolationShared IsolationLevel = "shared"
)

// TenantManager manages multi-tenancy isolation.
type TenantManager struct {
	broker    *Broker
	config    TenancyConfig
	tenants   map[string]*TenantState
	mu        sync.RWMutex
}

type TenantState struct {
	Name         string    `json:"name"`
	QueueCount   int       `json:"queue_count"`
	MessageCount int       `json:"message_count"`
	CreatedAt    time.Time `json:"created_at"`
	Quotas       TenantQuotas `json:"quotas"`
}

type TenantQuotas struct {
	MaxQueues     int `json:"max_queues"`
	MaxMessages   int `json:"max_messages"`
	RateLimit     int `json:"rate_limit"`
}

// NewTenantManager creates a new tenant manager.
func NewTenantManager(b *Broker, cfg TenancyConfig) *TenantManager {
	if cfg.DefaultTenant == "" {
		cfg.DefaultTenant = "default"
	}
	if cfg.IsolationLevel == "" {
		cfg.IsolationLevel = IsolationQueuePrefix
	}
	if cfg.QueuePrefix == "" {
		cfg.QueuePrefix = "t."
	}
	return &TenantManager{
		broker:  b,
		config:  cfg,
		tenants: make(map[string]*TenantState),
	}
}

// GetTenant returns the state for a tenant.
func (tm *TenantManager) GetTenant(name string) *TenantState {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.tenants[name]
}

// ListTenants returns all tenant states.
func (tm *TenantManager) ListTenants() []*TenantState {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	out := make([]*TenantState, 0, len(tm.tenants))
	for _, t := range tm.tenants {
		out = append(out, t)
	}
	return out
}

// EnsureTenant creates a tenant if it doesn't exist.
func (tm *TenantManager) EnsureTenant(name string) *TenantState {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if t, ok := tm.tenants[name]; ok {
		return t
	}
	t := &TenantState{
		Name:      name,
		CreatedAt: time.Now(),
		Quotas: TenantQuotas{
			MaxQueues:   tm.config.MaxQueuesPerTenant,
			MaxMessages: tm.config.MaxMessagesPerTenant,
			RateLimit:   tm.config.RateLimitPerTenant,
		},
	}
	tm.tenants[name] = t
	return t
}

// QueueName returns the tenant-isolated queue name.
func (tm *TenantManager) QueueName(tenant, queue string) string {
	if !tm.config.Enabled {
		return queue
	}
	switch tm.config.IsolationLevel {
	case IsolationQueuePrefix:
		return tm.config.QueuePrefix + tenant + "." + queue
	case IsolationVHost:
		return tenant + ":" + queue
	case IsolationShared:
		return queue
	default:
		return queue
	}
}

// TopicName returns the tenant-isolated topic name.
func (tm *TenantManager) TopicName(tenant, topic string) string {
	if !tm.config.Enabled {
		return topic
	}
	switch tm.config.IsolationLevel {
	case IsolationQueuePrefix:
		return tm.config.QueuePrefix + tenant + "." + topic
	case IsolationVHost:
		return tenant + ":" + topic
	default:
		return topic
	}
}

// CheckQuota checks if a tenant has exceeded its quotas.
func (tm *TenantManager) CheckQuota(tenant string) error {
	if !tm.config.Enabled {
		return nil
	}
	t := tm.GetTenant(tenant)
	if t == nil {
		return nil
	}
	if t.Quotas.MaxQueues > 0 && t.QueueCount >= t.Quotas.MaxQueues {
		return errf(ErrBackpressure, "tenant %s queue quota exceeded: %d/%d", tenant, t.QueueCount, t.Quotas.MaxQueues)
	}
	if t.Quotas.MaxMessages > 0 && t.MessageCount >= t.Quotas.MaxMessages {
		return errf(ErrBackpressure, "tenant %s message quota exceeded: %d/%d", tenant, t.MessageCount, t.Quotas.MaxMessages)
	}
	return nil
}

// TenantAwarePublish wraps publish with tenant isolation.
func (tm *TenantManager) TenantAwarePublish(ctx context.Context, tenant string, topic string, payload any, opts ...PublishOption) (PublishResult, error) {
	if !tm.config.Enabled {
		return tm.broker.Publisher().Publish(ctx, topic, payload, opts...)
	}
	isolatedTopic := tm.TopicName(tenant, topic)
	opts = append(opts, WithTenantID(tenant))
	return tm.broker.Publisher().Publish(ctx, isolatedTopic, payload, opts...)
}

// TenantAwareSubscribe wraps subscribe with tenant isolation.
func (tm *TenantManager) TenantAwareSubscribe(ctx context.Context, tenant string, cfg ConsumerConfig, handler Handler) (*Subscription, error) {
	if !tm.config.Enabled {
		return tm.broker.Subscribe(ctx, cfg, handler)
	}
	cfg.Queue = tm.QueueName(tenant, cfg.Queue)
	if cfg.Topic != "" {
		cfg.Topic = tm.TopicName(tenant, cfg.Topic)
	}
	return tm.broker.Subscribe(ctx, cfg, handler)
}

// TenantStats returns stats filtered by tenant.
type TenantStats struct {
	Tenant      string     `json:"tenant"`
	QueueCount  int        `json:"queue_count"`
	TopicCount  int        `json:"topic_count"`
	MessageCount int       `json:"message_count"`
	LeasedCount int        `json:"leased_count"`
	DLQCount    int        `json:"dlq_count"`
}

// StatsForTenant returns stats for a specific tenant.
func (tm *TenantManager) StatsForTenant(ctx context.Context, tenant string) (*TenantStats, error) {
	stats, err := tm.broker.Store().Stats(ctx)
	if err != nil {
		return nil, err
	}
	ts := &TenantStats{Tenant: tenant}
	prefix := tm.config.QueuePrefix + tenant + "."
	for name, count := range stats.Queues {
		if tm.config.Enabled && len(name) > len(prefix) && name[:len(prefix)] == prefix {
			ts.QueueCount++
			ts.MessageCount += count
		}
	}
	for name := range stats.Topics {
		if tm.config.Enabled && len(name) > len(prefix) && name[:len(prefix)] == prefix {
			ts.TopicCount++
		}
	}
	ts.LeasedCount = stats.Leased
	ts.DLQCount = stats.DeadLetter
	return ts, nil
}

// TenantAccessor on Broker.
func (b *Broker) TenantManager() *TenantManager {
	return b.tenantMgr
}

// PartitionConfig configures partition-based routing.
type PartitionConfig struct {
	// Partitions is the number of partitions per topic. Default: 1.
	Partitions int `json:"partitions,omitempty"`
	// Strategy is the partitioning strategy. Default: "hash".
	Strategy PartitionStrategy `json:"strategy,omitempty"`
}

// PartitionStrategy determines how messages are assigned to partitions.
type PartitionStrategy string

const (
	PartitionHash      PartitionStrategy = "hash"
	PartitionRoundRobin PartitionStrategy = "round_robin"
	PartitionRandom    PartitionStrategy = "random"
	PartitionKey       PartitionStrategy = "key"
)

// PartitionRouter routes messages to specific partitions.
type PartitionRouter struct {
	config  PartitionConfig
	counter uint64
}

// NewPartitionRouter creates a new partition router.
func NewPartitionRouter(cfg PartitionConfig) *PartitionRouter {
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.Strategy == "" {
		cfg.Strategy = PartitionHash
	}
	return &PartitionRouter{config: cfg}
}

// SelectPartition returns the partition index for a message.
func (pr *PartitionRouter) SelectPartition(msg Message) int {
	switch pr.config.Strategy {
	case PartitionRoundRobin:
		pr.counter++
		return int(pr.counter % uint64(pr.config.Partitions))
	case PartitionRandom:
		return int(time.Now().UnixNano() % int64(pr.config.Partitions))
	case PartitionKey:
		key := msg.Key
		if key == "" {
			key = msg.RoutingKey
		}
		return int(hashKey(key) % uint32(pr.config.Partitions))
	case PartitionHash:
		fallthrough
	default:
		key := msg.Key
		if key == "" {
			key = msg.ID
		}
		return int(hashKey(key) % uint32(pr.config.Partitions))
	}
}

// PartitionCount returns the configured partition count.
func (pr *PartitionRouter) PartitionCount() int {
	return pr.config.Partitions
}

// TopicPartition represents a partition within a topic.
type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

// PartitionedTopicName returns the partitioned topic name.
func PartitionedTopicName(topic string, partition int) string {
	return fmt.Sprintf("%s.p%d", topic, partition)
}

// init ensures json import is used.
func init() {
	_ = json.Marshal
	_ = json.Unmarshal
}
