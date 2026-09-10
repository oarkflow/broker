package broker

import (
	"context"
	"fmt"
	"runtime"
	"time"
)

// Version remains pre-v1 until the documented production release gates pass.
const Version = "0.1.0-dev"

type HealthStatus struct {
	Status  string        `json:"status"` // "healthy", "degraded", "unhealthy"
	State   BrokerState   `json:"state"`
	Mode    Mode          `json:"mode"`
	NodeID  string        `json:"node_id"`
	Leader  string        `json:"leader,omitempty"`
	Uptime  time.Duration `json:"uptime"`
	Store   StoreStats    `json:"store"`
	Version string        `json:"version"`
	Checks  []HealthCheck `json:"checks"`
}

type HealthCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"` // "ok", "warn", "fail"
	Detail string `json:"detail,omitempty"`
}

type BrokerMetrics struct {
	MessagesPublished    uint64        `json:"messages_published"`
	MessagesDelivered    uint64        `json:"messages_delivered"`
	MessagesAcked        uint64        `json:"messages_acked"`
	MessagesNacked       uint64        `json:"messages_nacked"`
	MessagesDeadLettered uint64        `json:"messages_dead_lettered"`
	ActiveConsumers      int           `json:"active_consumers"`
	ActiveSubscriptions  int           `json:"active_subscriptions"`
	QueueCount           int           `json:"queue_count"`
	TopicCount           int           `json:"topic_count"`
	StreamCount          int           `json:"stream_count"`
	StoreStats           StoreStats    `json:"store_stats"`
	Uptime               time.Duration `json:"uptime"`
	RaftState            string        `json:"raft_state"`
	RaftTerm             uint64        `json:"raft_term"`
	RaftCommitIndex      uint64        `json:"raft_commit_index"`
	RaftAppliedIndex     uint64        `json:"raft_applied_index"`
	IsLeader             bool          `json:"is_leader"`
}

func (b *Broker) StartTime() time.Time {
	return b.startedAt
}

func (b *Broker) Health() HealthStatus {
	ctx := context.Background()
	checks := make([]HealthCheck, 0, 4)
	status := "healthy"

	storeCheck := b.checkStore(ctx)
	checks = append(checks, storeCheck)
	if storeCheck.Status == "fail" {
		status = "unhealthy"
	} else if storeCheck.Status == "warn" && status != "unhealthy" {
		status = "degraded"
	}

	memCheck := b.checkMemory()
	checks = append(checks, memCheck)
	if memCheck.Status == "warn" && status == "healthy" {
		status = "degraded"
	}

	goroutineCheck := b.checkGoroutines()
	checks = append(checks, goroutineCheck)
	if goroutineCheck.Status == "warn" && status == "healthy" {
		status = "degraded"
	}

	subsCheck := b.checkSubscriptions()
	checks = append(checks, subsCheck)
	if subsCheck.Status == "fail" {
		status = "unhealthy"
	} else if subsCheck.Status == "warn" && status == "healthy" {
		status = "degraded"
	}

	storeStats, _ := b.store.Stats(ctx)
	leader := ""
	if b.cluster != nil {
		leader = b.cluster.Leader()
	}

	return HealthStatus{
		Status:  status,
		State:   b.State(),
		Mode:    b.cfg.Mode,
		NodeID:  b.cfg.NodeID,
		Leader:  leader,
		Uptime:  time.Since(b.startedAt),
		Store:   storeStats,
		Version: Version,
		Checks:  checks,
	}
}

func (b *Broker) Metrics() BrokerMetrics {
	ctx := context.Background()
	storeStats, _ := b.store.Stats(ctx)

	b.subsMu.RLock()
	activeSubs := len(b.subs)
	b.subsMu.RUnlock()

	var consumers []ConsumerInfo
	if b.consumers != nil {
		consumers = b.consumers.List()
	}
	activeConsumers := 0
	for _, c := range consumers {
		if c.State == ConsumerReady || c.State == ConsumerPolling || c.State == ConsumerProcessing || c.State == ConsumerIdle {
			activeConsumers++
		}
	}

	isLeader := false
	leader := ""
	if b.cluster != nil {
		isLeader = b.cluster.IsLeader()
		leader = b.cluster.Leader()
	}
	_ = leader

	return BrokerMetrics{
		ActiveConsumers:     activeConsumers,
		ActiveSubscriptions: activeSubs,
		QueueCount:          len(storeStats.Queues),
		TopicCount:          len(storeStats.Topics),
		StreamCount:         len(storeStats.Streams),
		StoreStats:          storeStats,
		Uptime:              time.Since(b.startedAt),
		IsLeader:            isLeader,
	}
}

func (b *Broker) checkStore(ctx context.Context) HealthCheck {
	_, err := b.store.Stats(ctx)
	if err != nil {
		return HealthCheck{
			Name:   "store",
			Status: "fail",
			Detail: err.Error(),
		}
	}
	return HealthCheck{Name: "store", Status: "ok"}
}

func (b *Broker) checkMemory() HealthCheck {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	allocMB := float64(m.Alloc) / 1024 / 1024
	if allocMB > 1024 {
		return HealthCheck{
			Name:   "memory",
			Status: "warn",
			Detail: fmt.Sprintf("high memory usage: %.1f MB allocated", allocMB),
		}
	}
	return HealthCheck{Name: "memory", Status: "ok"}
}

func (b *Broker) checkGoroutines() HealthCheck {
	n := runtime.NumGoroutine()
	if n > 10000 {
		return HealthCheck{
			Name:   "goroutines",
			Status: "warn",
			Detail: fmt.Sprintf("high goroutine count: %d", n),
		}
	}
	return HealthCheck{Name: "goroutines", Status: "ok"}
}

func (b *Broker) checkSubscriptions() HealthCheck {
	b.subsMu.RLock()
	defer b.subsMu.RUnlock()
	paused := 0
	for _, s := range b.subs {
		if s.paused.Load() {
			paused++
		}
	}
	total := len(b.subs)
	if paused == total && total > 0 {
		return HealthCheck{
			Name:   "subscriptions",
			Status: "warn",
			Detail: fmt.Sprintf("all %d subscription(s) paused", total),
		}
	}
	return HealthCheck{Name: "subscriptions", Status: "ok"}
}
