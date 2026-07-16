package broker

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PrometheusMetrics holds all broker metrics in Prometheus exposition format.
type PrometheusMetrics struct {
	mu sync.RWMutex

	// Counters
	published   atomic.Uint64
	delivered   atomic.Uint64
	acked       atomic.Uint64
	nacked      atomic.Uint64
	deadLettered atomic.Uint64
	retried     atomic.Uint64
	moved       atomic.Uint64
	publishedBytes atomic.Uint64
	deliveredBytes atomic.Uint64

	// Per-queue counters
	queueDepth   sync.Map // queue -> *atomic.Int64
	queueLeased  sync.Map // queue -> *atomic.Int64
	queueAcked   sync.Map // queue -> *atomic.Int64
	queueNacked  sync.Map // queue -> *atomic.Int64
	queueDLQ     sync.Map // queue -> *atomic.Int64

	// Per-topic counters
	topicPublished sync.Map // topic -> *atomic.Uint64

	// Gauge
	activeConsumers     atomic.Int64
	activeSubscriptions atomic.Int64
	storeMessages       atomic.Int64
	storeLeased         atomic.Int64
	storeDeadLetter     atomic.Int64
	goroutines          atomic.Int64
	memoryAllocBytes    atomic.Int64

	// Histograms (simple bucket-based)
	publishLatencyBuckets  [10]atomic.Uint64 // 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 5s
	consumeLatencyBuckets [10]atomic.Uint64
	ackLatencyBuckets     [10]atomic.Uint64

	// Rate tracking
	publishRate     atomic.Uint64
	consumeRate     atomic.Uint64
	lastRateReset   atomic.Int64
	rateWindowStart atomic.Int64
}

var (
	defaultMetrics     *PrometheusMetrics
	defaultMetricsOnce sync.Once
)

// GlobalPrometheusMetrics returns the global metrics instance.
func GlobalPrometheusMetrics() *PrometheusMetrics {
	defaultMetricsOnce.Do(func() {
		defaultMetrics = NewPrometheusMetrics()
	})
	return defaultMetrics
}

// NewPrometheusMetrics creates a new metrics collector.
func NewPrometheusMetrics() *PrometheusMetrics {
	m := &PrometheusMetrics{}
	m.lastRateReset.Store(time.Now().UnixNano())
	m.rateWindowStart.Store(time.Now().UnixNano())
	return m
}

// RecordPublished records a published message.
func (m *PrometheusMetrics) RecordPublished(topic string, size int, latency time.Duration) {
	m.published.Add(1)
	m.publishedBytes.Add(uint64(size))
	m.recordLatency(&m.publishLatencyBuckets, latency)
	if topic != "" {
		v, _ := m.topicPublished.LoadOrStore(topic, &atomic.Uint64{})
		v.(*atomic.Uint64).Add(1)
	}
	m.publishRate.Add(1)
}

// RecordDelivered records a delivered message.
func (m *PrometheusMetrics) RecordDelivered(queue string, size int, latency time.Duration) {
	m.delivered.Add(1)
	m.deliveredBytes.Add(uint64(size))
	m.recordLatency(&m.consumeLatencyBuckets, latency)
	if queue != "" {
		v, _ := m.queueDepth.LoadOrStore(queue, &atomic.Int64{})
		v.(*atomic.Int64).Add(1)
	}
	m.consumeRate.Add(1)
}

// RecordAcked records an acknowledged message.
func (m *PrometheusMetrics) RecordAcked(queue string, latency time.Duration) {
	m.acked.Add(1)
	m.recordLatency(&m.ackLatencyBuckets, latency)
	if queue != "" {
		v, _ := m.queueAcked.LoadOrStore(queue, &atomic.Uint64{})
		v.(*atomic.Uint64).Add(1)
		if v, ok := m.queueDepth.Load(queue); ok {
			v.(*atomic.Int64).Add(-1)
		}
	}
}

// RecordNacked records a nacked message.
func (m *PrometheusMetrics) RecordNacked(queue string) {
	m.nacked.Add(1)
	if queue != "" {
		v, _ := m.queueNacked.LoadOrStore(queue, &atomic.Uint64{})
		v.(*atomic.Uint64).Add(1)
	}
}

// RecordDeadLettered records a dead-lettered message.
func (m *PrometheusMetrics) RecordDeadLettered(queue string) {
	m.deadLettered.Add(1)
	if queue != "" {
		v, _ := m.queueDLQ.LoadOrStore(queue, &atomic.Uint64{})
		v.(*atomic.Uint64).Add(1)
	}
}

// RecordRetried records a retried message.
func (m *PrometheusMetrics) RecordRetried() {
	m.retried.Add(1)
}

// RecordMoved records a moved message.
func (m *PrometheusMetrics) RecordMoved() {
	m.moved.Add(1)
}

// SetActiveConsumers sets the active consumer count.
func (m *PrometheusMetrics) SetActiveConsumers(n int) {
	m.activeConsumers.Store(int64(n))
}

// SetActiveSubscriptions sets the active subscription count.
func (m *PrometheusMetrics) SetActiveSubscriptions(n int) {
	m.activeSubscriptions.Store(int64(n))
}

// UpdateStoreStats updates store-level gauge metrics.
func (m *PrometheusMetrics) UpdateStoreStats(stats StoreStats) {
	m.storeMessages.Store(int64(stats.Messages))
	m.storeLeased.Store(int64(stats.Leased))
	m.storeDeadLetter.Store(int64(stats.DeadLetter))
}

// recordLatency records a latency value into the appropriate bucket.
func (m *PrometheusMetrics) recordLatency(buckets *[10]atomic.Uint64, d time.Duration) {
	us := d.Microseconds()
	switch {
	case us < 1000:
		buckets[0].Add(1)
	case us < 5000:
		buckets[1].Add(1)
	case us < 10000:
		buckets[2].Add(1)
	case us < 25000:
		buckets[3].Add(1)
	case us < 50000:
		buckets[4].Add(1)
	case us < 100000:
		buckets[5].Add(1)
	case us < 250000:
		buckets[6].Add(1)
	case us < 500000:
		buckets[7].Add(1)
	case us < 1000000:
		buckets[8].Add(1)
	default:
		buckets[9].Add(1)
	}
}

// UpdateGauges refreshes gauge metrics from runtime.
func (m *PrometheusMetrics) UpdateGauges() {
	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)
	m.goroutines.Store(int64(runtime.NumGoroutine()))
	m.memoryAllocBytes.Store(int64(rtm.Alloc))
}

// PromHandler returns an HTTP handler that serves Prometheus exposition format.
func (m *PrometheusMetrics) PromHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.UpdateGauges()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		fmt.Fprint(w, m.exposition())
	})
}

// exposition returns Prometheus text exposition format.
func (m *PrometheusMetrics) exposition() string {
	var b strings.Builder

	b.WriteString("# HELP broker_messages_published_total Total messages published.\n")
	b.WriteString("# TYPE broker_messages_published_total counter\n")
	fmt.Fprintf(&b, "broker_messages_published_total %d\n", m.published.Load())

	b.WriteString("# HELP broker_messages_delivered_total Total messages delivered to consumers.\n")
	b.WriteString("# TYPE broker_messages_delivered_total counter\n")
	fmt.Fprintf(&b, "broker_messages_delivered_total %d\n", m.delivered.Load())

	b.WriteString("# HELP broker_messages_acked_total Total messages acknowledged.\n")
	b.WriteString("# TYPE broker_messages_acked_total counter\n")
	fmt.Fprintf(&b, "broker_messages_acked_total %d\n", m.acked.Load())

	b.WriteString("# HELP broker_messages_nacked_total Total messages nacked.\n")
	b.WriteString("# TYPE broker_messages_nacked_total counter\n")
	fmt.Fprintf(&b, "broker_messages_nacked_total %d\n", m.nacked.Load())

	b.WriteString("# HELP broker_messages_dead_lettered_total Total messages dead-lettered.\n")
	b.WriteString("# TYPE broker_messages_dead_lettered_total counter\n")
	fmt.Fprintf(&b, "broker_messages_dead_lettered_total %d\n", m.deadLettered.Load())

	b.WriteString("# HELP broker_messages_retried_total Total messages retried.\n")
	b.WriteString("# TYPE broker_messages_retried_total counter\n")
	fmt.Fprintf(&b, "broker_messages_retried_total %d\n", m.retried.Load())

	b.WriteString("# HELP broker_messages_moved_total Total messages moved.\n")
	b.WriteString("# TYPE broker_messages_moved_total counter\n")
	fmt.Fprintf(&b, "broker_messages_moved_total %d\n", m.moved.Load())

	b.WriteString("# HELP broker_published_bytes_total Total bytes published.\n")
	b.WriteString("# TYPE broker_published_bytes_total counter\n")
	fmt.Fprintf(&b, "broker_published_bytes_total %d\n", m.publishedBytes.Load())

	b.WriteString("# HELP broker_delivered_bytes_total Total bytes delivered.\n")
	b.WriteString("# TYPE broker_delivered_bytes_total counter\n")
	fmt.Fprintf(&b, "broker_delivered_bytes_total %d\n", m.deliveredBytes.Load())

	b.WriteString("# HELP broker_active_consumers Number of active consumers.\n")
	b.WriteString("# TYPE broker_active_consumers gauge\n")
	fmt.Fprintf(&b, "broker_active_consumers %d\n", m.activeConsumers.Load())

	b.WriteString("# HELP broker_active_subscriptions Number of active subscriptions.\n")
	b.WriteString("# TYPE broker_active_subscriptions gauge\n")
	fmt.Fprintf(&b, "broker_active_subscriptions %d\n", m.activeSubscriptions.Load())

	b.WriteString("# HELP broker_store_messages Total messages in store.\n")
	b.WriteString("# TYPE broker_store_messages gauge\n")
	fmt.Fprintf(&b, "broker_store_messages %d\n", m.storeMessages.Load())

	b.WriteString("# HELP broker_store_leased Total leased messages.\n")
	b.WriteString("# TYPE broker_store_leased gauge\n")
	fmt.Fprintf(&b, "broker_store_leased %d\n", m.storeLeased.Load())

	b.WriteString("# HELP broker_store_dead_letter Total dead-lettered messages.\n")
	b.WriteString("# TYPE broker_store_dead_letter gauge\n")
	fmt.Fprintf(&b, "broker_store_dead_letter %d\n", m.storeDeadLetter.Load())

	b.WriteString("# HELP broker_goroutines Number of goroutines.\n")
	b.WriteString("# TYPE broker_goroutines gauge\n")
	fmt.Fprintf(&b, "broker_goroutines %d\n", m.goroutines.Load())

	b.WriteString("# HELP broker_memory_alloc_bytes Memory allocated.\n")
	b.WriteString("# TYPE broker_memory_alloc_bytes gauge\n")
	fmt.Fprintf(&b, "broker_memory_alloc_bytes %d\n", m.memoryAllocBytes.Load())

	// Per-queue metrics
	m.queueDepth.Range(func(k, v any) bool {
		queue := k.(string)
		depth := v.(*atomic.Int64).Load()
		fmt.Fprintf(&b, "broker_queue_depth{queue=%q} %d\n", queue, depth)
		return true
	})

	m.queueAcked.Range(func(k, v any) bool {
		queue := k.(string)
		acked := v.(*atomic.Uint64).Load()
		fmt.Fprintf(&b, "broker_queue_acked_total{queue=%q} %d\n", queue, acked)
		return true
	})

	m.queueNacked.Range(func(k, v any) bool {
		queue := k.(string)
		nacked := v.(*atomic.Uint64).Load()
		fmt.Fprintf(&b, "broker_queue_nacked_total{queue=%q} %d\n", queue, nacked)
		return true
	})

	m.queueDLQ.Range(func(k, v any) bool {
		queue := k.(string)
		dlq := v.(*atomic.Uint64).Load()
		fmt.Fprintf(&b, "broker_queue_dlq_total{queue=%q} %d\n", queue, dlq)
		return true
	})

	// Per-topic metrics
	m.topicPublished.Range(func(k, v any) bool {
		topic := k.(string)
		published := v.(*atomic.Uint64).Load()
		fmt.Fprintf(&b, "broker_topic_published_total{topic=%q} %d\n", topic, published)
		return true
	})

	// Histogram buckets
	latencyBuckets := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0}
	b.WriteString("# HELP broker_publish_latency_seconds Publish latency histogram.\n")
	b.WriteString("# TYPE broker_publish_latency_seconds histogram\n")
	cumCount := uint64(0)
	for i, bucket := range latencyBuckets {
		cumCount += m.publishLatencyBuckets[i].Load()
		fmt.Fprintf(&b, "broker_publish_latency_seconds_bucket{le=\"%g\"} %d\n", bucket, cumCount)
	}
	fmt.Fprintf(&b, "broker_publish_latency_seconds_bucket{le=\"+Inf\"} %d\n", m.published.Load())
	fmt.Fprintf(&b, "broker_publish_latency_seconds_sum %f\n", estimateSum(m.publishLatencyBuckets[:]))
	fmt.Fprintf(&b, "broker_publish_latency_seconds_count %d\n", m.published.Load())

	b.WriteString("# HELP broker_consume_latency_seconds Consume latency histogram.\n")
	b.WriteString("# TYPE broker_consume_latency_seconds histogram\n")
	cumCount = 0
	for i, bucket := range latencyBuckets {
		cumCount += m.consumeLatencyBuckets[i].Load()
		fmt.Fprintf(&b, "broker_consume_latency_seconds_bucket{le=\"%g\"} %d\n", bucket, cumCount)
	}
	fmt.Fprintf(&b, "broker_consume_latency_seconds_bucket{le=\"+Inf\"} %d\n", m.delivered.Load())
	fmt.Fprintf(&b, "broker_consume_latency_seconds_sum %f\n", estimateSum(m.consumeLatencyBuckets[:]))
	fmt.Fprintf(&b, "broker_consume_latency_seconds_count %d\n", m.delivered.Load())

	return b.String()
}

func estimateSum(buckets []atomic.Uint64) float64 {
	// Rough estimate: assume each bucket at midpoint
	midpoints := []float64{0.0005, 0.003, 0.0075, 0.0175, 0.0375, 0.075, 0.175, 0.375, 0.75, 3.0}
	var sum float64
	for i := 0; i < len(buckets) && i < 10; i++ {
		sum += float64(buckets[i].Load()) * midpoints[i]
	}
	return sum
}

// Middleware returns an HTTP middleware that records request metrics.
func (m *PrometheusMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rr := &promResponseWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rr, r)
		latency := time.Since(start)
		m.recordLatency(&m.publishLatencyBuckets, latency)
		_ = rr.status
	})
}

type promResponseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *promResponseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// HookMetrics creates a Hooks struct that records metrics for all broker operations.
func (m *PrometheusMetrics) HookMetrics() Hooks {
	return Hooks{
		BeforePublish: func(ctx context.Context, e PublishEvent) error {
			start := time.Now()
			e.Time = start
			return nil
		},
		AfterPublish: func(ctx context.Context, e PublishEvent) {
			size := len(e.Message.Payload)
			latency := time.Since(e.Time)
			m.RecordPublished(e.Message.Topic, size, latency)
		},
		OnPublishRejected: func(ctx context.Context, e PublishEvent) {
			m.RecordNacked(e.Message.Queue)
		},
		OnPublishFailed: func(ctx context.Context, e PublishEvent) {
			m.RecordDeadLettered(e.Message.Queue)
		},
		AfterHandle: func(ctx context.Context, e MessageEvent) {
			latency := time.Since(e.Time)
			m.RecordAcked(e.Queue, latency)
		},
		OnHandlerError: func(ctx context.Context, e MessageEvent) {
			m.RecordNacked(e.Queue)
		},
		OnDeadLettered: func(ctx context.Context, e DLQEvent) {
			m.RecordDeadLettered(e.Queue)
		},
		OnRetryScheduled: func(ctx context.Context, e RetryEvent) {
			m.RecordRetried()
		},
		OnMessageMoved: func(ctx context.Context, e MovementEvent) {
			m.RecordMoved()
		},
		OnMessageCopied: func(ctx context.Context, e MovementEvent) {
			m.RecordMoved()
		},
	}
}
