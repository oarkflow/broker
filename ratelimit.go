package broker

import (
	"context"
	"sync"
	"time"
)

// RateLimiter provides per-key rate limiting using a token bucket algorithm.
type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*tokenBucket
	rate     int           // tokens per second
	burst    int           // max burst size
	cleanup  time.Duration // how often to clean stale buckets
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

type tokenBucket struct {
	tokens   float64
	lastTime time.Time
}

// NewRateLimiter creates a rate limiter with the given rate (tokens/sec) and burst size.
func NewRateLimiter(ratePerSecond, burst int) *RateLimiter {
	rl := &RateLimiter{
		buckets: make(map[string]*tokenBucket),
		rate:    ratePerSecond,
		burst:   burst,
		cleanup: 5 * time.Minute,
		stopCh:  make(chan struct{}),
	}
	rl.wg.Add(1)
	go rl.cleanupLoop()
	return rl
}

func (rl *RateLimiter) cleanupLoop() {
	defer rl.wg.Done()
	t := time.NewTicker(rl.cleanup)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			rl.cleanupStale()
		case <-rl.stopCh:
			return
		}
	}
}

func (rl *RateLimiter) cleanupStale() {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	cutoff := time.Now().Add(-5 * rl.cleanup)
	for k, b := range rl.buckets {
		if b.lastTime.Before(cutoff) {
			delete(rl.buckets, k)
		}
	}
}

// Allow checks if a request with the given key is allowed.
func (rl *RateLimiter) Allow(key string) bool {
	return rl.AllowN(key, 1)
}

// AllowN checks if n requests with the given key are allowed.
func (rl *RateLimiter) AllowN(key string, n int) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	b, ok := rl.buckets[key]
	if !ok {
		b = &tokenBucket{tokens: float64(rl.burst), lastTime: now}
		rl.buckets[key] = b
	}
	elapsed := now.Sub(b.lastTime).Seconds()
	b.lastTime = now
	b.tokens += elapsed * float64(rl.rate)
	if b.tokens > float64(rl.burst) {
		b.tokens = float64(rl.burst)
	}
	if b.tokens < float64(n) {
		return false
	}
	b.tokens -= float64(n)
	return true
}

// Wait blocks until the request with the given key is allowed or context is cancelled.
func (rl *RateLimiter) Wait(ctx context.Context, key string) error {
	return rl.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests are allowed or context is cancelled.
func (rl *RateLimiter) WaitN(ctx context.Context, key string, n int) error {
	for {
		if rl.AllowN(key, n) {
			return nil
		}
		waitTime := time.Second / time.Duration(rl.rate)
		if waitTime < time.Millisecond {
			waitTime = time.Millisecond
		}
		t := time.NewTimer(waitTime)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
}

// Stop halts the cleanup goroutine.
func (rl *RateLimiter) Stop() {
	rl.stopOnce.Do(func() { close(rl.stopCh) })
	rl.wg.Wait()
}

// Reset clears all buckets for the given key.
func (rl *RateLimiter) Reset(key string) {
	rl.mu.Lock()
	delete(rl.buckets, key)
	rl.mu.Unlock()
}

// RateLimitConfig configures per-queue or per-topic rate limits.
type RateLimitConfig struct {
	// PerQueue limits messages per second for a specific queue.
	PerQueue map[string]int `json:"per_queue,omitempty"`
	// PerTopic limits messages per second for a specific topic.
	PerTopic map[string]int `json:"per_topic,omitempty"`
	// DefaultRate is the default limit if no specific rate is configured. 0 = unlimited.
	DefaultRate int `json:"default_rate,omitempty"`
	// DefaultBurst is the default burst size. Default: uses rate value.
	DefaultBurst int `json:"default_burst,omitempty"`
}

// QueueRateLimiter manages rate limiters for multiple queues.
type QueueRateLimiter struct {
	limiter *RateLimiter
	config  RateLimitConfig
}

// NewQueueRateLimiter creates a new queue/topic rate limiter.
func NewQueueRateLimiter(cfg RateLimitConfig) *QueueRateLimiter {
	return &QueueRateLimiter{
		limiter: NewRateLimiter(10000, 10000), // High default, per-queue overrides
		config:  cfg,
	}
}

// AllowQueue checks if a message for the given queue is rate-limited.
func (q *QueueRateLimiter) AllowQueue(queue string) bool {
	rate, ok := q.config.PerQueue[queue]
	if !ok {
		rate = q.config.DefaultRate
	}
	if rate <= 0 {
		return true // unlimited
	}
	return q.limiter.Allow("queue:" + queue)
}

// AllowTopic checks if a message for the given topic is rate-limited.
func (q *QueueRateLimiter) AllowTopic(topic string) bool {
	rate, ok := q.config.PerTopic[topic]
	if !ok {
		rate = q.config.DefaultRate
	}
	if rate <= 0 {
		return true // unlimited
	}
	return q.limiter.Allow("topic:" + topic)
}

// Stop halts the rate limiter.
func (q *QueueRateLimiter) Stop() {
	q.limiter.Stop()
}
