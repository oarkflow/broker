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
	rate     int
	burst    int
}

// NewRateLimiter creates a rate limiter with the given rate (tokens/sec) and
// burst size. A non-positive rate is unlimited; a non-positive burst defaults
// to the rate for limited instances.
func NewRateLimiter(ratePerSecond, burst int) *RateLimiter {
	if ratePerSecond <= 0 {
		ratePerSecond = 0
		burst = 0
	} else if burst <= 0 {
		burst = ratePerSecond
	}
	rl := &RateLimiter{
		buckets: make(map[string]*tokenBucket),
		rate:    ratePerSecond,
		burst:   burst,
		cleanup: 5 * time.Minute,
		stopCh:  make(chan struct{}),
	}
	if rl.rate > 0 {
		rl.wg.Add(1)
		go rl.cleanupLoop()
	}
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
	if rl.rate <= 0 || n <= 0 {
		return true
	}
	return rl.allowN(key, n, rl.rate, rl.burst)
}

func (rl *RateLimiter) allowN(key string, n, rate, burst int) bool {
	if n <= 0 {
		return true
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	b, ok := rl.buckets[key]
	if !ok {
		b = &tokenBucket{tokens: float64(burst), lastTime: now, rate: rate, burst: burst}
		rl.buckets[key] = b
	}
	elapsed := now.Sub(b.lastTime).Seconds()
	b.lastTime = now
	b.tokens += elapsed * float64(b.rate)
	if b.tokens > float64(b.burst) {
		b.tokens = float64(b.burst)
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if rl.rate <= 0 || n <= 0 {
		return nil
	}
	if n > rl.burst {
		return errf(ErrRateLimited, "request size %d exceeds rate-limit burst %d", n, rl.burst)
	}
	for {
		if rl.AllowN(key, n) {
			return nil
		}
		waitTime := time.Second / time.Duration(rl.rate)
		if waitTime <= 0 {
			waitTime = time.Nanosecond
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

// NewQueueRateLimiter creates a new queue/topic rate limiter. It snapshots the
// configuration maps, so callers can safely reuse or mutate their input maps.
func NewQueueRateLimiter(cfg RateLimitConfig) *QueueRateLimiter {
	cfg.PerQueue = cloneRateMap(cfg.PerQueue)
	cfg.PerTopic = cloneRateMap(cfg.PerTopic)
	return &QueueRateLimiter{
		limiter: NewRateLimiter(10000, 10000), // High default, per-queue overrides
		config:  cfg,
	}
}

func cloneRateMap(src map[string]int) map[string]int {
	if src == nil {
		return nil
	}
	dst := make(map[string]int, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
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
	burst := q.config.DefaultBurst
	if burst <= 0 {
		burst = rate
	}
	return q.limiter.allowN("queue:"+queue, 1, rate, burst)
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
	burst := q.config.DefaultBurst
	if burst <= 0 {
		burst = rate
	}
	return q.limiter.allowN("topic:"+topic, 1, rate, burst)
}

// AllowDestinations charges all distinct limited destinations together. A
// publish routed to multiple queues/topics must either consume every required
// token or consume none, so a later rejection cannot penalize an earlier
// destination for a publish that was not accepted.
func (q *QueueRateLimiter) AllowDestinations(dests []Destination) (Destination, bool) {
	requests := make([]rateLimitRequest, 0, len(dests))
	seen := make(map[string]struct{}, len(dests))
	for _, dest := range dests {
		var key string
		var rate int
		switch dest.Kind {
		case DestinationQueue, DestinationDLQ:
			key = "queue:" + dest.Name
			rate = q.config.DefaultRate
			if specific, ok := q.config.PerQueue[dest.Name]; ok {
				rate = specific
			}
		case DestinationTopic:
			key = "topic:" + dest.Name
			rate = q.config.DefaultRate
			if specific, ok := q.config.PerTopic[dest.Name]; ok {
				rate = specific
			}
		default:
			continue
		}
		if dest.Name == "" || rate <= 0 {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		burst := q.config.DefaultBurst
		if burst <= 0 {
			burst = rate
		}
		requests = append(requests, rateLimitRequest{key: key, rate: rate, burst: burst, dest: dest})
	}
	if len(requests) == 0 {
		return Destination{}, true
	}

	limiter := q.limiter
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	now := time.Now()
	for i := range requests {
		req := &requests[i]
		bucket, ok := limiter.buckets[req.key]
		if !ok {
			bucket = &tokenBucket{tokens: float64(req.burst), lastTime: now, rate: req.rate, burst: req.burst}
			limiter.buckets[req.key] = bucket
		}
		elapsed := now.Sub(bucket.lastTime).Seconds()
		bucket.lastTime = now
		bucket.tokens += elapsed * float64(bucket.rate)
		if bucket.tokens > float64(bucket.burst) {
			bucket.tokens = float64(bucket.burst)
		}
		if bucket.tokens < 1 {
			return req.dest, false
		}
	}
	for _, req := range requests {
		limiter.buckets[req.key].tokens--
	}
	return Destination{}, true
}

type rateLimitRequest struct {
	key   string
	rate  int
	burst int
	dest  Destination
}

// Stop halts the rate limiter.
func (q *QueueRateLimiter) Stop() {
	q.limiter.Stop()
}
