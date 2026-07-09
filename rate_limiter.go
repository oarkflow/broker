package fastworker

import (
	"sync"
	"time"
)

type RateLimitMode uint8

const (
	// RateLimitQueue accepts the job and smooths execution in the background.
	// This is the default and is the recommended mode for request buffering.
	RateLimitQueue RateLimitMode = iota
	// RateLimitReject preserves traditional limiter behavior: Submit returns ErrRateLimited.
	RateLimitReject
	// RateLimitBlock waits during Submit until a token is available. Use carefully.
	RateLimitBlock
)

type RateLimit struct {
	// Rate is tokens per second. Values <= 0 disable the limiter for the key.
	Rate int
	// Burst is the immediate burst size. Values <= 0 default to Rate.
	Burst int
	// Mode controls admission behavior. Zero value RateLimitQueue accepts and smooths.
	Mode RateLimitMode
}

type tokenBucket struct {
	rate   float64
	burst  float64
	tokens float64
	mode   RateLimitMode
	last   time.Time
	mu     sync.Mutex
}

func newBucket(r RateLimit) *tokenBucket {
	if r.Burst <= 0 {
		r.Burst = r.Rate
	}
	return &tokenBucket{rate: float64(r.Rate), burst: float64(r.Burst), tokens: float64(r.Burst), mode: r.Mode, last: time.Now()}
}
func (b *tokenBucket) allow() bool {
	delay, ok := b.reserveDelay(false)
	return ok && delay <= 0
}

// reserveDelay reserves a token and returns how long the caller should wait
// before executing. When queueMode is true, the bucket can reserve future
// tokens by allowing tokens to go negative. This spaces a large burst over time
// without blocking Submit or returning rate-limit errors.
func (b *tokenBucket) reserveDelay(queueMode bool) (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rate <= 0 {
		return 0, true
	}
	now := time.Now()
	if b.last.IsZero() {
		b.last = now
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		b.last = now
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
	}
	if b.tokens >= 1 {
		b.tokens--
		return 0, true
	}
	if !queueMode {
		return 0, false
	}
	waitSeconds := (1 - b.tokens) / b.rate
	b.tokens-- // reserve the future token for this job
	if waitSeconds < 0 {
		waitSeconds = 0
	}
	return time.Duration(waitSeconds * float64(time.Second)), true
}

type keyedSemaphore struct {
	mu     sync.Mutex
	active map[string]int
}

func newKeyedSemaphore() *keyedSemaphore { return &keyedSemaphore{active: make(map[string]int)} }
func (s *keyedSemaphore) tryAcquire(key string, max int) bool {
	if key == "" || max <= 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active[key] >= max {
		return false
	}
	s.active[key]++
	return true
}
func (s *keyedSemaphore) release(key string) {
	if key == "" {
		return
	}
	s.mu.Lock()
	if s.active[key] > 1 {
		s.active[key]--
	} else {
		delete(s.active, key)
	}
	s.mu.Unlock()
}
