package fastworker

import (
	"math/rand"
	"time"
)

type Backoff interface {
	Delay(attempt int) time.Duration
}

type ConstantBackoff time.Duration

func (b ConstantBackoff) Delay(int) time.Duration { return time.Duration(b) }

type ExponentialBackoff struct {
	Initial, Max time.Duration
	Factor       float64
	Jitter       bool
}

func (b ExponentialBackoff) Delay(attempt int) time.Duration {
	if attempt <= 1 {
		attempt = 1
	}
	initial := b.Initial
	if initial <= 0 {
		initial = 10 * time.Millisecond
	}
	factor := b.Factor
	if factor <= 1 {
		factor = 2
	}
	d := float64(initial)
	for i := 1; i < attempt; i++ {
		d *= factor
	}
	out := time.Duration(d)
	if b.Max > 0 && out > b.Max {
		out = b.Max
	}
	if b.Jitter && out > 0 {
		out = time.Duration(rand.Int63n(int64(out/2)+1)) + out/2
	}
	return out
}
