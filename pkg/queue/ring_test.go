package queue

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestNewRingSizeRoundsToPowerOfTwo(t *testing.T) {
	tests := []struct {
		name    string
		size    uint64
		wantCap uint64
	}{
		{"zero rounds to minimum 2", 0, 2},
		{"one rounds to minimum 2", 1, 2},
		{"exact power of two kept", 8, 8},
		{"non power of two rounds up", 5, 8},
		{"non power of two rounds up large", 17, 32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRing[int](tt.size)
			if got := r.Cap(); got != tt.wantCap {
				t.Fatalf("Cap() = %d, want %d", got, tt.wantCap)
			}
			if got := r.Len(); got != 0 {
				t.Fatalf("Len() = %d, want 0", got)
			}
		})
	}
}

func TestRingTryPushTryPop(t *testing.T) {
	r := NewRing[int](4)

	for i := 1; i <= 4; i++ {
		if !r.TryPush(i) {
			t.Fatalf("TryPush(%d) should succeed", i)
		}
	}
	if r.TryPush(5) {
		t.Fatalf("TryPush(5) should fail, ring full")
	}
	if got := r.Len(); got != 4 {
		t.Fatalf("Len() = %d, want 4", got)
	}

	for i := 1; i <= 4; i++ {
		v, ok := r.TryPop()
		if !ok || v != i {
			t.Fatalf("TryPop() = (%d, %v), want (%d, true)", v, ok, i)
		}
	}
	if _, ok := r.TryPop(); ok {
		t.Fatalf("TryPop() on empty ring should fail")
	}
}

func TestRingTryPopEmpty(t *testing.T) {
	r := NewRing[string](2)
	if _, ok := r.TryPop(); ok {
		t.Fatalf("TryPop() on empty ring should fail")
	}
}

func TestRingWrapAround(t *testing.T) {
	r := NewRing[int](4)
	for i := 0; i < 4; i++ {
		r.TryPush(i)
	}
	// drain two, push two more, forcing index wrap.
	for i := 0; i < 2; i++ {
		v, ok := r.TryPop()
		if !ok || v != i {
			t.Fatalf("TryPop() = (%d, %v), want (%d, true)", v, ok, i)
		}
	}
	r.TryPush(4)
	r.TryPush(5)
	want := []int{2, 3, 4, 5}
	for _, w := range want {
		v, ok := r.TryPop()
		if !ok || v != w {
			t.Fatalf("TryPop() = (%d, %v), want (%d, true)", v, ok, w)
		}
	}
}

func TestRingReuseAfterDrain(t *testing.T) {
	r := NewRing[int](2)
	for round := 0; round < 3; round++ {
		if !r.TryPush(round) {
			t.Fatalf("round %d: TryPush should succeed", round)
		}
		if !r.TryPush(round + 100) {
			t.Fatalf("round %d: TryPush should succeed", round)
		}
		v, ok := r.TryPop()
		if !ok || v != round {
			t.Fatalf("round %d: TryPop() = (%d, %v), want (%d, true)", round, v, ok, round)
		}
		v, ok = r.TryPop()
		if !ok || v != round+100 {
			t.Fatalf("round %d: TryPop() = (%d, %v), want (%d, true)", round, v, ok, round+100)
		}
	}
}

func TestRingConcurrentMPMC(t *testing.T) {
	const (
		numProducers = 4
		numConsumers = 4
		perProducer  = 2000
	)
	r := NewRing[int](64)

	var produced sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		produced.Add(1)
		go func(base int) {
			defer produced.Done()
			for i := 0; i < perProducer; i++ {
				v := base*perProducer + i
				for !r.TryPush(v) {
					// spin until space is available
				}
			}
		}(p)
	}

	total := numProducers * perProducer
	results := make(chan int, total)
	var consumedCount atomic.Int64
	var consumers sync.WaitGroup
	for c := 0; c < numConsumers; c++ {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for {
				if consumedCount.Load() >= int64(total) {
					return
				}
				v, ok := r.TryPop()
				if !ok {
					continue
				}
				results <- v
				if consumedCount.Add(1) >= int64(total) {
					return
				}
			}
		}()
	}

	produced.Wait()
	consumers.Wait()
	close(results)

	seen := make(map[int]bool)
	count := 0
	for v := range results {
		if seen[v] {
			t.Fatalf("duplicate value received: %d", v)
		}
		seen[v] = true
		count++
	}
	if count != total {
		t.Fatalf("got %d values, want %d", count, total)
	}
}
