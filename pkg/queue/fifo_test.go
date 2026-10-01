package queue

import (
	"sync"
	"testing"
	"time"
)

func TestNewFIFOCapacity(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		wantCap  int
	}{
		{"positive capacity", 4, 4},
		{"zero capacity clamps to 1", 0, 1},
		{"negative capacity clamps to 1", -5, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewFIFO[int](tt.capacity)
			if got := q.Cap(); got != tt.wantCap {
				t.Fatalf("Cap() = %d, want %d", got, tt.wantCap)
			}
			if got := q.Len(); got != 0 {
				t.Fatalf("Len() = %d, want 0", got)
			}
		})
	}
}

func TestFIFOTryPushTryPop(t *testing.T) {
	q := NewFIFO[int](2)

	if !q.TryPush(1) {
		t.Fatalf("TryPush(1) should succeed")
	}
	if !q.TryPush(2) {
		t.Fatalf("TryPush(2) should succeed")
	}
	if q.TryPush(3) {
		t.Fatalf("TryPush(3) should fail, queue full")
	}
	if got := q.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}

	v, ok := q.TryPop()
	if !ok || v != 1 {
		t.Fatalf("TryPop() = (%d, %v), want (1, true)", v, ok)
	}
	v, ok = q.TryPop()
	if !ok || v != 2 {
		t.Fatalf("TryPop() = (%d, %v), want (2, true)", v, ok)
	}
	_, ok = q.TryPop()
	if ok {
		t.Fatalf("TryPop() on empty queue should fail")
	}
}

func TestFIFOWrapAround(t *testing.T) {
	q := NewFIFO[int](3)
	// Fill, drain partially, and refill to exercise head/tail wrap.
	for i := 0; i < 3; i++ {
		if !q.TryPush(i) {
			t.Fatalf("TryPush(%d) should succeed", i)
		}
	}
	for i := 0; i < 2; i++ {
		v, ok := q.TryPop()
		if !ok || v != i {
			t.Fatalf("TryPop() = (%d, %v), want (%d, true)", v, ok, i)
		}
	}
	// buffer now has one element (2), push two more to wrap tail around.
	if !q.TryPush(3) {
		t.Fatalf("TryPush(3) should succeed")
	}
	if !q.TryPush(4) {
		t.Fatalf("TryPush(4) should succeed")
	}
	want := []int{2, 3, 4}
	for _, w := range want {
		v, ok := q.TryPop()
		if !ok || v != w {
			t.Fatalf("TryPop() = (%d, %v), want (%d, true)", v, ok, w)
		}
	}
}

func TestFIFOPushBlocksUntilSpace(t *testing.T) {
	q := NewFIFO[int](1)
	if !q.TryPush(1) {
		t.Fatalf("TryPush(1) should succeed")
	}

	done := make(chan bool, 1)
	go func() {
		done <- q.Push(2)
	}()

	select {
	case <-done:
		t.Fatalf("Push should have blocked while queue is full")
	case <-time.After(50 * time.Millisecond):
	}

	v, ok := q.TryPop()
	if !ok || v != 1 {
		t.Fatalf("TryPop() = (%d, %v), want (1, true)", v, ok)
	}

	select {
	case ok := <-done:
		if !ok {
			t.Fatalf("Push(2) should have succeeded once space freed")
		}
	case <-time.After(time.Second):
		t.Fatalf("Push did not unblock after space freed")
	}

	v, ok = q.TryPop()
	if !ok || v != 2 {
		t.Fatalf("TryPop() = (%d, %v), want (2, true)", v, ok)
	}
}

func TestFIFOPopBlocksUntilItem(t *testing.T) {
	q := NewFIFO[string](2)

	type result struct {
		v  string
		ok bool
	}
	resCh := make(chan result, 1)
	go func() {
		v, ok := q.Pop()
		resCh <- result{v, ok}
	}()

	select {
	case <-resCh:
		t.Fatalf("Pop should have blocked on empty queue")
	case <-time.After(50 * time.Millisecond):
	}

	if !q.Push("hello") {
		t.Fatalf("Push should succeed")
	}

	select {
	case r := <-resCh:
		if !r.ok || r.v != "hello" {
			t.Fatalf("Pop() = (%q, %v), want (hello, true)", r.v, r.ok)
		}
	case <-time.After(time.Second):
		t.Fatalf("Pop did not unblock after push")
	}
}

func TestFIFOCloseUnblocksPushAndPop(t *testing.T) {
	q := NewFIFO[int](1)
	if !q.TryPush(1) { // fill so Push would normally block
		t.Fatalf("TryPush should succeed")
	}

	pushDone := make(chan bool, 1)
	go func() {
		pushDone <- q.Push(2)
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()

	select {
	case ok := <-pushDone:
		if ok {
			t.Fatalf("Push should fail after Close")
		}
	case <-time.After(time.Second):
		t.Fatalf("Push did not unblock after Close")
	}

	// Pop should still drain the remaining buffered item.
	v, ok := q.Pop()
	if !ok || v != 1 {
		t.Fatalf("Pop() = (%d, %v), want (1, true)", v, ok)
	}

	// Now empty and closed: Pop should return immediately with ok=false.
	done := make(chan bool, 1)
	go func() {
		_, ok := q.Pop()
		done <- ok
	}()
	select {
	case ok := <-done:
		if ok {
			t.Fatalf("Pop() on closed empty queue should return false")
		}
	case <-time.After(time.Second):
		t.Fatalf("Pop did not return promptly on closed empty queue")
	}
}

func TestFIFOTryPushTryPopAfterClose(t *testing.T) {
	q := NewFIFO[int](2)
	q.Close()
	if q.TryPush(1) {
		t.Fatalf("TryPush should fail after Close")
	}
	_, ok := q.TryPop()
	if ok {
		t.Fatalf("TryPop should fail on empty closed queue")
	}
}

func TestFIFOPopTimeoutImmediateValue(t *testing.T) {
	q := NewFIFO[int](2)
	q.TryPush(42)
	v, ok := q.PopTimeout(time.Second)
	if !ok || v != 42 {
		t.Fatalf("PopTimeout() = (%d, %v), want (42, true)", v, ok)
	}
}

func TestFIFOPopTimeoutExpires(t *testing.T) {
	q := NewFIFO[int](2)
	start := time.Now()
	v, ok := q.PopTimeout(50 * time.Millisecond)
	elapsed := time.Since(start)
	if ok {
		t.Fatalf("PopTimeout() should fail on empty queue, got %d", v)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("PopTimeout returned too early: %v", elapsed)
	}
}

func TestFIFOPopTimeoutNonPositiveActsAsTryPop(t *testing.T) {
	q := NewFIFO[int](2)
	// Empty queue, d <= 0 should behave like TryPop (immediate failure).
	start := time.Now()
	_, ok := q.PopTimeout(0)
	if ok {
		t.Fatalf("PopTimeout(0) on empty queue should fail")
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("PopTimeout(0) should return immediately, took %v", elapsed)
	}

	q.TryPush(7)
	v, ok := q.PopTimeout(-time.Second)
	if !ok || v != 7 {
		t.Fatalf("PopTimeout(negative) = (%d, %v), want (7, true)", v, ok)
	}
}

func TestFIFOPopTimeoutUnblocksOnPush(t *testing.T) {
	q := NewFIFO[int](2)
	type result struct {
		v  int
		ok bool
	}
	resCh := make(chan result, 1)
	go func() {
		v, ok := q.PopTimeout(time.Second)
		resCh <- result{v, ok}
	}()
	time.Sleep(20 * time.Millisecond)
	q.Push(99)
	select {
	case r := <-resCh:
		if !r.ok || r.v != 99 {
			t.Fatalf("PopTimeout() = (%d, %v), want (99, true)", r.v, r.ok)
		}
	case <-time.After(time.Second):
		t.Fatalf("PopTimeout did not unblock after push")
	}
}

func TestFIFOPopTimeoutReturnsFalseWhenClosed(t *testing.T) {
	q := NewFIFO[int](2)
	type result struct {
		v  int
		ok bool
	}
	resCh := make(chan result, 1)
	go func() {
		v, ok := q.PopTimeout(time.Second)
		resCh <- result{v, ok}
	}()
	time.Sleep(20 * time.Millisecond)
	q.Close()
	select {
	case r := <-resCh:
		if r.ok {
			t.Fatalf("PopTimeout() should fail on closed empty queue")
		}
	case <-time.After(time.Second):
		t.Fatalf("PopTimeout did not unblock after Close")
	}
}

func TestFIFOConcurrentProducersConsumers(t *testing.T) {
	const (
		numProducers = 4
		numConsumers = 4
		perProducer  = 500
	)
	q := NewFIFO[int](16)

	var produced sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		produced.Add(1)
		go func(base int) {
			defer produced.Done()
			for i := 0; i < perProducer; i++ {
				q.Push(base*perProducer + i)
			}
		}(p)
	}

	results := make(chan int, numProducers*perProducer)
	var consumed sync.WaitGroup
	for c := 0; c < numConsumers; c++ {
		consumed.Add(1)
		go func() {
			defer consumed.Done()
			for {
				v, ok := q.Pop()
				if !ok {
					return
				}
				results <- v
			}
		}()
	}

	produced.Wait()
	q.Close()
	consumed.Wait()
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
	if count != numProducers*perProducer {
		t.Fatalf("got %d values, want %d", count, numProducers*perProducer)
	}
}

func TestMinDuration(t *testing.T) {
	tests := []struct {
		a, b, want time.Duration
	}{
		{time.Second, 2 * time.Second, time.Second},
		{2 * time.Second, time.Second, time.Second},
		{time.Second, time.Second, time.Second},
	}
	for _, tt := range tests {
		if got := minDuration(tt.a, tt.b); got != tt.want {
			t.Fatalf("minDuration(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}
