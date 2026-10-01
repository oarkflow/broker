package queue

import (
	"sync"
	"testing"
	"time"
)

func TestNewPriorityQueue(t *testing.T) {
	q := NewPriorityQueue[int](4)
	if got := q.Cap(); got != 4 {
		t.Fatalf("Cap() = %d, want 4", got)
	}
	if got := q.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
	if q.Closed() {
		t.Fatalf("Closed() should be false initially")
	}

	unbounded := NewPriorityQueue[int](0)
	if got := unbounded.Cap(); got != 0 {
		t.Fatalf("Cap() = %d, want 0", got)
	}
}

func TestPriorityQueuePushPopOrderByPriority(t *testing.T) {
	q := NewPriorityQueue[string](0)
	now := time.Now().Add(-time.Second) // already runnable
	items := []Item[string]{
		{Value: "low", Priority: 1, Seq: 1, RunAt: now},
		{Value: "high", Priority: 10, Seq: 2, RunAt: now},
		{Value: "mid", Priority: 5, Seq: 3, RunAt: now},
	}
	for _, it := range items {
		if !q.Push(it) {
			t.Fatalf("Push(%v) should succeed", it)
		}
	}
	wantOrder := []string{"high", "mid", "low"}
	for _, want := range wantOrder {
		it, ok := q.Pop()
		if !ok || it.Value != want {
			t.Fatalf("Pop() = (%v, %v), want (%s, true)", it, ok, want)
		}
	}
}

func TestPriorityQueueOrderBySeqWhenPriorityEqual(t *testing.T) {
	q := NewPriorityQueue[int](0)
	now := time.Now().Add(-time.Second)
	for i := 1; i <= 5; i++ {
		q.Push(Item[int]{Value: i, Priority: 1, Seq: uint64(i), RunAt: now})
	}
	for i := 1; i <= 5; i++ {
		it, ok := q.Pop()
		if !ok || it.Value != i {
			t.Fatalf("Pop() = (%d, %v), want (%d, true)", it.Value, ok, i)
		}
	}
}

func TestPriorityQueueOrderByRunAtFirst(t *testing.T) {
	q := NewPriorityQueue[string](0)
	base := time.Now().Add(-time.Hour)
	// Earlier RunAt wins even with lower priority.
	q.Push(Item[string]{Value: "later-high-pri", Priority: 100, Seq: 1, RunAt: base.Add(time.Minute)})
	q.Push(Item[string]{Value: "earlier-low-pri", Priority: 0, Seq: 2, RunAt: base})

	it, ok := q.Pop()
	if !ok || it.Value != "earlier-low-pri" {
		t.Fatalf("Pop() = (%v, %v), want (earlier-low-pri, true)", it, ok)
	}
}

func TestPriorityQueueTryPopRespectsRunAt(t *testing.T) {
	q := NewPriorityQueue[int](0)
	future := time.Now().Add(time.Hour)
	q.Push(Item[int]{Value: 1, RunAt: future})

	if _, ok := q.TryPop(); ok {
		t.Fatalf("TryPop() should fail when item's RunAt is in the future")
	}

	past := time.Now().Add(-time.Hour)
	q.Push(Item[int]{Value: 2, RunAt: past})
	it, ok := q.TryPop()
	if !ok || it.Value != 2 {
		t.Fatalf("TryPop() = (%d, %v), want (2, true)", it.Value, ok)
	}
}

func TestPriorityQueueTryPopEmpty(t *testing.T) {
	q := NewPriorityQueue[int](0)
	if _, ok := q.TryPop(); ok {
		t.Fatalf("TryPop() on empty queue should fail")
	}
}

func TestPriorityQueueCapacityEnforced(t *testing.T) {
	q := NewPriorityQueue[int](2)
	now := time.Now().Add(-time.Second)
	if !q.Push(Item[int]{Value: 1, RunAt: now}) {
		t.Fatalf("Push 1 should succeed")
	}
	if !q.Push(Item[int]{Value: 2, RunAt: now}) {
		t.Fatalf("Push 2 should succeed")
	}
	if q.Push(Item[int]{Value: 3, RunAt: now}) {
		t.Fatalf("Push 3 should fail, queue at capacity")
	}
}

func TestPriorityQueuePushAfterCloseFails(t *testing.T) {
	q := NewPriorityQueue[int](0)
	q.Close()
	if !q.Closed() {
		t.Fatalf("Closed() should be true after Close")
	}
	if q.Push(Item[int]{Value: 1}) {
		t.Fatalf("Push should fail after Close")
	}
}

func TestPriorityQueueDropOldestPush(t *testing.T) {
	q := NewPriorityQueue[int](2)
	now := time.Now().Add(-time.Second)
	q.Push(Item[int]{Value: 1, Priority: 1, Seq: 1, RunAt: now})
	q.Push(Item[int]{Value: 2, Priority: 5, Seq: 2, RunAt: now})

	// Queue full; DropOldestPush should evict the current head (highest
	// priority item, since pop() removes the root) and insert the new item.
	ok := q.DropOldestPush(Item[int]{Value: 3, Priority: 1, Seq: 3, RunAt: now})
	if !ok {
		t.Fatalf("DropOldestPush should succeed")
	}
	if got := q.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2 after drop-push", got)
	}
}

func TestPriorityQueueDropOldestPushValue(t *testing.T) {
	q := NewPriorityQueue[int](1)
	now := time.Now().Add(-time.Second)
	q.Push(Item[int]{Value: 1, Seq: 1, RunAt: now})

	dropped, didDrop, ok := q.DropOldestPushValue(Item[int]{Value: 2, Seq: 2, RunAt: now})
	if !ok {
		t.Fatalf("DropOldestPushValue should succeed")
	}
	if !didDrop {
		t.Fatalf("expected an item to be dropped")
	}
	if dropped.Value != 1 {
		t.Fatalf("dropped.Value = %d, want 1", dropped.Value)
	}

	// Queue has capacity 1 and currently holds the just-pushed item (no drop needed).
	q2 := NewPriorityQueue[int](2)
	q2.Push(Item[int]{Value: 1, Seq: 1, RunAt: now})
	_, didDrop2, ok2 := q2.DropOldestPushValue(Item[int]{Value: 2, Seq: 2, RunAt: now})
	if !ok2 {
		t.Fatalf("DropOldestPushValue should succeed")
	}
	if didDrop2 {
		t.Fatalf("no item should have been dropped, queue had room")
	}
}

func TestPriorityQueueDropOldestPushValueAfterClose(t *testing.T) {
	q := NewPriorityQueue[int](1)
	q.Close()
	_, didDrop, ok := q.DropOldestPushValue(Item[int]{Value: 1})
	if ok {
		t.Fatalf("DropOldestPushValue should fail after Close")
	}
	if didDrop {
		t.Fatalf("didDrop should be false after Close")
	}
}

func TestPriorityQueuePopBlocksUntilRunAt(t *testing.T) {
	q := NewPriorityQueue[string](0)
	runAt := time.Now().Add(80 * time.Millisecond)
	q.Push(Item[string]{Value: "delayed", RunAt: runAt})

	start := time.Now()
	it, ok := q.Pop()
	elapsed := time.Since(start)
	if !ok || it.Value != "delayed" {
		t.Fatalf("Pop() = (%v, %v), want (delayed, true)", it, ok)
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("Pop() returned too early: %v", elapsed)
	}
}

func TestPriorityQueuePopBlocksOnEmptyThenUnblocksOnPush(t *testing.T) {
	q := NewPriorityQueue[int](0)
	type result struct {
		it Item[int]
		ok bool
	}
	resCh := make(chan result, 1)
	go func() {
		it, ok := q.Pop()
		resCh <- result{it, ok}
	}()

	select {
	case <-resCh:
		t.Fatalf("Pop should block on empty queue")
	case <-time.After(50 * time.Millisecond):
	}

	q.Push(Item[int]{Value: 7, RunAt: time.Now().Add(-time.Second)})

	select {
	case r := <-resCh:
		if !r.ok || r.it.Value != 7 {
			t.Fatalf("Pop() = (%v, %v), want (7, true)", r.it, r.ok)
		}
	case <-time.After(time.Second):
		t.Fatalf("Pop did not unblock after push")
	}
}

func TestPriorityQueuePopReturnsFalseAfterClose(t *testing.T) {
	q := NewPriorityQueue[int](0)
	type result struct {
		ok bool
	}
	resCh := make(chan result, 1)
	go func() {
		_, ok := q.Pop()
		resCh <- result{ok}
	}()
	time.Sleep(20 * time.Millisecond)
	q.Close()
	select {
	case r := <-resCh:
		if r.ok {
			t.Fatalf("Pop() should fail on closed empty queue")
		}
	case <-time.After(time.Second):
		t.Fatalf("Pop did not unblock after Close")
	}
}

func TestPriorityQueuePopTimeoutImmediateValue(t *testing.T) {
	q := NewPriorityQueue[int](0)
	q.Push(Item[int]{Value: 5, RunAt: time.Now().Add(-time.Second)})
	it, ok := q.PopTimeout(time.Second)
	if !ok || it.Value != 5 {
		t.Fatalf("PopTimeout() = (%v, %v), want (5, true)", it, ok)
	}
}

func TestPriorityQueuePopTimeoutExpiresOnEmptyQueue(t *testing.T) {
	q := NewPriorityQueue[int](0)
	start := time.Now()
	_, ok := q.PopTimeout(50 * time.Millisecond)
	elapsed := time.Since(start)
	if ok {
		t.Fatalf("PopTimeout should fail on empty queue")
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("PopTimeout returned too early: %v", elapsed)
	}
}

func TestPriorityQueuePopTimeoutExpiresWaitingForRunAt(t *testing.T) {
	q := NewPriorityQueue[int](0)
	// Item not runnable until long after the timeout expires.
	q.Push(Item[int]{Value: 1, RunAt: time.Now().Add(time.Hour)})
	start := time.Now()
	_, ok := q.PopTimeout(50 * time.Millisecond)
	elapsed := time.Since(start)
	if ok {
		t.Fatalf("PopTimeout should fail: item not runnable within timeout")
	}
	if elapsed < 40*time.Millisecond || elapsed > time.Second {
		t.Fatalf("PopTimeout took unexpected time: %v", elapsed)
	}
}

func TestPriorityQueuePopTimeoutReturnsFalseWhenClosed(t *testing.T) {
	q := NewPriorityQueue[int](0)
	type result struct{ ok bool }
	resCh := make(chan result, 1)
	go func() {
		_, ok := q.PopTimeout(time.Second)
		resCh <- result{ok}
	}()
	time.Sleep(20 * time.Millisecond)
	q.Close()
	select {
	case r := <-resCh:
		if r.ok {
			t.Fatalf("PopTimeout should fail on closed empty queue")
		}
	case <-time.After(time.Second):
		t.Fatalf("PopTimeout did not unblock after Close")
	}
}

func TestPriorityQueuePopTimeoutUnblocksOnPush(t *testing.T) {
	q := NewPriorityQueue[int](0)
	type result struct {
		it Item[int]
		ok bool
	}
	resCh := make(chan result, 1)
	go func() {
		it, ok := q.PopTimeout(time.Second)
		resCh <- result{it, ok}
	}()
	time.Sleep(20 * time.Millisecond)
	q.Push(Item[int]{Value: 42, RunAt: time.Now().Add(-time.Second)})
	select {
	case r := <-resCh:
		if !r.ok || r.it.Value != 42 {
			t.Fatalf("PopTimeout() = (%v, %v), want (42, true)", r.it, r.ok)
		}
	case <-time.After(time.Second):
		t.Fatalf("PopTimeout did not unblock after push")
	}
}

func TestPriorityQueueConcurrentPushPop(t *testing.T) {
	const (
		numProducers = 4
		numConsumers = 4
		perProducer  = 300
	)
	q := NewPriorityQueue[int](0)
	now := time.Now().Add(-time.Second)

	var produced sync.WaitGroup
	var seq uint64
	var seqMu sync.Mutex
	for p := 0; p < numProducers; p++ {
		produced.Add(1)
		go func(base int) {
			defer produced.Done()
			for i := 0; i < perProducer; i++ {
				seqMu.Lock()
				seq++
				s := seq
				seqMu.Unlock()
				q.Push(Item[int]{Value: base*perProducer + i, Priority: i % 5, Seq: s, RunAt: now})
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
				it, ok := q.Pop()
				if !ok {
					return
				}
				results <- it.Value
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

func TestPqHeapPushPopMany(t *testing.T) {
	var p pq[int]
	now := time.Now()
	n := 50
	for i := 0; i < n; i++ {
		p.push(Item[int]{Value: i, Priority: i % 7, Seq: uint64(i), RunAt: now})
	}
	if len(p) != n {
		t.Fatalf("len(p) = %d, want %d", len(p), n)
	}
	var prev Item[int]
	has := false
	for len(p) > 0 {
		it := p.pop()
		if has && lessItem(it, prev) {
			t.Fatalf("heap pop out of order: %v before %v", prev, it)
		}
		prev = it
		has = true
	}
}
