package queue

import (
	"sync"
	"time"
)

// FIFO is a bounded blocking queue optimized for immediate jobs. It uses a
// fixed-size circular buffer, no per-item allocation, and O(1) push/pop.
type FIFO[T any] struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	buf      []T
	head     int
	tail     int
	len      int
	closed   bool
}

func NewFIFO[T any](capacity int) *FIFO[T] {
	if capacity <= 0 {
		capacity = 1
	}
	q := &FIFO[T]{buf: make([]T, capacity)}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

func (q *FIFO[T]) Len() int { q.mu.Lock(); n := q.len; q.mu.Unlock(); return n }
func (q *FIFO[T]) Cap() int { return len(q.buf) }

func (q *FIFO[T]) Close() {
	q.mu.Lock()
	q.closed = true
	q.notEmpty.Broadcast()
	q.notFull.Broadcast()
	q.mu.Unlock()
}

func (q *FIFO[T]) TryPush(v T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.len == len(q.buf) {
		return false
	}
	q.buf[q.tail] = v
	q.tail++
	if q.tail == len(q.buf) {
		q.tail = 0
	}
	q.len++
	q.notEmpty.Signal()
	return true
}

func (q *FIFO[T]) Push(v T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.len == len(q.buf) && !q.closed {
		q.notFull.Wait()
	}
	if q.closed {
		return false
	}
	q.buf[q.tail] = v
	q.tail++
	if q.tail == len(q.buf) {
		q.tail = 0
	}
	q.len++
	q.notEmpty.Signal()
	return true
}

func (q *FIFO[T]) TryPop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.len == 0 {
		var z T
		return z, false
	}
	return q.popLocked(), true
}

func (q *FIFO[T]) Pop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.len == 0 && !q.closed {
		q.notEmpty.Wait()
	}
	if q.len == 0 && q.closed {
		var z T
		return z, false
	}
	return q.popLocked(), true
}

func (q *FIFO[T]) PopTimeout(d time.Duration) (T, bool) {
	if d <= 0 {
		return q.TryPop()
	}
	deadline := time.Now().Add(d)
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.len == 0 && !q.closed {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			var z T
			return z, false
		}
		// sync.Cond has no timed wait. Use a small helper goroutine would allocate;
		// for worker loops, unlock/sleep/relock is cheaper and sufficient.
		q.mu.Unlock()
		time.Sleep(minDuration(remaining, time.Millisecond))
		q.mu.Lock()
	}
	if q.len == 0 && q.closed {
		var z T
		return z, false
	}
	return q.popLocked(), true
}

func (q *FIFO[T]) popLocked() T {
	v := q.buf[q.head]
	var z T
	q.buf[q.head] = z
	q.head++
	if q.head == len(q.buf) {
		q.head = 0
	}
	q.len--
	q.notFull.Signal()
	return v
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
