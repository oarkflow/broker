package queue

import (
	"sync"
	"time"
)

type Item[T any] struct {
	Value    T
	Priority int
	Seq      uint64
	RunAt    time.Time
}

type pq[T any] []Item[T]

func lessItem[T any](a, b Item[T]) bool {
	if !a.RunAt.Equal(b.RunAt) {
		return a.RunAt.Before(b.RunAt)
	}
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return a.Seq < b.Seq
}

func (p *pq[T]) push(it Item[T]) {
	a := append(*p, it)
	i := len(a) - 1
	for i > 0 {
		parent := (i - 1) >> 1
		if !lessItem(a[i], a[parent]) {
			break
		}
		a[i], a[parent] = a[parent], a[i]
		i = parent
	}
	*p = a
}

func (p *pq[T]) pop() Item[T] {
	a := *p
	n := len(a) - 1
	out := a[0]
	last := a[n]
	var zero Item[T]
	a[n] = zero
	a = a[:n]
	if n > 0 {
		a[0] = last
		i := 0
		for {
			left := i*2 + 1
			if left >= n {
				break
			}
			child := left
			right := left + 1
			if right < n && lessItem(a[right], a[left]) {
				child = right
			}
			if !lessItem(a[child], a[i]) {
				break
			}
			a[i], a[child] = a[child], a[i]
			i = child
		}
	}
	*p = a
	return out
}

type PriorityQueue[T any] struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	items    pq[T]
	cap      int
	closed   bool
	wake     chan struct{}
}

func NewPriorityQueue[T any](capacity int) *PriorityQueue[T] {
	q := &PriorityQueue[T]{cap: capacity, wake: make(chan struct{}, 1)}
	if capacity > 0 {
		q.items = make(pq[T], 0, capacity)
	}
	q.notEmpty = sync.NewCond(&q.mu)
	return q
}
func (q *PriorityQueue[T]) Len() int     { q.mu.Lock(); n := len(q.items); q.mu.Unlock(); return n }
func (q *PriorityQueue[T]) Cap() int     { return q.cap }
func (q *PriorityQueue[T]) Closed() bool { q.mu.Lock(); v := q.closed; q.mu.Unlock(); return v }
func (q *PriorityQueue[T]) Close() {
	q.mu.Lock()
	q.closed = true
	q.signalLocked()
	q.notEmpty.Broadcast()
	q.mu.Unlock()
}
func (q *PriorityQueue[T]) signalLocked() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
func (q *PriorityQueue[T]) Push(it Item[T]) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if q.cap > 0 && len(q.items) >= q.cap {
		return false
	}
	q.items.push(it)
	q.signalLocked()
	q.notEmpty.Signal()
	return true
}
func (q *PriorityQueue[T]) DropOldestPush(it Item[T]) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if q.cap > 0 && len(q.items) >= q.cap {
		_ = q.items.pop()
	}
	q.items.push(it)
	q.signalLocked()
	q.notEmpty.Signal()
	return true
}
func (q *PriorityQueue[T]) Pop() (Item[T], bool) {
	for {
		q.mu.Lock()
		for len(q.items) == 0 && !q.closed {
			q.notEmpty.Wait()
		}
		if len(q.items) == 0 && q.closed {
			q.mu.Unlock()
			var z Item[T]
			return z, false
		}
		first := q.items[0]
		wait := time.Until(first.RunAt)
		if wait <= 0 {
			it := q.items.pop()
			q.mu.Unlock()
			return it, true
		}
		wake := q.wake
		q.mu.Unlock()
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-wake:
		}
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
	}
}
func (q *PriorityQueue[T]) TryPop() (Item[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 || q.items[0].RunAt.After(time.Now()) {
		var z Item[T]
		return z, false
	}
	return q.items.pop(), true
}

func (q *PriorityQueue[T]) PopTimeout(timeout time.Duration) (Item[T], bool) {
	deadline := time.Now().Add(timeout)
	for {
		q.mu.Lock()
		for len(q.items) == 0 && !q.closed {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				q.mu.Unlock()
				var z Item[T]
				return z, false
			}
			wake := q.wake
			q.mu.Unlock()
			t := time.NewTimer(remaining)
			select {
			case <-t.C:
			case <-wake:
			}
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			q.mu.Lock()
		}
		if len(q.items) == 0 && q.closed {
			q.mu.Unlock()
			var z Item[T]
			return z, false
		}
		first := q.items[0]
		wait := time.Until(first.RunAt)
		remaining := time.Until(deadline)
		if remaining <= 0 {
			q.mu.Unlock()
			var z Item[T]
			return z, false
		}
		if wait <= 0 {
			it := q.items.pop()
			q.mu.Unlock()
			return it, true
		}
		if wait > remaining {
			wait = remaining
		}
		wake := q.wake
		q.mu.Unlock()
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-wake:
		}
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
	}
}
