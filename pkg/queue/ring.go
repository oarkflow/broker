package queue

import (
	"runtime"
	"sync/atomic"
)

// Ring is a bounded MPMC ring buffer using sequence numbers. It is optimized for
// immediate FIFO jobs and exposed for users that need a very fast custom queue.
type Ring[T any] struct {
	mask  uint64
	cells []ringCell[T]
	head  atomic.Uint64
	tail  atomic.Uint64
}

type ringCell[T any] struct {
	seq atomic.Uint64
	val T
}

func NewRing[T any](size uint64) *Ring[T] {
	if size < 2 {
		size = 2
	}
	// power of two
	n := uint64(1)
	for n < size {
		n <<= 1
	}
	r := &Ring[T]{mask: n - 1, cells: make([]ringCell[T], n)}
	for i := range r.cells {
		r.cells[i].seq.Store(uint64(i))
	}
	return r
}

func (r *Ring[T]) Cap() uint64 { return uint64(len(r.cells)) }
func (r *Ring[T]) Len() uint64 { return r.tail.Load() - r.head.Load() }

func (r *Ring[T]) TryPush(v T) bool {
	var cell *ringCell[T]
	pos := r.tail.Load()
	for {
		cell = &r.cells[pos&r.mask]
		seq := cell.seq.Load()
		dif := int64(seq) - int64(pos)
		if dif == 0 {
			if r.tail.CompareAndSwap(pos, pos+1) {
				break
			}
		} else if dif < 0 {
			return false
		} else {
			pos = r.tail.Load()
		}
		runtime.Gosched()
	}
	cell.val = v
	cell.seq.Store(pos + 1)
	return true
}

func (r *Ring[T]) TryPop() (T, bool) {
	var zero T
	var cell *ringCell[T]
	pos := r.head.Load()
	for {
		cell = &r.cells[pos&r.mask]
		seq := cell.seq.Load()
		dif := int64(seq) - int64(pos+1)
		if dif == 0 {
			if r.head.CompareAndSwap(pos, pos+1) {
				break
			}
		} else if dif < 0 {
			return zero, false
		} else {
			pos = r.head.Load()
		}
		runtime.Gosched()
	}
	v := cell.val
	cell.val = zero
	cell.seq.Store(pos + r.mask + 1)
	return v, true
}
