package broker

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"
)

// groupCommitMaxWait bounds how long a commit waits to coalesce writers.
const groupCommitMaxWait = 300 * time.Microsecond

// groupCommitWriter batches concurrent WAL append calls into one buffered
// write/flush/fsync cycle. A caller returns only after its batch is synced.
type groupCommitWriter struct {
	file *os.File
	buf  *bufio.Writer

	closeMu  sync.RWMutex
	closed   bool
	writeCh  chan *groupCommitReq
	doneCh   chan struct{}
	closeErr error
	faultErr error
	onFault  func(error)
}

type groupCommitReq struct {
	data    []byte
	prefix  [binary.MaxVarintLen64]byte
	prefixN uint8
	done    chan error
}

var groupCommitReqPool = sync.Pool{New: func() any {
	return &groupCommitReq{done: make(chan error, 1)}
}}

func newGroupCommitWriter(f *os.File) *groupCommitWriter {
	w := &groupCommitWriter{
		file:    f,
		buf:     bufio.NewWriterSize(f, 4<<20),
		writeCh: make(chan *groupCommitReq, 16384),
		doneCh:  make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *groupCommitWriter) run() {
	defer close(w.doneCh)
	batch := make([]*groupCommitReq, 0, 4096)
	// target is the previous batch size. Writers that were released by the last
	// fsync re-enqueue slightly staggered; starting the next fsync on the first
	// arrival would split them into many tiny batches (a commit convoy), so wait
	// briefly for about as many writers as last time. A lone writer has
	// target==1 and never waits.
	target := 1
	for req := range w.writeCh {
		batch = append(batch[:0], req)
	drain:
		for len(batch) < cap(batch) {
			select {
			case next, ok := <-w.writeCh:
				if !ok {
					break drain
				}
				batch = append(batch, next)
			default:
				break drain
			}
		}
		if len(batch) < target {
			timer := time.NewTimer(groupCommitMaxWait)
		coalesce:
			for len(batch) < target {
				select {
				case next, ok := <-w.writeCh:
					if !ok {
						break coalesce
					}
					batch = append(batch, next)
				case <-timer.C:
					break coalesce
				}
			}
			timer.Stop()
		}
		target = len(batch)
		w.closeMu.RLock()
		writeErr := w.faultErr
		w.closeMu.RUnlock()
		for _, item := range batch {
			if writeErr != nil {
				break
			}
			if item.prefixN > 0 {
				if _, err := w.buf.Write(item.prefix[:item.prefixN]); err != nil {
					writeErr = err
					break
				}
			}
			if _, err := w.buf.Write(item.data); err != nil {
				writeErr = err
				break
			}
		}
		if writeErr == nil {
			writeErr = w.buf.Flush()
		}
		if writeErr == nil {
			writeErr = w.file.Sync()
		}
		if writeErr != nil {
			w.closeMu.Lock()
			firstFault := false
			if w.faultErr == nil {
				w.faultErr = fmt.Errorf("%w: WAL group commit failed: %v", ErrStorageUnavailable, writeErr)
				firstFault = true
			}
			writeErr = w.faultErr
			fn := w.onFault
			w.closeMu.Unlock()
			if firstFault && fn != nil {
				go fn(writeErr)
			}
		}
		for _, item := range batch {
			item.done <- writeErr
		}
	}
	w.closeMu.RLock()
	faultErr := w.faultErr
	w.closeMu.RUnlock()
	if faultErr != nil {
		w.closeErr = faultErr
	} else {
		if err := w.buf.Flush(); err != nil {
			w.closeErr = err
		}
		if w.closeErr == nil {
			w.closeErr = w.file.Sync()
		}
	}
}

func (w *groupCommitWriter) write(data []byte) error {
	req, err := w.enqueue(data, false)
	if err != nil {
		return err
	}
	return w.await(req)
}

// writeUvarintPrefixed writes a uvarint record length and the record as two
// buffered segments. It avoids allocating and copying a combined prefix+record
// slice on every durable mutation.
func (w *groupCommitWriter) writeUvarintPrefixed(data []byte) error {
	req, err := w.enqueue(data, true)
	if err != nil {
		return err
	}
	return w.await(req)
}

// enqueue hands a record to the commit goroutine and returns without waiting
// for the fsync. Records reach the log in enqueue order, so a caller that
// enqueues while holding its store lock fixes the WAL order, then releases the
// lock and calls await. That lets concurrent callers share one fsync instead of
// serializing one fsync per record.
func (w *groupCommitWriter) enqueue(data []byte, prefixed bool) (*groupCommitReq, error) {
	w.closeMu.RLock()
	if w.closed || w.faultErr != nil {
		err := w.faultErr
		w.closeMu.RUnlock()
		if err != nil {
			return nil, err
		}
		return nil, ErrClosed
	}
	req := groupCommitReqPool.Get().(*groupCommitReq)
	req.data = data
	if prefixed {
		req.prefixN = uint8(binary.PutUvarint(req.prefix[:], uint64(len(data))))
	}
	w.writeCh <- req
	w.closeMu.RUnlock()
	return req, nil
}

// await blocks until the record's batch is synced and recycles the request.
func (w *groupCommitWriter) await(req *groupCommitReq) error {
	err := <-req.done
	req.data = nil
	req.prefixN = 0
	groupCommitReqPool.Put(req)
	return err
}

func (w *groupCommitWriter) close() error {
	w.closeMu.Lock()
	if !w.closed {
		w.closed = true
		close(w.writeCh)
	}
	w.closeMu.Unlock()
	<-w.doneCh
	return w.closeErr
}

func (w *groupCommitWriter) healthError() error {
	w.closeMu.RLock()
	defer w.closeMu.RUnlock()
	if w.faultErr != nil {
		return w.faultErr
	}
	if w.closed {
		return ErrClosed
	}
	return nil
}

func (w *groupCommitWriter) setFaultCallback(fn func(error)) {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	w.onFault = fn
}
