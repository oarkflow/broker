package broker

import (
	"os"
	"sync"
)

// groupCommitWriter batches concurrent WAL append calls into a single fsync
// per batch instead of one fsync per call. Durability is unchanged: every
// write() call still blocks until its bytes are durably fsynced — it just
// shares that fsync with whatever else was queued at the same instant, which
// is how most WAL-based stores (Postgres, etc.) get high write throughput
// under concurrency without relaxing durability.
type groupCommitWriter struct {
	file *os.File

	closeMu sync.RWMutex
	closed  bool
	writeCh chan *groupCommitReq
	doneCh  chan struct{}
}

type groupCommitReq struct {
	data []byte
	done chan error
}

func newGroupCommitWriter(f *os.File) *groupCommitWriter {
	w := &groupCommitWriter{
		file:    f,
		writeCh: make(chan *groupCommitReq, 4096),
		doneCh:  make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *groupCommitWriter) run() {
	defer close(w.doneCh)
	for req := range w.writeCh {
		batch := make([]*groupCommitReq, 0, 64)
		batch = append(batch, req)
	drain:
		for len(batch) < 4096 {
			select {
			case r2, ok := <-w.writeCh:
				if !ok {
					break drain
				}
				batch = append(batch, r2)
			default:
				break drain
			}
		}
		var writeErr error
		for _, b := range batch {
			if _, err := w.file.Write(b.data); err != nil {
				writeErr = err
				break
			}
		}
		if writeErr == nil {
			writeErr = w.file.Sync()
		}
		for _, b := range batch {
			b.done <- writeErr
		}
	}
}

// write appends data and blocks until it (and possibly a batch of
// concurrent writers) has been durably fsynced. Safe for concurrent use.
func (w *groupCommitWriter) write(data []byte) error {
	w.closeMu.RLock()
	if w.closed {
		w.closeMu.RUnlock()
		return ErrClosed
	}
	req := &groupCommitReq{data: data, done: make(chan error, 1)}
	w.writeCh <- req
	w.closeMu.RUnlock()
	return <-req.done
}

func (w *groupCommitWriter) close() error {
	w.closeMu.Lock()
	if !w.closed {
		w.closed = true
		close(w.writeCh)
	}
	w.closeMu.Unlock()
	<-w.doneCh
	return nil
}
