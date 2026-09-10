package broker

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

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
			if w.faultErr == nil {
				w.faultErr = fmt.Errorf("%w: WAL group commit failed: %v", ErrStorageUnavailable, writeErr)
			}
			writeErr = w.faultErr
			w.closeMu.Unlock()
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
	w.closeMu.RLock()
	if w.closed || w.faultErr != nil {
		err := w.faultErr
		w.closeMu.RUnlock()
		if err != nil {
			return err
		}
		return ErrClosed
	}
	req := groupCommitReqPool.Get().(*groupCommitReq)
	req.data = data
	w.writeCh <- req
	w.closeMu.RUnlock()
	err := <-req.done
	req.data = nil
	req.prefixN = 0
	groupCommitReqPool.Put(req)
	return err
}

// writeUvarintPrefixed writes a uvarint record length and the record as two
// buffered segments. It avoids allocating and copying a combined prefix+record
// slice on every durable mutation.
func (w *groupCommitWriter) writeUvarintPrefixed(data []byte) error {
	w.closeMu.RLock()
	if w.closed || w.faultErr != nil {
		err := w.faultErr
		w.closeMu.RUnlock()
		if err != nil {
			return err
		}
		return ErrClosed
	}
	req := groupCommitReqPool.Get().(*groupCommitReq)
	req.data = data
	req.prefixN = uint8(binary.PutUvarint(req.prefix[:], uint64(len(data))))
	w.writeCh <- req
	w.closeMu.RUnlock()
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
