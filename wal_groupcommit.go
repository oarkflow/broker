package broker

import (
	"hash/fnv"
	"os"
	"sync"
)

// walShardKey derives the routing key (queue/topic/stream name) a WAL
// record belongs to, so that all durability records for one queue always
// land in the same WAL shard file and keep their relative order, while
// records for different queues can be sharded onto independent writers.
// Records with no natural key (or a rare/admin op not worth partitioning)
// return "" and are routed to shard 0.
func walShardKey(r walRecord) string {
	switch r.Op {
	case "declare_queue":
		return r.Queue.Name
	case "declare_topic":
		return r.Topic.Name
	case "declare_stream":
		return r.Stream.Name
	case "append", "lease", "update":
		return firstNonEmpty(r.Message.Queue, firstNonEmpty(r.Message.Topic, r.Message.Stream))
	case "append_atomic":
		if len(r.Entries) > 0 {
			return firstNonEmpty(r.Entries[0].Message.Queue, firstNonEmpty(r.Entries[0].Message.Topic, r.Entries[0].Message.Stream))
		}
	case "ack":
		return r.Ack.Queue
	case "nack":
		return r.Nack.Queue
	case "move":
		return r.Move.Source
	case "pause_queue", "resume_queue", "purge_queue":
		return r.QueueName
	case "commit_offset":
		return r.Offset.Stream
	}
	return ""
}

// walShardIndex maps a WAL record to one of n shards by hashing its
// routing key. n<=1 or an empty key always resolves to shard 0.
func walShardIndex(r walRecord, n int) int {
	if n <= 1 {
		return 0
	}
	key := walShardKey(r)
	if key == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

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
