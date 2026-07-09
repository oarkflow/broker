package fastworker

import (
	"time"

	"github.com/oarkflow/fastworker/queue"
)

// ReplayDeadLetters re-enqueues all currently captured dead-lettered jobs and
// returns the number accepted. Jobs keep their original options but attempts are
// reset. The in-memory DLQ is cleared only for successfully requeued jobs.
func (p *Pool) ReplayDeadLetters() (int, error) {
	if State(p.state.Load()) >= stateStopping {
		return 0, ErrClosed
	}
	p.dlqMu.Lock()
	items := append([]*queuedJob(nil), p.dlq...)
	p.dlq = p.dlq[:0]
	p.dlqMu.Unlock()
	accepted := 0
	var firstErr error
	for _, j := range items {
		j.attempt = 0
		j.runAt = time.Now()
		if !p.q.Push(queue.Item[*queuedJob]{Value: j, Priority: int(j.opts.Priority), Seq: j.seq, RunAt: j.runAt}) {
			if firstErr == nil {
				firstErr = ErrQueueFull
			}
			p.dlqMu.Lock()
			p.dlq = append(p.dlq, j)
			p.dlqMu.Unlock()
			continue
		}
		accepted++
		p.c.accepted.Add(1)
	}
	return accepted, firstErr
}

// PurgeDeadLetters clears the in-memory DLQ and returns the number of removed jobs.
func (p *Pool) PurgeDeadLetters() int {
	p.dlqMu.Lock()
	n := len(p.dlq)
	p.dlq = nil
	p.dlqMu.Unlock()
	return n
}
