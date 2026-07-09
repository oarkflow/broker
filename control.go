package fastworker

import (
	"context"
	"time"
)

func (s State) String() string {
	switch s {
	case stateNew:
		return "new"
	case stateRunning:
		return "running"
	case statePaused:
		return "paused"
	case stateStopping:
		return "stopping"
	case stateStopped:
		return "stopped"
	case stateTerminated:
		return "terminated"
	default:
		return "unknown"
	}
}

func (p *Pool) State() State     { return State(p.state.Load()) }
func (p *Pool) IsRunning() bool  { return p.State() == stateRunning }
func (p *Pool) IsPaused() bool   { return p.State() == statePaused }
func (p *Pool) QueueDepth() int  { return p.q.Len() + p.fastq.Len() + p.basicq.Len() + p.SpillDepth() }
func (p *Pool) WorkerCount() int { return int(p.workers.Load()) }

// WaitIdle waits until the queue is empty and no worker is executing a job.
// It does not stop the pool.
func (p *Pool) WaitIdle(ctx context.Context) error {
	t := time.NewTicker(time.Millisecond)
	defer t.Stop()
	for {
		if p.QueueDepth() == 0 && p.c.busy.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Drain rejects new jobs by entering stopping state, waits for all accepted
// jobs to finish, and then leaves the pool stopped. It is equivalent to a
// graceful Stop but named for operational clarity.
func (p *Pool) Drain(ctx context.Context) error {
	p.emit(Event{Type: EventDrainStarted})
	err := p.Shutdown(ctx)
	if err == nil {
		p.emit(Event{Type: EventDrainCompleted})
	}
	return err
}

// Wait waits for all worker goroutines to exit. It is useful after Terminate.
func (p *Pool) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
