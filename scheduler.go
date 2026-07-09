package fastworker

import (
	"context"
	"sync"
	"time"
)

type ScheduleHandle struct {
	id     string
	cancel context.CancelFunc
	done   chan struct{}
}

func (h ScheduleHandle) Cancel() {
	if h.cancel != nil {
		h.cancel()
	}
}
func (h ScheduleHandle) Done() <-chan struct{} { return h.done }

func (p *Pool) ScheduleEvery(id string, every time.Duration, job Job, opts ...JobOptions) ScheduleHandle {
	ctx, cancel := context.WithCancel(p.ctx)
	h := ScheduleHandle{id: id, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(h.done)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				o := JobOptions{ID: id}
				if len(opts) > 0 {
					o = opts[0]
					if o.ID == "" {
						o.ID = id
					}
				}
				_ = p.Submit(job, o)
			case <-ctx.Done():
				return
			}
		}
	}()
	return h
}
func (p *Pool) ScheduleAt(id string, at time.Time, job Job, opts ...JobOptions) error {
	o := JobOptions{ID: id, RunAt: at}
	if len(opts) > 0 {
		o = opts[0]
		o.RunAt = at
		if o.ID == "" {
			o.ID = id
		}
	}
	return p.Submit(job, o)
}
func (p *Pool) ScheduleAfter(id string, after time.Duration, job Job, opts ...JobOptions) error {
	o := JobOptions{ID: id, Delay: after}
	if len(opts) > 0 {
		o = opts[0]
		o.Delay = after
		if o.ID == "" {
			o.ID = id
		}
	}
	return p.Submit(job, o)
}

type Group struct {
	wg    sync.WaitGroup
	errMu sync.Mutex
	errs  []error
}

func (g *Group) Go(p *Pool, fn func(context.Context) error, opts ...JobOptions) error {
	g.wg.Add(1)
	return p.SubmitFunc(func(ctx context.Context) error {
		defer g.wg.Done()
		err := fn(ctx)
		if err != nil {
			g.errMu.Lock()
			g.errs = append(g.errs, err)
			g.errMu.Unlock()
		}
		return err
	}, opts...)
}
func (g *Group) Wait() []error {
	g.wg.Wait()
	g.errMu.Lock()
	defer g.errMu.Unlock()
	return append([]error(nil), g.errs...)
}
