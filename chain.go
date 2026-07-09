package fastworker

import "context"

type ChainBuilder struct {
	pool *Pool
	jobs []Job
	opt  JobOptions
}

func (p *Pool) Chain() *ChainBuilder               { return &ChainBuilder{pool: p} }
func (c *ChainBuilder) Then(job Job) *ChainBuilder { c.jobs = append(c.jobs, job); return c }
func (c *ChainBuilder) ThenFunc(fn func(context.Context) error) *ChainBuilder {
	return c.Then(JobFunc(fn))
}
func (c *ChainBuilder) Options(opt JobOptions) *ChainBuilder { c.opt = opt; return c }
func (c *ChainBuilder) Submit(ctx context.Context) error {
	jobs := append([]Job(nil), c.jobs...)
	return c.pool.SubmitFunc(func(run context.Context) error {
		for _, j := range jobs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := j.Run(run); err != nil {
				return err
			}
		}
		return nil
	}, c.opt)
}

func (p *Pool) WhenAll(jobs ...Job) *WhenAllBuilder { return &WhenAllBuilder{pool: p, jobs: jobs} }

type WhenAllBuilder struct {
	pool *Pool
	jobs []Job
	next Job
	opt  JobOptions
}

func (w *WhenAllBuilder) Then(job Job) *WhenAllBuilder { w.next = job; return w }
func (w *WhenAllBuilder) ThenFunc(fn func(context.Context) error) *WhenAllBuilder {
	w.next = JobFunc(fn)
	return w
}
func (w *WhenAllBuilder) Options(opt JobOptions) *WhenAllBuilder { w.opt = opt; return w }
func (w *WhenAllBuilder) Submit(ctx context.Context) error {
	jobs := append([]Job(nil), w.jobs...)
	next := w.next
	return w.pool.SubmitFunc(func(run context.Context) error {
		done := make(chan error, len(jobs))
		for _, j := range jobs {
			jj := j
			go func() { done <- jj.Run(run) }()
		}
		for range jobs {
			if err := <-done; err != nil {
				return err
			}
		}
		if next != nil {
			return next.Run(run)
		}
		return nil
	}, w.opt)
}
