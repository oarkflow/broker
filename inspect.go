package fastworker

import "context"

// InspectJob returns a snapshot of a queued/running/recent job when it is still
// tracked by the pool. Completed successful jobs are removed from the hot index
// to avoid unbounded memory growth.
func (p *Pool) InspectJob(id string) (JobInfo, bool) {
	v, ok := p.jobs.Load(id)
	if !ok {
		return JobInfo{}, false
	}
	qj, ok := v.(*queuedJob)
	if !ok || qj == nil {
		return JobInfo{}, false
	}
	return qj.info(), true
}

// CancelJob cancels a queued or running job by ID. Queued jobs are skipped when
// a worker reaches them; running jobs receive context cancellation.
func (p *Pool) CancelJob(id string) bool {
	v, ok := p.jobs.Load(id)
	if !ok {
		return false
	}
	qj, ok := v.(*queuedJob)
	if !ok || qj == nil {
		return false
	}
	qj.state.Store(uint32(JobCancelled))
	qj.finishedAt = nowUTC()
	if qj.cancel != nil {
		qj.cancel()
	}
	if qj.future != nil {
		qj.future.complete(nil, ErrCancelled)
	}
	p.jobs.Delete(id)
	return true
}

func (p *Pool) ActiveJobs() []JobInfo {
	out := make([]JobInfo, 0)
	p.jobs.Range(func(_, v any) bool {
		if qj, ok := v.(*queuedJob); ok && qj != nil {
			out = append(out, qj.info())
		}
		return true
	})
	return out
}

func (p *Pool) CancelAll(ctx context.Context) int {
	n := 0
	p.jobs.Range(func(k, v any) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		if id, ok := k.(string); ok && p.CancelJob(id) {
			n++
		}
		return true
	})
	return n
}
