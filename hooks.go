package fastworker

import "context"

type Hooks struct {
	OnPoolStart     func(*Pool)
	OnPoolStop      func(*Pool)
	OnWorkerStart   func(workerID int)
	OnWorkerStop    func(workerID int)
	OnJobSubmit     func(JobOptions)
	OnJobStart      func(context.Context, JobOptions, int)
	OnJobSuccess    func(context.Context, JobOptions, int)
	OnJobError      func(context.Context, JobOptions, int, error)
	OnJobPanic      func(context.Context, JobOptions, int, any)
	OnJobRetry      func(JobOptions, int, error)
	OnJobDeadLetter func(JobOptions, int, error)
}

type Middleware func(Job) Job

func Chain(job Job, m ...Middleware) Job {
	for i := len(m) - 1; i >= 0; i-- {
		job = m[i](job)
	}
	return job
}
