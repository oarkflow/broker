package fastworker

import "context"

type Callback struct {
	OnQueued     func(context.Context, JobInfo)
	OnStart      func(context.Context, JobInfo)
	OnSuccess    func(context.Context, JobInfo)
	OnError      func(context.Context, JobInfo, error)
	OnRetry      func(context.Context, JobInfo, int, error)
	OnDeadLetter func(context.Context, JobInfo, error)
	OnCancel     func(context.Context, JobInfo)
	OnPanic      func(context.Context, JobInfo, any)
	OnProgress   func(context.Context, JobInfo, Progress)
	OnFinally    func(context.Context, JobInfo, error)
}
