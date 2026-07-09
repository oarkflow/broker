package fastworker

import "context"

type Progress struct {
	Percent int    `json:"percent,omitempty"`
	Message string `json:"message,omitempty"`
}

type progressContextKey struct{}

func contextWithProgress(ctx context.Context, qj *queuedJob) context.Context {
	if qj == nil {
		return ctx
	}
	return context.WithValue(ctx, progressContextKey{}, qj)
}

func ReportProgress(ctx context.Context, percent int, message string) {
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	qj, _ := ctx.Value(progressContextKey{}).(*queuedJob)
	if qj == nil {
		return
	}
	pr := Progress{Percent: percent, Message: message}
	qj.progress.Store(pr)
	if qj.opts.Callback != nil && qj.opts.Callback.OnProgress != nil {
		qj.opts.Callback.OnProgress(ctx, qj.info(), pr)
	}
	if qj.pool != nil {
		qj.pool.emit(Event{Type: EventJobProgress, Job: qj.info(), Options: qj.opts, Queue: qj.opts.Queue, Attempt: qj.attempt, Message: message})
	}
}
