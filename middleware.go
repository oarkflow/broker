package fastworker

import (
	"context"
	"log"
	"time"
)

// Timeout wraps each job with a context timeout. JobOptions.Timeout is usually
// preferred for per-job deadlines, but middleware is useful for global policy.
func Timeout(d time.Duration) Middleware {
	return func(next Job) Job {
		return JobFunc(func(ctx context.Context) error {
			if d <= 0 {
				return next.Run(ctx)
			}
			c, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next.Run(c)
		})
	}
}

// Logger records duration and final error using the standard logger.
func Logger(prefix string) Middleware {
	return func(next Job) Job {
		return JobFunc(func(ctx context.Context) error {
			start := time.Now()
			err := next.Run(ctx)
			if err != nil {
				log.Printf("%s job_error duration=%s error=%v", prefix, time.Since(start), err)
			} else {
				log.Printf("%s job_success duration=%s", prefix, time.Since(start))
			}
			return err
		})
	}
}

// Metadata injects immutable metadata into the job context.
type metadataKey struct{}

func WithMetadata(md map[string]string) Middleware {
	copyMD := make(map[string]string, len(md))
	for k, v := range md {
		copyMD[k] = v
	}
	return func(next Job) Job {
		return JobFunc(func(ctx context.Context) error {
			return next.Run(context.WithValue(ctx, metadataKey{}, copyMD))
		})
	}
}

func MetadataFromContext(ctx context.Context) map[string]string {
	md, _ := ctx.Value(metadataKey{}).(map[string]string)
	return md
}
