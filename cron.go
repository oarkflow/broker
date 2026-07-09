package fastworker

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ScheduleCron supports @every <duration> and compact step expressions such as
// "*/5 * * * * *" (every five seconds) or "*/2 * * * *" (every two minutes).
// It intentionally avoids third-party dependencies while covering common worker
// scheduling use cases.
func (p *Pool) ScheduleCron(id, expr string, job Job, opts ...JobOptions) (ScheduleHandle, error) {
	every, err := parseCronEvery(expr)
	if err != nil {
		return ScheduleHandle{}, err
	}
	return p.ScheduleEvery(id, every, job, opts...), nil
}

func parseCronEvery(expr string) (time.Duration, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return 0, fmt.Errorf("%w: empty cron expression", ErrInvalidConfig)
	}
	if strings.HasPrefix(expr, "@every ") {
		d, err := time.ParseDuration(strings.TrimSpace(strings.TrimPrefix(expr, "@every ")))
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("%w: invalid @every duration", ErrInvalidConfig)
		}
		return d, nil
	}
	parts := strings.Fields(expr)
	switch len(parts) {
	case 6:
		if strings.HasPrefix(parts[0], "*/") {
			n, err := strconv.Atoi(strings.TrimPrefix(parts[0], "*/"))
			if err == nil && n > 0 {
				return time.Duration(n) * time.Second, nil
			}
		}
	case 5:
		if strings.HasPrefix(parts[0], "*/") {
			n, err := strconv.Atoi(strings.TrimPrefix(parts[0], "*/"))
			if err == nil && n > 0 {
				return time.Duration(n) * time.Minute, nil
			}
		}
	}
	return 0, fmt.Errorf("%w: unsupported cron expression %q", ErrInvalidConfig, expr)
}
