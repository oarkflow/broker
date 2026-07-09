package fastworker

import (
	"fmt"
	"strings"
)

func (p *Pool) Prometheus() string {
	s := p.Stats()
	name := sanitizeMetricName(p.cfg.Name)
	var b strings.Builder
	emit := func(metric string, value any) { fmt.Fprintf(&b, "%s_%s %v\n", name, metric, value) }
	emit("submitted_total", s.Submitted)
	emit("accepted_total", s.Accepted)
	emit("rejected_total", s.Rejected)
	emit("started_total", s.Started)
	emit("completed_total", s.Completed)
	emit("failed_total", s.Failed)
	emit("retried_total", s.Retried)
	emit("panicked_total", s.Panicked)
	emit("timed_out_total", s.TimedOut)
	emit("dead_lettered_total", s.DeadLettered)
	emit("rate_delayed_total", s.RateDelayed)
	emit("queue_depth", s.QueueDepth)
	emit("workers", s.Workers)
	emit("busy_workers", s.BusyWorkers)
	emit("uptime_seconds", int64(s.Uptime.Seconds()))
	emit("state", int(p.State()))
	return b.String()
}

func sanitizeMetricName(s string) string {
	if s == "" {
		return "fastworker"
	}
	var b strings.Builder
	for i, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}
