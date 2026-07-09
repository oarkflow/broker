package fastworker

import (
	"sync/atomic"
	"time"
)

type Stats struct {
	Submitted, Accepted, Rejected, Started, Completed, Failed, Retried, Panicked, TimedOut, DeadLettered, RateDelayed uint64
	QueueDepth                                                                                                        int
	Workers                                                                                                           int
	BusyWorkers                                                                                                       int
	Paused                                                                                                            bool
	Stopped                                                                                                           bool
	Terminated                                                                                                        bool
	Uptime                                                                                                            time.Duration
}

type counters struct {
	submitted, accepted, rejected, started, completed, failed, retried, panicked, timedout, dlq, rateDelayed atomic.Uint64
	busy                                                                                                     atomic.Int64
}
