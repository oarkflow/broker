package pool

import "time"

type BackpressurePolicy uint8

const (
	BackpressureBlock BackpressurePolicy = iota
	BackpressureReject
	BackpressureDropOldest
	BackpressureDropNewest
)

type Config struct {
	Name                  string
	MinWorkers            int
	MaxWorkers            int
	QueueSize             int
	Backpressure          BackpressurePolicy
	SubmitTimeout         time.Duration
	IdleTimeout           time.Duration
	ScaleInterval         time.Duration
	EnableAutoScale       bool
	RecoverPanics         bool
	DefaultTimeout        time.Duration
	DefaultMaxAttempts    int
	DefaultBackoff        Backoff
	MetricsSampleInterval time.Duration
	// EnableJobTracking stores queued/running jobs in an inspection map and auto-generates IDs.
	// It is disabled by default to keep the submit/execute hot path allocation-light.
	EnableJobTracking bool
	// EnableJobIDs generates job IDs even when tracking is disabled. Disabled by default.
	EnableJobIDs bool
	// DisableStats removes atomic counter updates from the submit/run hot path.
	// Use this for ultra-low-latency in-process execution where Stats() are not needed.
	DisableStats bool
}

func DefaultConfig() Config {
	return Config{
		Name:               "fastworker",
		MinWorkers:         1,
		MaxWorkers:         0,
		QueueSize:          1024,
		Backpressure:       BackpressureBlock,
		SubmitTimeout:      0,
		IdleTimeout:        30 * time.Second,
		ScaleInterval:      100 * time.Millisecond,
		EnableAutoScale:    true,
		RecoverPanics:      true,
		DefaultMaxAttempts: 1,
		DefaultBackoff:     ExponentialBackoff{Initial: 25 * time.Millisecond, Max: time.Second, Factor: 2, Jitter: true},
	}
}

func normalizeConfig(c Config) (Config, error) {
	d := DefaultConfig()
	if c.Name == "" {
		c.Name = d.Name
	}
	if c.MinWorkers <= 0 {
		c.MinWorkers = d.MinWorkers
	}
	if c.MaxWorkers <= 0 {
		c.MaxWorkers = c.MinWorkers
	}
	if c.MaxWorkers < c.MinWorkers {
		c.MaxWorkers = c.MinWorkers
	}
	if c.QueueSize <= 0 {
		c.QueueSize = d.QueueSize
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = d.IdleTimeout
	}
	if c.ScaleInterval <= 0 {
		c.ScaleInterval = d.ScaleInterval
	}
	if c.DefaultMaxAttempts <= 0 {
		c.DefaultMaxAttempts = d.DefaultMaxAttempts
	}
	if c.DefaultBackoff == nil {
		c.DefaultBackoff = d.DefaultBackoff
	}
	return c, nil
}
