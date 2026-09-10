package broker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type LagAlertLevel string

const (
	LagAlertWarning  LagAlertLevel = "warning"
	LagAlertCritical LagAlertLevel = "critical"
)

// LagAlert represents an alert fired when consumer lag exceeds a threshold.
type LagAlert struct {
	Target        string        `json:"target"` // Stream or Queue name
	ConsumerGroup string        `json:"consumer_group,omitempty"`
	CurrentLag    uint64        `json:"current_lag"`
	Threshold     uint64        `json:"threshold"`
	Level         LagAlertLevel `json:"level"`
	Timestamp     time.Time     `json:"timestamp"`
}

// LagThreshold defines warning and critical thresholds for a target.
type LagThreshold struct {
	Warning  uint64 `json:"warning"`
	Critical uint64 `json:"critical"`
}

// LagAlertConfig configures the LagMonitor.
type LagAlertConfig struct {
	DefaultWarning   uint64
	DefaultCritical  uint64
	StreamThresholds map[string]LagThreshold
	QueueThresholds  map[string]LagThreshold
	CheckInterval    time.Duration
	OnAlert          func(alert LagAlert)
}

// LagMonitor inspects consumer offsets and queue depths and fires alerts on threshold violations.
type LagMonitor struct {
	b       *Broker
	cfg     LagAlertConfig
	mu      sync.RWMutex
	alerts  []LagAlert
	stopCh  chan struct{}
	running bool
}

// NewLagMonitor creates a new consumer lag monitor.
func NewLagMonitor(b *Broker, cfg LagAlertConfig) *LagMonitor {
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 5 * time.Second
	}
	if cfg.DefaultCritical == 0 {
		cfg.DefaultCritical = 1000
	}
	if cfg.DefaultWarning == 0 {
		cfg.DefaultWarning = 200
	}
	if cfg.StreamThresholds == nil {
		cfg.StreamThresholds = make(map[string]LagThreshold)
	}
	if cfg.QueueThresholds == nil {
		cfg.QueueThresholds = make(map[string]LagThreshold)
	}

	return &LagMonitor{
		b:      b,
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

// Start launches the background monitoring loop.
func (lm *LagMonitor) Start(ctx context.Context) {
	lm.mu.Lock()
	if lm.running {
		lm.mu.Unlock()
		return
	}
	lm.running = true
	lm.mu.Unlock()

	go func() {
		ticker := time.NewTicker(lm.cfg.CheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-lm.stopCh:
				return
			case <-ticker.C:
				_, _ = lm.CheckLag(ctx)
			}
		}
	}()
}

// Stop halts the lag monitor.
func (lm *LagMonitor) Stop() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if !lm.running {
		return
	}
	close(lm.stopCh)
	lm.running = false
}

// CheckLag inspects the current broker state and evaluates all configured lag thresholds.
func (lm *LagMonitor) CheckLag(ctx context.Context) ([]LagAlert, error) {
	stats, err := lm.b.Store().Stats(ctx)
	if err != nil {
		return nil, err
	}

	var newAlerts []LagAlert
	now := time.Now()

	// 1. Check Stream Consumer Groups
	for stream, groups := range stats.Offsets {
		highWatermark := uint64(stats.Streams[stream])
		thresh := lm.getStreamThreshold(stream)

		for group, committed := range groups {
			if highWatermark > committed {
				lag := highWatermark - committed
				alert := lm.evaluateThreshold(stream, group, lag, thresh, now)
				if alert != nil {
					newAlerts = append(newAlerts, *alert)
				}
			}
		}
	}

	// 2. Check Queue Depths
	for queue, depth := range stats.Queues {
		thresh := lm.getQueueThreshold(queue)
		lag := uint64(depth)
		alert := lm.evaluateThreshold(queue, "", lag, thresh, now)
		if alert != nil {
			newAlerts = append(newAlerts, *alert)
		}
	}

	lm.mu.Lock()
	lm.alerts = append([]LagAlert(nil), newAlerts...)
	lm.mu.Unlock()

	for _, a := range newAlerts {
		if lm.cfg.OnAlert != nil {
			lm.cfg.OnAlert(a)
		}
		if lm.b.notify != nil {
			topic := fmt.Sprintf("consumer.lag.%s", a.Level)
			lm.b.notify.Publish(Notification{
				Type:          topic,
				Topic:         topic,
				Stream:        a.Target,
				ConsumerGroup: a.ConsumerGroup,
				Timestamp:     a.Timestamp,
				Data: map[string]any{
					"target":         a.Target,
					"consumer_group": a.ConsumerGroup,
					"current_lag":    a.CurrentLag,
					"threshold":      a.Threshold,
					"level":          string(a.Level),
				},
			})
		}
	}

	return newAlerts, nil
}

func (lm *LagMonitor) evaluateThreshold(target, group string, lag uint64, thresh LagThreshold, now time.Time) *LagAlert {
	if lag >= thresh.Critical && thresh.Critical > 0 {
		return &LagAlert{
			Target:        target,
			ConsumerGroup: group,
			CurrentLag:    lag,
			Threshold:     thresh.Critical,
			Level:         LagAlertCritical,
			Timestamp:     now,
		}
	}
	if lag >= thresh.Warning && thresh.Warning > 0 {
		return &LagAlert{
			Target:        target,
			ConsumerGroup: group,
			CurrentLag:    lag,
			Threshold:     thresh.Warning,
			Level:         LagAlertWarning,
			Timestamp:     now,
		}
	}
	return nil
}

func (lm *LagMonitor) getStreamThreshold(stream string) LagThreshold {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	if t, ok := lm.cfg.StreamThresholds[stream]; ok {
		return t
	}
	return LagThreshold{Warning: lm.cfg.DefaultWarning, Critical: lm.cfg.DefaultCritical}
}

func (lm *LagMonitor) getQueueThreshold(queue string) LagThreshold {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	if t, ok := lm.cfg.QueueThresholds[queue]; ok {
		return t
	}
	return LagThreshold{Warning: lm.cfg.DefaultWarning, Critical: lm.cfg.DefaultCritical}
}

// ActiveAlerts returns the latest set of active alerts.
func (lm *LagMonitor) ActiveAlerts() []LagAlert {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	out := make([]LagAlert, len(lm.alerts))
	copy(out, lm.alerts)
	return out
}
