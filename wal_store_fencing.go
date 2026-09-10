package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// StoreHealthCallbackRegistrar allows registering an immediate notification
// callback when a store suffers an unrecoverable I/O or fsync fault.
type StoreHealthCallbackRegistrar interface {
	SetHealthCallback(fn func(error))
}

// StoreHealthMonitor watches storage health and transitions the broker to
// BrokerDegraded when storage becomes unavailable. While fenced, write operations
// fail fast with ErrStorageUnavailable.
type StoreHealthMonitor struct {
	broker   *Broker
	store    Store
	interval time.Duration
	fenced   atomic.Bool
	fenceErr atomic.Pointer[error]
	mu       sync.Mutex
	stopCh   chan struct{}
	stopped  bool
	onFenced func(err error)
}

// NewStoreHealthMonitor creates a health monitor for the given broker and store.
func NewStoreHealthMonitor(b *Broker, store Store, interval time.Duration) *StoreHealthMonitor {
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	m := &StoreHealthMonitor{
		broker:   b,
		store:    store,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
	if reg, ok := store.(StoreHealthCallbackRegistrar); ok {
		reg.SetHealthCallback(func(err error) {
			m.Fence(err)
		})
	}
	return m
}

// SetOnFenced sets a hook to execute when the store enters a fenced state.
func (m *StoreHealthMonitor) SetOnFenced(fn func(error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onFenced = fn
}

// Start launches the background health check poller.
func (m *StoreHealthMonitor) Start(ctx context.Context) {
	go m.run(ctx)
}

func (m *StoreHealthMonitor) run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			_ = m.CheckHealth(ctx)
		}
	}
}

// Stop terminates the polling loop.
func (m *StoreHealthMonitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.stopped {
		m.stopped = true
		close(m.stopCh)
	}
}

// IsFenced reports whether the store is currently fenced.
func (m *StoreHealthMonitor) IsFenced() bool {
	return m != nil && m.fenced.Load()
}

// FenceError returns the error that caused the store to fence, or nil.
func (m *StoreHealthMonitor) FenceError() error {
	if m == nil {
		return nil
	}
	if ptr := m.fenceErr.Load(); ptr != nil {
		return *ptr
	}
	if m.fenced.Load() {
		return ErrStorageUnavailable
	}
	return nil
}

// Fence immediately marks the store as degraded and fenced.
func (m *StoreHealthMonitor) Fence(err error) {
	if m == nil {
		return
	}
	if err == nil {
		err = ErrStorageUnavailable
	} else if !errors.Is(err, ErrStorageUnavailable) {
		err = fmt.Errorf("%w: %v", ErrStorageUnavailable, err)
	}
	if !m.fenced.CompareAndSwap(false, true) {
		return // already fenced
	}
	m.fenceErr.Store(&err)

	if m.broker != nil {
		m.broker.setState(context.Background(), BrokerDegraded, err)
		if m.broker.promMetrics != nil {
			m.broker.promMetrics.RecordStoreFenced()
		}
		if m.broker.notify != nil {
			m.broker.notify.Publish(Notification{
				ID:        fmt.Sprintf("fenced-%d", time.Now().UnixNano()),
				Type:      "store.fenced",
				Source:    "store_health_monitor",
				Severity:  SeverityError,
				Timestamp: time.Now(),
				Data:      map[string]any{"error": err.Error()},
			})
		}
	}

	m.mu.Lock()
	cb := m.onFenced
	m.mu.Unlock()
	if cb != nil {
		cb(err)
	}
}

// CheckHealth queries the underlying store for health issues and fences if faulty.
func (m *StoreHealthMonitor) CheckHealth(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if m.IsFenced() {
		return m.FenceError()
	}
	if provider, ok := m.store.(StoreHealthProvider); ok {
		if err := provider.StoreHealth(); err != nil {
			m.Fence(err)
			return err
		}
	}
	return nil
}
