package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid configuration")
)

// DynamicConfig represents runtime reloadable configuration properties.
type DynamicConfig struct {
	Routes     []Route             `json:"routes,omitempty"`
	RateLimits map[string]int      `json:"rate_limits,omitempty"`
	ACLTokens  map[string][]string `json:"acl_tokens,omitempty"`
	Metadata   map[string]string   `json:"metadata,omitempty"`
}

// Validate verifies that the dynamic configuration is valid before applying.
func (dc *DynamicConfig) Validate() error {
	for i, r := range dc.Routes {
		if r.Name == "" {
			return fmt.Errorf("%w: route[%d] missing name", ErrInvalidConfig, i)
		}
		if len(r.To) == 0 {
			return fmt.Errorf("%w: route '%s' has no destinations", ErrInvalidConfig, r.Name)
		}
	}
	return nil
}

// ConfigWatcher monitors a configuration file and reloads broker state when changed.
type ConfigWatcher struct {
	b            *Broker
	filePath     string
	pollInterval time.Duration
	lastModTime  time.Time
	mu           sync.Mutex
	stopCh       chan struct{}
	running      bool
}

// NewConfigWatcher creates a hot-reload config watcher.
func NewConfigWatcher(b *Broker, filePath string, pollInterval time.Duration) *ConfigWatcher {
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	return &ConfigWatcher{
		b:            b,
		filePath:     filePath,
		pollInterval: pollInterval,
		stopCh:       make(chan struct{}),
	}
}

// Start runs the hot reload monitoring loop.
func (cw *ConfigWatcher) Start(ctx context.Context) {
	cw.mu.Lock()
	if cw.running {
		cw.mu.Unlock()
		return
	}
	cw.running = true
	cw.mu.Unlock()

	// Capture initial mod time if file exists
	if fi, err := os.Stat(cw.filePath); err == nil {
		cw.lastModTime = fi.ModTime()
	}

	go func() {
		ticker := time.NewTicker(cw.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-cw.stopCh:
				return
			case <-ticker.C:
				cw.checkAndReload(ctx)
			}
		}
	}()
}

// Stop stops the config watcher.
func (cw *ConfigWatcher) Stop() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	if !cw.running {
		return
	}
	close(cw.stopCh)
	cw.running = false
}

func (cw *ConfigWatcher) checkAndReload(ctx context.Context) {
	fi, err := os.Stat(cw.filePath)
	if err != nil {
		return
	}

	cw.mu.Lock()
	if !fi.ModTime().After(cw.lastModTime) {
		cw.mu.Unlock()
		return
	}
	cw.lastModTime = fi.ModTime()
	cw.mu.Unlock()

	_ = cw.Reload(ctx)
}

// Reload reads, validates, and atomically applies the configuration file.
func (cw *ConfigWatcher) Reload(ctx context.Context) error {
	data, err := os.ReadFile(cw.filePath)
	if err != nil {
		return err
	}

	var dc DynamicConfig
	if err := json.Unmarshal(data, &dc); err != nil {
		return fmt.Errorf("%w: parse error: %v", ErrInvalidConfig, err)
	}

	return cw.ApplyConfig(ctx, dc)
}

// ApplyConfig validates and atomically applies dynamic configuration to the broker.
func (cw *ConfigWatcher) ApplyConfig(ctx context.Context, dc DynamicConfig) error {
	if err := dc.Validate(); err != nil {
		return err
	}

	// 1. Update Routes
	for _, r := range dc.Routes {
		cw.b.AddRoute(r)
	}

	// 2. Update ACLs
	if cw.b.ACL() != nil && len(dc.ACLTokens) > 0 {
		for token, perms := range dc.ACLTokens {
			cw.b.ACL().AddToken(AdminToken{
				Name:        "token-" + token,
				Token:       token,
				Permissions: perms,
			})
		}
	}

	// 3. Emit notification
	if cw.b.notify != nil {
		cw.b.notify.Publish(Notification{
			Type:      "config.reloaded",
			Topic:     "config.reloaded",
			Timestamp: time.Now(),
			Data: map[string]any{
				"routes_count": len(dc.Routes),
				"acls_count":   len(dc.ACLTokens),
			},
		})
	}

	return nil
}
