package cluster

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// RaftGroupHostConfig configures a shared transport and durable root for a set
// of independent Raft groups on one broker process.
type RaftGroupHostConfig struct {
	Transport        Transport
	ListenAddr       string
	StorageDir       string
	TickInterval     time.Duration
	ReadIndexTimeout time.Duration
	SnapshotEntries  uint64
	SnapshotCatchUp  uint64
}

// RaftGroupConfig defines the local member and initial membership of one
// independent group.
type RaftGroupConfig struct {
	GroupID       string
	ID            uint64
	NodeID        string
	Peers         map[uint64]string
	StateMachine  StateMachine
	ElectionTick  int
	HeartbeatTick int
	SkipBootstrap bool
}

// RaftGroupHost owns the group controllers and their shared transport listener.
// Each group has independent consensus state and a group-specific durable path.
type RaftGroupHost struct {
	mu sync.Mutex

	cfg     RaftGroupHostConfig
	mux     *RaftMultiplexer
	groups  map[string]*RaftController
	started bool
	stopped bool
}

func NewRaftGroupHost(cfg RaftGroupHostConfig) (*RaftGroupHost, error) {
	if cfg.Transport == nil {
		return nil, fmt.Errorf("Raft group host requires a transport")
	}
	if cfg.StorageDir == "" {
		return nil, fmt.Errorf("Raft group host requires a durable storage root")
	}
	if _, tcp := cfg.Transport.(*TCPTransport); tcp && cfg.ListenAddr == "" {
		return nil, fmt.Errorf("Raft group host requires a listen address")
	}
	mux, err := NewRaftMultiplexer(cfg.Transport)
	if err != nil {
		return nil, err
	}
	return &RaftGroupHost{cfg: cfg, mux: mux, groups: make(map[string]*RaftController)}, nil
}

// AddGroup creates and registers an independent controller. When the host is
// already running, the group starts immediately and joins the shared listener.
func (h *RaftGroupHost) AddGroup(cfg RaftGroupConfig) (*RaftController, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return nil, ErrClosed
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("Raft group ID must not be empty")
	}
	if _, exists := h.groups[cfg.GroupID]; exists {
		return nil, fmt.Errorf("Raft group %q already exists", cfg.GroupID)
	}
	groupTransport, err := h.mux.GroupTransport(cfg.GroupID)
	if err != nil {
		return nil, err
	}
	controller, err := NewRaftController(RaftControllerConfig{
		ID: cfg.ID, GroupID: cfg.GroupID, NodeID: cfg.NodeID, Peers: cfg.Peers,
		ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		TickInterval: h.cfg.TickInterval, ReadIndexTimeout: h.cfg.ReadIndexTimeout,
		Transport: groupTransport, StateMachine: cfg.StateMachine,
		StorageDir: h.cfg.StorageDir, SkipBootstrap: cfg.SkipBootstrap,
		SnapshotEntries: h.cfg.SnapshotEntries, SnapshotCatchUpEntries: h.cfg.SnapshotCatchUp,
	})
	if err != nil {
		return nil, err
	}
	if err := h.mux.Register(controller); err != nil {
		_ = controller.Stop()
		return nil, err
	}
	h.groups[cfg.GroupID] = controller
	if h.started {
		if err := controller.Start(); err != nil {
			delete(h.groups, cfg.GroupID)
			_ = h.mux.Unregister(controller)
			return nil, err
		}
		if controller.PeerCount() <= 1 {
			if err := controller.Campaign(); err != nil {
				_ = controller.Stop()
				delete(h.groups, cfg.GroupID)
				_ = h.mux.Unregister(controller)
				return nil, err
			}
		}
	}
	return controller, nil
}

// Start begins the shared listener before any group starts its election loop.
func (h *RaftGroupHost) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return ErrClosed
	}
	if h.started {
		return nil
	}
	if len(h.groups) == 0 {
		return fmt.Errorf("Raft group host has no registered groups")
	}
	if err := h.mux.Listen(h.cfg.ListenAddr); err != nil {
		return err
	}
	started := make([]*RaftController, 0, len(h.groups))
	for _, controller := range h.groups {
		if err := controller.Start(); err != nil {
			return h.failStartLocked(err)
		}
		started = append(started, controller)
	}
	h.started = true
	for _, controller := range started {
		if controller.PeerCount() <= 1 {
			if err := controller.Campaign(); err != nil {
				return h.failStartLocked(err)
			}
		}
	}
	return nil
}

func (h *RaftGroupHost) failStartLocked(startErr error) error {
	var cleanupErr error
	for _, controller := range h.groups {
		cleanupErr = errors.Join(cleanupErr, controller.Stop())
	}
	for _, controller := range h.groups {
		cleanupErr = errors.Join(cleanupErr, h.mux.Unregister(controller))
	}
	cleanupErr = errors.Join(cleanupErr, h.cfg.Transport.Close())
	h.started = false
	h.stopped = true
	return errors.Join(startErr, cleanupErr)
}

// RemoveGroup stops a controller before removing it from shared dispatch.
func (h *RaftGroupHost) RemoveGroup(groupID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	controller := h.groups[groupID]
	if controller == nil {
		return fmt.Errorf("Raft group %q not found", groupID)
	}
	if err := controller.Stop(); err != nil {
		return err
	}
	if err := h.mux.Unregister(controller); err != nil {
		return err
	}
	delete(h.groups, groupID)
	return nil
}

// Group returns the active controller registered for groupID.
func (h *RaftGroupHost) Group(groupID string) (*RaftController, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	controller, ok := h.groups[groupID]
	return controller, ok
}

// GroupIDs returns the registered groups in stable order.
func (h *RaftGroupHost) GroupIDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]string, 0, len(h.groups))
	for id := range h.groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Stop stops every group before closing their shared transport.
func (h *RaftGroupHost) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return nil
	}
	var stopErr error
	for _, controller := range h.groups {
		stopErr = errors.Join(stopErr, controller.Stop())
	}
	for _, controller := range h.groups {
		stopErr = errors.Join(stopErr, h.mux.Unregister(controller))
	}
	stopErr = errors.Join(stopErr, h.cfg.Transport.Close())
	h.started = false
	h.stopped = true
	return stopErr
}
