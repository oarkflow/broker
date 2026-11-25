package broker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type OpsServer struct {
	broker    *Broker
	app       *fh.App
	addr      string
	opts      HTTPServerOptions
	startedAt time.Time
	lifecycle *opsLifecycle
	// authorizeOpsRead protects operational data such as metrics, detailed
	// statistics, and cluster topology. Health probes remain public.
	authorizeOpsRead func(fh.Ctx) bool
	// authorizeClusterMutation is required before the operational API may alter
	// Raft membership.
	authorizeClusterMutation func(fh.Ctx) bool
	maxBodyBytes             int64
}

type opsLifecycle struct {
	mu       sync.Mutex
	listener net.Listener
	started  bool
	stopped  bool
}

type OpsConfig struct {
	ListenAddr string
	Broker     *Broker
	// AuthorizeOpsRead must authenticate and authorize detailed operational
	// reads. A nil callback disables metrics, stats, and cluster-state routes.
	AuthorizeOpsRead func(fh.Ctx) bool
	// AuthorizeClusterMutation must authenticate and authorize requests to
	// change cluster membership. A nil callback disables membership mutations.
	AuthorizeClusterMutation func(fh.Ctx) bool
	// MaxBodyBytes limits JSON requests to the operational API. Defaults to 1 MiB.
	MaxBodyBytes      int64
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

type StatsResponse struct {
	State          BrokerState      `json:"state"`
	StoreStats     StoreStats       `json:"store_stats"`
	Goroutines     int              `json:"goroutines"`
	Memory         runtime.MemStats `json:"memory"`
	Uptime         string           `json:"uptime"`
	StartedAt      time.Time        `json:"started_at"`
	LastGCDuration string           `json:"last_gc_duration"`
	NumGC          uint32           `json:"num_gc"`
	PauseTotal     string           `json:"pause_total"`
}

type RaftStateResponse struct {
	State        string   `json:"state"`
	Term         uint64   `json:"term"`
	CommitIndex  uint64   `json:"commit_index"`
	AppliedIndex uint64   `json:"applied_index"`
	IsLeader     bool     `json:"is_leader"`
	Leader       string   `json:"leader"`
	Peers        []string `json:"peers"`
}

func NewOpsServer(cfg OpsConfig) *OpsServer {
	addr := cfg.ListenAddr
	if addr == "" {
		addr = ":9090"
	}
	maxBodyBytes := cfg.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = 1 << 20
	}
	return &OpsServer{
		broker:                   cfg.Broker,
		addr:                     addr,
		startedAt:                time.Now(),
		lifecycle:                &opsLifecycle{},
		authorizeOpsRead:         cfg.AuthorizeOpsRead,
		authorizeClusterMutation: cfg.AuthorizeClusterMutation,
		maxBodyBytes:             maxBodyBytes,
		opts: HTTPServerOptions{
			ReadTimeout:       cfg.ReadTimeout,
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
			MaxBodyBytes:      maxBodyBytes + 1<<20,
		},
	}
}

func (s *OpsServer) registerProbes(app *fh.App) {
	app.Get("/healthz", s.handleHealthz)
	app.Get("/readyz", s.handleReadyz)
	app.Get("/metricsz", s.requireOpsRead, s.handleMetrics)
	app.Get("/statsz", s.requireOpsRead, s.handleStats)
	app.Get("/clusterz", s.requireOpsRead, s.handleCluster)
}

func (s *OpsServer) Start() error {
	return s.start(s.registerProbes)
}

func (s *OpsServer) Stop() error {
	s.lifecycle.mu.Lock()
	if s.lifecycle.stopped {
		s.lifecycle.mu.Unlock()
		return nil
	}
	s.lifecycle.stopped = true
	started := s.lifecycle.started
	s.lifecycle.mu.Unlock()
	if !started {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ShutdownHTTPApp(ctx, s.app)
}

func (s *OpsServer) start(register func(*fh.App)) error {
	s.lifecycle.mu.Lock()
	defer s.lifecycle.mu.Unlock()
	if s.lifecycle.started || s.lifecycle.stopped {
		return errors.New("operational server cannot be started more than once")
	}
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	app := NewHTTPApp(s.opts, fh.WithNotFoundHandler(plainNotFound), fh.WithMethodNotAllowedHandler(opsMethodNotAllowed))
	app.Use(s.limitRequestBody)
	register(app)
	s.app = app
	s.lifecycle.listener = listener
	s.lifecycle.started = true
	go func() {
		if err := ServeHTTPApp(app, listener, nil); err != nil {
			log.Printf("[ops] HTTP server stopped unexpectedly: %v", err)
		}
	}()
	return nil
}

// opsJSON writes v as the operational API's JSON response.
func opsJSON(c fh.Ctx, status int, v any) error {
	return writeBrokerJSONStatus(c, status, v)
}

func opsError(c fh.Ctx, status int, msg string) error {
	return opsJSON(c, status, map[string]string{"error": msg})
}

func opsMethodNotAllowed(c fh.Ctx, allowed []string) error {
	c.Set("Allow", strings.Join(allowed, ", "))
	return opsError(c, fh.StatusMethodNotAllowed, "method not allowed")
}

func (s *OpsServer) handleHealthz(c fh.Ctx) error {
	return opsJSON(c, fh.StatusOK, map[string]string{"status": "ok"})
}

func (s *OpsServer) handleReadyz(c fh.Ctx) error {
	if s.broker.State() == BrokerRunning && s.broker.checkStore(c.Context()).Status != "fail" {
		return opsJSON(c, fh.StatusOK, map[string]string{"status": "ok"})
	}
	return opsJSON(c, fh.StatusServiceUnavailable, map[string]string{
		"status": "not ready",
		"state":  string(s.broker.State()),
	})
}

func (s *OpsServer) handleMetrics(c fh.Ctx) error {
	return opsJSON(c, fh.StatusOK, s.broker.Metrics())
}

func (s *OpsServer) handleStats(c fh.Ctx) error {
	storeStats, _ := s.broker.store.Stats(context.Background())
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	resp := StatsResponse{
		State:      s.broker.State(),
		StoreStats: storeStats,
		Goroutines: runtime.NumGoroutine(),
		Memory:     mem,
		Uptime:     time.Since(s.broker.startedAt).String(),
		StartedAt:  s.broker.startedAt,
		NumGC:      mem.NumGC,
	}
	if mem.NumGC > 0 {
		resp.LastGCDuration = time.Duration(mem.PauseNs[(mem.NumGC-1)%256]).String()
		resp.PauseTotal = time.Duration(mem.PauseTotalNs).String()
	}
	return opsJSON(c, fh.StatusOK, resp)
}

func (s *OpsServer) handleCluster(c fh.Ctx) error {
	if s.broker.cluster == nil {
		return opsError(c, fh.StatusNotFound, "cluster not available")
	}
	return opsJSON(c, fh.StatusOK, map[string]any{
		"self":         s.broker.cluster.Self(),
		"leader":       s.broker.cluster.Leader(),
		"is_leader":    s.broker.cluster.IsLeader(),
		"nodes":        s.broker.cluster.Nodes(),
		"active_nodes": s.broker.cluster.ActiveNodes(),
		"assignments":  s.broker.cluster.Assignments(),
	})
}

// requireOpsRead is middleware guarding detailed operational reads.
func (s *OpsServer) requireOpsRead(c fh.Ctx) error {
	if s.authorizeOpsRead == nil {
		return opsError(c, fh.StatusServiceUnavailable, "operational reads are disabled: no authorizer is configured")
	}
	if !s.authorizeOpsRead(c) {
		return opsError(c, fh.StatusForbidden, "forbidden")
	}
	return c.Next()
}

type DistributedOpsServer struct {
	OpsServer
	dist *DistributedBroker
}

func NewDistributedOpsServer(cfg OpsConfig, dist *DistributedBroker) *DistributedOpsServer {
	s := &DistributedOpsServer{
		OpsServer: *NewOpsServer(cfg),
		dist:      dist,
	}
	s.broker = dist.Broker
	return s
}

func (s *DistributedOpsServer) Start() error {
	return s.start(func(app *fh.App) {
		s.registerProbes(app)
		app.Post("/cluster/join", s.requireClusterMutation, s.handleClusterJoin)
		app.Post("/cluster/promote", s.requireClusterMutation, s.handleClusterPromote)
		app.Post("/cluster/leave", s.requireClusterMutation, s.handleClusterLeave)
		app.Get("/cluster/raft", s.requireOpsRead, s.handleRaftState)
	})
}

func (s *DistributedOpsServer) handleClusterJoin(c fh.Ctx) error {
	var body struct {
		Addr string `json:"addr"`
	}
	if ok, err := s.decodeJSONBody(c, &body); !ok {
		return err
	}
	peerID, err := s.dist.AddRaftLearner(c.Context(), body.Addr)
	if err != nil {
		return opsError(c, fh.StatusInternalServerError, err.Error())
	}
	role := s.dist.RaftPeerRole(peerID)
	result := "learner_added"
	if role != "learner" {
		result = "already_member"
	}
	response := map[string]any{"status": result, "role": role}
	if peerID != 0 {
		response["peer_id"] = peerID
	}
	return opsJSON(c, fh.StatusOK, response)
}

func (s *DistributedOpsServer) handleClusterPromote(c fh.Ctx) error {
	var body struct {
		PeerID uint64 `json:"peer_id"`
	}
	if ok, err := s.decodeJSONBody(c, &body); !ok {
		return err
	}
	if err := s.dist.PromoteRaftLearner(c.Context(), body.PeerID); err != nil {
		return opsError(c, fh.StatusConflict, err.Error())
	}
	return opsJSON(c, fh.StatusOK, map[string]any{"status": "promoted", "peer_id": body.PeerID, "role": "voter"})
}

func (s *DistributedOpsServer) handleClusterLeave(c fh.Ctx) error {
	var body struct {
		ID string `json:"id"`
	}
	if ok, err := s.decodeJSONBody(c, &body); !ok {
		return err
	}
	if err := s.dist.RemoveNode(body.ID); err != nil {
		return opsError(c, fh.StatusInternalServerError, err.Error())
	}
	return opsJSON(c, fh.StatusOK, map[string]string{"status": "ok"})
}

// requireClusterMutation is middleware guarding Raft membership changes.
func (s *DistributedOpsServer) requireClusterMutation(c fh.Ctx) error {
	if s.authorizeClusterMutation == nil {
		return opsError(c, fh.StatusServiceUnavailable, "cluster membership changes are disabled: no authorizer is configured")
	}
	if !s.authorizeClusterMutation(c) {
		return opsError(c, fh.StatusForbidden, "forbidden")
	}
	return c.Next()
}

func (s *DistributedOpsServer) handleRaftState(c fh.Ctx) error {
	return opsJSON(c, fh.StatusOK, RaftStateResponse{
		State:        s.dist.RaftState(),
		Term:         s.dist.raft.CurrentTerm(),
		CommitIndex:  s.dist.raft.CommitIndex(),
		AppliedIndex: s.dist.raft.LastApplied(),
		IsLeader:     s.dist.IsLeader(),
		Leader:       s.dist.Leader(),
		Peers:        s.dist.raft.Peers(),
	})
}

// limitRequestBody bounds operational request bodies to maxBodyBytes.
func (s *OpsServer) limitRequestBody(c fh.Ctx) error {
	if err := fh.LimitRequestBody(c, s.maxBodyBytes); err != nil {
		return opsError(c, fh.StatusPayloadTooLarge, "request body too large")
	}
	return c.Next()
}

// decodeJSONBody decodes the request body into dst. When it returns ok=false
// the 400 response has been written and the handler should return err.
func (s *OpsServer) decodeJSONBody(c fh.Ctx, dst any) (ok bool, err error) {
	if err := json.Unmarshal(c.Body(), dst); err != nil {
		return false, opsError(c, fh.StatusBadRequest, err.Error())
	}
	return true, nil
}
