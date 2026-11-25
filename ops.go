package broker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"
)

type OpsServer struct {
	broker    *Broker
	server    *http.Server
	startedAt time.Time
	lifecycle *opsLifecycle
	// authorizeOpsRead protects operational data such as metrics, detailed
	// statistics, and cluster topology. Health probes remain public.
	authorizeOpsRead func(*http.Request) bool
	// authorizeClusterMutation is required before the operational API may alter
	// Raft membership.
	authorizeClusterMutation func(*http.Request) bool
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
	AuthorizeOpsRead func(*http.Request) bool
	// AuthorizeClusterMutation must authenticate and authorize requests to
	// change cluster membership. A nil callback disables membership mutations.
	AuthorizeClusterMutation func(*http.Request) bool
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
	readTimeout := cfg.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = 15 * time.Second
	}
	readHeaderTimeout := cfg.ReadHeaderTimeout
	if readHeaderTimeout <= 0 {
		readHeaderTimeout = 5 * time.Second
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = 30 * time.Second
	}
	idleTimeout := cfg.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 60 * time.Second
	}
	return &OpsServer{
		broker:                   cfg.Broker,
		startedAt:                time.Now(),
		lifecycle:                &opsLifecycle{},
		authorizeOpsRead:         cfg.AuthorizeOpsRead,
		authorizeClusterMutation: cfg.AuthorizeClusterMutation,
		maxBodyBytes:             maxBodyBytes,
		server: &http.Server{
			Addr:              addr,
			Handler:           http.NewServeMux(),
			ReadTimeout:       readTimeout,
			ReadHeaderTimeout: readHeaderTimeout,
			WriteTimeout:      writeTimeout,
			IdleTimeout:       idleTimeout,
		},
	}
}

func (s *OpsServer) Start() error {
	return s.start(func(mux *http.ServeMux) {
		mux.HandleFunc("/healthz", s.handleHealthz)
		mux.HandleFunc("/readyz", s.handleReadyz)
		mux.HandleFunc("/metricsz", s.handleMetrics)
		mux.HandleFunc("/statsz", s.handleStats)
		mux.HandleFunc("/clusterz", s.handleCluster)
	})
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
	return s.server.Shutdown(ctx)
}

func (s *OpsServer) start(register func(*http.ServeMux)) error {
	s.lifecycle.mu.Lock()
	defer s.lifecycle.mu.Unlock()
	if s.lifecycle.started || s.lifecycle.stopped {
		return errors.New("operational server cannot be started more than once")
	}
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	register(mux)
	s.server.Handler = s.limitRequestBody(mux)
	s.lifecycle.listener = listener
	s.lifecycle.started = true
	go func() {
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[ops] HTTP server stopped unexpectedly: %v", err)
		}
	}()
	return nil
}

func (s *OpsServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *OpsServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.broker.State() == BrokerRunning && s.broker.checkStore(r.Context()).Status != "fail" {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "not ready",
			"state":  string(s.broker.State()),
		})
	}
}

func (s *OpsServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authorizeOpsReadRequest(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	metrics := s.broker.Metrics()
	json.NewEncoder(w).Encode(metrics)
}

func (s *OpsServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authorizeOpsReadRequest(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
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
	json.NewEncoder(w).Encode(resp)
}

func (s *OpsServer) handleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authorizeOpsReadRequest(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if s.broker.cluster == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "cluster not available"})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{
		"self":         s.broker.cluster.Self(),
		"leader":       s.broker.cluster.Leader(),
		"is_leader":    s.broker.cluster.IsLeader(),
		"nodes":        s.broker.cluster.Nodes(),
		"active_nodes": s.broker.cluster.ActiveNodes(),
		"assignments":  s.broker.cluster.Assignments(),
	})
}

func (s *OpsServer) authorizeOpsReadRequest(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Content-Type", "application/json")
	if s.authorizeOpsRead == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "operational reads are disabled: no authorizer is configured"})
		return false
	}
	if !s.authorizeOpsRead(r) {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{"error": "forbidden"})
		return false
	}
	return true
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
	return s.start(func(mux *http.ServeMux) {
		mux.HandleFunc("/healthz", s.handleHealthz)
		mux.HandleFunc("/readyz", s.handleReadyz)
		mux.HandleFunc("/metricsz", s.handleMetrics)
		mux.HandleFunc("/statsz", s.handleStats)
		mux.HandleFunc("/clusterz", s.handleCluster)
		mux.HandleFunc("/cluster/join", s.handleClusterJoin)
		mux.HandleFunc("/cluster/promote", s.handleClusterPromote)
		mux.HandleFunc("/cluster/leave", s.handleClusterLeave)
		mux.HandleFunc("/cluster/raft", s.handleRaftState)
	})
}

func (s *DistributedOpsServer) handleClusterJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	if !s.authorizeClusterMutationRequest(w, r) {
		return
	}
	var body struct {
		Addr string `json:"addr"`
	}
	if !s.decodeJSONBody(w, r, &body) {
		return
	}
	var peerID uint64
	var err error
	peerID, err = s.dist.AddRaftLearner(r.Context(), body.Addr)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	role := s.dist.RaftPeerRole(peerID)
	result := "learner_added"
	if role != "learner" {
		result = "already_member"
	}
	response := map[string]any{"status": result, "role": role}
	if peerID != 0 {
		response["peer_id"] = peerID
	}
	json.NewEncoder(w).Encode(response)
}

func (s *DistributedOpsServer) handleClusterPromote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	if !s.authorizeClusterMutationRequest(w, r) {
		return
	}
	var body struct {
		PeerID uint64 `json:"peer_id"`
	}
	if !s.decodeJSONBody(w, r, &body) {
		return
	}
	if err := s.dist.PromoteRaftLearner(r.Context(), body.PeerID); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"status": "promoted", "peer_id": body.PeerID, "role": "voter"})
}

func (s *DistributedOpsServer) handleClusterLeave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	if !s.authorizeClusterMutationRequest(w, r) {
		return
	}
	var body struct {
		ID string `json:"id"`
	}
	if !s.decodeJSONBody(w, r, &body) {
		return
	}
	if err := s.dist.RemoveNode(body.ID); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *DistributedOpsServer) authorizeClusterMutationRequest(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Content-Type", "application/json")
	if s.authorizeClusterMutation == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "cluster membership changes are disabled: no authorizer is configured"})
		return false
	}
	if !s.authorizeClusterMutation(r) {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{"error": "forbidden"})
		return false
	}
	return true
}

func (s *DistributedOpsServer) handleRaftState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !s.authorizeOpsReadRequest(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	resp := RaftStateResponse{
		State:        s.dist.RaftState(),
		Term:         s.dist.raft.CurrentTerm(),
		CommitIndex:  s.dist.raft.CommitIndex(),
		AppliedIndex: s.dist.raft.LastApplied(),
		IsLeader:     s.dist.IsLeader(),
		Leader:       s.dist.Leader(),
		Peers:        s.dist.raft.Peers(),
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *OpsServer) limitRequestBody(next http.Handler) http.Handler {
	maxBodyBytes := s.maxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = 1 << 20
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}

func (s *OpsServer) decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	body, err := io.ReadAll(r.Body)
	if err == nil {
		err = json.Unmarshal(body, dst)
	}
	if err == nil {
		return true
	}
	status := http.StatusBadRequest
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		status = http.StatusRequestEntityTooLarge
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	return false
}
