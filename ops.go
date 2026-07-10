package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"time"
)

type OpsServer struct {
	broker    *Broker
	server    *http.Server
	startedAt time.Time
}

type OpsConfig struct {
	ListenAddr string
	Broker     *Broker
}

type StatsResponse struct {
	State        BrokerState      `json:"state"`
	StoreStats   StoreStats       `json:"store_stats"`
	Goroutines   int              `json:"goroutines"`
	Memory       runtime.MemStats `json:"memory"`
	Uptime       string           `json:"uptime"`
	StartedAt    time.Time        `json:"started_at"`
	LastGCDuration string         `json:"last_gc_duration"`
	NumGC        uint32           `json:"num_gc"`
	PauseTotal   string           `json:"pause_total"`
}

type RaftStateResponse struct {
	State       string   `json:"state"`
	Term        uint64   `json:"term"`
	CommitIndex uint64   `json:"commit_index"`
	AppliedIndex uint64  `json:"applied_index"`
	IsLeader    bool     `json:"is_leader"`
	Leader      string   `json:"leader"`
	Peers       []string `json:"peers"`
}

func NewOpsServer(cfg OpsConfig) *OpsServer {
	addr := cfg.ListenAddr
	if addr == "" {
		addr = ":9090"
	}
	return &OpsServer{
		broker:    cfg.Broker,
		startedAt: time.Now(),
		server: &http.Server{
			Addr:    addr,
			Handler: http.NewServeMux(),
		},
	}
}

func (s *OpsServer) Start() error {
	mux := s.server.Handler.(*http.ServeMux)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/metricsz", s.handleMetrics)
	mux.HandleFunc("/statsz", s.handleStats)
	mux.HandleFunc("/clusterz", s.handleCluster)
	go func() {
		s.server.ListenAndServe()
	}()
	return nil
}

func (s *OpsServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *OpsServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *OpsServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.broker.State() == BrokerRunning {
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
	w.Header().Set("Content-Type", "application/json")
	metrics := s.broker.Metrics()
	json.NewEncoder(w).Encode(metrics)
}

func (s *OpsServer) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	storeStats, _ := s.broker.store.Stats(context.Background())
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	resp := StatsResponse{
		State:         s.broker.State(),
		StoreStats:    storeStats,
		Goroutines:    runtime.NumGoroutine(),
		Memory:        mem,
		Uptime:        time.Since(s.broker.startedAt).String(),
		StartedAt:     s.broker.startedAt,
		NumGC:         mem.NumGC,
	}
	if mem.NumGC > 0 {
		resp.LastGCDuration = time.Duration(mem.PauseNs[(mem.NumGC-1)%256]).String()
		resp.PauseTotal = time.Duration(mem.PauseTotalNs).String()
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *OpsServer) handleCluster(w http.ResponseWriter, r *http.Request) {
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
	mux := s.server.Handler.(*http.ServeMux)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/metricsz", s.handleMetrics)
	mux.HandleFunc("/statsz", s.handleStats)
	mux.HandleFunc("/clusterz", s.handleCluster)
	mux.HandleFunc("/cluster/join", s.handleClusterJoin)
	mux.HandleFunc("/cluster/leave", s.handleClusterLeave)
	mux.HandleFunc("/cluster/raft", s.handleRaftState)
	go func() {
		s.server.ListenAndServe()
	}()
	return nil
}

func (s *DistributedOpsServer) handleClusterJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	var body struct {
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if err := s.dist.AddNode(body.Addr); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *DistributedOpsServer) handleClusterLeave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
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

func (s *DistributedOpsServer) handleRaftState(w http.ResponseWriter, r *http.Request) {
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
