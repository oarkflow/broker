package fastworker

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type AdminServer struct {
	Pool  *Pool
	Token string
}

func (a AdminServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.wrap(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}))
	mux.HandleFunc("/readyz", a.wrap(a.ready))
	mux.HandleFunc("/stats", a.wrap(a.stats))
	mux.HandleFunc("/metrics", a.wrap(a.prometheus))
	mux.HandleFunc("/pause", a.wrap(a.pause))
	mux.HandleFunc("/resume", a.wrap(a.resume))
	mux.HandleFunc("/terminate", a.wrap(a.terminate))
	mux.HandleFunc("/jobs", a.wrap(a.jobs))
	mux.HandleFunc("/jobs/", a.wrap(a.job))
	mux.HandleFunc("/dlq", a.wrap(a.dlq))
	mux.HandleFunc("/dlq/replay", a.wrap(a.replayDLQ))
	mux.HandleFunc("/dlq/purge", a.wrap(a.purgeDLQ))
	return mux
}

func (a AdminServer) ListenAndServe(addr string) *http.Server {
	srv := &http.Server{Addr: addr, Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()
	return srv
}

func (a AdminServer) wrap(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.Token != "" && r.Header.Get("Authorization") != "Bearer "+a.Token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (a AdminServer) ready(w http.ResponseWriter, r *http.Request) {
	if a.Pool == nil || !(a.Pool.IsRunning() || a.Pool.IsPaused()) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready\n"))
}
func (a AdminServer) stats(w http.ResponseWriter, r *http.Request) { writeJSON(w, a.Pool.Stats()) }
func (a AdminServer) prometheus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(a.Pool.Prometheus()))
}
func (a AdminServer) pause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		method(w)
		return
	}
	writeErr(w, a.Pool.Pause())
}
func (a AdminServer) resume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		method(w)
		return
	}
	writeErr(w, a.Pool.Resume())
}
func (a AdminServer) terminate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		method(w)
		return
	}
	writeErr(w, a.Pool.Terminate())
}
func (a AdminServer) jobs(w http.ResponseWriter, r *http.Request) { writeJSON(w, a.Pool.ActiveJobs()) }
func (a AdminServer) job(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	if strings.HasSuffix(id, "/cancel") {
		if r.Method != http.MethodPost {
			method(w)
			return
		}
		id = strings.TrimSuffix(id, "/cancel")
		writeJSON(w, map[string]any{"cancelled": a.Pool.CancelJob(id)})
		return
	}
	info, ok := a.Pool.InspectJob(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, info)
}
func (a AdminServer) dlq(w http.ResponseWriter, r *http.Request) { writeJSON(w, a.Pool.DeadLetters()) }
func (a AdminServer) replayDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		method(w)
		return
	}
	n, err := a.Pool.ReplayDeadLetters()
	if err != nil {
		writeJSON(w, map[string]any{"replayed": n, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"replayed": n})
}
func (a AdminServer) purgeDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		method(w)
		return
	}
	writeJSON(w, map[string]any{"purged": a.Pool.PurgeDeadLetters()})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func writeErr(w http.ResponseWriter, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}
func method(w http.ResponseWriter) { http.Error(w, "method not allowed", http.StatusMethodNotAllowed) }

func (a AdminServer) Shutdown(ctx context.Context, srv *http.Server) error { return srv.Shutdown(ctx) }
