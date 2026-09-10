package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type AdminServer struct {
	Broker *Broker

	// Token is a convenience single-token credential with admin:* access.
	// For production, prefer ACL with named tokens and narrow permissions.
	Token string
	ACL   *ACL

	// InsecureDev explicitly disables authentication. It is intended only for
	// local tests and examples. Production brokerd refuses to start this way
	// unless --insecure-dev is provided.
	InsecureDev bool

	// MaxBodyBytes limits JSON request bodies. Defaults to 1 MiB.
	MaxBodyBytes int64

	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	TLSCertFile       string
	TLSKeyFile        string
	ClientCAFile      string
	RequireClientCert bool

	// AllowedOrigins is the list of allowed CORS origins.
	// Default: same-origin only (""). Set to ["*"] for public APIs.
	AllowedOrigins []string

	Audit AdminAuditLogger
}

func (a AdminServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/", a.wrap(a.dataPlane))
	mux.HandleFunc("/healthz", a.wrap(a.health))
	mux.HandleFunc("/readyz", a.wrap(a.ready))
	mux.HandleFunc("/stats", a.wrap(a.stats))
	mux.HandleFunc("/metrics", a.wrap(a.metrics))
	mux.HandleFunc("/lag", a.wrap(a.lag))
	mux.HandleFunc("/audit", a.wrap(a.audit))
	mux.HandleFunc("/publish", a.wrap(a.publish))
	mux.HandleFunc("/publish/batch", a.wrap(a.publishBatch))
	mux.HandleFunc("/topics", a.wrap(a.topics))
	mux.HandleFunc("/queues", a.wrap(a.queues))
	mux.HandleFunc("/queues/", a.wrap(a.queue))
	mux.HandleFunc("/streams", a.wrap(a.streams))
	mux.HandleFunc("/streams/", a.wrap(a.stream))
	mux.HandleFunc("/dlq", a.wrap(a.dlq))
	mux.HandleFunc("/dlq/", a.wrap(a.dlqQueue))
	mux.HandleFunc("/messages/", a.wrap(a.message))
	mux.HandleFunc("/ack/batch", a.wrap(a.ackBatch))
	mux.HandleFunc("/move", a.wrap(a.move))
	mux.HandleFunc("/notifications", a.wrap(a.notifications))
	mux.HandleFunc("/events", a.wrap(a.events))
	mux.HandleFunc("/routes", a.wrap(a.routes))
	mux.HandleFunc("/cluster/status", a.wrap(a.clusterStatus))
	return mux
}
func (a AdminServer) ListenAndServe(addr string) *http.Server {
	srv := a.HTTPServer(addr)
	go func() {
		if a.TLSCertFile != "" || a.TLSKeyFile != "" {
			_ = srv.ListenAndServeTLS(a.TLSCertFile, a.TLSKeyFile)
			return
		}
		_ = srv.ListenAndServe()
	}()
	return srv
}

// GracefulShutdown performs a graceful shutdown of the HTTP server, waiting for
// in-flight requests to complete within the given timeout.
func (a AdminServer) GracefulShutdown(ctx context.Context, srv *http.Server, drainTimeout time.Duration) error {
	if drainTimeout <= 0 {
		drainTimeout = 15 * time.Second
	}
	drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()
	return srv.Shutdown(drainCtx)
}

func (a AdminServer) HTTPServer(addr string) *http.Server {
	readHeader := a.ReadHeaderTimeout
	if readHeader <= 0 {
		readHeader = 5 * time.Second
	}
	readTimeout := a.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = 15 * time.Second
	}
	writeTimeout := a.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = 30 * time.Second
	}
	idleTimeout := a.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 60 * time.Second
	}
	srv := &http.Server{Addr: addr, Handler: a.Handler(), ReadHeaderTimeout: readHeader, ReadTimeout: readTimeout, WriteTimeout: writeTimeout, IdleTimeout: idleTimeout}
	if a.TLSCertFile != "" || a.TLSKeyFile != "" || a.ClientCAFile != "" || a.RequireClientCert {
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		if a.ClientCAFile != "" {
			pem, err := os.ReadFile(a.ClientCAFile)
			if err == nil {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(pem) {
					srv.TLSConfig.ClientCAs = pool
				}
			}
		}
		if a.RequireClientCert {
			srv.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}
	return srv
}
func (a AdminServer) Shutdown(ctx context.Context, srv *http.Server) error { return srv.Shutdown(ctx) }
func (a AdminServer) wrap(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		maxBody := a.MaxBodyBytes
		if maxBody <= 0 {
			maxBody = 1 << 20
		}
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		}
		// Security headers
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		// CORS headers
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}
		if a.isAllowedOrigin(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Max-Age", "86400")
		// Handle preflight
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		audit := a.Audit
		if audit == nil && a.Broker != nil {
			audit = NotificationAuditLogger{Bus: a.Broker.Notifications()}
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		action := adminAction(r)
		principal, ok := a.authenticate(r)
		if !ok {
			if audit != nil {
				audit.LogAdmin(r.Context(), AdminAuditEvent{Time: time.Now(), Principal: "anonymous", RemoteAddr: r.RemoteAddr, Method: r.Method, Path: r.URL.Path, Action: action, Allowed: false, Status: http.StatusUnauthorized, Reason: "missing or invalid bearer token"})
			}
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		acl := a.effectiveACL()
		if !acl.Authorize(principal, action) {
			if audit != nil {
				audit.LogAdmin(r.Context(), AdminAuditEvent{Time: time.Now(), Principal: principal.Name, RemoteAddr: r.RemoteAddr, Method: r.Method, Path: r.URL.Path, Action: action, Allowed: false, Status: http.StatusForbidden, Reason: "permission denied"})
			}
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		release, err := acl.Admit(principal)
		if err != nil {
			w.Header().Set("Retry-After", "1")
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}
		defer release()
		rr := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		ctx := context.WithValue(r.Context(), adminPrincipalKey{}, principal)
		next(rr, r.WithContext(ctx))
		if audit != nil && isMutation(r.Method) {
			audit.LogAdmin(r.Context(), AdminAuditEvent{Time: time.Now(), Principal: principal.Name, RemoteAddr: r.RemoteAddr, Method: r.Method, Path: r.URL.Path, Action: action, Allowed: rr.status < 400, Status: rr.status})
		}
	}
}

func (a AdminServer) authenticate(r *http.Request) (AdminPrincipal, bool) {
	// Health probes can be unauthenticated. readyz reveals only binary readiness.
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
		return AdminPrincipal{Name: "probe", Permissions: []string{PermissionBrokerRead}}, true
	}
	if a.InsecureDev {
		log.Printf("[SECURITY WARNING] InsecureDev mode is active - all requests authenticated as admin. Do NOT use in production!")
		return AdminPrincipal{Name: "insecure-dev", Permissions: []string{PermissionAdminAll}}, true
	}
	acl := a.effectiveACL()
	if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
		if principal, ok := acl.AuthenticateCertificate(r.TLS.VerifiedChains[0][0]); ok {
			return principal, true
		}
	}
	return acl.Authenticate(bearerToken(r))
}

func (a AdminServer) effectiveACL() *ACL {
	if a.ACL != nil {
		return a.ACL
	}
	if a.Token != "" {
		return NewACL(AdminToken{Name: "admin", Token: a.Token, Permissions: []string{PermissionAdminAll}})
	}
	return NewACL()
}

func isMutation(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch || method == http.MethodDelete
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
func (a AdminServer) health(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok\n")) }
func (a AdminServer) ready(w http.ResponseWriter, r *http.Request) {
	if a.Broker == nil || a.Broker.State() != BrokerRunning {
		http.Error(w, "not ready", 503)
		return
	}
	if a.Broker.IsStoreFenced() {
		http.Error(w, "storage fenced: storage unavailable", http.StatusServiceUnavailable)
		return
	}
	if check := a.Broker.checkStore(r.Context()); check.Status == "fail" {
		http.Error(w, "not ready: "+check.Detail, http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready\n"))
}
func (a AdminServer) stats(w http.ResponseWriter, r *http.Request) {
	st, err := a.Broker.store.Stats(r.Context())
	if hasRestrictedTenantScope(r.Context()) {
		filteredQueues := make(map[string]int)
		for k, v := range st.Queues {
			if tenantAllowed(r.Context(), k) {
				filteredQueues[k] = v
			}
		}
		st.Queues = filteredQueues

		filteredTopics := make(map[string]int)
		for k, v := range st.Topics {
			if tenantAllowed(r.Context(), k) {
				filteredTopics[k] = v
			}
		}
		st.Topics = filteredTopics

		filteredStreams := make(map[string]int)
		for k, v := range st.Streams {
			if tenantAllowed(r.Context(), k) {
				filteredStreams[k] = v
			}
		}
		st.Streams = filteredStreams

		filteredDLQ := make(map[string]int)
		for k, v := range st.DLQ {
			if tenantAllowed(r.Context(), k) {
				filteredDLQ[k] = v
			}
		}
		st.DLQ = filteredDLQ

		if st.Offsets != nil {
			filteredOffsets := make(map[string]map[string]uint64)
			for k, v := range st.Offsets {
				if tenantAllowed(r.Context(), k) {
					filteredOffsets[k] = v
				}
			}
			st.Offsets = filteredOffsets
		}
	}
	writeBrokerJSON(w, map[string]any{"broker_state": a.Broker.State(), "store": st, "error": errString(err)})
}
func (a AdminServer) metrics(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "global metrics") {
		return
	}
	if a.Broker.promMetrics == nil {
		http.Error(w, "prometheus metrics not enabled", 503)
		return
	}
	a.Broker.promMetrics.UpdateGauges()
	st, _ := a.Broker.store.Stats(r.Context())
	a.Broker.promMetrics.UpdateStoreStats(st)
	a.Broker.promMetrics.PromHandler().ServeHTTP(w, r)
}
func (a AdminServer) lag(w http.ResponseWriter, r *http.Request) {
	stream := r.URL.Query().Get("stream")
	if stream == "" {
		http.Error(w, "stream parameter required", 400)
		return
	}
	report, err := a.Broker.StreamLagReport(r.Context(), stream)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	writeBrokerJSON(w, report)
}
func (a AdminServer) audit(w http.ResponseWriter, r *http.Request) {
	if a.Broker.auditLogger == nil {
		writeBrokerJSON(w, []AuditEvent{})
		return
	}
	history := a.Broker.auditLogger.History(queryLimit(r, 100))
	if hasRestrictedTenantScope(r.Context()) {
		filtered := make([]AuditEvent, 0, len(history))
		for _, ev := range history {
			if ev.TenantID != "" && tenantAllowed(r.Context(), ev.TenantID) {
				filtered = append(filtered, ev)
			}
		}
		writeBrokerJSON(w, filtered)
		return
	}
	writeBrokerJSON(w, history)
}
func (a AdminServer) topics(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var cfg TopicConfig
		if json.NewDecoder(r.Body).Decode(&cfg) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		writeBrokerErr(w, a.Broker.DeclareTopic(r.Context(), cfg))
		return
	}
	st, _ := a.Broker.store.Stats(r.Context())
	writeBrokerJSON(w, st.Topics)
}
func (a AdminServer) queues(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var cfg QueueConfig
		if json.NewDecoder(r.Body).Decode(&cfg) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		writeBrokerErr(w, a.Broker.DeclareQueue(r.Context(), cfg))
		return
	}
	st, _ := a.Broker.store.Stats(r.Context())
	writeBrokerJSON(w, st.Queues)
}
func (a AdminServer) queue(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/queues/")
	if path == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	name := parts[0]
	if validated, err := sanitizeIdentifier(name); err != nil {
		http.Error(w, "invalid queue name: "+err.Error(), 400)
		return
	} else {
		name = validated
	}
	if len(parts) == 2 {
		switch parts[1] {
		case "messages":
			msgs, err := a.Broker.store.ListQueue(r.Context(), name, queryLimit(r, 100))
			if err != nil {
				http.Error(w, err.Error(), 404)
				return
			}
			writeBrokerJSON(w, msgs)
			return
		case "pull":
			if r.Method != http.MethodPost && r.Method != http.MethodGet {
				methodBroker(w)
				return
			}
			var req struct {
				ConsumerID        string `json:"consumer_id"`
				ConsumerGroup     string `json:"consumer_group"`
				Limit             int    `json:"limit"`
				VisibilityTimeout int64  `json:"visibility_timeout_ns"`
			}
			if r.Method == http.MethodPost && r.Body != nil {
				_ = json.NewDecoder(r.Body).Decode(&req)
			}
			if req.ConsumerID == "" {
				req.ConsumerID = firstNonEmpty(r.URL.Query().Get("consumer"), "admin")
			}
			if req.ConsumerGroup == "" {
				req.ConsumerGroup = firstNonEmpty(r.URL.Query().Get("group"), req.ConsumerID)
			}
			if req.Limit <= 0 {
				req.Limit = queryLimit(r, 1)
			}
			vis := time.Duration(req.VisibilityTimeout)
			msgs, err := a.Broker.store.Lease(r.Context(), LeaseRequest{Queue: name, ConsumerID: req.ConsumerID, ConsumerGroup: req.ConsumerGroup, Limit: req.Limit, VisibilityTimeout: vis})
			if err != nil {
				http.Error(w, err.Error(), 409)
				return
			}
			writeBrokerJSON(w, msgs)
			return
		case "pause":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			writeBrokerErr(w, a.Broker.PauseQueue(r.Context(), name))
			return
		case "resume":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			writeBrokerErr(w, a.Broker.ResumeQueue(r.Context(), name))
			return
		case "purge":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			n, err := a.Broker.PurgeQueue(r.Context(), name)
			if err != nil {
				http.Error(w, err.Error(), 409)
				return
			}
			writeBrokerJSON(w, map[string]any{"ok": true, "purged": n})
			return
		}
	}
	http.NotFound(w, r)
}

// ackBatch acks many messages (possibly across queues/consumers) in one HTTP
// call, amortizing per-request network and JSON overhead for consumers.
func (a AdminServer) ackBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var reqs []AckRequest
	if json.NewDecoder(r.Body).Decode(&reqs) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	var batchErrs []error
	if store, ok := a.Broker.store.(BatchAckStore); ok {
		batchErrs = store.AckBatch(r.Context(), reqs)
	} else {
		batchErrs = make([]error, len(reqs))
		for i := range reqs {
			batchErrs[i] = a.Broker.store.Ack(r.Context(), reqs[i])
		}
	}
	errs := make([]string, len(reqs))
	acked := 0
	for i, err := range batchErrs {
		if err != nil {
			errs[i] = err.Error()
			continue
		}
		acked++
		a.Broker.Consumers().AddStats(reqs[i].ConsumerID, 0, 1, 0, 0, "")
	}
	if acked == len(reqs) && preferMinimal(r) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if acked != len(reqs) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMultiStatus)
	}
	writeBrokerJSON(w, map[string]any{"acked": acked, "errors": errs})
}
func (a AdminServer) streams(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var cfg StreamConfig
		if json.NewDecoder(r.Body).Decode(&cfg) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		writeBrokerErr(w, a.Broker.DeclareStream(r.Context(), cfg))
		return
	}
	st, _ := a.Broker.store.Stats(r.Context())
	writeBrokerJSON(w, map[string]any{"streams": st.Streams, "offsets": st.Offsets})
}
func (a AdminServer) stream(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/streams/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	name := parts[0]
	if validated, err := sanitizeIdentifier(name); err != nil {
		http.Error(w, "invalid stream name: "+err.Error(), 400)
		return
	} else {
		name = validated
	}
	if len(parts) == 1 || (len(parts) == 2 && parts[1] == "messages") {
		afterRaw := firstNonEmpty(r.URL.Query().Get("after"), r.URL.Query().Get("after_offset"))
		after, _ := strconv.ParseUint(afterRaw, 10, 64)
		group := firstNonEmpty(r.URL.Query().Get("group"), r.URL.Query().Get("consumer_group"))
		msgs, err := a.Broker.ReadStream(r.Context(), StreamReadRequest{Stream: name, ConsumerGroup: group, AfterOffset: after, Limit: queryLimit(r, 100)})
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, msgs)
		return
	}
	if len(parts) == 2 && parts[1] == "commit" {
		if r.Method != http.MethodPost {
			methodBroker(w)
			return
		}
		var req OffsetCommit
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		req.Stream = name
		writeBrokerErr(w, a.Broker.CommitOffset(r.Context(), req))
		return
	}
	if len(parts) == 2 && parts[1] == "offset" {
		group := firstNonEmpty(r.URL.Query().Get("group"), r.URL.Query().Get("consumer_group"))
		off, err := a.Broker.ConsumerOffset(r.Context(), OffsetCommit{Stream: name, ConsumerGroup: group})
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, map[string]any{"stream": name, "group": group, "offset": off})
		return
	}
	http.NotFound(w, r)
}
func (a AdminServer) dlq(w http.ResponseWriter, r *http.Request) {
	st, _ := a.Broker.store.Stats(r.Context())
	writeBrokerJSON(w, st.DLQ)
}
func (a AdminServer) dlqQueue(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/dlq/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	name := parts[0]
	if len(parts) == 1 || (len(parts) == 2 && parts[1] == "messages") {
		msgs, err := a.Broker.store.ListDLQ(r.Context(), name, queryLimit(r, 100))
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeBrokerJSON(w, msgs)
		return
	}
	if len(parts) == 2 && parts[1] == "replay" {
		if r.Method != http.MethodPost {
			methodBroker(w)
			return
		}
		dest := r.URL.Query().Get("queue")
		if dest == "" {
			dest = strings.TrimSuffix(name, ".dlq")
		}
		res, err := a.Broker.store.Move(r.Context(), MoveRequest{SourceType: DestinationQueue, Source: name, DestinationType: DestinationQueue, Destination: dest, Mode: MoveReplay, ResetAttempts: true, ResetError: true, ResetLease: true, Reason: "dlq replay", Actor: "admin"})
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		writeBrokerJSON(w, res)
		return
	}
	http.NotFound(w, r)
}
func (a AdminServer) message(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/messages/"), "/")
	if path == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(path, "/")
	id := parts[0]
	if validated, err := sanitizeIdentifier(id); err != nil {
		http.Error(w, "invalid message id: "+err.Error(), 400)
		return
	} else {
		id = validated
	}
	if len(parts) == 2 {
		switch parts[1] {
		case "history":
			h, err := a.Broker.store.History(r.Context(), id)
			if err != nil {
				http.Error(w, err.Error(), 404)
				return
			}
			writeBrokerJSON(w, h)
			return
		case "ack":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			var req AckRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			req.MessageID = id
			writeBrokerErr(w, a.Broker.store.Ack(r.Context(), req))
			return
		case "nack":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			var req NackRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			req.MessageID = id
			if req.Action == "" {
				req.Action = NackRequeue
			}
			writeBrokerErr(w, a.Broker.store.Nack(r.Context(), req))
			return
		}
	}
	m, ok, err := a.Broker.store.Get(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeBrokerJSON(w, m)
}
func (a AdminServer) publish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var req struct {
		Topic   string            `json:"topic"`
		Queue   string            `json:"queue"`
		Stream  string            `json:"stream"`
		Type    string            `json:"type"`
		Payload json.RawMessage   `json:"payload"`
		Headers map[string]string `json:"headers"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	opts := []PublishOption{WithHeaders(req.Headers), WithType(req.Type)}
	if req.Queue != "" {
		opts = append(opts, WithQueue(req.Queue))
	}
	if req.Stream != "" {
		opts = append(opts, WithStream(req.Stream))
	}
	res, err := a.Broker.Publisher().Publish(r.Context(), req.Topic, []byte(req.Payload), opts...)
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	writeBrokerJSON(w, res)
}

// publishBatch accepts an array of publish requests (each may target a
// different topic/queue/stream) in one HTTP call, amortizing per-request
// network and JSON overhead across many messages.
func (a AdminServer) publishBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var items []struct {
		Topic   string            `json:"topic"`
		Queue   string            `json:"queue"`
		Stream  string            `json:"stream"`
		Type    string            `json:"type"`
		Payload json.RawMessage   `json:"payload"`
		Headers map[string]string `json:"headers"`
	}
	if json.NewDecoder(r.Body).Decode(&items) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	pub := a.Broker.Publisher()
	if preferMinimal(r) {
		messages := make([]Message, len(items))
		for i, req := range items {
			messages[i] = Message{Topic: req.Topic, Queue: req.Queue, Stream: req.Stream, Type: req.Type, Payload: []byte(req.Payload), Headers: req.Headers}
		}
		errs, first := pub.publishMessagesMinimal(r.Context(), messages)
		writeMinimalPublishResponse(w, errs, first)
		return
	}
	batchItems := make([]PublishBatchItem, len(items))
	for i, req := range items {
		opts := []PublishOption{WithHeaders(req.Headers), WithType(req.Type)}
		if req.Queue != "" {
			opts = append(opts, WithQueue(req.Queue))
		}
		if req.Stream != "" {
			opts = append(opts, WithStream(req.Stream))
		}
		batchItems[i] = PublishBatchItem{Topic: req.Topic, Payload: []byte(req.Payload), Options: opts}
	}
	results, publishErrs, _ := pub.publishItemsAligned(r.Context(), batchItems)
	errs := make([]string, len(publishErrs))
	for i, err := range publishErrs {
		if err != nil {
			errs[i] = err.Error()
		}
	}
	writeBrokerJSON(w, map[string]any{"results": results, "errors": errs})
}
func (a AdminServer) move(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodBroker(w)
		return
	}
	var req MoveRequest
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	res, err := a.Broker.store.Move(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	writeBrokerJSON(w, res)
}
func (a AdminServer) notifications(w http.ResponseWriter, r *http.Request) {
	writeBrokerJSON(w, visibleNotifications(r.Context(), a.Broker.notify.History(queryLimit(r, 100))))
}
func (a AdminServer) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	id := "sse-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	ch := make(chan Notification, 128)
	a.Broker.Notifications().Subscribe(id, CallbackFunc(func(ctx context.Context, n Notification) error {
		if !tenantAllowed(r.Context(), n.TenantID) {
			return nil
		}
		select {
		case ch <- n:
		default:
		}
		return nil
	}))
	defer a.Broker.Notifications().Unsubscribe(id)
	for _, n := range a.Broker.Notifications().History(queryLimit(r, 25)) {
		if tenantAllowed(r.Context(), n.TenantID) {
			writeSSE(w, n)
		}
	}
	flusher.Flush()
	for {
		select {
		case n := <-ch:
			writeSSE(w, n)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
func (a AdminServer) routes(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "global routes") {
		return
	}
	if r.Method == http.MethodPost {
		var route Route
		if json.NewDecoder(r.Body).Decode(&route) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		a.Broker.AddRoute(route)
		writeBrokerJSON(w, map[string]bool{"ok": true})
		return
	}
	writeBrokerJSON(w, a.Broker.router.Routes())
}
func rejectScopedControlPlane(w http.ResponseWriter, r *http.Request, resource string) bool {
	if !hasRestrictedTenantScope(r.Context()) {
		return false
	}
	http.Error(w, "tenant-scoped credentials cannot access "+resource, http.StatusForbidden)
	return true
}
func visibleNotifications(ctx context.Context, history []Notification) []Notification {
	if !hasRestrictedTenantScope(ctx) {
		return history
	}
	out := make([]Notification, 0, len(history))
	for _, n := range history {
		if tenantAllowed(ctx, n.TenantID) {
			out = append(out, n)
		}
	}
	return out
}
func queryLimit(r *http.Request, d int) int {
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			if n > 10000 {
				n = 10000
			}
			return n
		}
	}
	return d
}
func writeBrokerJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func writeSSE(w http.ResponseWriter, n Notification) {
	b, _ := json.Marshal(n)
	_, _ = fmt.Fprintf(w, "event: %s\nid: %s\ndata: %s\n\n", n.Type, n.ID, b)
}
func writeBrokerErr(w http.ResponseWriter, err error) {
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	writeBrokerJSON(w, map[string]bool{"ok": true})
}
func methodBroker(w http.ResponseWriter) { http.Error(w, "method not allowed", 405) }
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// sanitizeIdentifier validates that an identifier contains only safe characters.
// Returns the identifier if valid, or an error if invalid.
func sanitizeIdentifier(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("empty identifier")
	}
	if len(s) > 256 {
		return "", fmt.Errorf("identifier too long: %d > 256", len(s))
	}
	for _, c := range s {
		if c < 32 || c > 126 {
			return "", fmt.Errorf("identifier contains invalid character: %d", c)
		}
		if c == '/' || c == '\\' || c == '\x00' {
			return "", fmt.Errorf("identifier contains forbidden character: %c", c)
		}
	}
	return s, nil
}

func (a AdminServer) isAllowedOrigin(origin string) bool {
	if len(a.AllowedOrigins) == 0 {
		// Default: same-origin (no CORS header = browser default same-origin policy)
		return false
	}
	for _, allowed := range a.AllowedOrigins {
		if allowed == "*" || allowed == origin {
			return true
		}
	}
	return false
}

func adminAction(r *http.Request) string {
	path := r.URL.Path
	method := r.Method
	switch {
	case path == "/healthz" || path == "/readyz" || path == "/stats":
		return PermissionBrokerRead
	case path == "/publish" || path == "/publish/batch":
		return PermissionPublish
	case path == "/move":
		return PermissionMessageMove
	case path == "/notifications" || path == "/events" || strings.HasPrefix(path, "/v1/events"):
		return PermissionNotification
	case path == "/ack/batch" || path == "/v1/ack/frame":
		return PermissionMessageAck
	case strings.HasPrefix(path, "/v1/consumers/register"):
		return PermissionConsumerRegister
	case strings.HasPrefix(path, "/v1/consumers"):
		if isMutation(method) {
			if strings.HasSuffix(path, "/ban") {
				return PermissionConsumerBan
			}
			if strings.HasSuffix(path, "/unban") {
				return PermissionConsumerUnban
			}
			return PermissionConsumerControl
		}
		return PermissionConsumerRead
	case strings.HasPrefix(path, "/v1/poll"):
		return PermissionConsumerPoll
	case strings.HasPrefix(path, "/v1/messages/"):
		if strings.HasSuffix(path, "/ack") {
			return PermissionMessageAck
		}
		if strings.HasSuffix(path, "/nack") || strings.HasSuffix(path, "/reject") {
			return PermissionMessageNack
		}
		return PermissionMessageRead
	case strings.HasPrefix(path, "/v1/sagas"):
		if isMutation(method) {
			return PermissionTaskWrite
		}
		return PermissionTaskRead
	case strings.HasPrefix(path, "/v1/tasks/submit"):
		return PermissionTaskWrite
	case strings.HasPrefix(path, "/v1/tasks"):
		if isMutation(method) {
			return PermissionTaskWrite
		}
		return PermissionTaskRead
	case strings.HasPrefix(path, "/v1/request"):
		return PermissionPublish
	case strings.HasPrefix(path, "/v1/publish"):
		return PermissionPublish
	case strings.HasPrefix(path, "/v1/streams/") || path == "/v1/streams":
		if strings.HasSuffix(path, "/commit") {
			return PermissionStreamCommit
		}
		if isMutation(method) {
			return PermissionStreamWrite
		}
		return PermissionStreamRead
	case path == "/routes":
		if method == http.MethodPost {
			return PermissionRouteWrite
		}
		return PermissionRouteRead
	case path == "/topics":
		if method == http.MethodPost {
			return PermissionTopicWrite
		}
		return PermissionTopicRead
	case path == "/queues":
		if method == http.MethodPost {
			return PermissionQueueWrite
		}
		return PermissionQueueRead
	case path == "/streams":
		if method == http.MethodPost {
			return PermissionStreamWrite
		}
		return PermissionStreamRead
	case path == "/dlq" || strings.HasPrefix(path, "/dlq/"):
		if strings.HasSuffix(path, "/replay") {
			return PermissionDLQReplay
		}
		return PermissionDLQRead
	case strings.HasPrefix(path, "/messages/"):
		if strings.HasSuffix(path, "/ack") {
			return PermissionMessageAck
		}
		if strings.HasSuffix(path, "/nack") {
			return PermissionMessageNack
		}
		return PermissionMessageRead
	case strings.HasPrefix(path, "/queues/"):
		if strings.HasSuffix(path, "/pause") || strings.HasSuffix(path, "/resume") {
			return PermissionQueueControl
		}
		if strings.HasSuffix(path, "/purge") {
			return PermissionQueuePurge
		}
		if strings.HasSuffix(path, "/pull") {
			return PermissionQueueRead
		}
		return PermissionQueueRead
	case strings.HasPrefix(path, "/streams/"):
		if strings.HasSuffix(path, "/commit") {
			return PermissionStreamCommit
		}
		return PermissionStreamRead
	default:
		return PermissionBrokerRead
	}
}

func (a AdminServer) clusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := map[string]any{
		"mode":    string(a.Broker.cfg.Mode),
		"node_id": a.Broker.cfg.NodeID,
	}
	if a.Broker.Cluster() != nil {
		resp["nodes"] = a.Broker.Cluster().Nodes()
		resp["assignments"] = a.Broker.Cluster().Assignments()
	}
	writeBrokerJSON(w, resp)
}

