package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/oarkflow/broker/cluster"
	"github.com/oarkflow/broker/console"
)

type AdminServer struct {
	Broker      *Broker
	Distributed *DistributedBroker

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
	mux.HandleFunc("/dlq/dashboard", a.wrap(a.dlqDashboard))
	mux.HandleFunc("/dlq/", a.wrap(a.dlqQueue))
	mux.HandleFunc("/messages/", a.wrap(a.message))
	mux.HandleFunc("/ack/batch", a.wrap(a.ackBatch))
	mux.HandleFunc("/move", a.wrap(a.move))
	mux.HandleFunc("/notifications", a.wrap(a.notifications))
	mux.HandleFunc("/events", a.wrap(a.events))
	mux.HandleFunc("/routes", a.wrap(a.routes))
	mux.HandleFunc("/cluster/status", a.wrap(a.clusterStatus))
	mux.HandleFunc("/cluster/upgrade-plan", a.wrap(a.clusterUpgradePlan))
	mux.HandleFunc(internalPlacementSignalsPath, a.wrap(a.internalPlacementSignals))
	mux.HandleFunc(internalPartitionProposalPath, a.wrap(a.internalPartitionPropose))
	mux.HandleFunc(internalPartitionPreparePath, a.wrap(a.internalPartitionPrepare))
	mux.HandleFunc(internalPartitionOutcomePath, a.wrap(a.internalPartitionOutcome))
	mux.HandleFunc(internalPartitionStagedPath, a.wrap(a.internalPartitionStaged))
	mux.HandleFunc(internalPartitionResultPath, a.wrap(a.internalPartitionResult))
	mux.HandleFunc(internalPartitionPurgeResultPath, a.wrap(a.internalPartitionPurgeResult))
	mux.HandleFunc(internalPartitionLagPath, a.wrap(a.internalPartitionLag))
	mux.HandleFunc(internalPartitionQueueListPath, a.wrap(a.internalPartitionQueueList))
	mux.HandleFunc(internalPartitionQueueControlPath, a.wrap(a.internalPartitionQueueControl))
	mux.HandleFunc("/cluster/transactions", a.wrap(a.clusterTransaction))
	mux.HandleFunc("/cluster/transactions/prune", a.wrap(a.clusterTransaction))
	mux.HandleFunc("/cluster/leadership", a.wrap(a.clusterLeadership))
	mux.HandleFunc("/cluster/nodes", a.wrap(a.clusterNodes))
	mux.HandleFunc("/cluster/nodes/", a.wrap(a.clusterNode))
	mux.HandleFunc("/cluster/metadata/nodes", a.wrap(a.clusterMetadataNodes))
	mux.HandleFunc("/cluster/metadata/nodes/", a.wrap(a.clusterMetadataNode))
	mux.HandleFunc("/cluster/metadata/schemas", a.wrap(a.clusterMetadataSchemas))
	mux.HandleFunc("/cluster/metadata/schemas/", a.wrap(a.clusterMetadataSchema))
	mux.HandleFunc("/cluster/metadata/acls", a.wrap(a.clusterMetadataACLs))
	mux.HandleFunc("/cluster/metadata/acls/", a.wrap(a.clusterMetadataACL))
	mux.HandleFunc("/cluster/partitions", a.wrap(a.clusterPartitions))
	mux.HandleFunc("/cluster/partitions/", a.wrap(a.clusterPartition))
	// The console is mounted behind wrap, so it requires the same bearer token and
	// passes the same ACL checks as every other admin route. It is a static app and
	// reads no broker state server-side; all data is fetched by the browser from the
	// endpoints above using the operator's own credentials.
	mux.Handle("/console/", http.StripPrefix("/console", a.wrap(console.Handler().ServeHTTP)))
	// The bare root redirected nowhere, so typing the host gave a bare 404 with no
	// hint that a console existed. This is a static pointer, not broker state, so
	// it is deliberately outside wrap: an unauthenticated visitor is sent to the
	// sign-in gate rather than to a 401.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Location", "/console/")
		w.WriteHeader(http.StatusFound)
	})
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
			// Keep a non-nil, empty pool on load/parse errors. A nil ClientCAs
			// can select system roots for client-certificate verification, which
			// would silently broaden trust after a misconfiguration.
			pool := x509.NewCertPool()
			pem, err := os.ReadFile(a.ClientCAFile)
			if err != nil {
				log.Printf("[http] unable to read client CA file %q; client certificate verification will reject all clients: %v", a.ClientCAFile, err)
			} else if !pool.AppendCertsFromPEM(pem) {
				log.Printf("[http] client CA file %q contains no valid certificates; client certificate verification will reject all clients", a.ClientCAFile)
			}
			srv.TLSConfig.ClientCAs = pool
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
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			next(w, r)
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
			WriteBrokerError(w, ErrUnauthenticated)
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

var insecureDevWarnedAt atomic.Int64

// warnInsecureDevOnce logs the dev-mode warning at most once a minute. Logging it
// on every request cost a write syscall per call and flooded the log.
func warnInsecureDevOnce() {
	now := time.Now().UnixNano()
	last := insecureDevWarnedAt.Load()
	if last != 0 && now-last < int64(time.Minute) {
		return
	}
	if insecureDevWarnedAt.CompareAndSwap(last, now) {
		log.Printf("[SECURITY WARNING] InsecureDev mode is active - all requests authenticated as admin. Do NOT use in production!")
	}
}

func (a AdminServer) authenticate(r *http.Request) (AdminPrincipal, bool) {
	if a.InsecureDev {
		warnInsecureDevOnce()
		return AdminPrincipal{Name: "insecure-dev", Permissions: []string{PermissionAdminAll}}, true
	}
	acl := a.effectiveACL()
	var cert *x509.Certificate
	if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
		cert = r.TLS.VerifiedChains[0][0]
	}
	return authenticateAdminCredential(acl, bearerToken(r), cert)
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
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	if a.Broker.IsStoreFenced() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	if check := a.Broker.checkStore(r.Context()); check.Status == "fail" {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	if a.Distributed != nil {
		if err := a.Distributed.PartitionReadiness(r.Context()); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
	}
	_, _ = w.Write([]byte("ready\n"))
}
func (a AdminServer) stats(w http.ResponseWriter, r *http.Request) {
	st, err := a.Broker.store.Stats(r.Context())
	if hasRestrictedTenantScope(r.Context()) {
		// WAL group-commit timing and batch counters are process-wide. Returning
		// them here would expose activity from other tenants through /stats.
		st.WALGroupCommit = WALGroupCommitStats{}
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
	if rejectScopedControlPlane(w, r, "global topic configuration") {
		return
	}
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
	if rejectScopedControlPlane(w, r, "global queue configuration") {
		return
	}
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
	if len(parts) == 3 && parts[1] == "assigned" && parts[2] == "messages" {
		if r.Method != http.MethodGet {
			methodBroker(w)
			return
		}
		if rejectScopedControlPlane(w, r, "assigned queue listings") {
			return
		}
		if a.Distributed == nil {
			WriteBrokerError(w, ErrUnsupportedMode)
			return
		}
		report, err := a.Distributed.ListAssignedQueue(r.Context(), name, queryLimit(r, 100))
		if report == nil {
			WriteBrokerError(w, err)
			return
		}
		status := http.StatusOK
		if len(report.Failures) > 0 {
			status = http.StatusMultiStatus
		}
		writeBrokerJSONStatus(w, status, report)
		return
	}
	if len(parts) == 3 && parts[1] == "assigned" && (parts[2] == "pause" || parts[2] == "resume" || parts[2] == "purge") {
		if r.Method != http.MethodPost {
			methodBroker(w)
			return
		}
		if rejectScopedControlPlane(w, r, "assigned queue controls") {
			return
		}
		if a.Distributed == nil {
			WriteBrokerError(w, ErrUnsupportedMode)
			return
		}
		result, err := a.Distributed.ApplyAssignedQueueControl(r.Context(), name, parts[2])
		if result == nil {
			WriteBrokerError(w, err)
			return
		}
		status := http.StatusOK
		if len(result.Failures) > 0 {
			status = http.StatusMultiStatus
		}
		writeBrokerJSONStatus(w, status, result)
		return
	}
	if len(parts) == 4 && parts[1] == "partitions" {
		partition, parseErr := strconv.Atoi(parts[2])
		if parseErr != nil || partition < 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		if parts[3] != "messages" && rejectScopedControlPlane(w, r, "partition queue controls") {
			return
		}
		switch parts[3] {
		case "messages":
			if r.Method != http.MethodGet {
				methodBroker(w)
				return
			}
			messages, err := a.Broker.ListPartitionQueue(r.Context(), name, partition, queryLimit(r, 100))
			if err != nil {
				WriteBrokerError(w, err)
				return
			}
			writeBrokerJSON(w, messages)
		case "pause":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			writeBrokerErr(w, a.Broker.PausePartitionQueue(r.Context(), name, partition))
		case "resume":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			writeBrokerErr(w, a.Broker.ResumePartitionQueue(r.Context(), name, partition))
		case "purge":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			purged, err := a.Broker.PurgePartitionQueue(r.Context(), name, partition)
			if err != nil {
				WriteBrokerError(w, err)
				return
			}
			writeBrokerJSON(w, map[string]any{"ok": true, "purged": purged, "partition": partition})
		default:
			http.NotFound(w, r)
		}
		return
	}
	if len(parts) == 2 {
		switch parts[1] {
		case "messages":
			if a.Distributed != nil {
				assigned, err := a.Distributed.hasResourcePartitions(r.Context(), cluster.PartitionResourceQueue, name)
				if err != nil {
					WriteBrokerError(w, err)
					return
				}
				if assigned {
					WriteBrokerError(w, fmt.Errorf("%w: partitioned queue messages must be read through partition-specific consumers", ErrUnsupportedMode))
					return
				}
			}
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
				WriteBrokerError(w, err)
				return
			}
			writeBrokerJSON(w, msgs)
			return
		case "pause":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			if rejectScopedControlPlane(w, r, "global queue controls") {
				return
			}
			writeBrokerErr(w, a.Broker.PauseQueue(r.Context(), name))
			return
		case "resume":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			if rejectScopedControlPlane(w, r, "global queue controls") {
				return
			}
			writeBrokerErr(w, a.Broker.ResumeQueue(r.Context(), name))
			return
		case "purge":
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			if rejectScopedControlPlane(w, r, "global queue controls") {
				return
			}
			n, err := a.Broker.PurgeQueue(r.Context(), name)
			if err != nil {
				WriteBrokerError(w, err)
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
	if rejectScopedControlPlane(w, r, "global stream configuration") {
		return
	}
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
	if len(parts) == 4 && parts[1] == "partitions" && parts[3] == "lag" {
		partition, parseErr := strconv.Atoi(parts[2])
		if parseErr != nil || partition < 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		if r.Method != http.MethodGet {
			methodBroker(w)
			return
		}
		if rejectScopedControlPlane(w, r, "partition stream lag reports") {
			return
		}
		group := firstNonEmpty(r.URL.Query().Get("consumer_group"), r.URL.Query().Get("group"))
		report, err := a.Broker.PartitionStreamLag(r.Context(), name, partition, group)
		if err != nil {
			WriteBrokerError(w, err)
			return
		}
		writeBrokerJSON(w, report)
		return
	}
	if len(parts) == 2 && parts[1] == "lag" {
		if r.Method != http.MethodGet {
			methodBroker(w)
			return
		}
		if rejectScopedControlPlane(w, r, "global stream lag reports") {
			return
		}
		if a.Distributed != nil {
			report, err := a.Distributed.StreamLagAssigned(r.Context(), name, firstNonEmpty(r.URL.Query().Get("consumer_group"), r.URL.Query().Get("group")))
			if report != nil {
				status := http.StatusOK
				if len(report.Failures) > 0 || report.Error != "" {
					status = http.StatusMultiStatus
				}
				writeBrokerJSONStatus(w, status, report)
				return
			}
			if err != nil && !errors.Is(err, errNoAssignedStreamPartitions) {
				WriteBrokerError(w, err)
				return
			}
			if err == nil && report == nil {
				WriteBrokerError(w, ErrBrokerUnavailable)
				return
			}
		}
		report, err := a.Broker.StreamLagReport(r.Context(), name)
		if err != nil {
			WriteBrokerError(w, err)
			return
		}
		writeBrokerJSON(w, report)
		return
	}
	if len(parts) == 4 && parts[1] == "partitions" && parts[3] == "messages" {
		partition, parseErr := strconv.Atoi(parts[2])
		if parseErr != nil || partition < 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		if r.Method != http.MethodGet {
			methodBroker(w)
			return
		}
		if a.Distributed == nil {
			WriteBrokerError(w, ErrUnsupportedMode)
			return
		}
		afterRaw := r.URL.Query().Get("after_offset")
		if afterRaw == "" {
			afterRaw = r.URL.Query().Get("after")
		}
		after, err := strconv.ParseUint(afterRaw, 10, 64)
		if afterRaw != "" && err != nil {
			WriteBrokerError(w, errf(ErrInvalidMessage, "invalid stream partition offset"))
			return
		}
		messages, err := a.Distributed.ReadPartitionStream(r.Context(), name, partition, after, queryLimit(r, 100))
		if err != nil {
			WriteBrokerError(w, err)
			return
		}
		writeBrokerJSON(w, messages)
		return
	}
	if len(parts) == 4 && parts[1] == "partitions" && parts[3] == "commit-and-append" {
		partition, parseErr := strconv.Atoi(parts[2])
		if parseErr != nil || partition < 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		if r.Method != http.MethodPost {
			methodBroker(w)
			return
		}
		var req struct {
			ConsumerGroup string        `json:"consumer_group"`
			Offset        uint64        `json:"offset"`
			TenantID      string        `json:"tenant_id,omitempty"`
			TransactionID string        `json:"transaction_id,omitempty"`
			Entries       []AppendEntry `json:"entries"`
		}
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		if len(req.Entries) > 0 {
			acl := a.effectiveACL()
			principal := principalFromContext(r.Context())
			if !acl.Authorize(principal, PermissionPublish) {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		commit := OffsetCommit{Stream: name, ConsumerGroup: req.ConsumerGroup, Offset: req.Offset, TenantID: req.TenantID, TransactionID: req.TransactionID, Partition: partition, Partitioned: true}
		messages, err := a.Broker.CommitOffsetAndAppend(r.Context(), commit, req.Entries)
		if err != nil {
			WriteBrokerError(w, err)
			return
		}
		writeBrokerJSON(w, messages)
		return
	}
	if len(parts) == 4 && parts[1] == "partitions" && (parts[3] == "commit" || parts[3] == "offset") {
		partition, parseErr := strconv.Atoi(parts[2])
		if parseErr != nil || partition < 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		if parts[3] == "commit" {
			if r.Method != http.MethodPost {
				methodBroker(w)
				return
			}
			var req OffsetCommit
			if json.NewDecoder(r.Body).Decode(&req) != nil {
				http.Error(w, "invalid json", 400)
				return
			}
			req.Stream, req.Partition, req.Partitioned = name, partition, true
			writeBrokerErr(w, a.Broker.CommitOffset(r.Context(), req))
			return
		}
		if r.Method != http.MethodGet {
			methodBroker(w)
			return
		}
		group := firstNonEmpty(r.URL.Query().Get("group"), r.URL.Query().Get("consumer_group"))
		off, err := a.Broker.ConsumerOffset(r.Context(), OffsetCommit{Stream: name, ConsumerGroup: group, Partition: partition, Partitioned: true})
		if err != nil {
			WriteBrokerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"stream": name, "group": group, "partition": partition, "offset": off})
		return
	}
	if len(parts) == 1 || (len(parts) == 2 && parts[1] == "messages") {
		afterRaw := firstNonEmpty(r.URL.Query().Get("after"), r.URL.Query().Get("after_offset"))
		after, _ := strconv.ParseUint(afterRaw, 10, 64)
		group := firstNonEmpty(r.URL.Query().Get("group"), r.URL.Query().Get("consumer_group"))
		msgs, err := a.Broker.ReadStream(r.Context(), StreamReadRequest{Stream: name, ConsumerGroup: group, AfterOffset: after, Limit: queryLimit(r, 100)})
		if err != nil {
			WriteBrokerError(w, err)
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
			WriteBrokerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"stream": name, "group": group, "offset": off})
		return
	}
	http.NotFound(w, r)
}
func (a AdminServer) dlq(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "global dead-letter queues") {
		return
	}
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
	if parts[0] == "dashboard" {
		a.dlqDashboard(w, r)
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
	if len(parts) == 2 {
		switch parts[1] {
		case "replay":
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
				WriteBrokerError(w, err)
				return
			}
			writeBrokerJSON(w, res)
			return
		case "replay-all":
			a.dlqReplayAll(w, r, name)
			return
		case "errors":
			a.dlqErrors(w, r, name)
			return
		case "archive":
			a.dlqArchive(w, r, name)
			return
		case "export":
			a.dlqExport(w, r, name)
			return
		}
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
		WriteBrokerError(w, err)
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
	results, publishErrs, firstErr := pub.publishItemsAligned(r.Context(), batchItems)
	errs := make([]string, len(publishErrs))
	failed := false
	for i, err := range publishErrs {
		if err != nil {
			failed = true
			errs[i] = err.Error()
		}
	}
	if firstErr != nil && !failed {
		WriteBrokerError(w, firstErr)
		return
	}
	if failed {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMultiStatus)
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
		WriteBrokerError(w, err)
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
	writeBrokerJSONStatus(w, http.StatusOK, v)
}
func writeBrokerJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	if status != http.StatusOK {
		w.WriteHeader(status)
	}
	_ = json.NewEncoder(w).Encode(v)
}
func writeSSE(w http.ResponseWriter, n Notification) {
	b, _ := json.Marshal(n)
	_, _ = fmt.Fprintf(w, "event: %s\nid: %s\ndata: %s\n\n", n.Type, n.ID, b)
}
func writeBrokerErr(w http.ResponseWriter, err error) {
	if err != nil {
		WriteBrokerError(w, err)
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

func (a AdminServer) clusterPartitions(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster partitions") {
		return
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	cq := a.Distributed.ControllerQuorum()
	switch r.Method {
	case http.MethodGet:
		partitions, err := cq.ListPartitionsLinearizable(r.Context())
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, partitions)
	case http.MethodPost:
		var body struct {
			Topic        string                        `json:"topic,omitempty"`
			Name         string                        `json:"name,omitempty"`
			ResourceKind cluster.PartitionResourceKind `json:"resource_kind,omitempty"`
			Partition    int                           `json:"partition"`
			Replicas     []string                      `json:"replicas"`
			Leader       string                        `json:"leader"`
			AutoAssign   bool                          `json:"auto_assign,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteBrokerError(w, err)
			return
		}
		name := firstNonEmpty(body.Name, body.Topic)
		kind := body.ResourceKind
		if kind == "" {
			kind = cluster.PartitionResourceTopic
		}
		var err error
		var partition *cluster.PartitionMetadata
		if body.AutoAssign {
			if len(body.Replicas) != 0 || body.Leader != "" {
				WriteBrokerError(w, fmt.Errorf("auto_assign cannot be combined with explicit replicas or leader"))
				return
			}
			partition, err = a.Distributed.AssignResourcePartitionBalanced(r.Context(), kind, name, body.Partition)
		} else {
			err = a.Distributed.AssignResourcePartition(r.Context(), kind, name, body.Partition, body.Replicas, body.Leader)
			if err == nil {
				partition, err = cq.GetResourcePartitionLinearizable(r.Context(), kind, name, body.Partition)
			}
		}
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, partition)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a AdminServer) clusterPartition(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster partitions") {
		return
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/cluster/partitions/"), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] == "" {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	partitionID, err := strconv.Atoi(parts[1])
	if err != nil || partitionID < 0 {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	action := ""
	kind := cluster.PartitionResourceKind(r.URL.Query().Get("resource_kind"))
	if kind == "" {
		kind = cluster.PartitionResourceTopic
	}
	if kind != cluster.PartitionResourceTopic && kind != cluster.PartitionResourceStream && kind != cluster.PartitionResourceQueue {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	if len(parts) == 3 {
		action = parts[2]
	}
	cq := a.Distributed.ControllerQuorum()
	switch {
	case r.Method == http.MethodGet && action == "":
		partition, err := cq.GetResourcePartitionLinearizable(r.Context(), kind, parts[0], partitionID)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, partition)
	case r.Method == http.MethodPut && action == "isr":
		var body struct {
			ISR []string `json:"isr"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteBrokerError(w, err)
			return
		}
		if _, err := a.Distributed.UpdateResourcePartitionISR(r.Context(), kind, parts[0], partitionID, body.ISR); err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"status": "updated", "name": parts[0], "resource_kind": kind, "partition": partitionID})
	case r.Method == http.MethodGet && action == "isr":
		partitionGroupID := (cluster.PartitionGroupID{Topic: parts[0], Partition: partitionID, Kind: kind}).Canonical()
		group, ok := a.Distributed.PartitionGroup(partitionGroupID)
		if !ok {
			WriteBrokerError(w, ErrBrokerUnavailable)
			return
		}
		if !group.IsLeader() {
			WriteBrokerError(w, ErrNotLeader)
			return
		}
		writeBrokerJSON(w, map[string]any{
			"id":           partitionGroupID,
			"leader_id":    group.LeaderID(),
			"leader_epoch": group.LeaderEpoch(),
			"isr":          group.ISR(),
			"replicas":     group.Replicas(),
		})
	case r.Method == http.MethodGet && action == "route":
		route, err := a.Distributed.PartitionResourceRoute(r.Context(), kind, parts[0], partitionID)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, route)
	case r.Method == http.MethodGet && action == "reassignment-plan":
		replicas := strings.Split(r.URL.Query().Get("replicas"), ",")
		if len(replicas) == 1 && replicas[0] == "" {
			replicas = nil
		}
		for i := range replicas {
			replicas[i] = strings.TrimSpace(replicas[i])
		}
		plan, err := a.Distributed.PlanResourcePartitionReassignment(r.Context(), kind, parts[0], partitionID, replicas, r.URL.Query().Get("leader"))
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, plan)
	case r.Method == http.MethodPost && action == "reassign":
		var body struct {
			Replicas []string `json:"replicas"`
			Leader   string   `json:"leader,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteBrokerError(w, err)
			return
		}
		progress, err := a.Distributed.ReassignResourcePartition(r.Context(), kind, parts[0], partitionID, body.Replicas, body.Leader)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, progress)
	case r.Method == http.MethodPost && action == "cancel-reassignment":
		progress, err := a.Distributed.CancelResourcePartitionReassignment(r.Context(), kind, parts[0], partitionID)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, progress)
	case r.Method == http.MethodGet && action == "placement-plan":
		plan, err := a.Distributed.PlanResourcePartitionPlacement(r.Context(), kind, parts[0], partitionID)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, plan)
	case r.Method == http.MethodPost && action == "epoch":
		var body struct {
			Leader string `json:"leader,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteBrokerError(w, err)
			return
		}
		var partition *cluster.PartitionMetadata
		if body.Leader != "" {
			partition, err = a.Distributed.TransferResourcePartitionLeader(r.Context(), kind, parts[0], partitionID, body.Leader)
		} else {
			err = cluster.ErrPartitionTransferRequired
		}
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, partition)
	default:
		w.Header().Set("Allow", "GET, PUT, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a AdminServer) writeControllerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, cluster.ErrNotLeader), errors.Is(err, cluster.ErrLeaderLost):
		var retry *RetryError
		if errors.As(err, &retry) && retry.Class == RetryNotLeader {
			// Preserve a resource-specific redirect. Replacing it with the
			// controller leader can route partition operations to the wrong group.
			break
		}
		if a.Distributed != nil {
			err = NotLeaderError(a.Distributed.controllerLeaderClientAddress(), a.Distributed.raftTerm())
		} else {
			err = ErrNotLeader
		}
	case errors.Is(err, cluster.ErrQuorumLost), errors.Is(err, cluster.ErrQuorumNotMet), errors.Is(err, cluster.ErrPartitioned):
		err = fmt.Errorf("%w: %v", ErrQuorumUnavailable, err)
	case errors.Is(err, cluster.ErrNodeNotFound):
		err = fmt.Errorf("%w: %v", ErrClusterNodeNotFound, err)
	case errors.Is(err, cluster.ErrClosed):
		err = fmt.Errorf("%w: %v", ErrBrokerUnavailable, err)
	case errors.Is(err, cluster.ErrConfChangePending):
		err = fmt.Errorf("%w: %v", ErrClusterChangePending, err)
	case errors.Is(err, cluster.ErrStaleEpoch), errors.Is(err, cluster.ErrTermMismatch):
		err = fmt.Errorf("%w: %v", ErrStaleEpoch, err)
	case errors.Is(err, cluster.ErrUnknownReplica):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrPartitionNotFound):
		err = fmt.Errorf("%w: %v", ErrPartitionNotFound, err)
	case errors.Is(err, cluster.ErrSchemaNotFound):
		err = fmt.Errorf("%w: %v", ErrSchemaNotFound, err)
	case errors.Is(err, cluster.ErrInvalidSchema):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrACLPrincipalNotFound):
		err = fmt.Errorf("%w: %v", ErrClusterACLPrincipalNotFound, err)
	case errors.Is(err, cluster.ErrTransactionNotFound):
		err = fmt.Errorf("%w: %v", ErrClusterTransactionNotFound, err)
	case errors.Is(err, cluster.ErrInvalidTransaction):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidACLPrincipal):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrNodeInUse):
		err = fmt.Errorf("%w: %v", ErrClusterNodeInUse, err)
	case errors.Is(err, cluster.ErrNodeUnavailable):
		err = fmt.Errorf("%w: %v", ErrClusterNodeUnavailable, err)
	case errors.Is(err, cluster.ErrPartitionTransferRequired):
		err = fmt.Errorf("%w: %v", ErrPartitionTransferRequired, err)
	case errors.Is(err, cluster.ErrReplicaLagging):
		err = fmt.Errorf("%w: %v", ErrClusterNodeUnavailable, err)
	case errors.Is(err, cluster.ErrPartitionReassignmentRequired):
		err = fmt.Errorf("%w: %v", ErrPartitionReassignmentRequired, err)
	case errors.Is(err, cluster.ErrInvalidPartitionAssignment), errors.Is(err, cluster.ErrInvalidISR):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidNodeState):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidNodeClientAddress):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidNodeFailureDomain):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidNodeCapacityWeight):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, cluster.ErrInvalidNodeBrokerVersion):
		err = fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	case errors.Is(err, context.DeadlineExceeded):
		err = fmt.Errorf("%w: %v", ErrRequestTimeout, err)
	case errors.Is(err, context.Canceled):
		err = fmt.Errorf("%w: %v", ErrCancelled, err)
	}
	WriteBrokerError(w, err)
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
	case strings.HasPrefix(path, "/cluster/") && isMutation(method):
		return PermissionClusterWrite
	case strings.HasPrefix(path, "/cluster/"):
		return PermissionClusterRead
	case path == "/healthz" || path == "/readyz":
		return PermissionBrokerRead
	case path == "/stats":
		return PermissionStatsRead
	case path == "/metrics":
		return PermissionMetricsRead
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
		if strings.HasSuffix(path, "/commit-and-append") {
			return PermissionStreamCommit
		}
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
		if strings.HasSuffix(path, "/replay") || strings.HasSuffix(path, "/replay-all") || strings.HasSuffix(path, "/archive") {
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
	if rejectScopedControlPlane(w, r, "cluster topology") {
		return
	}
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
	if a.Distributed != nil && a.Distributed.raftCtrl != nil {
		resp["raft"] = distributedRaftStatus(a.Distributed)
		resp["raft_groups"] = distributedRaftGroupStatuses(a.Distributed)
		resp["local_partition_progress"] = a.Distributed.LivePartitionProgress()
	}
	if a.Distributed != nil && a.Distributed.ControllerQuorum() != nil {
		stats, err := a.Distributed.ControllerQuorum().StatsLinearizable(r.Context())
		if err != nil {
			resp["controller_metadata_read_error"] = err.Error()
		} else {
			resp["controller_metadata"] = stats
		}
	}
	writeBrokerJSON(w, resp)
}

// clusterTransaction returns one quorum-confirmed coordinator record. It is
// keyed by query parameter because transaction IDs may contain path separators.
func (a AdminServer) clusterTransaction(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster transactions") {
		return
	}
	if r.URL.Path == "/cluster/transactions/prune" {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
			WriteBrokerError(w, ErrUnsupportedMode)
			return
		}
		var body struct {
			Before time.Time `json:"before"`
			Limit  int       `json:"limit,omitempty"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		writeDecodeError := func(err error) {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				WriteBrokerError(w, err)
				return
			}
			WriteBrokerError(w, ErrInvalidMessage)
		}
		if err := decoder.Decode(&body); err != nil {
			writeDecodeError(err)
			return
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			writeDecodeError(err)
			return
		}
		if body.Before.IsZero() || body.Limit < 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		pruned, err := a.Distributed.ControllerQuorum().PruneResolvedTransactions(r.Context(), body.Before, body.Limit)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"pruned": pruned})
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	id := r.URL.Query().Get("id")
	cq := a.Distributed.ControllerQuorum()
	if id != "" {
		if len(id) > cluster.MaxTransactionIDBytes || !utf8.ValidString(id) {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		transaction, err := cq.GetTransactionLinearizable(r.Context(), id)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, transaction)
		return
	}
	limit := cluster.DefaultTransactionPageSize
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		limit = parsed
	}
	transactions, next, err := cq.ListTransactionsPageLinearizable(r.Context(), r.URL.Query().Get("after"), limit)
	if err != nil {
		if errors.Is(err, cluster.ErrInvalidTransaction) {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		a.writeControllerError(w, err)
		return
	}
	writeBrokerJSON(w, map[string]any{"transactions": transactions, "next_cursor": next})
}

func (a AdminServer) clusterUpgradePlan(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster upgrade planning") {
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	plan, err := a.Distributed.PlanClusterUpgrade(r.Context(), r.URL.Query().Get("target_version"))
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	writeBrokerJSON(w, plan)
}

// distributedRaftGroupStatuses exposes bounded local progress for application
// groups beside the controller group's status. Group IDs are returned in
// stable order so operators can diff snapshots without map-order noise.
func distributedRaftGroupStatuses(db *DistributedBroker) []map[string]any {
	groupIDs := db.RaftGroupIDs()
	groups := make([]map[string]any, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		controller, ok := db.RaftGroup(groupID)
		if !ok {
			continue
		}
		status := controller.Status()
		commitIndex := status.HardState.GetCommit()
		memberIDs := make(map[uint64]struct{})
		voters := make(map[uint64]struct{})
		for _, voterGroup := range status.Config.Voters {
			for id := range voterGroup {
				memberIDs[id] = struct{}{}
				voters[id] = struct{}{}
			}
		}
		for id := range status.Config.Learners {
			memberIDs[id] = struct{}{}
		}
		for id := range status.Config.LearnersNext {
			memberIDs[id] = struct{}{}
		}
		jointPending := status.Config.AutoLeave || (len(status.Config.Voters) > 1 && len(status.Config.Voters[1]) != 0)
		ids := make([]uint64, 0, len(memberIDs))
		for id := range memberIDs {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		members := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			address, _ := controller.PeerAddress(id)
			member := map[string]any{"peer_id": id, "address": address}
			_, member["voter"] = voters[id]
			_, member["learner"] = status.Config.Learners[id]
			_, member["learner_next"] = status.Config.LearnersNext[id]
			if progress, ok := status.Progress[id]; ok {
				lag := uint64(0)
				if commitIndex > progress.Match {
					lag = commitIndex - progress.Match
				}
				member["match_index"] = progress.Match
				member["next_index"] = progress.Next
				member["lag"] = lag
			}
			members = append(members, member)
		}
		groupInfo := map[string]any{
			"group_id":      groupID,
			"leader_id":     controller.LeaderID(),
			"is_leader":     controller.IsLeader(),
			"term":          status.HardState.GetTerm(),
			"commit_index":  commitIndex,
			"applied_index": status.Applied,
			"joint_pending": jointPending,
			"members":       members,
		}
		if strings.HasPrefix(groupID, "partition/") {
			var partitionID cluster.PartitionGroupID
			if err := partitionID.UnmarshalText([]byte(strings.TrimPrefix(groupID, "partition/"))); err == nil {
				if partitionGroup, ok := db.PartitionGroup(partitionID); ok {
					waits, failures, duration := partitionGroup.MinISRWaitStats()
					groupInfo["replication_factor"] = partitionGroup.ReplicationFactor()
					groupInfo["minimum_isr"] = partitionGroup.MinimumISR()
					groupInfo["live_isr"] = partitionGroup.ISR()
					groupInfo["min_isr_wait_metrics_scope"] = "this_node"
					groupInfo["min_isr_wait_count"] = waits
					groupInfo["min_isr_wait_failures"] = failures
					groupInfo["min_isr_wait_duration_ms"] = float64(duration) / float64(time.Millisecond)
				}
			}
		}
		groups = append(groups, groupInfo)
	}
	return groups
}

func distributedRaftStatus(db *DistributedBroker) map[string]any {
	status := db.raftCtrl.Status()
	commitIndex := status.HardState.GetCommit()
	term := status.HardState.GetTerm()

	voterGroups := make([][]uint64, 0, len(status.Config.Voters))
	voterIDs := make(map[uint64]struct{})
	for _, group := range status.Config.Voters {
		ids := make([]uint64, 0, len(group))
		for id := range group {
			ids = append(ids, id)
			voterIDs[id] = struct{}{}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if len(ids) > 0 {
			voterGroups = append(voterGroups, ids)
		}
	}
	learnerIDs := sortedRaftIDs(status.Config.Learners)
	learnerNextIDs := sortedRaftIDs(status.Config.LearnersNext)
	memberIDs := make(map[uint64]struct{})
	for id := range voterIDs {
		memberIDs[id] = struct{}{}
	}
	for _, id := range learnerIDs {
		memberIDs[id] = struct{}{}
	}
	for _, id := range learnerNextIDs {
		memberIDs[id] = struct{}{}
	}
	ids := make([]uint64, 0, len(memberIDs))
	for id := range memberIDs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	learners := make(map[uint64]struct{}, len(learnerIDs))
	for _, id := range learnerIDs {
		learners[id] = struct{}{}
	}
	learnersNext := make(map[uint64]struct{}, len(learnerNextIDs))
	for _, id := range learnerNextIDs {
		learnersNext[id] = struct{}{}
	}
	members := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		address, _ := db.raftCtrl.PeerAddress(id)
		member := map[string]any{"peer_id": id, "address": address}
		_, member["voter"] = voterIDs[id]
		_, member["learner"] = learners[id]
		_, member["learner_next"] = learnersNext[id]
		if progress, ok := status.Progress[id]; ok {
			lag := uint64(0)
			if commitIndex > progress.Match {
				lag = commitIndex - progress.Match
			}
			member["match_index"] = progress.Match
			member["next_index"] = progress.Next
			member["lag"] = lag
		}
		members = append(members, member)
	}
	return map[string]any{
		"state":         db.RaftState(),
		"leader":        db.Leader(),
		"leader_id":     db.raftCtrl.LeaderID(),
		"is_leader":     db.IsLeader(),
		"term":          term,
		"commit_index":  commitIndex,
		"applied_index": status.Applied,
		"auto_leave":    status.Config.AutoLeave,
		"voter_groups":  voterGroups,
		"learners":      learnerIDs,
		"learners_next": learnerNextIDs,
		"members":       members,
	}
}

func sortedRaftIDs(ids map[uint64]struct{}) []uint64 {
	result := make([]uint64, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func (a AdminServer) clusterNodes(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster membership") {
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Address string `json:"address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteBrokerError(w, err)
		return
	}
	peerID, err := a.Distributed.AddRaftLearner(r.Context(), body.Address)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	role := a.Distributed.RaftPeerRole(peerID)
	result := "learner_added"
	if role != "learner" {
		result = "already_member"
	}
	writeBrokerJSON(w, map[string]any{"status": result, "peer_id": peerID, "role": role})
}

// clusterMetadataNodes manages the replicated logical node registry. These
// records are controller metadata and are distinct from Raft voter membership.
func (a AdminServer) clusterMetadataNodes(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster metadata nodes") {
		return
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	cq := a.Distributed.ControllerQuorum()
	switch r.Method {
	case http.MethodGet:
		nodes, err := cq.GetNodesLinearizable(r.Context())
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, nodes)
	case http.MethodPost:
		var body struct {
			ID            string            `json:"id"`
			Address       string            `json:"address"`
			ClientAddress string            `json:"client_address,omitempty"`
			State         string            `json:"state,omitempty"`
			Metadata      map[string]string `json:"metadata,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteBrokerError(w, err)
			return
		}
		id, err := sanitizeIdentifier(body.ID)
		if err != nil || strings.TrimSpace(body.Address) == "" || len(body.Address) > 1024 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		node := &cluster.NodeInfo{
			ID: id, Address: strings.TrimSpace(body.Address), ClientAddress: strings.TrimSpace(body.ClientAddress),
			State: cluster.NodeState(strings.TrimSpace(body.State)), Metadata: body.Metadata,
		}
		if err := cq.RegisterNode(r.Context(), node); err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"status": "registered", "id": id})
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a AdminServer) clusterMetadataNode(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster metadata nodes") {
		return
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	if r.Method != http.MethodDelete {
		w.Header().Set("Allow", http.MethodDelete)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id, err := sanitizeIdentifier(strings.TrimPrefix(r.URL.Path, "/cluster/metadata/nodes/"))
	if err != nil {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	cq := a.Distributed.ControllerQuorum()
	if err := cq.DeregisterNode(r.Context(), id); err != nil {
		a.writeControllerError(w, err)
		return
	}
	writeBrokerJSON(w, map[string]any{"status": "deregistered", "id": id})
}

func (a AdminServer) clusterMetadataSchemas(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster metadata schemas") {
		return
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	cq := a.Distributed.ControllerQuorum()
	switch r.Method {
	case http.MethodGet:
		name := r.URL.Query().Get("name")
		if name != "" {
			validated, err := sanitizeIdentifier(name)
			if err != nil {
				WriteBrokerError(w, ErrInvalidMessage)
				return
			}
			name = validated
		}
		schemas, err := cq.ListSchemasLinearizable(r.Context(), name)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, schemas)
	case http.MethodPost:
		var definition SchemaDefinition
		if err := json.NewDecoder(r.Body).Decode(&definition); err != nil {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		if err := cq.RegisterSchema(r.Context(), cluster.SchemaDefinition{
			Name: definition.Name, Version: definition.Version, ContentType: definition.ContentType,
			Required: definition.Required, AllowUnknown: definition.AllowUnknown,
			CompatibleWith: definition.CompatibleWith, Deprecated: definition.Deprecated,
			Description: definition.Description,
		}); err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"status": "registered", "name": definition.Name, "version": firstNonEmpty(definition.Version, "v1")})
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a AdminServer) clusterMetadataSchema(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster metadata schemas") {
		return
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/cluster/metadata/schemas/"), "/")
	if len(parts) < 2 || len(parts) > 3 {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	name, err := sanitizeIdentifier(parts[0])
	if err != nil {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	version, err := sanitizeIdentifier(parts[1])
	if err != nil {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	cq := a.Distributed.ControllerQuorum()
	if len(parts) == 3 && parts[2] == "deprecate" && r.Method == http.MethodPost {
		if err := cq.DeprecateSchema(r.Context(), name, version); err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"status": "deprecated", "name": name, "version": version})
		return
	}
	if len(parts) != 2 || r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	definition, err := cq.GetSchemaLinearizable(r.Context(), name, version)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	writeBrokerJSON(w, definition)
}

type clusterACLPrincipalView struct {
	Name                  string    `json:"name"`
	CertificateIdentities []string  `json:"certificate_identities,omitempty"`
	Permissions           []string  `json:"permissions,omitempty"`
	Tenants               []string  `json:"tenants,omitempty"`
	RequestsPerSecond     int       `json:"requests_per_second,omitempty"`
	Burst                 int       `json:"burst,omitempty"`
	MaxConcurrent         int       `json:"max_concurrent,omitempty"`
	ExpiresAt             time.Time `json:"expires_at,omitempty"`
	Revoked               bool      `json:"revoked,omitempty"`
	UpdatedAt             time.Time `json:"updated_at"`
}

func aclPrincipalView(principal cluster.ACLPrincipal) clusterACLPrincipalView {
	return clusterACLPrincipalView{
		Name: principal.Name, CertificateIdentities: principal.CertificateIdentities,
		Permissions: principal.Permissions, Tenants: principal.Tenants,
		RequestsPerSecond: principal.RequestsPerSecond, Burst: principal.Burst,
		MaxConcurrent: principal.MaxConcurrent, ExpiresAt: principal.ExpiresAt,
		Revoked: principal.Revoked, UpdatedAt: principal.UpdatedAt,
	}
}

func (a AdminServer) clusterMetadataACLs(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster metadata ACLs") {
		return
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	cq := a.Distributed.ControllerQuorum()
	switch r.Method {
	case http.MethodGet:
		principals, err := cq.ListACLPrincipalsLinearizable(r.Context())
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		views := make([]clusterACLPrincipalView, 0, len(principals))
		for _, principal := range principals {
			views = append(views, aclPrincipalView(principal))
		}
		writeBrokerJSON(w, views)
	case http.MethodPost:
		var body struct {
			Name                  string    `json:"name"`
			Token                 string    `json:"token,omitempty"`
			CertificateIdentities []string  `json:"certificate_identities,omitempty"`
			Permissions           []string  `json:"permissions,omitempty"`
			Tenants               []string  `json:"tenants,omitempty"`
			RequestsPerSecond     int       `json:"requests_per_second,omitempty"`
			Burst                 int       `json:"burst,omitempty"`
			MaxConcurrent         int       `json:"max_concurrent,omitempty"`
			ExpiresAt             time.Time `json:"expires_at,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		principal := cluster.ACLPrincipal{
			Name: body.Name, CertificateIdentities: body.CertificateIdentities,
			Permissions: body.Permissions, Tenants: body.Tenants,
			RequestsPerSecond: body.RequestsPerSecond, Burst: body.Burst,
			MaxConcurrent: body.MaxConcurrent, ExpiresAt: body.ExpiresAt,
		}
		if err := cq.PutACLPrincipal(r.Context(), principal, body.Token); err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"status": "stored", "name": body.Name})
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a AdminServer) clusterMetadataACL(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster metadata ACLs") {
		return
	}
	if a.Distributed == nil || a.Distributed.ControllerQuorum() == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/cluster/metadata/acls/"), "/")
	if len(parts) == 0 || len(parts) > 2 {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	name, err := sanitizeIdentifier(parts[0])
	if err != nil {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	cq := a.Distributed.ControllerQuorum()
	if len(parts) == 2 && parts[1] == "revoke" && r.Method == http.MethodPost {
		if err := cq.RevokeACLPrincipal(r.Context(), name); err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"status": "revoked", "name": name})
		return
	}
	if len(parts) != 1 || r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	principal, err := cq.GetACLPrincipalLinearizable(r.Context(), name)
	if err != nil {
		a.writeControllerError(w, err)
		return
	}
	writeBrokerJSON(w, aclPrincipalView(principal))
}

func (a AdminServer) clusterNode(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster membership") {
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	idPath := strings.TrimPrefix(r.URL.Path, "/cluster/nodes/")
	parts := strings.Split(idPath, "/")
	if len(parts) == 2 && parts[1] == "drain" {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		peerID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || peerID == 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		node, err := a.Distributed.BeginRaftNodeDrain(r.Context(), peerID)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"status": "draining", "node": node})
		return
	}
	if len(parts) == 3 && parts[1] == "drain" && parts[2] == "complete" {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		peerID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || peerID == 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		node, err := a.Distributed.CompleteRaftNodeDrain(r.Context(), peerID)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"status": "left", "node": node})
		return
	}
	if len(parts) == 2 && parts[1] == "removal-plan" {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		peerID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || peerID == 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		plan, err := a.Distributed.PlanRaftNodeRemoval(r.Context(), peerID)
		if err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, plan)
		return
	}
	if len(parts) == 2 && parts[1] == "promote" {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		peerID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || peerID == 0 {
			WriteBrokerError(w, ErrInvalidMessage)
			return
		}
		if err := a.Distributed.PromoteRaftLearner(r.Context(), peerID); err != nil {
			a.writeControllerError(w, err)
			return
		}
		writeBrokerJSON(w, map[string]any{"status": "promoted", "peer_id": peerID, "role": "voter"})
		return
	}
	if len(parts) != 1 {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	if r.Method != http.MethodDelete {
		w.Header().Set("Allow", http.MethodDelete)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	idText := parts[0]
	peerID, err := strconv.ParseUint(idText, 10, 64)
	if err != nil || peerID == 0 {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	if err := a.Distributed.RemoveRaftNode(r.Context(), peerID); err != nil {
		a.writeControllerError(w, err)
		return
	}
	writeBrokerJSON(w, map[string]any{"status": "ok", "peer_id": peerID})
}

func (a AdminServer) clusterLeadership(w http.ResponseWriter, r *http.Request) {
	if rejectScopedControlPlane(w, r, "cluster leadership") {
		return
	}
	if a.Distributed == nil {
		WriteBrokerError(w, ErrUnsupportedMode)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		PeerID uint64 `json:"peer_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteBrokerError(w, err)
		return
	}
	if body.PeerID == 0 {
		WriteBrokerError(w, ErrInvalidMessage)
		return
	}
	if err := a.Distributed.TransferRaftLeadership(r.Context(), body.PeerID); err != nil {
		a.writeControllerError(w, err)
		return
	}
	writeBrokerJSON(w, map[string]any{"status": "transferred", "leader_id": body.PeerID})
}
