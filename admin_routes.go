package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"time"

	"github.com/oarkflow/broker/console"
	"github.com/oarkflow/fh"
)

// App builds the admin, data-plane and console HTTP application.
//
// Every route is registered with an explicit method and the permission it
// requires, so authorization is decided by the matched route rather than by
// re-parsing the request path.
func (a AdminServer) App() *fh.App {
	app := NewHTTPApp(HTTPServerOptions{
		ReadTimeout:       a.ReadTimeout,
		ReadHeaderTimeout: a.ReadHeaderTimeout,
		WriteTimeout:      a.WriteTimeout,
		IdleTimeout:       a.IdleTimeout,
		// The transport ceiling sits above the per-request limit so oversized
		// bodies are rejected by bodyLimit with the broker error envelope.
		MaxBodyBytes: a.maxBodyBytes() + 1<<20,
	}, fh.WithNotFoundHandler(plainNotFound), fh.WithMethodNotAllowedHandler(plainMethodNotAllowed))
	app.Use(a.responseHeaders, a.bodyLimit)

	// Unauthenticated probes and the console pointer.
	app.Get("/healthz", a.health)
	app.Get("/readyz", a.ready)
	app.Get("/", func(c fh.Ctx) error {
		// A static pointer, not broker state: an unauthenticated visitor is sent
		// to the console sign-in gate rather than to a 401.
		c.Set("Location", "/console/")
		return c.SendStatus(fh.StatusFound)
	})

	r := adminRouter{a: a, app: app}
	a.registerAdminRoutes(r)
	a.registerClusterRoutes(r)
	a.registerDataPlaneRoutes(r.group("/v1"))

	// The console is mounted behind authorize, so it requires the same bearer
	// token and passes the same ACL checks as every other admin route. It is a
	// static app and reads no broker state server-side.
	consoleHandler := console.Handler("path")
	// fh routes "/console/" to "/console", so the bare mount serves the shell;
	// its asset URLs are absolute.
	r.get("/console", PermissionBrokerRead, consoleHandler)
	r.get("/console/*path", PermissionBrokerRead, consoleHandler)
	return app
}

// adminRouter registers routes guarded by a per-route permission.
type adminRouter struct {
	a      AdminServer
	app    *fh.App
	prefix string
}

func (r adminRouter) group(prefix string) adminRouter {
	return adminRouter{a: r.a, app: r.app, prefix: r.prefix + prefix}
}

func (r adminRouter) handle(method, path, permission string, h fh.HandlerFunc) {
	r.app.Add(method, r.prefix+path, r.a.authorize(permission), h)
}

func (r adminRouter) get(path, permission string, h fh.HandlerFunc) {
	r.handle(fh.MethodGET, path, permission, h)
}

func (r adminRouter) post(path, permission string, h fh.HandlerFunc) {
	r.handle(fh.MethodPOST, path, permission, h)
}

func (r adminRouter) put(path, permission string, h fh.HandlerFunc) {
	r.handle(fh.MethodPUT, path, permission, h)
}

func (r adminRouter) delete(path, permission string, h fh.HandlerFunc) {
	r.handle(fh.MethodDELETE, path, permission, h)
}

// getPost registers the same handler for GET and POST, each with its own
// permission.
func (r adminRouter) getPost(path, getPermission, postPermission string, h fh.HandlerFunc) {
	r.get(path, getPermission, h)
	r.post(path, postPermission, h)
}

func (a AdminServer) registerAdminRoutes(r adminRouter) {
	r.get("/stats", PermissionStatsRead, a.stats)
	r.get("/metrics", PermissionMetricsRead, a.metrics)
	r.get("/lag", PermissionBrokerRead, a.lag)
	r.get("/audit", PermissionBrokerRead, a.audit)
	r.get("/notifications", PermissionNotification, a.notifications)
	r.get("/events", PermissionNotification, a.events)

	r.post("/publish", PermissionPublish, a.publish)
	r.post("/publish/batch", PermissionPublish, a.publishBatch)
	r.post("/ack/batch", PermissionMessageAck, a.ackBatch)
	r.post("/move", PermissionMessageMove, a.move)

	r.getPost("/topics", PermissionTopicRead, PermissionTopicWrite, a.topics)
	r.getPost("/routes", PermissionRouteRead, PermissionRouteWrite, a.routes)

	r.getPost("/queues", PermissionQueueRead, PermissionQueueWrite, a.queues)
	a.registerQueueRoutes(r.group("/queues/:queue"))

	r.getPost("/streams", PermissionStreamRead, PermissionStreamWrite, a.streams)
	a.registerStreamRoutes(r.group("/streams/:stream"), PermissionStreamRead)

	r.get("/dlq", PermissionDLQRead, a.dlq)
	r.get("/dlq/dashboard", PermissionDLQRead, a.dlqDashboard)
	a.registerDLQRoutes(r.group("/dlq/:queue"), PermissionDLQRead, PermissionDLQReplay)

	msg := r.group("/messages/:id")
	msg.get("", PermissionMessageRead, a.messageGet)
	msg.get("/history", PermissionMessageRead, a.messageHistory)
	msg.post("/ack", PermissionMessageAck, a.messageAck)
	msg.post("/nack", PermissionMessageNack, a.messageNack)
}

func (a AdminServer) registerQueueRoutes(q adminRouter) {
	q.get("/messages", PermissionQueueRead, a.queueMessages)
	q.get("/pull", PermissionQueueRead, a.queuePull)
	q.post("/pull", PermissionQueueRead, a.queuePull)
	q.post("/pause", PermissionQueueControl, a.queuePause)
	q.post("/resume", PermissionQueueControl, a.queueResume)
	q.post("/purge", PermissionQueuePurge, a.queuePurge)

	q.get("/assigned/messages", PermissionQueueRead, a.queueAssignedMessages)
	q.post("/assigned/pause", PermissionQueueControl, a.queueAssignedControl("pause"))
	q.post("/assigned/resume", PermissionQueueControl, a.queueAssignedControl("resume"))
	q.post("/assigned/purge", PermissionQueuePurge, a.queueAssignedControl("purge"))

	p := q.group("/partitions/:partition")
	p.get("/messages", PermissionQueueRead, a.queuePartitionMessages)
	p.post("/pause", PermissionQueueControl, a.queuePartitionPause)
	p.post("/resume", PermissionQueueControl, a.queuePartitionResume)
	p.post("/purge", PermissionQueuePurge, a.queuePartitionPurge)
}

// registerStreamRoutes mounts the per-stream routes. The admin surface and the
// /v1 data plane share handlers but historically differed in the permission
// needed for partition commit-and-append, so the read permission is supplied by
// the caller.
func (a AdminServer) registerStreamRoutes(s adminRouter, commitAndAppend string) {
	s.get("", PermissionStreamRead, a.streamMessages)
	s.get("/messages", PermissionStreamRead, a.streamMessages)
	s.get("/lag", PermissionStreamRead, a.streamLag)
	s.post("/commit", PermissionStreamCommit, a.streamCommit)
	s.get("/offset", PermissionStreamRead, a.streamOffset)
	s.post("/transactions", PermissionStreamCommit, a.streamTransaction)

	p := s.group("/partitions/:partition")
	p.get("/lag", PermissionStreamRead, a.streamPartitionLag)
	p.get("/messages", PermissionStreamRead, a.streamPartitionMessages)
	p.post("/commit", PermissionStreamCommit, a.streamPartitionCommit)
	p.get("/offset", PermissionStreamRead, a.streamPartitionOffset)
	p.post("/commit-and-append", commitAndAppend, a.streamPartitionCommitAndAppend)
}

func (a AdminServer) registerDLQRoutes(d adminRouter, read, replay string) {
	d.get("", read, a.dlqMessages)
	d.get("/messages", read, a.dlqMessages)
	d.post("/replay", replay, a.dlqReplay)
	d.post("/replay-all", replay, a.dlqReplayAllRoute)
	d.get("/errors", read, a.dlqErrorsRoute)
	d.post("/archive", replay, a.dlqArchiveRoute)
	d.get("/export", read, a.dlqExportRoute)
}

func (a AdminServer) registerClusterRoutes(r adminRouter) {
	cl := r.group("/cluster")
	read, write := PermissionClusterRead, PermissionClusterWrite

	cl.get("/status", read, a.clusterStatus)
	cl.get("/upgrade-plan", read, a.clusterUpgradePlan)
	cl.get("/transactions", read, a.clusterTransaction)
	cl.post("/transactions/prune", write, a.clusterTransactionPrune)
	cl.post("/leadership", write, a.clusterLeadership)

	cl.post("/nodes", write, a.clusterNodes)
	node := cl.group("/nodes/:node")
	node.delete("", write, a.clusterNodeRemove)
	node.post("/drain", write, a.clusterNodeDrain)
	node.post("/drain/complete", write, a.clusterNodeDrainComplete)
	node.get("/removal-plan", read, a.clusterNodeRemovalPlan)
	node.post("/promote", write, a.clusterNodePromote)

	meta := cl.group("/metadata")
	meta.get("/nodes", read, a.clusterMetadataNodesList)
	meta.post("/nodes", write, a.clusterMetadataNodesRegister)
	meta.delete("/nodes/:node", write, a.clusterMetadataNodeDeregister)
	meta.get("/schemas", read, a.clusterMetadataSchemasList)
	meta.post("/schemas", write, a.clusterMetadataSchemasRegister)
	meta.get("/schemas/:name/:version", read, a.clusterMetadataSchema)
	meta.post("/schemas/:name/:version/deprecate", write, a.clusterMetadataSchemaDeprecate)
	meta.get("/acls", read, a.clusterMetadataACLsList)
	meta.post("/acls", write, a.clusterMetadataACLsPut)
	meta.get("/acls/:name", read, a.clusterMetadataACL)
	meta.post("/acls/:name/revoke", write, a.clusterMetadataACLRevoke)

	cl.get("/partitions", read, a.clusterPartitionsList)
	cl.post("/partitions", write, a.clusterPartitionsAssign)
	part := cl.group("/partitions/:name/:partition")
	part.get("", read, a.clusterPartitionGet)
	part.get("/isr", read, a.clusterPartitionISR)
	part.put("/isr", write, a.clusterPartitionUpdateISR)
	part.get("/route", read, a.clusterPartitionRoute)
	part.get("/reassignment-plan", read, a.clusterPartitionReassignmentPlan)
	part.post("/reassign", write, a.clusterPartitionReassign)
	part.post("/cancel-reassignment", write, a.clusterPartitionCancelReassignment)
	part.get("/placement-plan", read, a.clusterPartitionPlacementPlan)
	part.post("/epoch", write, a.clusterPartitionEpoch)

	// Node-to-node endpoints. They are authenticated like every other cluster
	// mutation and are not part of the operator API.
	r.get(internalPlacementSignalsPath, read, a.internalPlacementSignals)
	r.post(internalPartitionProposalPath, write, a.internalPartitionPropose)
	r.post(internalPartitionPreparePath, write, a.internalPartitionPrepare)
	r.post(internalPartitionOutcomePath, write, a.internalPartitionOutcome)
	r.post(internalPartitionStagedPath, write, a.internalPartitionStaged)
	r.post(internalPartitionResultPath, write, a.internalPartitionResult)
	r.post(internalPartitionPurgeResultPath, write, a.internalPartitionPurgeResult)
	r.post(internalPartitionLagPath, write, a.internalPartitionLag)
	r.post(internalPartitionQueueListPath, write, a.internalPartitionQueueList)
	r.post(internalPartitionQueueControlPath, write, a.internalPartitionQueueControl)
}

// ---- middleware ----

func (a AdminServer) maxBodyBytes() int64 {
	if a.MaxBodyBytes > 0 {
		return a.MaxBodyBytes
	}
	return 1 << 20
}

// responseHeaders applies security and CORS headers to every response and
// answers CORS preflight requests for any path.
func (a AdminServer) responseHeaders(c fh.Ctx) error {
	c.Set("X-Content-Type-Options", "nosniff")
	c.Set("Cache-Control", "no-store")
	c.Set("X-Frame-Options", "DENY")
	c.Set("X-XSS-Protection", "1; mode=block")
	origin := c.Get("Origin")
	if origin == "" {
		origin = "*"
	}
	if a.isAllowedOrigin(origin) {
		c.Set("Access-Control-Allow-Origin", origin)
	}
	c.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	c.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	c.Set("Access-Control-Max-Age", "86400")
	if c.Method() == fh.MethodOPTIONS {
		return c.SendStatus(fh.StatusNoContent)
	}
	return c.Next()
}

// bodyLimit bounds every request body to MaxBodyBytes.
func (a AdminServer) bodyLimit(c fh.Ctx) error {
	if err := fh.LimitRequestBody(c, a.maxBodyBytes()); err != nil {
		return WriteBrokerError(c, errf(ErrMessageTooLarge, "request body too large"))
	}
	return c.Next()
}

// authorize authenticates the caller, enforces permission and the principal's
// admission limits, and audits mutations.
func (a AdminServer) authorize(permission string) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		audit := a.Audit
		if audit == nil && a.Broker != nil {
			audit = NotificationAuditLogger{Bus: a.Broker.Notifications()}
		}
		event := func(principal string, allowed bool, status int, reason string) AdminAuditEvent {
			return AdminAuditEvent{Time: time.Now(), Principal: principal, RemoteAddr: c.IP(), Method: c.Method(), Path: c.Path(), Action: permission, Allowed: allowed, Status: status, Reason: reason}
		}
		principal, ok := a.authenticate(c)
		if !ok {
			if audit != nil {
				audit.LogAdmin(c.Context(), event("anonymous", false, fh.StatusUnauthorized, "missing or invalid bearer token"))
			}
			return WriteBrokerError(c, ErrUnauthenticated)
		}
		acl := a.effectiveACL()
		if !acl.Authorize(principal, permission) {
			if audit != nil {
				audit.LogAdmin(c.Context(), event(principal.Name, false, fh.StatusForbidden, "permission denied"))
			}
			return httpError(c, "forbidden", fh.StatusForbidden)
		}
		release, err := acl.Admit(principal)
		if err != nil {
			c.Set("Retry-After", "1")
			return httpError(c, err.Error(), fh.StatusTooManyRequests)
		}
		defer release()
		c.SetContext(context.WithValue(c.Context(), adminPrincipalKey{}, principal))
		err = c.Next()
		if audit != nil && isMutation(c.Method()) {
			status := c.StatusCode()
			if status == 0 {
				status = fh.StatusOK
			}
			audit.LogAdmin(c.Context(), event(principal.Name, status < 400, status, ""))
		}
		return err
	}
}

func (a AdminServer) authenticate(c fh.Ctx) (AdminPrincipal, bool) {
	if a.InsecureDev {
		warnInsecureDevOnce()
		return AdminPrincipal{Name: "insecure-dev", Permissions: []string{PermissionAdminAll}}, true
	}
	return authenticateAdminCredential(a.effectiveACL(), bearerToken(c), verifiedPeerCertificate(c))
}

// ---- lifecycle ----

// TLSConfig returns the admin listener's TLS configuration, or nil when TLS is
// not configured.
func (a AdminServer) TLSConfig() (*tls.Config, error) {
	if a.TLSCertFile == "" && a.TLSKeyFile == "" && a.ClientCAFile == "" && !a.RequireClientCert {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if a.TLSCertFile != "" || a.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(a.TLSCertFile, a.TLSKeyFile)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
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
		cfg.ClientCAs = pool
	}
	if a.RequireClientCert {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// ListenAndServe starts the admin application on addr in the background and
// returns it so the caller can shut it down.
func (a AdminServer) ListenAndServe(addr string) *fh.App {
	app := a.App()
	go func() {
		tlsConfig, err := a.TLSConfig()
		if err != nil {
			log.Printf("[http] admin TLS configuration failed: %v", err)
			return
		}
		if err := ListenHTTPApp(app, addr, tlsConfig); err != nil {
			log.Printf("[http] admin server stopped: %v", err)
		}
	}()
	return app
}

// GracefulShutdown drains in-flight requests within drainTimeout.
func (a AdminServer) GracefulShutdown(ctx context.Context, app *fh.App, drainTimeout time.Duration) error {
	if drainTimeout <= 0 {
		drainTimeout = 15 * time.Second
	}
	drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()
	return ShutdownHTTPApp(drainCtx, app)
}

func (a AdminServer) Shutdown(ctx context.Context, app *fh.App) error {
	return ShutdownHTTPApp(ctx, app)
}
