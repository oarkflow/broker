package broker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestTenantScopedPrincipalCannotCrossTenant(t *testing.T) {
	store := NewMemoryStore()
	b := MustNew(Config{Store: store})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	ctxA := context.WithValue(context.Background(), adminPrincipalKey{}, AdminPrincipal{Name: "tenant-a", Tenants: []string{"a"}})
	ctxB := context.WithValue(context.Background(), adminPrincipalKey{}, AdminPrincipal{Name: "tenant-b", Tenants: []string{"b"}})
	result, err := b.Publisher().Publish(ctxA, "events", []byte("secret"), WithQueue("shared"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Messages[0].TenantID != "a" {
		t.Fatalf("tenant was not derived from credential: %q", result.Messages[0].TenantID)
	}
	if _, err := b.Publisher().Publish(ctxA, "events", []byte("x"), WithQueue("shared"), WithTenantID("b")); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-tenant publish should fail, got %v", err)
	}
	msgs, err := store.Lease(ctxB, LeaseRequest{Queue: "shared", ConsumerID: "b", TenantID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("tenant b leased tenant a messages: %+v", msgs)
	}
	if _, ok, err := store.Get(ctxB, result.MessageID); !errors.Is(err, ErrUnauthorized) || ok {
		t.Fatalf("cross-tenant get: ok=%t err=%v", ok, err)
	}
	if err := store.Update(ctxB, result.Messages[0]); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-tenant update should fail, got %v", err)
	}
	if _, err := store.History(ctxB, result.MessageID); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-tenant history should fail, got %v", err)
	}
	if err := store.DeclareStream(context.Background(), StreamConfig{Name: "shared-stream"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), Message{ID: "stream-a", TenantID: "a"}, []Destination{{Kind: DestinationStream, Name: "shared-stream"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), Message{ID: "stream-b", TenantID: "b"}, []Destination{{Kind: DestinationStream, Name: "shared-stream"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitOffset(ctxA, OffsetCommit{Stream: "shared-stream", ConsumerGroup: "workers", Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if off, err := store.ConsumerOffset(ctxB, OffsetCommit{Stream: "shared-stream", ConsumerGroup: "workers"}); err != nil || off != 0 {
		t.Fatalf("tenant offset leaked: offset=%d err=%v", off, err)
	}
	readB, err := store.ReadStream(ctxB, StreamReadRequest{Stream: "shared-stream", ConsumerGroup: "workers", Limit: 10})
	if err != nil || len(readB) != 1 || readB[0].TenantID != "b" {
		t.Fatalf("tenant stream isolation failed: messages=%+v err=%v", readB, err)
	}
}

func TestTenantScopedStatsLagAndNotificationsDoNotLeak(t *testing.T) {
	ctx := context.Background()
	ctxA := context.WithValue(ctx, adminPrincipalKey{}, AdminPrincipal{Name: "tenant-a", Tenants: []string{"a"}})
	ctxB := context.WithValue(ctx, adminPrincipalKey{}, AdminPrincipal{Name: "tenant-b", Tenants: []string{"b"}})
	store := NewMemoryStore()
	if err := store.DeclareQueue(ctx, QueueConfig{Name: "shared"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeclareStream(ctx, StreamConfig{Name: "events"}); err != nil {
		t.Fatal(err)
	}
	for _, msg := range []Message{
		{ID: "a-1", TenantID: "a"},
		{ID: "b-1", TenantID: "b"},
		{ID: "a-2", TenantID: "a"},
		{ID: "unowned"},
	} {
		if _, err := store.Append(ctx, msg, []Destination{{Kind: DestinationQueue, Name: "shared"}, {Kind: DestinationStream, Name: "events"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.CommitOffset(ctxA, OffsetCommit{Stream: "events", ConsumerGroup: "workers", Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitOffset(ctxB, OffsetCommit{Stream: "events", ConsumerGroup: "workers", Offset: 2}); err != nil {
		t.Fatal(err)
	}
	statsA, err := store.Stats(ctxA)
	if err != nil {
		t.Fatal(err)
	}
	if statsA.Messages != 4 || statsA.Queues["shared"] != 2 || statsA.Streams["events"] != 2 {
		t.Fatalf("tenant A stats leaked or omitted data: %+v", statsA)
	}
	if len(statsA.Offsets["events"]) != 1 || statsA.Offsets["events"]["workers"] != 1 {
		t.Fatalf("tenant A offsets leaked: %+v", statsA.Offsets)
	}
	lag, err := store.StreamLagContext(ctxA, "events", "workers", "")
	if err != nil || lag != 1 {
		t.Fatalf("tenant A lag=%d err=%v, want one visible message", lag, err)
	}
	head, err := store.StreamHeadOffsetContext(ctxA, "events", "")
	if err != nil || head != 3 {
		t.Fatalf("tenant A head=%d err=%v", head, err)
	}
	history := []Notification{
		{ID: "a", TenantID: "a"},
		{ID: "b", TenantID: "b"},
		{ID: "global"},
	}
	visible := visibleNotifications(ctxA, history)
	if len(visible) != 1 || visible[0].ID != "a" {
		t.Fatalf("notification metadata leaked: %+v", visible)
	}
}

func TestTenantScopedCredentialCannotAccessGlobalRoutes(t *testing.T) {
	b := MustNew(Config{})
	server := httptest.NewServer(AdminServer{Broker: b, ACL: NewACL(AdminToken{
		Name:        "tenant-router",
		Token:       "tenant-token",
		Permissions: []string{PermissionRouteRead, PermissionRouteWrite},
		Tenants:     []string{"a"},
	})}.Handler())
	defer server.Close()
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		req, err := http.NewRequest(method, server.URL+"/routes", bytes.NewBufferString(`{"name":"private"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer tenant-token")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s /routes status=%d, want 403", method, resp.StatusCode)
		}
	}
}

func TestTenantScopedTaskAndSagaIsolation(t *testing.T) {
	ctx := context.Background()
	ctxA := context.WithValue(ctx, adminPrincipalKey{}, AdminPrincipal{Name: "tenant-a", Tenants: []string{"a"}})
	ctxB := context.WithValue(ctx, adminPrincipalKey{}, AdminPrincipal{Name: "tenant-b", Tenants: []string{"b"}})
	b := MustNew(Config{})
	now := time.Now()
	b.taskMu.Lock()
	b.tasks["task-a"] = &TaskInfo{ID: "task-a", TenantID: "a", Status: TaskQueued, CreatedAt: now, UpdatedAt: now}
	b.taskMu.Unlock()
	if _, err := (taskHandle{b: b, id: "task-a"}).Info(ctxB); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-tenant task read error=%v", err)
	}
	if _, err := (taskHandle{b: b, id: "task-a"}).Info(ctxA); err != nil {
		t.Fatalf("own task read failed: %v", err)
	}
	created, err := b.SagaEngine().CreateSagaChecked(ctxA, SagaDefinition{Name: "saga-a"})
	if err != nil || created.TenantID != "a" {
		t.Fatalf("tenant saga create=%+v err=%v", created, err)
	}
	if _, ok := b.SagaEngine().GetSagaContext(ctxB, created.ID); ok {
		t.Fatal("cross-tenant saga read succeeded")
	}
	if err := b.SagaEngine().ExecuteSaga(ctxB, created.ID); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-tenant saga execute error=%v", err)
	}
}

func TestSSESecureDefaultAndTenantBroadcastFilter(t *testing.T) {
	b := MustNew(Config{})
	server := httptest.NewServer(NewSSEServer(b, SSEConfig{Path: "/events"}).Handler())
	defer server.Close()
	resp, err := http.Get(server.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("SSE without configured authentication status=%d", resp.StatusCode)
	}
	tenantA := AdminPrincipal{Name: "a", Tenants: []string{"a"}}
	if !broadcastAllowed(tenantA, Notification{TenantID: "a"}) {
		t.Fatal("own tenant broadcast was rejected")
	}
	if broadcastAllowed(tenantA, Notification{TenantID: "b"}) || broadcastAllowed(tenantA, map[string]string{"global": "metadata"}) {
		t.Fatal("tenant-scoped SSE principal received foreign or global broadcast")
	}
}

func TestAckNackRequireLeaseToken(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	if err := st.DeclareQueue(ctx, QueueConfig{Name: "secure"}); err != nil {
		t.Fatal(err)
	}
	_, err := st.Append(ctx, Message{ID: "m1", Payload: []byte("x"), Status: MessageQueued, CreatedAt: time.Now()}, []Destination{{Kind: DestinationQueue, Name: "secure"}})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := st.Lease(ctx, LeaseRequest{Queue: "secure", ConsumerID: "c1", Limit: 1, VisibilityTimeout: time.Minute})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease failed: %v len=%d", err, len(leased))
	}
	if err := st.Ack(ctx, AckRequest{MessageID: "m1", Queue: "secure", ConsumerID: "attacker"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("ack without lease token should fail, got %v", err)
	}
	if err := st.Nack(ctx, NackRequest{MessageID: "m1", Queue: "secure", ConsumerID: "attacker", Action: NackRequeue}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("nack without lease token should fail, got %v", err)
	}
	if err := st.Ack(ctx, AckRequest{MessageID: "m1", Queue: "secure", ConsumerID: "c1", LeaseToken: leased[0].LeaseToken}); err != nil {
		t.Fatalf("ack with lease token should succeed: %v", err)
	}
}

func TestAdminServerRequiresAuthAndACL(t *testing.T) {
	b := MustNew(Config{})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	srv := httptest.NewServer(AdminServer{Broker: b, ACL: NewACL(AdminToken{Name: "reader", Token: "read-token", Permissions: []string{PermissionQueueRead}}, AdminToken{Name: "admin", Token: "admin-token", Permissions: []string{PermissionAdminAll}})}.Handler())
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/publish", "application/json", bytes.NewBufferString(`{"queue":"q","payload":{"x":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized without token, got %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/publish", bytes.NewBufferString(`{"queue":"q","payload":{"x":1}}`))
	req.Header.Set("Authorization", "Bearer read-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden for reader publish, got %d", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/publish", bytes.NewBufferString(`{"queue":"q","payload":{"x":1}}`))
	req.Header.Set("Authorization", "Bearer admin-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected admin publish ok, got %d", resp.StatusCode)
	}
}

func TestACLCredentialHashExpiryAndRevocation(t *testing.T) {
	acl := NewACL(AdminToken{Name: "worker", Token: "high-entropy-secret", Permissions: []string{PermissionPublish}})
	if len(acl.Tokens) != 1 || acl.Tokens[0].Token != "" {
		t.Fatal("ACL retained plaintext credential")
	}
	if _, ok := acl.Authenticate("high-entropy-secret"); !ok {
		t.Fatal("hashed credential did not authenticate")
	}
	if !acl.Revoke("worker") {
		t.Fatal("credential was not revoked")
	}
	if _, ok := acl.Authenticate("high-entropy-secret"); ok {
		t.Fatal("revoked credential authenticated")
	}
	acl.AddToken(AdminToken{Name: "expired", Token: "old-secret", ExpiresAt: time.Now().Add(-time.Second)})
	if _, ok := acl.Authenticate("old-secret"); ok {
		t.Fatal("expired credential authenticated")
	}
}

func TestACLCertificateIdentityMapping(t *testing.T) {
	certificate := &x509.Certificate{Subject: pkix.Name{CommonName: "worker-a"}, DNSNames: []string{"worker.internal"}}
	acl := NewACL(AdminToken{Name: "mtls-worker", CertificateIdentities: []string{"dns:worker.internal"}, Permissions: []string{PermissionPublish}, Tenants: []string{"a"}})
	if _, ok := acl.Authenticate(""); ok {
		t.Fatal("certificate-only ACL entry authenticated an empty bearer token")
	}
	principal, ok := acl.AuthenticateCertificate(certificate)
	if !ok || principal.Name != "mtls-worker" || len(principal.Tenants) != 1 || principal.Tenants[0] != "a" {
		t.Fatalf("certificate principal=%+v ok=%t", principal, ok)
	}
	request := httptest.NewRequest(http.MethodGet, "https://broker/stats", nil)
	request.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{certificate}}}
	server := AdminServer{ACL: acl}
	if got, ok := server.authenticate(request); !ok || got.Name != "mtls-worker" {
		t.Fatalf("HTTP certificate mapping principal=%+v ok=%t", got, ok)
	}
}

func TestACLPrincipalAdmissionLimits(t *testing.T) {
	concurrencyACL := NewACL(AdminToken{Name: "limited", Token: "secret", MaxConcurrent: 1})
	principal, ok := concurrencyACL.Authenticate("secret")
	if !ok {
		t.Fatal("principal did not authenticate")
	}
	release, err := concurrencyACL.Admit(principal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := concurrencyACL.Admit(principal); !errors.Is(err, ErrThrottled) {
		t.Fatalf("concurrency admission error=%v", err)
	}
	release()
	if releaseAgain, err := concurrencyACL.Admit(principal); err != nil {
		t.Fatalf("admission after release failed: %v", err)
	} else {
		releaseAgain()
	}
	rateACL := NewACL(AdminToken{Name: "rate", Token: "rate-secret", RequestsPerSecond: 1, Burst: 1})
	ratePrincipal, _ := rateACL.Authenticate("rate-secret")
	release, err = rateACL.Admit(ratePrincipal)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if _, err := rateACL.Admit(ratePrincipal); !errors.Is(err, ErrThrottled) {
		t.Fatalf("rate admission error=%v", err)
	}
}

func TestAdminServerRejectsOversizedBody(t *testing.T) {
	b := MustNew(Config{})
	srv := httptest.NewServer(AdminServer{Broker: b, InsecureDev: true, MaxBodyBytes: 16}.Handler())
	defer srv.Close()
	payload := map[string]any{"queue": "q", "payload": string(bytes.Repeat([]byte("x"), 128))}
	buf, _ := json.Marshal(payload)
	resp, err := http.Post(srv.URL+"/publish", "application/json", bytes.NewReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected body limit failure, got %d", resp.StatusCode)
	}
}

func TestFileWALPermissionsChecksumAndTailRecovery(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := st.Append(ctx, Message{ID: "m1", Payload: []byte("ok"), CreatedAt: time.Now(), Status: MessageQueued}, []Destination{{Kind: DestinationQueue, Name: "q"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	walPath := dir + "/broker.wal.jsonl"
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("WAL permissions should not be world/group readable: %v", info.Mode().Perm())
	}
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"op":"append"`)
	_ = f.Close()
	st2, err := NewFileWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	msgs, err := st2.ListQueue(ctx, "q", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].ID != "m1" {
		t.Fatalf("expected recovered intact record, got %+v", msgs)
	}
}

func TestCrossProtocolQuotaAdmissionEnforcement(t *testing.T) {
	acl := NewACL(
		AdminToken{
			Name:          "shared-client",
			Token:         "quota-token",
			Permissions:   []string{PermissionAdminAll},
			MaxConcurrent: 2,
		},
	)

	principal, ok := acl.Authenticate("quota-token")
	if !ok {
		t.Fatalf("failed to authenticate token")
	}

	// 1. Connection 1 admitted
	release1, err := acl.AdmitConnection(principal)
	if err != nil {
		t.Fatalf("first connection admission failed: %v", err)
	}

	// 2. Connection 2 admitted
	release2, err := acl.AdmitConnection(principal)
	if err != nil {
		t.Fatalf("second connection admission failed: %v", err)
	}

	// 3. Connection 3 exceeds MaxConcurrent: 2 -> ErrThrottled
	_, err = acl.AdmitConnection(principal)
	if !errors.Is(err, ErrThrottled) {
		t.Fatalf("expected ErrThrottled on exceeding quota, got: %v", err)
	}

	// 4. Release connection 1
	release1()

	// 5. Connection 4 now succeeds
	release3, err := acl.AdmitConnection(principal)
	if err != nil {
		t.Fatalf("connection admission after release failed: %v", err)
	}
	defer release2()
	defer release3()
}
