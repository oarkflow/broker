package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTenantEnforcementStatsAndAudit(t *testing.T) {
	b, err := New(Config{
		Name:         "test-broker",
		AuditEnabled: true,
	})
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	defer b.Stop(context.Background())

	acl := NewACL(
		AdminToken{
			Name:        "superadmin",
			Token:       "super-token",
			Permissions: []string{PermissionAdminAll},
			Tenants:     []string{"*"},
		},
		AdminToken{
			Name:        "user-a",
			Token:       "token-a",
			Permissions: []string{PermissionAdminAll},
			Tenants:     []string{"tenant-a"},
		},
		AdminToken{
			Name:        "user-b",
			Token:       "token-b",
			Permissions: []string{PermissionAdminAll},
			Tenants:     []string{"tenant-b"},
		},
	)

	admin := AdminServer{
		Broker: b,
		ACL:    acl,
	}
	handler := admin.Handler()

	// Declare queues for different tenants
	ctx := context.Background()
	_ = b.DeclareQueue(ctx, QueueConfig{Name: "tenant-a.queue1"})
	_ = b.DeclareQueue(ctx, QueueConfig{Name: "tenant-b.queue1"})

	// Publish messages to the queues so stats have counts
	_, _ = b.Publisher().Publish(ctx, "events", []byte("hello-a"), WithQueue("tenant-a.queue1"), WithTenantID("tenant-a"))
	_, _ = b.Publisher().Publish(ctx, "events", []byte("hello-b"), WithQueue("tenant-b.queue1"), WithTenantID("tenant-b"))

	// 1. Stats endpoint check
	// A) Superadmin sees both queues
	reqSuper := httptest.NewRequest(http.MethodGet, "/stats", nil)
	reqSuper.Header.Set("Authorization", "Bearer super-token")
	recSuper := httptest.NewRecorder()
	handler.ServeHTTP(recSuper, reqSuper)
	if recSuper.Code != http.StatusOK {
		t.Fatalf("expected 200 from /stats for superadmin, got %d: %s", recSuper.Code, recSuper.Body.String())
	}
	var superStats struct {
		Store StoreStats `json:"store"`
	}
	if err := json.Unmarshal(recSuper.Body.Bytes(), &superStats); err != nil {
		t.Fatalf("unmarshal super stats: %v", err)
	}
	if _, ok := superStats.Store.Queues["tenant-a.queue1"]; !ok {
		t.Fatalf("expected tenant-a.queue1 in superadmin stats")
	}
	if _, ok := superStats.Store.Queues["tenant-b.queue1"]; !ok {
		t.Fatalf("expected tenant-b.queue1 in superadmin stats")
	}

	// B) User A sees ONLY tenant-a queues
	reqA := httptest.NewRequest(http.MethodGet, "/stats", nil)
	reqA.Header.Set("Authorization", "Bearer token-a")
	recA := httptest.NewRecorder()
	handler.ServeHTTP(recA, reqA)
	if recA.Code != http.StatusOK {
		t.Fatalf("expected 200 from /stats for user-a, got %d", recA.Code)
	}
	var aStats struct {
		Store StoreStats `json:"store"`
	}
	if err := json.Unmarshal(recA.Body.Bytes(), &aStats); err != nil {
		t.Fatalf("unmarshal a stats: %v", err)
	}
	if _, ok := aStats.Store.Queues["tenant-a.queue1"]; !ok {
		t.Fatalf("expected tenant-a.queue1 in user-a stats, got: %s", recA.Body.String())
	}
	if _, ok := aStats.Store.Queues["tenant-b.queue1"]; ok {
		t.Fatalf("tenant isolation violation: tenant-b.queue1 present in user-a stats")
	}

	// 2. Audit endpoint check
	// Log events for tenant-a and tenant-b
	if b.auditLogger != nil {
		b.auditLogger.Log(ctx, AuditEvent{Action: "publish", Principal: "user-a", TenantID: "tenant-a", Timestamp: time.Now()})
		b.auditLogger.Log(ctx, AuditEvent{Action: "publish", Principal: "user-b", TenantID: "tenant-b", Timestamp: time.Now()})
	}

	// Superadmin sees both
	reqAuditSuper := httptest.NewRequest(http.MethodGet, "/audit", nil)
	reqAuditSuper.Header.Set("Authorization", "Bearer super-token")
	recAuditSuper := httptest.NewRecorder()
	handler.ServeHTTP(recAuditSuper, reqAuditSuper)
	if recAuditSuper.Code != http.StatusOK {
		t.Fatalf("audit superadmin code: %d", recAuditSuper.Code)
	}
	var superAudit []AuditEvent
	_ = json.Unmarshal(recAuditSuper.Body.Bytes(), &superAudit)
	hasA, hasB := false, false
	for _, ev := range superAudit {
		if ev.TenantID == "tenant-a" {
			hasA = true
		}
		if ev.TenantID == "tenant-b" {
			hasB = true
		}
	}
	if !hasA || !hasB {
		t.Fatalf("superadmin should see events for both tenants, got hasA=%v hasB=%v", hasA, hasB)
	}

	// User A sees only tenant-a events
	reqAuditA := httptest.NewRequest(http.MethodGet, "/audit", nil)
	reqAuditA.Header.Set("Authorization", "Bearer token-a")
	recAuditA := httptest.NewRecorder()
	handler.ServeHTTP(recAuditA, reqAuditA)
	if recAuditA.Code != http.StatusOK {
		t.Fatalf("audit user-a code: %d", recAuditA.Code)
	}
	var aAudit []AuditEvent
	_ = json.Unmarshal(recAuditA.Body.Bytes(), &aAudit)
	if len(aAudit) == 0 {
		t.Fatalf("expected user-a to see at least one audit event")
	}
	for _, ev := range aAudit {
		if ev.TenantID != "tenant-a" {
			t.Fatalf("tenant isolation violation: user-a saw audit event with tenant %q", ev.TenantID)
		}
	}
}

func TestTenantEnforcementConsumersAndTasks(t *testing.T) {
	b, err := New(Config{Name: "test-broker"})
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	defer b.Stop(context.Background())

	acl := NewACL(
		AdminToken{
			Name:        "user-a",
			Token:       "token-a",
			Permissions: []string{PermissionAdminAll},
			Tenants:     []string{"tenant-a"},
		},
		AdminToken{
			Name:        "user-b",
			Token:       "token-b",
			Permissions: []string{PermissionAdminAll},
			Tenants:     []string{"tenant-b"},
		},
	)

	admin := AdminServer{
		Broker: b,
		ACL:    acl,
	}
	handler := admin.Handler()

	// 1. Consumer registration with mismatched tenant should fail with 403 Forbidden
	mismatchedReg := ConsumerRegistration{
		ConsumerID: "c-bad",
		Group:      "group-1",
		TenantID:   "tenant-b", // trying to register for tenant-b with token-a
	}
	body, _ := json.Marshal(mismatchedReg)
	req := httptest.NewRequest(http.MethodPost, "/v1/consumers/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-a")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden when registering mismatched tenant consumer, got %d: %s", rec.Code, rec.Body.String())
	}

	// 2. Consumer registration with matching (or omitted) tenant succeeds
	validReg := ConsumerRegistration{
		ConsumerID: "c-good-a",
		Group:      "group-1",
		TenantID:   "tenant-a",
	}
	body, _ = json.Marshal(validReg)
	req = httptest.NewRequest(http.MethodPost, "/v1/consumers/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-a")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK registering valid consumer, got %d: %s", rec.Code, rec.Body.String())
	}

	// Also register a consumer for tenant-b
	regB := ConsumerRegistration{
		ConsumerID: "c-good-b",
		Group:      "group-1",
		TenantID:   "tenant-b",
	}
	body, _ = json.Marshal(regB)
	req = httptest.NewRequest(http.MethodPost, "/v1/consumers/register", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-b")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK registering tenant-b consumer, got %d", rec.Code)
	}

	// 3. List consumers: user-a should only see c-good-a
	req = httptest.NewRequest(http.MethodGet, "/v1/consumers", nil)
	req.Header.Set("Authorization", "Bearer token-a")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 listing consumers, got %d", rec.Code)
	}
	var consumersA []ConsumerInfo
	_ = json.Unmarshal(rec.Body.Bytes(), &consumersA)
	if len(consumersA) != 1 || consumersA[0].ConsumerID != "c-good-a" {
		t.Fatalf("expected only c-good-a in user-a consumer list, got %+v", consumersA)
	}

	// 4. Heartbeat: user-a cannot heartbeat tenant-b consumer
	req = httptest.NewRequest(http.MethodPost, "/v1/consumers/c-good-b/heartbeat", nil)
	req.Header.Set("Authorization", "Bearer token-a")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized && rec.Code != http.StatusForbidden {
		t.Fatalf("expected 401/403 when user-a heartbeats user-b consumer, got %d", rec.Code)
	}

	// 5. Tasks: submit task with mismatched tenant fails
	taskMismatched := map[string]any{
		"name":      "report-gen",
		"payload":   "{}",
		"tenant_id": "tenant-b",
	}
	body, _ = json.Marshal(taskMismatched)
	req = httptest.NewRequest(http.MethodPost, "/v1/tasks/submit", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-a")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when submitting task with mismatched tenant, got %d", rec.Code)
	}

	// Submit task with user-a's tenant succeeds
	taskA := map[string]any{
		"name":      "report-a",
		"payload":   `{"type":"summary"}`,
		"tenant_id": "tenant-a",
	}
	body, _ = json.Marshal(taskA)
	req = httptest.NewRequest(http.MethodPost, "/v1/tasks/submit", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-a")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 submitting task-a, got %d: %s", rec.Code, rec.Body.String())
	}
	var taskResp struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &taskResp)
	if taskResp.ID == "" {
		t.Fatalf("expected non-empty task ID")
	}

	// User-b attempting to fetch user-a's task returns 404 (isolation)
	req = httptest.NewRequest(http.MethodGet, "/v1/tasks/"+taskResp.ID, nil)
	req.Header.Set("Authorization", "Bearer token-b")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 accessing other tenant's task, got %d: %s", rec.Code, rec.Body.String())
	}

	// User-a can fetch its own task
	req = httptest.NewRequest(http.MethodGet, "/v1/tasks/"+taskResp.ID, nil)
	req.Header.Set("Authorization", "Bearer token-a")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 accessing own task, got %d", rec.Code)
	}

	// List tasks: user-b sees 0 tasks, user-a sees 1 task
	req = httptest.NewRequest(http.MethodGet, "/v1/tasks", nil)
	req.Header.Set("Authorization", "Bearer token-b")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list tasks user-b code: %d", rec.Code)
	}
	var tasksB []any
	_ = json.Unmarshal(rec.Body.Bytes(), &tasksB)
	if len(tasksB) != 0 {
		t.Fatalf("expected 0 tasks for user-b, got %d", len(tasksB))
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/tasks", nil)
	req.Header.Set("Authorization", "Bearer token-a")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list tasks user-a code: %d", rec.Code)
	}
	var tasksA []any
	_ = json.Unmarshal(rec.Body.Bytes(), &tasksA)
	if len(tasksA) != 1 {
		t.Fatalf("expected 1 task for user-a, got %d", len(tasksA))
	}
}

func suppressUnused(a ...any) {}
