package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDLQDashboardEndpoints(t *testing.T) {
	b := MustNew(Config{
		Store: NewMemoryStore(),
		ACL:   NewACL(),
	})
	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)

	// Declare main queue and DLQ
	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "orders"})
	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "orders.dlq"})

	now := time.Now()
	// Insert 5 dead letter messages with varied errors
	msgs := []Message{
		{
			ID:               "dlq-msg-1",
			Queue:            "orders.dlq",
			Payload:          []byte(`{"order": 1}`),
			LastError:        "timeout connecting to payment gateway",
			DeadLetterReason: "max_retries_exceeded",
			CreatedAt:        now.Add(-10 * time.Minute),
			Attempts:         3,
		},
		{
			ID:               "dlq-msg-2",
			Queue:            "orders.dlq",
			Payload:          []byte(`{"order": 2}`),
			LastError:        "timeout connecting to payment gateway",
			DeadLetterReason: "max_retries_exceeded",
			CreatedAt:        now.Add(-8 * time.Minute),
			Attempts:         3,
		},
		{
			ID:               "dlq-msg-3",
			Queue:            "orders.dlq",
			Payload:          []byte(`{"order": 3}`),
			LastError:        "invalid credit card checksum",
			DeadLetterReason: "unrecoverable_validation_error",
			CreatedAt:        now.Add(-5 * time.Minute),
			Attempts:         1,
		},
		{
			ID:               "dlq-msg-4",
			Queue:            "orders.dlq",
			Payload:          []byte(`{"order": 4}`),
			LastError:        "timeout connecting to payment gateway",
			DeadLetterReason: "max_retries_exceeded",
			CreatedAt:        now.Add(-2 * time.Minute),
			Attempts:         3,
		},
		{
			ID:        "dlq-msg-5",
			Queue:     "orders.dlq",
			Payload:   []byte(`{"order": 5}`),
			LastError: "insufficient inventory",
			CreatedAt: now.Add(-1 * time.Minute),
			Attempts:  2,
		},
	}

	for _, m := range msgs {
		_, err := b.Store().Append(ctx, m, []Destination{{Kind: DestinationQueue, Name: "orders.dlq"}})
		if err != nil {
			t.Fatalf("failed to append DLQ message: %v", err)
		}
	}

	admin := AdminServer{Broker: b, InsecureDev: true}
	handler := admin.Handler()

	// 1. Test GET /dlq/dashboard
	t.Run("GET /dlq/dashboard", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/dlq/dashboard", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var dash DLQDashboardResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &dash); err != nil {
			t.Fatalf("failed to parse dashboard JSON: %v", err)
		}

		if dash.TotalDepth != 5 {
			t.Fatalf("expected total depth 5, got %d", dash.TotalDepth)
		}
		if dash.TotalDepth < 5 {
			t.Fatalf("expected total depth at least 5, got %d", dash.TotalDepth)
		}
		var qSummary *DLQQueueSummary
		for i := range dash.Queues {
			if dash.Queues[i].Name == "orders.dlq" {
				qSummary = &dash.Queues[i]
				break
			}
		}
		if qSummary == nil {
			t.Fatalf("expected orders.dlq in dashboard response, got: %+v", dash.Queues)
		}
		if len(qSummary.TopErrors) == 0 {
			t.Fatalf("expected top errors to be populated")
		}
		if qSummary.TopErrors[0].Error != "timeout connecting to payment gateway" || qSummary.TopErrors[0].Count != 3 {
			t.Fatalf("expected top error count 3 for timeout, got %+v", qSummary.TopErrors[0])
		}
		if qSummary.LastError != "insufficient inventory" {
			t.Fatalf("expected latest error 'insufficient inventory', got '%s'", qSummary.LastError)
		}
	})

	// 2. Test GET /dlq/:queue/errors
	t.Run("GET /dlq/:queue/errors", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/dlq/orders.dlq/errors?limit=2&page=1", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var errResp DLQErrorsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
			t.Fatalf("failed to parse errors JSON: %v", err)
		}

		if errResp.TotalErrors != 3 {
			t.Fatalf("expected 3 distinct errors, got %d", errResp.TotalErrors)
		}
		if len(errResp.Errors) != 2 {
			t.Fatalf("expected page limit of 2 errors, got %d", len(errResp.Errors))
		}
		if errResp.Errors[0].Error != "timeout connecting to payment gateway" || errResp.Errors[0].Count != 3 {
			t.Fatalf("expected top error timeout with count 3, got %+v", errResp.Errors[0])
		}
		if len(errResp.Errors[0].SampleMessageIDs) != 3 {
			t.Fatalf("expected 3 sample message IDs, got %d", len(errResp.Errors[0].SampleMessageIDs))
		}
	})

	// 3. Test GET /dlq/:queue/export (NDJSON streaming)
	t.Run("GET /dlq/:queue/export", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/dlq/orders.dlq/export?limit=10", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Content-Type") != "application/x-ndjson" {
			t.Fatalf("expected Content-Type application/x-ndjson, got %s", rec.Header().Get("Content-Type"))
		}

		scanner := bufio.NewScanner(rec.Body)
		lineCount := 0
		for scanner.Scan() {
			line := scanner.Text()
			if strings.TrimSpace(line) == "" {
				continue
			}
			var m Message
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Fatalf("invalid NDJSON line: %v", err)
			}
			lineCount++
		}
		if lineCount != 5 {
			t.Fatalf("expected 5 exported messages, got %d", lineCount)
		}
	})

	// 4. Test POST /dlq/:queue/archive (dry run and actual)
	t.Run("POST /dlq/:queue/archive", func(t *testing.T) {
		// Dry run first
		dryReq := httptest.NewRequest(http.MethodPost, "/dlq/orders.dlq/archive?limit=2&dry_run=true", nil)
		dryRec := httptest.NewRecorder()
		handler.ServeHTTP(dryRec, dryReq)

		if dryRec.Code != http.StatusOK {
			t.Fatalf("dry run failed: %d: %s", dryRec.Code, dryRec.Body.String())
		}
		var dryResp DLQArchiveResponse
		_ = json.Unmarshal(dryRec.Body.Bytes(), &dryResp)
		if dryResp.Archived != 2 || !dryResp.DryRun {
			t.Fatalf("expected 2 dry-run archived, got %+v", dryResp)
		}

		// Now actual archive of 1 message
		actReq := httptest.NewRequest(http.MethodPost, "/dlq/orders.dlq/archive?limit=1&dest=orders.archive", nil)
		actRec := httptest.NewRecorder()
		handler.ServeHTTP(actRec, actReq)

		if actRec.Code != http.StatusOK {
			t.Fatalf("actual archive failed: %d: %s", actRec.Code, actRec.Body.String())
		}
		var actResp DLQArchiveResponse
		_ = json.Unmarshal(actRec.Body.Bytes(), &actResp)
		if actResp.Archived != 1 {
			t.Fatalf("expected 1 archived message, got %+v", actResp)
		}

		// DLQ should now have 4 messages
		dlqRemaining, _ := b.Store().ListDLQ(ctx, "orders.dlq", 10)
		if len(dlqRemaining) != 4 {
			t.Fatalf("expected 4 remaining DLQ messages, got %d", len(dlqRemaining))
		}

		// Archive queue should have 1 message
		archived, _ := b.Store().ListQueue(ctx, "orders.archive", 10)
		if len(archived) != 1 {
			t.Fatalf("expected 1 archived message in destination, got %d", len(archived))
		}
	})

	// 5. Test POST /dlq/:queue/replay-all
	t.Run("POST /dlq/:queue/replay-all", func(t *testing.T) {
		// Replay remaining 4 messages in batches of 2
		replayReq := httptest.NewRequest(http.MethodPost, "/dlq/orders.dlq/replay-all?batch_size=2&dest=orders", nil)
		replayRec := httptest.NewRecorder()
		handler.ServeHTTP(replayRec, replayReq)

		if replayRec.Code != http.StatusOK {
			t.Fatalf("replay-all failed: %d: %s", replayRec.Code, replayRec.Body.String())
		}

		var replayResp DLQReplayAllResponse
		_ = json.Unmarshal(replayRec.Body.Bytes(), &replayResp)
		if replayResp.Replayed != 4 {
			t.Fatalf("expected 4 replayed messages, got %d", replayResp.Replayed)
		}
		if replayResp.Batches != 2 {
			t.Fatalf("expected 2 batches of size 2, got %d", replayResp.Batches)
		}

		// DLQ should now be empty
		dlqAfter, _ := b.Store().ListDLQ(ctx, "orders.dlq", 10)
		if len(dlqAfter) != 0 {
			t.Fatalf("expected 0 DLQ messages after replay-all, got %d", len(dlqAfter))
		}

		// orders queue should now contain the 4 replayed messages
		ordersMsgs, _ := b.Store().ListQueue(ctx, "orders", 10)
		if len(ordersMsgs) != 4 {
			t.Fatalf("expected 4 messages in orders queue, got %d", len(ordersMsgs))
		}
	})
}

func TestDLQDashboardACLPermissions(t *testing.T) {
	acl := NewACL(
		AdminToken{
			Name:        "viewer",
			Token:       "view-token",
			Permissions: []string{PermissionDLQRead},
		},
		AdminToken{
			Name:        "operator",
			Token:       "op-token",
			Permissions: []string{PermissionDLQRead, PermissionDLQReplay},
		},
	)
	b := MustNew(Config{
		Store: NewMemoryStore(),
		ACL:   acl,
	})
	ctx := context.Background()
	_ = b.Start(ctx)
	defer b.Stop(ctx)

	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "tasks.dlq"})
	_ = b.Store().DeclareQueue(ctx, QueueConfig{Name: "tasks"})
	_, _ = b.Store().Append(ctx, Message{
		ID:        "m1",
		Queue:     "tasks.dlq",
		Payload:   []byte("test"),
		LastError: "network failure",
	}, []Destination{{Kind: DestinationQueue, Name: "tasks.dlq"}})

	admin := AdminServer{Broker: b, ACL: acl}
	handler := admin.Handler()

	// 1. Viewer can read dashboard
	req := httptest.NewRequest(http.MethodGet, "/dlq/dashboard", nil)
	req.Header.Set("Authorization", "Bearer view-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for viewer, got %d", rec.Code)
	}

	// 2. Viewer cannot replay-all (forbidden 403)
	req = httptest.NewRequest(http.MethodPost, "/dlq/tasks.dlq/replay-all", nil)
	req.Header.Set("Authorization", "Bearer view-token")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for viewer replay-all, got %d", rec.Code)
	}

	// 3. Operator CAN replay-all (200 OK)
	req = httptest.NewRequest(http.MethodPost, "/dlq/tasks.dlq/replay-all", nil)
	req.Header.Set("Authorization", "Bearer op-token")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for operator replay-all, got %d: %s", rec.Code, rec.Body.String())
	}
}
