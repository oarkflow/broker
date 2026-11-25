package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStreamTransactionHTTPPermissionsAndConflict(t *testing.T) {
	b := MustNew(Config{})
	defer b.Stop(context.Background())
	seedTransactionInput(t, b.Store())
	acl := NewACL(
		AdminToken{Token: "commit", Permissions: []string{PermissionStreamCommit}},
		AdminToken{Token: "publish", Permissions: []string{PermissionPublish}},
		AdminToken{Token: "no-commit", Permissions: []string{PermissionPublish, PermissionStreamRead, PermissionStreamWrite}},
		AdminToken{Token: "both", Permissions: []string{PermissionStreamCommit, PermissionPublish}},
	)
	handler := (AdminServer{Broker: b, ACL: acl}).Handler()
	body := []byte(`{"consumer_group":"workers","expected_offset":0,"offset":1,"messages":[{"topic":"processed","queue":"jobs","payload":{"ok":true}}]}`)
	for _, path := range []string{"/streams/input/transactions", "/v1/streams/input/transactions", "/streams/input/transactions/", "/v1/streams/input/transactions/"} {
		for _, token := range []string{"commit", "publish", "no-commit"} {
			r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != http.StatusForbidden {
				t.Fatalf("%s token=%s: status=%d, body=%s", path, token, w.Code, w.Body.String())
			}
		}
	}
	for _, malformed := range []string{
		`[]`,
		`{"consumer_group":"workers","expected_offset":0,"offset":1,"unexpected":true}`,
		`{"consumer_group":"workers","expected_offset":0,"offset":1} {}`,
	} {
		r := httptest.NewRequest(http.MethodPost, "/v1/streams/input/transactions", bytes.NewBufferString(malformed))
		r.Header.Set("Authorization", "Bearer both")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("malformed body status=%d, body=%s", w.Code, w.Body.String())
		}
	}
	post := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/streams/input/transactions", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer both")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	w := post()
	if w.Code != http.StatusOK {
		t.Fatalf("transaction status=%d, body=%s", w.Code, w.Body.String())
	}
	var results []PublishResult
	if err := json.Unmarshal(w.Body.Bytes(), &results); err != nil || len(results) != 1 {
		t.Fatalf("results=%v, error=%v", results, err)
	}
	w = post()
	if w.Code != http.StatusConflict || !errors.Is(ParseRemoteError(w.Code, w.Body.Bytes()), ErrOffsetConflict) {
		t.Fatalf("stale transaction status=%d, body=%s", w.Code, w.Body.String())
	}
	messages, err := b.Store().ListQueue(context.Background(), "jobs", 10)
	if err != nil || len(messages) != 1 {
		t.Fatalf("retry produced duplicate output: %d, %v", len(messages), err)
	}
}

func TestStreamCommitAuthorizationNormalizesTrailingSlash(t *testing.T) {
	for _, path := range []string{"/streams/input/commit/", "/v1/streams/input/commit/", "/streams/input/transactions/", "/v1/streams/input/transactions/"} {
		if got := adminAction(httptest.NewRequest(http.MethodPost, path, nil)); got != PermissionStreamCommit {
			t.Fatalf("%s requires %q, got %q", path, PermissionStreamCommit, got)
		}
	}
}

type appendOnlyTransactionStore struct{ Store }

func TestStreamPublishTransactionUnsupportedAndTenantDefaults(t *testing.T) {
	unsupported := MustNew(Config{Store: &appendOnlyTransactionStore{Store: NewMemoryStore()}})
	defer unsupported.Stop(context.Background())
	if _, err := unsupported.Publisher().BeginStreamTx(context.Background(), OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1}, 0); !errors.Is(err, ErrUnsupportedMode) {
		t.Fatalf("unsupported store accepted transaction: %v", err)
	}
	b := MustNew(Config{})
	defer b.Stop(context.Background())
	seedTransactionInput(t, b.Store())
	tx, err := b.Publisher().BeginStreamTx(context.Background(), OffsetCommit{Stream: "input", ConsumerGroup: "workers", Offset: 1, TenantID: "tenant-a"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Publish("processed", "output", WithQueue("jobs")); err != nil {
		t.Fatal(err)
	}
	results, err := tx.Commit(context.Background())
	if err != nil || len(results) != 1 {
		t.Fatalf("commit results=%v, error=%v", results, err)
	}
	message, ok, err := b.Store().Get(context.Background(), results[0].MessageID)
	if err != nil || !ok || message.TenantID != "tenant-a" {
		t.Fatalf("output did not inherit tenant: %+v, %v", message, err)
	}
	if _, err := tx.Commit(context.Background()); !errors.Is(err, ErrTransactionClosed) {
		t.Fatalf("transaction allowed a second commit: %v", err)
	}
}
