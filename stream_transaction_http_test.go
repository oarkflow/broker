package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/oarkflow/fh"
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
	handler := (AdminServer{Broker: b, ACL: acl}).App()
	body := []byte(`{"consumer_group":"workers","expected_offset":0,"offset":1,"messages":[{"topic":"processed","queue":"jobs","payload":{"ok":true}}]}`)
	for _, path := range []string{"/streams/input/transactions", "/v1/streams/input/transactions", "/streams/input/transactions/", "/v1/streams/input/transactions/"} {
		for _, token := range []string{"commit", "publish", "no-commit"} {
			r := newTestRequest(fh.MethodPOST, path, bytes.NewReader(body))
			r.Header.Set("Authorization", "Bearer "+token)
			w := serveTest(t, handler, r)
			if w.Code != fh.StatusForbidden {
				t.Fatalf("%s token=%s: status=%d, body=%s", path, token, w.Code, w.Body.String())
			}
		}
	}
	for _, malformed := range []string{
		`[]`,
		`{"consumer_group":"workers","expected_offset":0,"offset":1,"unexpected":true}`,
		`{"consumer_group":"workers","expected_offset":0,"offset":1} {}`,
	} {
		r := newTestRequest(fh.MethodPOST, "/v1/streams/input/transactions", bytes.NewBufferString(malformed))
		r.Header.Set("Authorization", "Bearer both")
		w := serveTest(t, handler, r)
		if w.Code != fh.StatusBadRequest {
			t.Fatalf("malformed body status=%d, body=%s", w.Code, w.Body.String())
		}
	}
	post := func() *testRecorder {
		r := newTestRequest(fh.MethodPOST, "/v1/streams/input/transactions", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer both")
		w := serveTest(t, handler, r)
		return w
	}
	w := post()
	if w.Code != fh.StatusOK {
		t.Fatalf("transaction status=%d, body=%s", w.Code, w.Body.String())
	}
	var results []PublishResult
	if err := json.Unmarshal(w.Body.Bytes(), &results); err != nil || len(results) != 1 {
		t.Fatalf("results=%v, error=%v", results, err)
	}
	w = post()
	if w.Code != fh.StatusConflict || !errors.Is(ParseRemoteError(w.Code, w.Body.Bytes()), ErrOffsetConflict) {
		t.Fatalf("stale transaction status=%d, body=%s", w.Code, w.Body.String())
	}
	messages, err := b.Store().ListQueue(context.Background(), "jobs", 10)
	if err != nil || len(messages) != 1 {
		t.Fatalf("retry produced duplicate output: %d, %v", len(messages), err)
	}
}

// A trailing slash must not route a commit around the stream:commit check.
func TestStreamCommitAuthorizationNormalizesTrailingSlash(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	acl := NewACL(AdminToken{Name: "reader", Token: "reader", Permissions: []string{PermissionStreamRead, PermissionStreamWrite, PermissionPublish}})
	app := AdminServer{Broker: b, ACL: acl}.App()
	for _, path := range []string{"/streams/input/commit/", "/v1/streams/input/commit/", "/streams/input/transactions/", "/v1/streams/input/transactions/"} {
		req := newTestRequest(fh.MethodPOST, path, bytes.NewBufferString(`{}`))
		req.Header.Set("Authorization", "Bearer reader")
		if rec := serveTest(t, app, req); rec.Code != fh.StatusForbidden && rec.Code != fh.StatusNotFound {
			t.Fatalf("%s without %q: status %d body %s", path, PermissionStreamCommit, rec.Code, rec.Body.String())
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
