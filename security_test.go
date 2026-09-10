package broker

import (
	"bytes"
	"context"
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
