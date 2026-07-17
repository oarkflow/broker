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
	walPath := findNonEmptyWALFile(t, dir, "broker.wal.*-of-*.jsonl")
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
