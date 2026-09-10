package broker

import (
	"errors"
	"testing"
	"time"
)

func TestBST1AuthenticationAndPermissions(t *testing.T) {
	dir := t.TempDir()

	acl := NewACL(
		AdminToken{
			Name:        "writer-agent",
			Token:       "write-secret",
			Permissions: []string{PermissionStreamWrite},
		},
		AdminToken{
			Name:        "reader-agent",
			Token:       "read-secret",
			Permissions: []string{PermissionStreamRead},
		},
		AdminToken{
			Name:        "full-agent",
			Token:       "full-secret",
			Permissions: []string{PermissionStreamWrite, PermissionStreamRead, PermissionStreamCommit},
		},
	)

	s, err := NewNativeStreamServer(NativeStreamConfig{
		Dir:        dir,
		Partitions: 2,
		ACL:        acl,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer s.Close()

	addr, err := netListenLocal(s)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	// 1. Unauthenticated connection without token should fail
	_, err = DialNativeStreamWithOptions(addr, NativeStreamDialOptions{
		Timeout: 2 * time.Second,
	})
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized for unauthenticated connection, got: %v", err)
	}

	// 2. Bad token should fail
	_, err = DialNativeStreamWithOptions(addr, NativeStreamDialOptions{
		Token:   "invalid-token",
		Timeout: 2 * time.Second,
	})
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized for invalid token, got: %v", err)
	}

	// 3. Connect as reader-agent
	readerClient, err := DialNativeStreamWithOptions(addr, NativeStreamDialOptions{
		Token:   "read-secret",
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("reader connect: %v", err)
	}
	defer readerClient.Close()
	if readerClient.Principal() != "reader-agent" {
		t.Fatalf("expected principal reader-agent, got: %q", readerClient.Principal())
	}

	// 4. Reader attempts to produce -> server should reject / close or return error
	_, err = readerClient.Produce("logs", 0, [][]byte{[]byte("data")})
	if err == nil {
		t.Fatalf("expected produce to fail for reader-only client")
	}

	// 5. Connect as writer-agent
	writerClient, err := DialNativeStreamWithOptions(addr, NativeStreamDialOptions{
		Token:   "write-secret",
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("writer connect: %v", err)
	}
	defer writerClient.Close()

	// Writer should be able to produce
	_, err = writerClient.Produce("logs", 0, [][]byte{[]byte("log entry 1")})
	if err != nil {
		t.Fatalf("writer produce: %v", err)
	}

	// Writer attempts to fetch -> should fail
	_, _, err = writerClient.Fetch("logs", 0, 0, 10, 1024)
	if err == nil {
		t.Fatalf("expected fetch to fail for writer-only client")
	}

	// 6. Connect as full-agent
	fullClient, err := DialNativeStreamWithOptions(addr, NativeStreamDialOptions{
		Token:   "full-secret",
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("full connect: %v", err)
	}
	defer fullClient.Close()

	// Full client can produce, fetch, commit
	_, err = fullClient.Produce("logs", 0, [][]byte{[]byte("log entry 2")})
	if err != nil {
		t.Fatalf("full produce: %v", err)
	}
	records, next, err := fullClient.Fetch("logs", 0, 0, 10, 1024)
	if err != nil || len(records) < 2 {
		t.Fatalf("full fetch: err=%v, records=%d", err, len(records))
	}
	if err := fullClient.Commit("logs", "group1", 0, next); err != nil {
		t.Fatalf("full commit: %v", err)
	}
}

func TestBST1ConcurrencyThrottling(t *testing.T) {
	dir := t.TempDir()

	acl := NewACL(
		AdminToken{
			Name:          "limited-agent",
			Token:         "limit-token",
			Permissions:   []string{PermissionStreamWrite, PermissionStreamRead},
			MaxConcurrent: 1, // Only 1 concurrent connection allowed
		},
	)

	s, err := NewNativeStreamServer(NativeStreamConfig{
		Dir:        dir,
		Partitions: 1,
		ACL:        acl,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer s.Close()

	addr, err := netListenLocal(s)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	// First connection succeeds
	c1, err := DialNativeStreamWithOptions(addr, NativeStreamDialOptions{
		Token:   "limit-token",
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("first connection failed: %v", err)
	}

	// Second connection with same token should be throttled
	_, err = DialNativeStreamWithOptions(addr, NativeStreamDialOptions{
		Token:   "limit-token",
		Timeout: 2 * time.Second,
	})
	if !errors.Is(err, ErrThrottled) {
		t.Fatalf("expected ErrThrottled for second concurrent connection, got: %v", err)
	}

	// Close first connection
	if err := c1.Close(); err != nil {
		t.Fatalf("close c1: %v", err)
	}

	// Allow server to process connection close cleanup
	time.Sleep(50 * time.Millisecond)

	// Now third connection should succeed
	c2, err := DialNativeStreamWithOptions(addr, NativeStreamDialOptions{
		Token:   "limit-token",
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("connection after c1 closed should succeed, got: %v", err)
	}
	defer c2.Close()
}
