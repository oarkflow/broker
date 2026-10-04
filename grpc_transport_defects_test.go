package broker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"
)

// The native JSON/TCP transport had several defects that failed silently: a nack
// never incremented Attempts so retries could never exhaust, an ack carried no
// lease token so every ack of a leased message failed, the poll request dropped
// the consumer group, visibility timeout and tenant, the exported Subscribe
// method was never routed, and MaxMessageSize was not enforced. These tests drive
// the transport over a real socket.

type nativeClient struct {
	conn net.Conn
	dec  *json.Decoder
	enc  *json.Encoder
	t    *testing.T
}

// startNativeServer binds an ephemeral port, then runs the transport on it. The
// server does not expose its listener address, so the port is reserved here.
func startNativeServer(t *testing.T, b *Broker, cfg GRPCConfig) string {
	t.Helper()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	cfg.ListenAddr = addr
	cfg.BrokerToken = "secret"
	srv := NewNativeRPCServer(b, cfg)
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
	t.Cleanup(func() {
		srv.Stop()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, derr := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if derr == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("native rpc server did not start on %s", addr)
	return ""
}

func dialNative(t *testing.T, addr string) *nativeClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &nativeClient{conn: conn, dec: json.NewDecoder(conn), enc: json.NewEncoder(conn), t: t}
}

func (c *nativeClient) call(method string, body any) grpcResponse {
	c.t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		c.t.Fatal(err)
	}
	req := grpcRequest{
		ID:      fmt.Sprintf("r-%d", time.Now().UnixNano()),
		Method:  method,
		Body:    raw,
		Headers: map[string]string{"Authorization": "Bearer secret"},
	}
	if err := c.enc.Encode(req); err != nil {
		c.t.Fatalf("encode %s: %v", method, err)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var resp grpcResponse
	if err := c.dec.Decode(&resp); err != nil {
		c.t.Fatalf("decode %s: %v", method, err)
	}
	return resp
}

// A nack over the native transport must increment Attempts, otherwise the retry
// ceiling never trips and the message is redelivered forever.
func TestNativeNackIncrementsAttempts(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	res, err := b.Publisher().Publish(ctx, "", []byte("payload"), WithQueue("q"))
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := b.Store().Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c1", ConsumerGroup: "g1", Limit: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("lease: n=%d err=%v", len(msgs), err)
	}

	c := dialNative(t, startNativeServer(t, b, GRPCConfig{}))
	for i := 1; i <= 3; i++ {
		resp := c.call(GRPCMethodNack, map[string]any{
			"message_id": res.MessageID, "queue": "q", "action": "retry",
			"consumer_id": "c1", "lease_token": msgs[0].LeaseToken,
		})
		if resp.Status != 200 {
			t.Fatalf("nack %d: status %d %s", i, resp.Status, resp.Error)
		}
		got, ok, err := b.Store().Get(ctx, res.MessageID)
		if err != nil || !ok {
			t.Fatalf("get after nack %d: ok=%v err=%v", i, ok, err)
		}
		if got.Attempts != i {
			t.Fatalf("after nack %d Attempts = %d, want %d (native transport is not incrementing)", i, got.Attempts, i)
		}
		// Redeliveries is deliberately not asserted: it counts visibility-timeout
		// expiries, not explicit nacks. Attempts is the retry-budget counter, and
		// any ceiling a client applies is derived from it.
		// Re-lease so the next nack has a valid token.
		next, err := b.Store().Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c1", ConsumerGroup: "g1", Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(next) == 1 {
			msgs = next
		}
	}
}

// Acking a leased message over the native transport requires the lease token.
func TestNativeAckAcceptsLeaseToken(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	res, err := b.Publisher().Publish(ctx, "", []byte("payload"), WithQueue("q"))
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := b.Store().Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c1", ConsumerGroup: "g1", Limit: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("lease: n=%d err=%v", len(msgs), err)
	}

	c := dialNative(t, startNativeServer(t, b, GRPCConfig{}))
	resp := c.call(GRPCMethodAck, map[string]any{
		"message_id": res.MessageID, "queue": "q", "consumer_id": "c1", "lease_token": msgs[0].LeaseToken,
	})
	if resp.Status != 200 {
		t.Fatalf("ack with a valid lease token failed: status %d %s", resp.Status, resp.Error)
	}
	remaining, err := b.Store().ListQueue(ctx, "q", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("queue still holds %d messages after ack", len(remaining))
	}
}

// The poll request must carry the consumer group, visibility timeout and tenant.
func TestNativePollCarriesGroupVisibilityAndTenant(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "", []byte("payload"), WithQueue("q"), WithTenantID("tenant-a")); err != nil {
		t.Fatal(err)
	}

	c := dialNative(t, startNativeServer(t, b, GRPCConfig{}))
	resp := c.call(GRPCMethodPoll, map[string]any{
		"queue": "q", "consumer": "c1", "consumer_group": "grp-a",
		"limit": 5, "visibility_timeout_ns": int64(90 * time.Second),
	})
	if resp.Status != 200 {
		t.Fatalf("poll: status %d %s", resp.Status, resp.Error)
	}
	var msgs []Message
	if err := json.Unmarshal(resp.Body, &msgs); err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("polled %d messages, want 1", len(msgs))
	}
	if msgs[0].TenantID != "tenant-a" {
		t.Errorf("TenantID = %q, want tenant-a; the native poll dropped tenant_id", msgs[0].TenantID)
	}
	// 90s requested, so a 30s default would show up as a much earlier deadline.
	wantMin := 80 * time.Second
	if msgs[0].LeaseUntil.Sub(time.Now()) < wantMin {
		t.Errorf("LeaseUntil is %s away, want about 90s; visibility_timeout_ns was ignored",
			msgs[0].LeaseUntil.Sub(time.Now()).Round(time.Second))
	}
	// The group must be recorded so offsets and competing-consumer logic apply.
	got, ok, err := b.Store().Get(ctx, msgs[0].ID)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.ConsumerGroup != "grp-a" {
		t.Errorf("ConsumerGroup = %q, want grp-a", got.ConsumerGroup)
	}
}

// GRPCMethodSubscribe was an exported constant with no route behind it.
func TestNativeSubscribeIsRouted(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}

	// The transport flow is register, then subscribe: RemoteConsumerSubscribe
	// rejects an unregistered consumer.
	if _, err := b.RegisterRemoteConsumer(ctx, ConsumerRegistration{
		ConsumerID: "c1", Group: "g1", Capabilities: []string{"poll", "subscribe"},
	}); err != nil {
		t.Fatal(err)
	}

	c := dialNative(t, startNativeServer(t, b, GRPCConfig{}))
	resp := c.call(GRPCMethodSubscribe, map[string]any{
		"consumer_id": "c1", "queue": "q", "auto_create": true,
	})
	if resp.Status == 404 {
		t.Fatalf("broker.Subscribe returned 404; the method constant is exported but was never routed")
	}
	if resp.Status != 200 {
		t.Fatalf("subscribe: status %d %s", resp.Status, resp.Error)
	}
	var info ConsumerInfo
	if err := json.Unmarshal(resp.Body, &info); err != nil {
		t.Fatal(err)
	}
	if info.ConsumerID != "c1" {
		t.Fatalf("ConsumerID = %q, want c1", info.ConsumerID)
	}
	if _, ok := b.Consumers().Get("c1"); !ok {
		t.Fatal("consumer was not registered by subscribe")
	}
}

// Authentication must still be enforced after the context change.
func TestNativeStillRequiresToken(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	c := dialNative(t, startNativeServer(t, b, GRPCConfig{}))
	raw, err := json.Marshal(map[string]any{"topic": "t", "payload": json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.enc.Encode(grpcRequest{ID: "no-auth", Method: GRPCMethodPublish, Body: raw}); err != nil {
		t.Fatal(err)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var resp grpcResponse
	if err := c.dec.Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != 401 {
		t.Fatalf("unauthenticated publish returned %d, want 401", resp.Status)
	}
}

// A client deadline must actually cancel the request instead of being ignored by
// a background context.
func TestNativeRequestHonoursDeadline(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	c := dialNative(t, startNativeServer(t, b, GRPCConfig{}))
	raw, err := json.Marshal(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.enc.Encode(grpcRequest{
		ID: "expired", Method: GRPCMethodPoll, Body: raw,
		Headers: map[string]string{"Authorization": "Bearer secret"},
		// A deadline already in the past.
		Context: time.Now().Add(-time.Second).UnixNano(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var resp grpcResponse
	if err := c.dec.Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status == 200 {
		t.Fatalf("a request with an expired deadline succeeded; the server ignored the client context")
	}
}

// MaxMessageSize was documented as bounding incoming frames but nothing enforced
// it, so a client could stream an unbounded frame.
func TestNativeEnforcesMaxMessageSize(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	c := dialNative(t, startNativeServer(t, b, GRPCConfig{MaxMessageSize: 4096}))

	huge := make([]byte, 256*1024)
	for i := range huge {
		huge[i] = 'a'
	}
	body, err := json.Marshal(map[string]any{"topic": "t", "payload": json.RawMessage(`"` + string(huge) + `"`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.enc.Encode(grpcRequest{
		ID: "huge", Method: GRPCMethodPublish, Body: json.RawMessage(body),
		Headers: map[string]string{"Authorization": "Bearer secret"},
	}); err != nil {
		t.Fatal(err)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var resp grpcResponse
	err = c.dec.Decode(&resp)
	if err == nil && resp.Status == 200 {
		t.Fatal("a 256KB frame was accepted with MaxMessageSize=4096; the limit is not enforced")
	}
}

// A binary payload must survive the native transport, matching the HTTP and
// embedded paths.
func TestNativePublishBinaryPayload(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	c := dialNative(t, startNativeServer(t, b, GRPCConfig{}))

	want := []byte{0x00, 0x01, 0xfe, 0xff, 'b', 'i', 'n'}
	resp := c.call(GRPCMethodPublish, map[string]any{
		"topic":            "t",
		"payload":          encodeNativeBase64(want),
		"payload_encoding": "base64",
	})
	if resp.Status != 200 {
		t.Fatalf("publish: status %d %s", resp.Status, resp.Error)
	}
	var res PublishResult
	if err := json.Unmarshal(resp.Body, &res); err != nil {
		t.Fatal(err)
	}
	got, ok, err := b.Store().Get(ctx, res.MessageID)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if string(got.Payload) != string(want) {
		t.Fatalf("payload = %q, want %q", got.Payload, want)
	}
}

// Tenant scoping must reach the store on the native publish path.
func TestNativePublishCarriesTenant(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	c := dialNative(t, startNativeServer(t, b, GRPCConfig{}))
	resp := c.call(GRPCMethodPublish, map[string]any{
		"topic": "t", "tenant_id": "tenant-a", "content_type": "application/x-native",
		"payload": json.RawMessage(`{"v":1}`),
	})
	if resp.Status != 200 {
		t.Fatalf("publish: status %d %s", resp.Status, resp.Error)
	}
	var res PublishResult
	if err := json.Unmarshal(resp.Body, &res); err != nil {
		t.Fatal(err)
	}
	got, _, err := b.Store().Get(ctx, res.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TenantID != "tenant-a" {
		t.Errorf("TenantID = %q, want tenant-a", got.TenantID)
	}
	if got.ContentType != "application/x-native" {
		t.Errorf("ContentType = %q, want application/x-native", got.ContentType)
	}
}

// A tenant-scoped principal must not be able to widen its scope over the native
// transport, which previously discarded the principal entirely.
func TestNativeTenantScopeIsNotWidened(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	c := dialNative(t, startNativeServer(t, b, GRPCConfig{}))

	// Publish into tenant-a, then read it back scoped to tenant-b.
	resp := c.call(GRPCMethodPublish, map[string]any{
		"topic": "t", "queue": "q", "tenant_id": "tenant-a", "payload": json.RawMessage(`{"secret":1}`),
	})
	if resp.Status != 200 {
		t.Fatalf("publish: status %d %s", resp.Status, resp.Error)
	}
	var res PublishResult
	if err := json.Unmarshal(resp.Body, &res); err != nil {
		t.Fatal(err)
	}
	poll := c.call(GRPCMethodPoll, map[string]any{
		"queue": "q", "consumer": "c1", "limit": 10, "tenant_id": "tenant-b",
	})
	if poll.Status == 200 {
		var msgs []Message
		if err := json.Unmarshal(poll.Body, &msgs); err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if m.ID == res.MessageID {
				t.Fatal("a tenant-b poll received a tenant-a message; tenant scoping is not applied on the native poll path")
			}
		}
	}
}

func encodeNativeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}
