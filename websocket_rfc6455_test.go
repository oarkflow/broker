package broker

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// wsTestClient is a minimal RFC 6455 client used to exercise the fh WebSocket
// server: it performs the opening handshake, writes masked frames and reads
// unmasked single-frame server messages.
type wsTestClient struct {
	conn net.Conn
	r    *bufio.Reader
}

// dialWebSocket opens a WebSocket to addr+path. On a rejected handshake it
// returns the HTTP status code and a nil client.
func dialWebSocket(t *testing.T, addr, path string, header map[string]string) (*wsTestClient, int) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	nonce := make([]byte, 16)
	_, _ = rand.Read(nonce)
	key := base64.StdEncoding.EncodeToString(nonce)
	var req strings.Builder
	fmt.Fprintf(&req, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n", path, addr, key)
	for name, v := range header {
		fmt.Fprintf(&req, "%s: %s\r\n", name, v)
	}
	req.WriteString("\r\n")
	if _, err := io.WriteString(conn, req.String()); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	tp := textproto.NewReader(r)
	statusLine, err := tp.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(statusLine)
	if len(fields) < 2 {
		t.Fatalf("malformed status line %q", statusLine)
	}
	status, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatalf("malformed status line %q", statusLine)
	}
	respHeader, err := tp.ReadMIMEHeader()
	if err != nil {
		t.Fatal(err)
	}
	if status != fh.StatusSwitchingProtocols {
		_ = conn.Close()
		return nil, status
	}
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if got := respHeader.Get("Sec-WebSocket-Accept"); got != base64.StdEncoding.EncodeToString(sum[:]) {
		t.Fatalf("Sec-WebSocket-Accept=%q", got)
	}
	return &wsTestClient{conn: conn, r: r}, status
}

func (c *wsTestClient) writeText(payload []byte) error {
	header := []byte{0x81}
	switch n := len(payload); {
	case n < 126:
		header = append(header, 0x80|byte(n))
	case n <= 0xFFFF:
		header = append(header, 0x80|126, byte(n>>8), byte(n))
	default:
		header = append(header, 0x80|127)
		header = binary.BigEndian.AppendUint64(header, uint64(n))
	}
	mask := make([]byte, 4)
	_, _ = rand.Read(mask)
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	_, err := c.conn.Write(append(append(header, mask...), masked...))
	return err
}

func (c *wsTestClient) readMessage() (byte, []byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(c.r, head[:]); err != nil {
		return 0, nil, err
	}
	n := uint64(head[1] & 0x7F)
	switch n {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.r, ext[:]); err != nil {
			return 0, nil, err
		}
		n = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.r, ext[:]); err != nil {
			return 0, nil, err
		}
		n = binary.BigEndian.Uint64(ext[:])
	}
	payload := make([]byte, n)
	_, err := io.ReadFull(c.r, payload)
	return head[0] & 0x0F, payload, err
}

func (c *wsTestClient) Close() { _ = c.conn.Close() }

func TestRFC6455AuthenticationAndTenantPublish(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	acl := NewACL(AdminToken{Name: "tenant-a", Token: "secret", Permissions: []string{PermissionPublish}, Tenants: []string{"a"}})
	server := NewRFC6455Server(b, RFC6455Config{ACL: acl, Path: "/ws", ReadTimeout: time.Second, WriteTimeout: time.Second})
	httpServer := newTestServer(t, server.App())
	addr := strings.TrimPrefix(httpServer.URL, "http://")

	if conn, status := dialWebSocket(t, addr, "/ws", nil); conn != nil || status != fh.StatusUnauthorized {
		if conn != nil {
			conn.Close()
		}
		t.Fatalf("unauthenticated dial: status=%d", status)
	}

	conn, status := dialWebSocket(t, addr, "/ws", map[string]string{"Authorization": "Bearer secret"})
	if conn == nil {
		t.Fatalf("authenticated dial: status=%d", status)
	}
	defer conn.Close()
	body := json.RawMessage(`{"topic":"events","queue":"shared","payload":{"ok":true}}`)
	request, _ := json.Marshal(webSocketRequest{ID: "1", Action: "publish", Body: body})
	if err := conn.writeText(request); err != nil {
		t.Fatal(err)
	}
	opcode, responseData, err := conn.readMessage()
	if err != nil {
		t.Fatal(err)
	}
	if opcode != 0x1 {
		t.Fatalf("opcode=%d", opcode)
	}
	var response webSocketResponse
	if err := json.Unmarshal(responseData, &response); err != nil || !response.OK {
		t.Fatalf("response=%s err=%v", responseData, err)
	}
	if got := server.ConnectionCount(); got != 1 {
		t.Fatalf("connection count=%d, want 1", got)
	}
	ctxA := context.WithValue(context.Background(), adminPrincipalKey{}, AdminPrincipal{Name: "tenant-a", Tenants: []string{"a"}})
	messages, err := b.Store().ListQueue(ctxA, "shared", 10)
	if err != nil || len(messages) != 1 || messages[0].TenantID != "a" {
		t.Fatalf("tenant publish messages=%+v err=%v", messages, err)
	}

	// Unknown actions are rejected by the action router, not dropped.
	unknown, _ := json.Marshal(webSocketRequest{ID: "2", Action: "drop_tables"})
	if err := conn.writeText(unknown); err != nil {
		t.Fatal(err)
	}
	if _, data, err := conn.readMessage(); err != nil || !strings.Contains(string(data), "unknown action") {
		t.Fatalf("unknown action response=%s err=%v", data, err)
	}
}
