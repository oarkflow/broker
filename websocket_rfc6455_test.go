package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRFC6455AuthenticationAndTenantPublish(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	acl := NewACL(AdminToken{Name: "tenant-a", Token: "secret", Permissions: []string{PermissionPublish}, Tenants: []string{"a"}})
	server := NewRFC6455Server(b, RFC6455Config{ACL: acl, Path: "/ws", ReadTimeout: time.Second, WriteTimeout: time.Second})
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	url := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if conn, resp, err := websocket.Dial(ctx, url, nil); err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		if conn != nil {
			conn.CloseNow()
		}
		t.Fatalf("unauthenticated dial: response=%v err=%v", resp, err)
	}

	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer secret"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	body := json.RawMessage(`{"topic":"events","queue":"shared","payload":{"ok":true}}`)
	request, _ := json.Marshal(webSocketRequest{ID: "1", Action: "publish", Body: body})
	if err := conn.Write(ctx, websocket.MessageText, request); err != nil {
		t.Fatal(err)
	}
	_, responseData, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var response webSocketResponse
	if err := json.Unmarshal(responseData, &response); err != nil || !response.OK {
		t.Fatalf("response=%s err=%v", responseData, err)
	}
	ctxA := context.WithValue(context.Background(), adminPrincipalKey{}, AdminPrincipal{Name: "tenant-a", Tenants: []string{"a"}})
	messages, err := b.Store().ListQueue(ctxA, "shared", 10)
	if err != nil || len(messages) != 1 || messages[0].TenantID != "a" {
		t.Fatalf("tenant publish messages=%+v err=%v", messages, err)
	}
}
