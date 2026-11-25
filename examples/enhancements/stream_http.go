package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/oarkflow/fh"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/pkg/client"
)

func streamHTTPExample(ctx context.Context, dir string) error {
	store, err := broker.NewSegmentedWALStore(broker.SegmentedWALConfig{Dir: dir, DisableCompactor: true})
	if err != nil {
		return err
	}
	return withBroker(ctx, store, func(b *broker.Broker) error {
		if _, err := b.Publisher().Publish(ctx, "input", "order", broker.WithStream("input"), broker.WithTenantID("tenant-a")); err != nil {
			return err
		}
		// Local loopback fixture: no external broker or credentials are required.
		acl := broker.NewACL(
			broker.AdminToken{Token: "worker-a", Tenants: []string{"tenant-a"}, Permissions: []string{broker.PermissionStreamCommit, broker.PermissionPublish, broker.PermissionStreamRead}},
			broker.AdminToken{Token: "commit-only", Permissions: []string{broker.PermissionStreamCommit}},
			broker.AdminToken{Token: "no-commit", Permissions: []string{broker.PermissionPublish, broker.PermissionStreamRead, broker.PermissionStreamWrite}},
		)
		serverURL, stopServer, err := serveAdmin(broker.AdminServer{Broker: b, ACL: acl})
		if err != nil {
			return err
		}
		defer stopServer()
		c := client.New(client.Config{BrokerURL: serverURL, Token: "worker-a"})
		binaryPayload := []byte{0, 255, 1, 254}
		encoded, err := json.Marshal(base64.StdEncoding.EncodeToString(binaryPayload))
		if err != nil {
			return err
		}
		req := broker.StreamTransactionHTTPPayload{ConsumerGroup: "billing", ExpectedOffset: 0, Offset: 1, Messages: []broker.PublishHTTPPayload{{Topic: "billed", Queue: "jobs", Payload: encoded, PayloadEncoding: "base64"}}}
		results, err := c.CommitStreamTransaction(ctx, "input", req)
		if err != nil || len(results) != 1 || results[0].Confirmed != broker.ConfirmPersisted {
			return fmt.Errorf("HTTP results=%v, error=%v", results, err)
		}
		message, exists, err := store.Get(ctx, results[0].MessageID)
		if err != nil || !exists || !bytes.Equal(message.Payload, binaryPayload) || message.TenantID != "tenant-a" {
			return fmt.Errorf("binary/tenant output=%+v, error=%v", message, err)
		}
		_, err = c.CommitStreamTransaction(ctx, "input", req)
		if err := expectError(err, broker.ErrOffsetConflict); err != nil {
			return err
		}
		body, err := json.Marshal(req)
		if err != nil {
			return err
		}
		for _, denied := range []struct{ token, path string }{
			{"commit-only", "/v1/streams/input/transactions"},
			{"no-commit", "/v1/streams/input/transactions/"},
			{"no-commit", "/streams/input/transactions/"},
		} {
			status, err := postExampleJSON(ctx, serverURL+denied.path, denied.token, body)
			if err != nil || status != fh.StatusForbidden {
				return fmt.Errorf("permission check status=%d, error=%v", status, err)
			}
		}
		status, err := postExampleJSON(ctx, serverURL+"/v1/streams/input/transactions", "worker-a", []byte(`{"consumer_group":"billing","expected_offset":1,"offset":2,"unexpected":true}`))
		if err != nil || status != fh.StatusBadRequest {
			return fmt.Errorf("unknown field status=%d, error=%v", status, err)
		}
		fmt.Println("  Go client: persisted binary output, inherited tenant and typed offset conflict")
		fmt.Println("  HTTP: both permissions required, including trailing-slash paths; unknown fields rejected")
		return nil
	})
}

var exampleHTTPClient = broker.NewHTTPClient(broker.HTTPClientConfig{Timeout: 10 * time.Second})

func postExampleJSON(ctx context.Context, url, token string, body []byte) (int, error) {
	resp, err := broker.SendHTTP(ctx, exampleHTTPClient, fh.MethodPOST, url, body, map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + token,
	})
	if err != nil {
		return 0, err
	}
	defer resp.Close()
	_, err = io.Copy(io.Discard, resp.Reader())
	return resp.StatusCode(), err
}

// serveAdmin serves admin on an ephemeral loopback port and returns its base URL
// and a stop function.
func serveAdmin(admin broker.AdminServer) (string, func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	app := admin.App()
	go func() { _ = broker.ServeHTTPApp(app, ln, nil) }()
	stop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = broker.ShutdownHTTPApp(ctx, app)
	}
	return "http://" + ln.Addr().String(), stop, nil
}
