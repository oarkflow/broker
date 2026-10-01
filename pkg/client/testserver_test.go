package client

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

// newTestBrokerServer starts a real in-process broker behind an httptest
// server using the admin HTTP data-plane, mirroring the production wire
// protocol the client package speaks. It declares the given queues before
// returning. Tests get back the broker (for direct assertions) and the base
// URL to point a Client/Worker at.
func newTestBrokerServer(t *testing.T, queues ...string) (*broker.Broker, string) {
	t.Helper()
	b := broker.MustNew(broker.Config{
		DefaultVisibility:     50 * time.Millisecond,
		PollInterval:          time.Millisecond,
		LeaseSweepInterval:    5 * time.Millisecond,
		EnableTaskTracking:    true,
		EnableNotifications:   true,
		EnableRequestResponse: true,
	})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	for _, q := range queues {
		if err := b.DeclareQueue(context.Background(), broker.QueueConfig{Name: q, VisibilityTimeout: 50 * time.Millisecond}); err != nil {
			t.Fatal(err)
		}
	}
	admin := broker.AdminServer{Broker: b, InsecureDev: true}
	srv := httptest.NewServer(admin.Handler())
	t.Cleanup(srv.Close)
	return b, srv.URL
}
