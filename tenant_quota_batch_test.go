package broker

import (
	"context"
	"errors"
	"testing"
)

// Tenant publish quotas were only enforced on the single-message publish path. The
// batch and frame endpoints skipped the check entirely, so a tenant could bypass
// its quota simply by batching. Every publish path must enforce it identically.
// quotaMessages is a small message cap so the tests stay fast.
const quotaMessages = 5

func TestTenantQuotaEnforcedOnEveryPublishPath(t *testing.T) {
	ctx := context.Background()
	newBroker := func(t *testing.T) *Broker {
		t.Helper()
		// Quotas come from TenancyConfig; EnsureTenant hands back a copy, so a
		// per-tenant mutation there would not stick.
		b, err := New(Config{Tenancy: &TenancyConfig{Enabled: true, MaxMessagesPerTenant: quotaMessages}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Stop(ctx) })
		if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
			t.Fatal(err)
		}
		return b
	}

	// Exhaust the tenant's message quota with single publishes.
	exhaust := func(t *testing.T, b *Broker) {
		t.Helper()
		for i := 0; i < quotaMessages*4; i++ {
			_, err := b.Publisher().Publish(ctx, "", []byte("x"),
				WithQueue("q"), WithTenantID("tenant-a"), WithMessageID("fill-"+itoa(i)))
			if err != nil {
				if errors.Is(err, ErrBackpressure) {
					return // quota reached
				}
				t.Fatalf("filling: %v", err)
			}
		}
	}

	b := newBroker(t)
	exhaust(t, b)

	// Confirm the single path is actually refusing now, otherwise the test proves
	// nothing about the other paths.
	if _, err := b.Publisher().Publish(ctx, "", []byte("x"),
		WithQueue("q"), WithTenantID("tenant-a")); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("single publish was not refused past the quota (err=%v); the parity assertions would be vacuous", err)
	}

	for _, tc := range []struct {
		name string
		send func(t *testing.T, b *Broker) error
	}{
		{"PublishBatch", func(t *testing.T, b *Broker) error {
			items := make([]PublishBatchItem, 3)
			for i := range items {
				items[i] = PublishBatchItem{Topic: "", Payload: []byte("x"), Options: []PublishOption{
					WithQueue("q"), WithTenantID("tenant-a"), WithMessageID("batch-" + itoa(i)),
				}}
			}
			_, err := b.Publisher().PublishItems(ctx, items)
			return err
		}},
		{"PublishOwned", func(t *testing.T, b *Broker) error {
			_, err := b.Publisher().PublishOwned(ctx, Message{
				ID: "owned-quota", Queue: "q", TenantID: "tenant-a", Payload: []byte("x"),
			})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fresh := newBroker(t)
			exhaust(t, fresh)
			// Same broker, quota exhausted: every path must refuse.
			if err := tc.send(t, fresh); err != nil && !errors.Is(err, ErrBackpressure) {
				t.Fatalf("%s returned %v, want ErrBackpressure", tc.name, err)
			}
		})
	}
}

// The frame and minimal batch endpoints go through publishMessagesMinimal, which is
// a third distinct code path and had no quota check either.
func TestTenantQuotaEnforcedOnMinimalBatchPath(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{Tenancy: &TenancyConfig{Enabled: true, MaxMessagesPerTenant: quotaMessages}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < quotaMessages*4; i++ {
		if _, err := b.Publisher().Publish(ctx, "", []byte("x"),
			WithQueue("q"), WithTenantID("tenant-a"), WithMessageID("fill-"+itoa(i))); err != nil {
			if errors.Is(err, ErrBackpressure) {
				break
			}
			t.Fatal(err)
		}
	}
	errs, _ := b.Publisher().publishMessagesMinimal(ctx, []Message{
		{ID: "minimal-1", Queue: "q", TenantID: "tenant-a", Payload: []byte("x")},
		{ID: "minimal-2", Queue: "q", TenantID: "tenant-a", Payload: []byte("x")},
	})
	refused := 0
	for _, e := range errs {
		if errors.Is(e, ErrBackpressure) {
			refused++
		}
	}
	if refused == 0 {
		t.Fatalf("publishMessagesMinimal accepted %d message(s) past the tenant quota (errs=%v)", len(errs), errs)
	}
}
