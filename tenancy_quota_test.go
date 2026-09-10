package broker

import (
	"context"
	"errors"
	"testing"
)

func TestTenantMessageQuotaIsEnforcedOnPublish(t *testing.T) {
	b := MustNew(Config{Tenancy: &TenancyConfig{Enabled: true, IsolationLevel: IsolationShared, MaxMessagesPerTenant: 1}})
	ctx := context.Background()
	if _, err := b.Publisher().Publish(ctx, "events", []byte("first"), WithQueue("shared"), WithTenantID("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "events", []byte("second"), WithQueue("shared"), WithTenantID("a")); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("second publish error=%v, want quota backpressure", err)
	}
	if _, err := b.Publisher().Publish(ctx, "events", []byte("other tenant"), WithQueue("shared"), WithTenantID("b")); err != nil {
		t.Fatalf("tenant B was affected by tenant A quota: %v", err)
	}
}

func TestTenantPublishRateLimitIsEnforced(t *testing.T) {
	b := MustNew(Config{Tenancy: &TenancyConfig{Enabled: true, IsolationLevel: IsolationShared, RateLimitPerTenant: 1}})
	defer b.TenantManager().Stop()
	ctx := context.Background()
	if _, err := b.Publisher().Publish(ctx, "events", []byte("first"), WithQueue("shared"), WithTenantID("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "events", []byte("second"), WithQueue("shared"), WithTenantID("a")); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("second publish error=%v, want rate limit", err)
	}
}

func TestTenantQueueQuotaAllowsExistingQueueAndRejectsNewQueue(t *testing.T) {
	b := MustNew(Config{Tenancy: &TenancyConfig{Enabled: true, IsolationLevel: IsolationShared, MaxQueuesPerTenant: 1}})
	ctx := context.Background()
	if _, err := b.Publisher().Publish(ctx, "events", []byte("first"), WithQueue("q1"), WithTenantID("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "events", []byte("same queue"), WithQueue("q1"), WithTenantID("a")); err != nil {
		t.Fatalf("existing queue was rejected: %v", err)
	}
	if _, err := b.Publisher().Publish(ctx, "events", []byte("new queue"), WithQueue("q2"), WithTenantID("a")); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("new queue error=%v, want quota backpressure", err)
	}
}
