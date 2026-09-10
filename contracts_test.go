package broker

import (
	"errors"
	"testing"
	"time"
)

func TestPerformanceProfileDefaults(t *testing.T) {
	tests := []struct {
		profile PerformanceProfile
		ack     AckPolicy
		rf      int
	}{
		{ProfileDurable, AckQuorum, 3},
		{ProfileLatency, AckLeader, 3},
		{ProfileThroughput, AckQuorum, 3},
		{ProfileLightweight, AckLeader, 1},
	}
	for _, tt := range tests {
		got, err := DefaultsForProfile(tt.profile)
		if err != nil {
			t.Fatalf("profile %q: %v", tt.profile, err)
		}
		if got.Ack != tt.ack || got.Replication.ReplicationFactor != tt.rf || got.PollInterval <= 0 {
			t.Fatalf("profile %q defaults: %+v", tt.profile, got)
		}
	}
	if _, err := DefaultsForProfile("unknown"); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("expected invalid profile error, got %v", err)
	}
}

func TestBrokerUsesDurableProfileByDefault(t *testing.T) {
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if b.cfg.PerformanceProfile != ProfileDurable || b.cfg.PollInterval != 5*time.Millisecond {
		t.Fatalf("unexpected defaults: profile=%q poll=%s", b.cfg.PerformanceProfile, b.cfg.PollInterval)
	}
}

func TestDistributedModeRequiresExplicitExperimentalOptIn(t *testing.T) {
	_, err := NewDistributed(Config{}, DistributedConfig{})
	if !errors.Is(err, ErrUnsupportedMode) {
		t.Fatalf("expected distributed mode to fail closed, got %v", err)
	}
}

func TestRetryClassification(t *testing.T) {
	class, ok := ClassifyRetry(errors.Join(errors.New("write failed"), ErrStorageUnavailable))
	if !ok || class != RetryStorageUnavailable {
		t.Fatalf("class=%q ok=%t", class, ok)
	}
	detail := &RetryError{Class: RetryNotLeader, Leader: "node-2", LeaderEpoch: 7, Cause: ErrNotLeader}
	class, ok = ClassifyRetry(detail)
	if !ok || class != RetryNotLeader || !errors.Is(detail, ErrNotLeader) {
		t.Fatalf("classified error mismatch: class=%q ok=%t err=%v", class, ok, detail)
	}
}
