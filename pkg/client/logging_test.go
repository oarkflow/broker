package client

import (
	"errors"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

// TestLoggingHooksWith exercises every hook returned by LoggingHooksWith to
// make sure they build their structured log attributes and call into the
// logger without panicking, for both the happy path and error-carrying
// events.
func TestLoggingHooksWith(t *testing.T) {
	for _, prefix := range []string{"", "custom"} {
		h := LoggingHooksWith(prefix)

		ev := Event{ConsumerID: "c1", Group: "g1", State: broker.ConsumerReady, Time: time.Now()}
		evErr := Event{ConsumerID: "c1", Group: "g1", Error: errors.New("boom"), Time: time.Now()}
		msgEv := MessageEvent{Message: broker.Message{ID: "m1", Topic: "t", Type: "ty", Attempts: 2, LeaseUntil: time.Now()}, Queue: "q", Time: time.Now()}
		msgEvErr := MessageEvent{Message: broker.Message{ID: "m1"}, Queue: "q", Error: errors.New("fail"), Time: time.Now()}
		notifyEv := NotificationEvent{Notification: broker.Notification{Type: "n", Severity: broker.SeverityInfo, MessageID: "m1", TaskID: "t1", Queue: "q", Topic: "top", Data: map[string]any{"k": "v"}}, Time: time.Now()}

		calls := []func(){
			func() { h.OnConnecting(ev) },
			func() { h.OnConnected(ev) },
			func() { h.OnAuthenticated(ev) },
			func() { h.OnRegistered(ev) },
			func() { h.OnSubscribed(ev) },
			func() { h.OnPollStart(ev) },
			func() { h.OnHeartbeat(ev) },
			func() { h.OnReconnect(ev) },
			func() { h.OnReconnect(evErr) },
			func() { h.OnDrain(ev) },
			func() { h.OnStopped(ev) },
			func() { h.OnNotification(notifyEv) },
			func() {
				if err := h.BeforeHandle(msgEv); err != nil {
					t.Fatalf("BeforeHandle returned error: %v", err)
				}
			},
			func() { h.AfterHandle(msgEv) },
			func() { h.OnHandlerError(msgEvErr) },
			func() { h.OnAck(msgEv) },
			func() { h.OnNack(msgEvErr) },
			func() { h.OnReject(msgEvErr) },
			func() { h.OnProgress(msgEv) },
			func() { h.OnLeaseExtended(msgEv) },
		}
		for i, fn := range calls {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("prefix=%q call[%d] panicked: %v", prefix, i, r)
					}
				}()
				fn()
			}()
		}
	}
}

func TestLoggingHooksDefaultsPrefix(t *testing.T) {
	// Just confirm an empty prefix doesn't blow up building attrs; the
	// behavior difference (default "worker" prefix) isn't independently
	// observable, so we only assert no panic occurs.
	h := LoggingHooks("")
	h.OnConnecting(Event{})
}

func TestStrAttrEmpty(t *testing.T) {
	a := strAttr("k", "")
	if a.Key != "" {
		t.Fatalf("expected zero-value attr for empty value, got %+v", a)
	}
}

func TestFormatTime(t *testing.T) {
	if got := formatTime(time.Time{}); got != "" {
		t.Fatalf("expected empty string for zero time, got %q", got)
	}
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	if got := formatTime(ts); got != ts.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected formatted time: %q", got)
	}
}
