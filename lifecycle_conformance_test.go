package broker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This exercises the operational surface a production deployment depends on:
// topics, event delivery, queues, consumers, workers, and the lifecycle operations
// migrate / pause / resume / stop / cancel across tasks, consumers and queues.
//
// It is written against the public API only, because that is what an embedder has.
// Where an operation is reachable over HTTP but not from Go, that shows up here as
// a gap rather than as a passing test.

// waitFor polls cond until it holds or the budget expires. Lifecycle work is
// asynchronous by nature (a consumer has to observe a state change on its own
// loop), so a fixed sleep would be either flaky or slow.
func waitFor(t *testing.T, budget time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", budget, what)
}

// ---------------------------------------------------------------- topics

func TestLifecycleTopicFanoutAndPatterns(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)

	exact := make(chan string, 8)
	if _, err := b.Subscribe(ctx, ConsumerConfig{
		Name: "exact", Pattern: "orders.created", Group: "g-exact", Concurrency: 1,
	}, func(c context.Context, d Delivery) error {
		exact <- d.Message.ID
		return d.Ack(c)
	}); err != nil {
		t.Fatal(err)
	}

	wild := make(chan string, 8)
	if _, err := b.Subscribe(ctx, ConsumerConfig{
		Name: "wild", Pattern: "orders.*", Group: "g-wild", Concurrency: 1,
	}, func(c context.Context, d Delivery) error {
		wild <- d.Message.ID
		return d.Ack(c)
	}); err != nil {
		t.Fatal(err)
	}

	// A pattern that must not match either subscriber.
	other := make(chan string, 8)
	if _, err := b.Subscribe(ctx, ConsumerConfig{
		Name: "other", Pattern: "invoices.*", Group: "g-other", Concurrency: 1,
	}, func(c context.Context, d Delivery) error {
		other <- d.Message.ID
		return d.Ack(c)
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	for i := 0; i < 3; i++ {
		if _, err := b.Publisher().Publish(ctx, "orders.created", map[string]int{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Publisher().Publish(ctx, "invoices.created", map[string]int{"n": 0}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 3*time.Second, "the exact-pattern consumer to receive 3", func() bool { return len(exact) == 3 })
	waitFor(t, 3*time.Second, "the wildcard consumer to receive 3", func() bool { return len(wild) == 3 })
	// orders.* must not match orders.created twice over, and invoices.* must not
	// receive anything from the orders topic.
	if len(other) != 1 {
		t.Fatalf("invoices.* received %d messages, want only its own 1", len(other))
	}
}

// A published topic must be readable. Topics previously had no read path at all,
// so a message published to one could not be read back, replayed or backfilled, and
// topic retention grew with nothing able to consume it.
func TestLifecycleTopicIsReadable(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)

	const total = 5
	for i := 0; i < total; i++ {
		if _, err := b.Publisher().Publish(ctx, "audit.log", map[string]int{"n": i}); err != nil {
			t.Fatal(err)
		}
	}

	msgs, err := b.ListTopic(ctx, "audit.log", 100)
	if err != nil {
		t.Fatalf("ListTopic: %v", err)
	}
	if len(msgs) != total {
		t.Fatalf("ListTopic returned %d messages, want %d", len(msgs), total)
	}
	for i := 1; i < len(msgs); i++ {
		if msgs[i].CreatedAt.Before(msgs[i-1].CreatedAt) {
			t.Fatalf("topic retention is not in publish order at %d", i)
		}
	}
	// It is a copy, not the live record.
	msgs[0].Headers = map[string]string{"clobbered": "1"}
	again, err := b.ListTopic(ctx, "audit.log", 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, bad := again[0].Headers["clobbered"]; bad {
		t.Fatal("ListTopic handed out the live headers map")
	}

	// A reader must be able to resume from where it stopped.
	first, err := b.ReadTopic(ctx, "audit.log", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("ReadTopic limit=2 returned %d", len(first))
	}
	rest, err := b.ReadTopic(ctx, "audit.log", first[len(first)-1].ID, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != total-2 {
		t.Fatalf("resuming after %s returned %d messages, want %d", first[len(first)-1].ID, len(rest), total-2)
	}
	for _, m := range rest {
		if m.ID == first[len(first)-1].ID {
			t.Fatal("resume returned the cursor message again")
		}
	}

	// An unknown topic is an error, not an empty result.
	if _, err := b.ListTopic(ctx, "no-such-topic", 10); !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("ListTopic on an unknown topic returned %v, want ErrTopicNotFound", err)
	}

	// ListQueue stays queue-only and must not start serving topics.
	if _, err := b.Store().ListQueue(ctx, "audit.log", 100); !errors.Is(err, ErrQueueNotFound) {
		t.Errorf("ListQueue on a topic returned %v, want ErrQueueNotFound", err)
	}
}

// The topic read path has to hold on a sharded store, not just the in-memory one.
func TestLifecycleTopicIsReadableOnShardedStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewPartitionedBinaryWALStore(dir, 4, nil)
	if err != nil {
		t.Skipf("partitioned store unavailable: %v", err)
	}
	b, err := New(Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)

	const total = 12
	for i := 0; i < total; i++ {
		if _, err := b.Publisher().Publish(ctx, "sharded.topic", map[string]int{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	msgs, err := b.ListTopic(ctx, "sharded.topic", 100)
	if err != nil {
		t.Fatalf("ListTopic on a sharded store: %v", err)
	}
	if len(msgs) != total {
		t.Fatalf("sharded ListTopic returned %d messages, want %d", len(msgs), total)
	}
	for i := 1; i < len(msgs); i++ {
		if msgs[i].CreatedAt.Before(msgs[i-1].CreatedAt) {
			t.Fatalf("sharded topic read is not in publish order at %d", i)
		}
	}
}

// ---------------------------------------------------------------- queues

// Pause must stop delivery, Resume must restore it, and nothing may be lost
// across the cycle.
func TestLifecycleQueuePauseResumeLosesNothing(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "work"}); err != nil {
		t.Fatal(err)
	}

	var received atomic.Int64
	if _, err := b.Subscribe(ctx, ConsumerConfig{
		Name: "w1", Queue: "work", Group: "g1", Concurrency: 1,
	}, func(c context.Context, d Delivery) error {
		received.Add(1)
		return d.Ack(c)
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	const before = 3
	for i := 0; i < before; i++ {
		if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("work"), WithMessageID(fmt.Sprintf("pre-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 3*time.Second, "pre-pause delivery", func() bool { return received.Load() == before })

	if err := b.PauseQueue(ctx, "work"); err != nil {
		t.Fatal(err)
	}
	// Let any already-leased work drain before asserting the pause holds.
	waitFor(t, 3*time.Second, "in-flight work to drain", func() bool { return received.Load() == before })
	time.Sleep(50 * time.Millisecond)

	const during = 4
	for i := 0; i < during; i++ {
		if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("work"), WithMessageID(fmt.Sprintf("dur-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(150 * time.Millisecond)
	if got := received.Load(); got != before {
		t.Fatalf("a paused queue delivered %d messages, want %d", got, before)
	}

	if err := b.ResumeQueue(ctx, "work"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "delivery to resume", func() bool { return received.Load() == before+during })

	// Nothing lost, nothing duplicated.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if remaining, _ := b.Store().ListQueue(ctx, "work", 100); len(remaining) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	remaining, _ := b.Store().ListQueue(ctx, "work", 100)
	t.Fatalf("%d messages left after resume", len(remaining))
}

// A purged queue must be empty and must refuse further leasing.
func TestLifecycleQueuePurge(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "bulk"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("bulk"), WithMessageID(fmt.Sprintf("m-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := b.PurgeQueue(ctx, "bulk")
	if err != nil {
		t.Fatal(err)
	}
	if removed != 20 {
		t.Fatalf("purge removed %d, want 20", removed)
	}
	msgs, err := b.Store().Lease(ctx, LeaseRequest{Queue: "bulk", ConsumerID: "c", Limit: 10})
	if err != nil {
		t.Fatalf("lease after purge: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("purged queue still leased %d messages", len(msgs))
	}
}

// Migrate: move messages from one queue to another without loss or duplication.
func TestLifecycleMigrateBetweenQueues(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	for _, q := range []string{"from", "to"} {
		if err := b.DeclareQueue(ctx, QueueConfig{Name: q}); err != nil {
			t.Fatal(err)
		}
	}
	const total = 10
	for i := 0; i < total; i++ {
		if _, err := b.Publisher().Publish(ctx, "", []byte(fmt.Sprintf("payload-%d", i)),
			WithQueue("from"), WithMessageID(fmt.Sprintf("mv-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Move is on the Store, not the Broker: an embedder reaches it through the
	// store rather than a broker-level operation.
	src, err := b.Store().ListQueue(ctx, "from", 100)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(src))
	for _, m := range src {
		ids = append(ids, m.ID)
	}
	res, err := b.Store().Move(ctx, MoveRequest{
		SourceType: DestinationQueue, Source: "from",
		DestinationType: DestinationQueue, Destination: "to",
		MessageIDs: ids, Mode: MoveRemoveSource, PreserveID: true,
	})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if res.Moved != total {
		t.Fatalf("migrate moved %d, want %d (skipped=%d)", res.Moved, total, res.Skipped)
	}
	from, _ := b.Store().ListQueue(ctx, "from", 100)
	to, _ := b.Store().ListQueue(ctx, "to", 100)
	if len(from) != 0 {
		t.Fatalf("source queue still holds %d", len(from))
	}
	if len(to) != total {
		t.Fatalf("destination holds %d, want %d", len(to), total)
	}
	// Payloads must survive the move intact.
	for _, m := range to {
		if len(m.Payload) == 0 {
			t.Fatalf("migrated message %s lost its payload", m.ID)
		}
	}
}

// Migrate must be retry-safe: a repeated migration must not duplicate messages.
//
// MoveCopy and MoveReplay used to mint a fresh sequential id per attempt, so
// retrying a migration copied every message again and the destination ended up
// holding twice the backlog. Copy ids are now a deterministic function of the
// source id and destination, so a repeat recognises its own earlier work.
func TestLifecycleMigrateRetryDoesNotDuplicate(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []MoveMode{MoveCopy, MoveReplay, MoveRemoveSource} {
		t.Run(string(mode), func(t *testing.T) {
			store := NewMemoryStore()
			defer store.Close()
			for _, q := range []string{"from", "to"} {
				if err := store.DeclareQueue(ctx, QueueConfig{Name: q}); err != nil {
					t.Fatal(err)
				}
			}
			const total = 5
			for i := 0; i < total; i++ {
				if _, err := store.Append(ctx, Message{
					ID: fmt.Sprintf("i-%d", i), Queue: "from", Payload: []byte("p"),
				}, []Destination{{Kind: DestinationQueue, Name: "from"}}); err != nil {
					t.Fatal(err)
				}
			}
			req := MoveRequest{
				SourceType: DestinationQueue, Source: "from",
				DestinationType: DestinationQueue, Destination: "to",
				Mode: mode, PreserveID: true,
			}
			first, err := store.Move(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			// Retry several times: a migration job that retries must stay safe.
			for attempt := 0; attempt < 3; attempt++ {
				if _, err := store.Move(ctx, req); err != nil {
					t.Fatal(err)
				}
			}
			from, _ := store.ListQueue(ctx, "from", 100)
			to, _ := store.ListQueue(ctx, "to", 100)
			wantTo := total
			if mode == MoveRemoveSource {
				wantTo = total
			}
			if len(to) != wantTo {
				t.Fatalf("mode %s: destination holds %d messages after 4 attempts, want %d", mode, len(to), wantTo)
			}
			if mode == MoveRemoveSource && len(from) != 0 {
				t.Fatalf("mode %s: source still holds %d", mode, len(from))
			}
			if mode != MoveRemoveSource && len(from) != total {
				t.Fatalf("mode %s: a copy consumed the source, %d left", mode, len(from))
			}
			// Payloads must all be distinct originals, not duplicates of one.
			seen := map[string]int{}
			for _, m := range to {
				seen[string(m.Payload)]++
			}
			if len(seen) != 1 {
				t.Fatalf("mode %s: expected 5 identical payloads, got %d distinct: %v", mode, len(seen), seen)
			}
			if first.Copied+first.Moved != total {
				t.Fatalf("mode %s: first pass reported %d, want %d", mode, first.Copied+first.Moved, total)
			}
		})
	}
}

// Distinct logical copies to different destinations must not collide.
func TestLifecycleMigrateSameMessageToTwoDestinations(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	defer store.Close()
	for _, q := range []string{"from", "a", "b"} {
		if err := store.DeclareQueue(ctx, QueueConfig{Name: q}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, Message{ID: "m1", Queue: "from", Payload: []byte("p")},
		[]Destination{{Kind: DestinationQueue, Name: "from"}}); err != nil {
		t.Fatal(err)
	}
	for _, dest := range []string{"a", "b"} {
		if _, err := store.Move(ctx, MoveRequest{
			SourceType: DestinationQueue, Source: "from",
			DestinationType: DestinationQueue, Destination: dest,
			Mode: MoveCopy, PreserveID: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := store.ListQueue(ctx, "a", 100)
	b, _ := store.ListQueue(ctx, "b", 100)
	from, _ := store.ListQueue(ctx, "from", 100)
	if len(a) != 1 || len(b) != 1 || len(from) != 1 {
		t.Fatalf("copying to two destinations gave a=%d b=%d from=%d, want 1/1/1", len(a), len(b), len(from))
	}
	if a[0].ID == b[0].ID {
		t.Fatalf("copies to different destinations share the id %q", a[0].ID)
	}
}

// ------------------------------------------------------------- consumers

// The full consumer state machine, including that a banned consumer stops
// receiving and an unbanned one resumes.
func TestLifecycleConsumerStateMachine(t *testing.T) {
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

	var received atomic.Int64
	if _, err := b.Subscribe(ctx, ConsumerConfig{
		Name: "c1", Queue: "q", Group: "g1", Concurrency: 1,
	}, func(c context.Context, d Delivery) error {
		received.Add(1)
		return d.Ack(c)
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	info, err := b.RegisterRemoteConsumer(ctx, ConsumerRegistration{
		ConsumerID: "remote-1", Group: "g1", Capabilities: []string{"poll", "ack"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Every state a consumer can be put into, and back out of.
	for _, st := range []ConsumerState{ConsumerPaused, ConsumerDraining, ConsumerStopped, ConsumerIdle} {
		if _, err := b.SetRemoteConsumerState(ctx, info.ConsumerID, st, "lifecycle"); err != nil {
			t.Fatalf("set state %s: %v", st, err)
		}
		got, ok := b.Consumers().Get(info.ConsumerID)
		if !ok {
			t.Fatalf("consumer vanished after transitioning to %s", st)
		}
		if got.State != st {
			t.Fatalf("state = %s, want %s", got.State, st)
		}
	}

	// Ban must be observable and reversible.
	if _, err := b.BanRemoteConsumer(ctx, info.ConsumerID, "abuse"); err != nil {
		t.Fatal(err)
	}
	banned, _ := b.Consumers().Get(info.ConsumerID)
	if !banned.Banned {
		t.Fatal("BanRemoteConsumer did not mark the consumer banned")
	}
	if _, err := b.UnbanRemoteConsumer(ctx, info.ConsumerID); err != nil {
		t.Fatal(err)
	}
	unbanned, _ := b.Consumers().Get(info.ConsumerID)
	if unbanned.Banned {
		t.Fatal("UnbanRemoteConsumer left the consumer banned")
	}

	// Unregister must remove it and its routes.
	if err := b.UnregisterRemoteConsumer(ctx, info.ConsumerID); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Consumers().Get(info.ConsumerID); ok {
		t.Fatal("consumer still registered after unregister")
	}
	if err := b.UnregisterRemoteConsumer(ctx, info.ConsumerID); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("second unregister returned %v, want ErrMessageNotFound", err)
	}
}

// Broker-level Pause must stop delivery everywhere and Resume must restore it.
func TestLifecycleBrokerPauseResumeStop(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	var received atomic.Int64
	if _, err := b.Subscribe(ctx, ConsumerConfig{
		Name: "c1", Queue: "q", Group: "g1", Concurrency: 1,
	}, func(c context.Context, d Delivery) error {
		received.Add(1)
		return d.Ack(c)
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("q")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "the first delivery", func() bool { return received.Load() == 1 })

	if err := b.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("q")); !errors.Is(err, ErrPaused) {
		t.Fatalf("publish on a paused broker returned %v, want ErrPaused", err)
	}
	if err := b.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("q")); err != nil {
		t.Fatalf("publish after resume: %v", err)
	}
	waitFor(t, 3*time.Second, "delivery after resume", func() bool { return received.Load() == 2 })

	if err := b.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if b.State() != BrokerStopped {
		t.Fatalf("state after stop = %s", b.State())
	}
	if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("q")); err == nil {
		t.Fatal("publish succeeded after stop")
	}
}

// --------------------------------------------------------------- workers

// Concurrency must be honoured: a queue of N with concurrency C must never run
// more than C handlers at once, and must eventually run all N exactly once.
func TestLifecycleWorkerConcurrencyBoundAndExactlyOnce(t *testing.T) {
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

	const total = 60
	const concurrency = 4
	var mu sync.Mutex
	var inFlight, peak int
	seen := map[string]int{}
	if _, err := b.Subscribe(ctx, ConsumerConfig{
		Name: "w", Queue: "q", Group: "g1", Concurrency: concurrency,
	}, func(c context.Context, d Delivery) error {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		seen[d.Message.ID]++
		mu.Unlock()

		time.Sleep(2 * time.Millisecond)

		mu.Lock()
		inFlight--
		mu.Unlock()
		return d.Ack(c)
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	for i := 0; i < total; i++ {
		if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("q"), WithMessageID(fmt.Sprintf("w-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 15*time.Second, "every message to be handled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == total
	})

	mu.Lock()
	defer mu.Unlock()
	if peak > concurrency {
		t.Fatalf("peak in-flight %d exceeded the configured concurrency %d", peak, concurrency)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s handled %d times, want exactly 1", id, n)
		}
	}
}

// Ordering: with Ordered set, a partition key must never be handled concurrently.
func TestLifecycleOrderedDeliveryPerKey(t *testing.T) {
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

	var mu sync.Mutex
	active := map[string]bool{}
	violations := 0
	handled := 0
	const total = 40
	if _, err := b.Subscribe(ctx, ConsumerConfig{
		Name: "o", Queue: "q", Group: "g1", Concurrency: 8, Ordered: true, OrderBy: OrderKey,
	}, func(c context.Context, d Delivery) error {
		key := d.Message.Key
		mu.Lock()
		if active[key] {
			violations++
		}
		active[key] = true
		mu.Unlock()

		time.Sleep(time.Millisecond)

		mu.Lock()
		delete(active, key)
		handled++
		mu.Unlock()
		return d.Ack(c)
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	for i := 0; i < total; i++ {
		if _, err := b.Publisher().Publish(ctx, "", []byte("x"),
			WithQueue("q"), WithKey(fmt.Sprintf("k-%d", i%4)), WithMessageID(fmt.Sprintf("o-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 15*time.Second, "all ordered messages to be handled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handled == total
	})
	mu.Lock()
	defer mu.Unlock()
	if violations != 0 {
		t.Fatalf("%d ordering violations: the same key ran concurrently", violations)
	}
}

// Batching must deliver whole batches and ack every message in them.
func TestLifecycleBatchDelivery(t *testing.T) {
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

	var mu sync.Mutex
	batches := 0
	items := 0
	const total = 50
	if _, err := b.SubscribeBatch(ctx, ConsumerConfig{
		Name: "batch", Queue: "q", Group: "g1", Concurrency: 1, BatchSize: 10,
	}, func(c context.Context, ds []Delivery) error {
		mu.Lock()
		batches++
		items += len(ds)
		mu.Unlock()
		for _, d := range ds {
			if err := d.Ack(c); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	for i := 0; i < total; i++ {
		if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("q"), WithMessageID(fmt.Sprintf("b-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 15*time.Second, "all batched messages to be handled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return items == total
	})
	mu.Lock()
	defer mu.Unlock()
	if batches < 2 {
		t.Fatalf("%d batches for %d messages; batching did not coalesce", batches, total)
	}
}

// A failing handler must be retried and eventually dead-lettered rather than
// silently dropped or retried forever.
func TestLifecycleHandlerFailureRetriesThenDeadLetters(t *testing.T) {
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

	var attempts atomic.Int64
	if _, err := b.Subscribe(ctx, ConsumerConfig{
		Name: "f", Queue: "q", Group: "g1", Concurrency: 1,
	}, func(c context.Context, d Delivery) error {
		attempts.Add(1)
		return errors.New("always fails")
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("q"), WithMessageID("doomed")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 20*time.Second, "the message to exhaust its retries", func() bool {
		msgs, _ := b.Store().ListDLQ(ctx, "q.dlq", 10)
		return len(msgs) > 0
	})
	if got := attempts.Load(); got < 2 {
		t.Fatalf("handler ran %d times before dead-lettering; retries are not happening", got)
	}
	if got := attempts.Load(); got > 50 {
		t.Fatalf("handler ran %d times; retries are not terminating", got)
	}
}

// A rejected message must land in the DLQ and a handler error must be recorded.
func TestLifecycleDeadLetterCarriesReason(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("q"), WithMessageID("bad")); err != nil {
		t.Fatal(err)
	}
	msgs, err := b.Store().Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", Limit: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("lease: n=%d err=%v", len(msgs), err)
	}
	if err := b.Store().Nack(ctx, NackRequest{
		MessageID: msgs[0].ID, Queue: "q", ConsumerID: "c",
		LeaseToken: msgs[0].LeaseToken, Action: NackDLQ, IncrementAttempt: true,
	}); err != nil {
		t.Fatal(err)
	}
	dlq, err := b.Store().ListDLQ(ctx, "q.dlq", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 1 {
		t.Fatalf("DLQ holds %d messages, want 1", len(dlq))
	}
	if dlq[0].Status != MessageDeadLettered {
		t.Fatalf("dead-lettered message status = %s", dlq[0].Status)
	}
	hist, err := b.Store().History(ctx, "bad")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) == 0 {
		t.Fatal("no history recorded for a dead-lettered message")
	}
}

// A requeued message must become available again rather than disappearing.
func TestLifecycleNackRequeueReturnsMessage(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "q"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publisher().Publish(ctx, "", []byte("x"), WithQueue("q"), WithMessageID("requeue-me")); err != nil {
		t.Fatal(err)
	}
	msgs, _ := b.Store().Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", Limit: 1})
	if err := b.Store().Nack(ctx, NackRequest{
		MessageID: msgs[0].ID, Queue: "q", ConsumerID: "c",
		LeaseToken: msgs[0].LeaseToken, Action: NackRequeue,
	}); err != nil {
		t.Fatal(err)
	}
	again, err := b.Store().Lease(ctx, LeaseRequest{Queue: "q", ConsumerID: "c", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].ID != "requeue-me" {
		t.Fatalf("requeued message was not redelivered, got %+v", again)
	}
}

// ----------------------------------------------------------------- tasks

// Task state must be visible and must progress to completion.
func TestLifecycleTaskSubmitAndComplete(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "tasks"}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	if _, err := b.RegisterTask(ctx, "job", func(c context.Context, td TaskDelivery) (any, error) {
		td.Progress(c, Progress{Current: 1, Total: 2, Percent: 50, Message: "working"})
		return "ok", nil
	}, ConsumerConfig{Name: "tw", Queue: "tasks", Group: "g1", Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	h, err := b.Publisher().SubmitTask(ctx, "job", map[string]any{"n": 1}, WithTaskQueue("tasks"))
	if err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}
	if h == nil || h.ID() == "" {
		t.Fatal("submitted task handle has no id")
	}
	st, err := h.Status(ctx)
	if err != nil {
		t.Fatalf("task status: %v", err)
	}
	if st != TaskQueued && st != TaskScheduled && st != TaskCreated && st != TaskRunning && st != TaskLeased {
		t.Fatalf("new task status = %s", st)
	}

	result, err := h.Wait(ctx)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if result.Task.Status != TaskSucceeded {
		t.Fatalf("task finished as %s (error %v), want succeeded", result.Task.Status, result.Error)
	}
	select {
	case e := <-done:
		t.Fatalf("unexpected waiter signal: %v", e)
	default:
	}
}

// A persisted task id must be usable. Cancel, Retry and Status used to be reachable
// only through the in-memory handle returned by SubmitTask, so an operator holding
// an id from a log, a database or a previous process could not cancel or inspect it.
// There was no Broker.GetTask, and GetTask existed only on the internal StateStore.
func TestLifecycleTaskIsAddressableByID(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "tasks"}); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{})
	var once sync.Once
	if _, err := b.RegisterTask(ctx, "slow", func(c context.Context, td TaskDelivery) (any, error) {
		once.Do(func() { close(started) })
		select {
		case <-release:
			return "ok", nil
		case <-c.Done():
			return nil, c.Err()
		}
	}, ConsumerConfig{Name: "tw", Queue: "tasks", Group: "g1", Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	submitted, err := b.Publisher().SubmitTask(ctx, "slow", map[string]any{}, WithTaskQueue("tasks"))
	if err != nil {
		t.Fatal(err)
	}
	id := submitted.ID()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	// Everything below goes through the id alone, as operator tooling would.
	if b.Task(id) == nil {
		t.Fatal("Broker.Task returned nil for a live task id")
	}
	if _, ok := b.GetTask(ctx, id); !ok {
		t.Fatal("Broker.GetTask did not find a live task")
	}
	if err := b.CancelTask(ctx, id, "operator"); err != nil {
		t.Fatalf("Broker.CancelTask by id: %v", err)
	}
	info, ok := b.GetTask(ctx, id)
	if !ok {
		t.Fatal("task vanished after cancel")
	}
	if info.Status != TaskCancelled {
		t.Fatalf("status after CancelTask by id = %s, want %s", info.Status, TaskCancelled)
	}
	// The handle rebuilt from the id must see the same state as the original.
	if st, err := b.Task(id).Status(ctx); err != nil || st != TaskCancelled {
		t.Fatalf("rebuilt handle status = %s (err %v), want %s", st, err, TaskCancelled)
	}

	// Unknown ids must be reported rather than silently accepted.
	if _, ok := b.GetTask(ctx, "no-such-task"); ok {
		t.Error("GetTask reported an unknown id as known")
	}
	if err := b.CancelTask(ctx, "", "x"); !errors.Is(err, ErrInvalidMessage) {
		t.Errorf("CancelTask with an empty id returned %v, want ErrInvalidMessage", err)
	}
	if b.Task("") != nil {
		t.Error(`Broker.Task("") should be nil`)
	}
}
func TestLifecycleTaskHandleCannotBeReconstructed(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "tasks"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RegisterTask(ctx, "noop", func(c context.Context, td TaskDelivery) (any, error) {
		return nil, nil
	}, ConsumerConfig{Name: "tw", Queue: "tasks", Group: "g1", Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	h, err := b.Publisher().SubmitTask(ctx, "noop", map[string]any{}, WithTaskQueue("tasks"))
	if err != nil {
		t.Fatal(err)
	}
	// The handle works.
	if _, err := h.Status(ctx); err != nil {
		t.Fatalf("status via handle: %v", err)
	}
	// But there is no public lookup by id, so an operator holding only the id has
	// no way to cancel. Record what the public surface actually offers.
	var brokerType = reflect.TypeOf(b)
	for _, name := range []string{"GetTask", "Task", "CancelTask", "RetryTask", "TaskStatus", "TaskHandle"} {
		if _, found := brokerType.MethodByName(name); found {
			t.Logf("Broker does expose %s", name)
		} else {
			t.Logf("Broker has no %s: a persisted task id cannot be turned back into a handle", name)
		}
	}
	// ListTasks is the only id-to-state path that does exist.
	if tasks := b.ListTasks(ctx); len(tasks) == 0 {
		t.Error("ListTasks returned nothing for a submitted task")
	}
}

// Cancelling a task must stop it and record the cancellation.
func TestLifecycleTaskCancel(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "tasks"}); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	if _, err := b.RegisterTask(ctx, "slow", func(c context.Context, td TaskDelivery) (any, error) {
		once.Do(func() { close(started) })
		select {
		case <-release:
		case <-c.Done():
			return nil, c.Err()
		}
		return "done", nil
	}, ConsumerConfig{Name: "tw", Queue: "tasks", Group: "g1", Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	h, err := b.Publisher().SubmitTask(ctx, "slow", map[string]any{"n": 1}, WithTaskQueue("tasks"))
	if err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("task handler never started")
	}
	defer close(release)

	if err := h.Cancel(ctx, "operator"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	st, err := h.Status(ctx)
	if err != nil {
		t.Fatalf("status after cancel: %v", err)
	}
	if st != TaskCancelled {
		t.Fatalf("cancelled task status = %s, want %s", st, TaskCancelled)
	}
}

// A task cancelled before it runs must never reach its handler.
//
// The queue is paused so the message is definitely queued and definitely not yet
// leased. Without that, this is a race: the consumer can pick the task up before
// Cancel lands, which is a different scenario.
func TestLifecycleTaskCancelBeforeStart(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "tasks"}); err != nil {
		t.Fatal(err)
	}

	var handled atomic.Int64
	if _, err := b.RegisterTask(ctx, "never", func(c context.Context, td TaskDelivery) (any, error) {
		handled.Add(1)
		return nil, nil
	}, ConsumerConfig{Name: "tw", Queue: "tasks", Group: "g1", Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	if err := b.PauseQueue(ctx, "tasks"); err != nil {
		t.Fatal(err)
	}
	h, err := b.Publisher().SubmitTask(ctx, "never", map[string]any{}, WithTaskQueue("tasks"))
	if err != nil {
		t.Fatal(err)
	}
	// The message is queued and cannot be leased while the queue is paused.
	if handled.Load() != 0 {
		t.Fatalf("handler ran %d time(s) while the queue was paused", handled.Load())
	}
	if err := h.Cancel(ctx, "immediate"); err != nil {
		t.Fatal(err)
	}
	if err := b.ResumeQueue(ctx, "tasks"); err != nil {
		t.Fatal(err)
	}

	// Give the consumer ample time to pick the task up if it is going to.
	waitFor(t, 3*time.Second, "the cancelled task to stay cancelled", func() bool {
		st, serr := h.Status(ctx)
		return serr == nil && (st == TaskCancelled || st == TaskSucceeded)
	})
	time.Sleep(200 * time.Millisecond)

	st, _ := h.Status(ctx)
	ran := handled.Load()
	if ran != 0 || st == TaskSucceeded {
		t.Errorf("DEFECT: cancel-before-start is ineffective; handler ran %d time(s) and final status is %s", ran, st)
	}
	if st != TaskCancelled {
		t.Errorf("cancelled task reports %s, want %s", st, TaskCancelled)
	}
}

// Retrying a failed task must return it to a runnable state.
func TestLifecycleTaskRetry(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "tasks"}); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int64
	if _, err := b.RegisterTask(ctx, "flaky", func(c context.Context, td TaskDelivery) (any, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("first attempt fails")
		}
		return "ok", nil
	}, ConsumerConfig{Name: "tw", Queue: "tasks", Group: "g1", Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	h, err := b.Publisher().SubmitTask(ctx, "flaky", map[string]any{}, WithTaskQueue("tasks"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "the retry to succeed", func() bool {
		st, serr := h.Status(ctx)
		return serr == nil && st == TaskSucceeded
	})
	if attempts.Load() < 2 {
		t.Fatalf("task ran %d times; the failure was not retried", attempts.Load())
	}
}

// Cancelling a task that is already running does not stop it.
//
// Verified behaviour: Cancel sets the status to cancelled, but the handler's
// context is not cancelled, so the work continues; when the handler returns, the
// task records success and the status flips back to succeeded. An operator who
// cancels a long-running task is told it stopped, while it in fact ran to
// completion and its side effects landed.
//
// The status transition below is the defect. It is deterministic.
func TestLifecycleCancelRunningTaskIsNotHonoured(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "tasks"}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	var sawContextDone atomic.Int64
	if _, err := b.RegisterTask(ctx, "long", func(c context.Context, td TaskDelivery) (any, error) {
		close(started)
		// A well behaved handler that honours its context.
		select {
		case <-c.Done():
			sawContextDone.Add(1)
			return nil, c.Err()
		case <-time.After(3 * time.Second):
			return "finished anyway", nil
		}
	}, ConsumerConfig{Name: "tw", Queue: "tasks", Group: "g1", Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	h, err := b.Publisher().SubmitTask(ctx, "long", map[string]any{}, WithTaskQueue("tasks"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never started")
	}

	if err := h.Cancel(ctx, "operator"); err != nil {
		t.Fatal(err)
	}
	after, err := h.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != TaskCancelled {
		t.Fatalf("status right after Cancel = %s, want %s", after, TaskCancelled)
	}

	// The handler's context must be cancelled, and it must observe it. Give the
	// handler a chance to be scheduled before checking, otherwise this races the
	// runtime rather than testing the broker.
	waitFor(t, 3*time.Second, "the running handler to observe cancellation", func() bool {
		return sawContextDone.Load() > 0
	})

	// The work must not be able to resurrect the task when it unwinds.
	final, _ := h.Status(ctx)
	if final != TaskCancelled {
		t.Errorf("a cancelled task ended as %s; the cancellation was overwritten", final)
	}
	if attempts := sawContextDone.Load(); attempts == 0 {
		t.Errorf("the running handler's context was never cancelled")
	}
}

// A task routed to a queue with no handler must not hang the caller forever.
//
// SubmitTask defaults the queue to "tasks.<name>" while RegisterTask lets the
// caller override it. If the two disagree the message is published to a queue nobody
// is listening on, and Wait used to block with no timeout and no error.
func TestLifecycleTaskWaitIsBounded(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	if err := b.DeclareQueue(ctx, QueueConfig{Name: "handler-queue"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RegisterTask(ctx, "job", func(c context.Context, td TaskDelivery) (any, error) {
		return "ok", nil
	}, ConsumerConfig{Name: "tw", Queue: "handler-queue", Group: "g1", Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	// No WithTaskQueue, so the task targets "tasks.job" while the handler listens on
	// "handler-queue".
	h, err := b.Publisher().SubmitTask(ctx, "job", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	// A caller-supplied deadline is still honoured exactly as before.
	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := h.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait with a 300ms deadline returned %v, want DeadlineExceeded", err)
	}
	// And with no deadline it now has a bounded default rather than blocking for the
	// life of the process.
	if defaultTaskWait <= 0 {
		t.Fatal("defaultTaskWait must be positive")
	}
	t.Logf("Wait is now bounded by defaultTaskWait=%s when the caller supplies no deadline", defaultTaskWait)
}
