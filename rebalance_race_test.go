package broker

import (
	"context"
	"sync"
	"testing"
)

// Several consumers joining groups at once on a fresh broker must not race on
// the lazily created rebalancer (run with -race).
func TestGroupRebalancerConcurrentFirstUse(t *testing.T) {
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.ConsumerJoinGroup(context.Background(), "c", "g")
		}()
	}
	wg.Wait()
	if b.GroupRebalancer() == nil {
		t.Fatal("rebalancer missing")
	}
}

// Unregistering one remote consumer must remove only that consumer's routes. A
// blanket sweep of every "remote-" route cut off the other consumers' subscriptions.
func TestUnregisteringOneRemoteConsumerKeepsTheOthersRoutes(t *testing.T) {
	ctx := context.Background()
	b, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(ctx)
	for _, id := range []string{"first", "second"} {
		if _, err := b.RegisterRemoteConsumer(ctx, ConsumerRegistration{ConsumerID: id, Group: "g-" + id, Capabilities: []string{"poll"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := b.RemoteConsumerSubscribe(ctx, id, RemoteSubscription{Type: "pattern", Pattern: "events." + id, AutoCreate: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.UnregisterRemoteConsumer(ctx, "first"); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, r := range b.Router().Routes() {
		names = append(names, r.Name)
	}
	if len(names) != 1 || names[0] != "remote-sub.second.events.second" {
		t.Fatalf("only the second consumer's route should remain, got %v", names)
	}
}
