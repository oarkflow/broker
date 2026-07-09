package broker

import (
	"context"
	"testing"
)

func TestClusterAssignmentAndDrain(t *testing.T) {
	c := NewCluster("n1")
	ctx := context.Background()
	if err := c.Join(ctx, ClusterNode{ID: "n2"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Join(ctx, ClusterNode{ID: "n3"}); err != nil {
		t.Fatal(err)
	}
	assignments, err := c.Assign("events", 6, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 6 {
		t.Fatalf("expected 6 assignments, got %d", len(assignments))
	}
	if assignments[0].Leader != "n1" || len(assignments[0].Replicas) != 2 {
		t.Fatalf("unexpected first assignment: %#v", assignments[0])
	}
	if c.Leader() != "n1" || !c.IsLeader() {
		t.Fatalf("unexpected cluster leader/self: leader=%s self=%s", c.Leader(), c.Self())
	}
	if err := c.Drain(ctx, "n1"); err != nil {
		t.Fatal(err)
	}
	if c.Leader() != "n2" {
		t.Fatalf("expected n2 leader after n1 drain, got %s", c.Leader())
	}
}
