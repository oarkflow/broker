package broker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// leaseTenantContext builds a credential restricted to a single tenant, the
// scope the Lease path must enforce.
func leaseTenantContext(t *testing.T, tenant string) context.Context {
	t.Helper()
	return context.WithValue(context.Background(), adminPrincipalKey{}, AdminPrincipal{Name: "t", Tenants: []string{tenant}})
}

func mustQueue(t *testing.T, st Store, name string) {
	t.Helper()
	if err := st.DeclareQueue(context.Background(), QueueConfig{Name: name}); err != nil {
		t.Fatalf("DeclareQueue(%s): %v", name, err)
	}
}

func mustAppend(t *testing.T, st Store, queue, id, tenant string) {
	t.Helper()
	msg := Message{ID: id, TenantID: tenant, Queue: queue, Status: MessageAccepted, CreatedAt: time.Now()}
	if _, err := st.Append(context.Background(), msg, []Destination{{Kind: DestinationQueue, Name: queue}}); err != nil {
		t.Fatalf("Append(%s): %v", id, err)
	}
}

// TestSweepLeasesIsDurable is a regression test for the memory-only sweep path:
// FileWALStore and BinaryFileWALStore used to promote MemoryStore.SweepLeases,
// so an un-lease was lost on crash and the message reappeared as leased.
func TestSweepLeasesIsDurable(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(t *testing.T, dir string) Store
	}{
		{"FileWALStore", func(t *testing.T, dir string) Store {
			st, err := NewFileWALStore(dir)
			if err != nil {
				t.Fatalf("NewFileWALStore: %v", err)
			}
			return st
		}},
		{"BinaryFileWALStore", func(t *testing.T, dir string) Store {
			st, err := NewBinaryFileWALStore(dir)
			if err != nil {
				t.Fatalf("NewBinaryFileWALStore: %v", err)
			}
			return st
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			st := tc.open(t, dir)
			mustQueue(t, st, "q")
			mustAppend(t, st, "q", "m1", "tenant-a")

			leased, err := st.Lease(context.Background(), LeaseRequest{Queue: "q", ConsumerID: "c1", VisibilityTimeout: 10 * time.Millisecond, Limit: 10})
			if err != nil {
				t.Fatalf("Lease: %v", err)
			}
			if len(leased) != 1 {
				t.Fatalf("leased=%d want 1", len(leased))
			}
			time.Sleep(20 * time.Millisecond)
			n, err := st.SweepLeases(context.Background(), time.Now())
			if err != nil {
				t.Fatalf("SweepLeases: %v", err)
			}
			if n != 1 {
				t.Fatalf("swept=%d want 1", n)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Reopen and verify the un-lease survived the crash.
			re := tc.open(t, dir)
			defer func() { _ = re.Close() }()
			msg, ok, err := re.Get(context.Background(), "m1")
			if err != nil || !ok {
				t.Fatalf("Get after reopen: ok=%v err=%v", ok, err)
			}
			if msg.Status != MessageQueued {
				t.Fatalf("status after reopen = %s, want %s (sweep must be durable)", msg.Status, MessageQueued)
			}
			if msg.LeaseToken != "" {
				t.Fatalf("lease token survived sweep: %q", msg.LeaseToken)
			}
			if msg.Redeliveries != 1 {
				t.Fatalf("redeliveries=%d want 1", msg.Redeliveries)
			}
		})
	}
}

// TestControlPlaneOnMissingQueueDoesNotPoisonWAL is a regression test: PauseQueue,
// ResumeQueue and PurgeQueue used to append their WAL record before discovering
// the queue was missing, and FileWALStore recovery treats apply errors as fatal,
// so a typo'd queue name bricked the store on the next restart.
func TestControlPlaneOnMissingQueueDoesNotPoisonWAL(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(t *testing.T, dir string) Store
	}{
		{"FileWALStore", func(t *testing.T, dir string) Store {
			st, err := NewFileWALStore(dir)
			if err != nil {
				t.Fatalf("NewFileWALStore: %v", err)
			}
			return st
		}},
		{"BinaryFileWALStore", func(t *testing.T, dir string) Store {
			st, err := NewBinaryFileWALStore(dir)
			if err != nil {
				t.Fatalf("NewBinaryFileWALStore: %v", err)
			}
			return st
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			st := tc.open(t, dir)
			mustQueue(t, st, "real")
			mustAppend(t, st, "real", "m1", "tenant-a")

			// Every one of these must fail without touching the WAL.
			if err := st.PauseQueue(context.Background(), "typo"); !errors.Is(err, ErrQueueNotFound) {
				t.Fatalf("PauseQueue(typo) err=%v want ErrQueueNotFound", err)
			}
			if err := st.ResumeQueue(context.Background(), "typo"); !errors.Is(err, ErrQueueNotFound) {
				t.Fatalf("ResumeQueue(typo) err=%v want ErrQueueNotFound", err)
			}
			if _, err := st.PurgeQueue(context.Background(), "typo"); !errors.Is(err, ErrQueueNotFound) {
				t.Fatalf("PurgeQueue(typo) err=%v want ErrQueueNotFound", err)
			}
			// And the real queue must still work.
			if err := st.PauseQueue(context.Background(), "real"); err != nil {
				t.Fatalf("PauseQueue(real): %v", err)
			}
			if err := st.ResumeQueue(context.Background(), "real"); err != nil {
				t.Fatalf("ResumeQueue(real): %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Recovery must succeed and keep the appended message.
			re := tc.open(t, dir)
			defer func() { _ = re.Close() }()
			msg, ok, err := re.Get(context.Background(), "m1")
			if err != nil || !ok {
				t.Fatalf("Get after reopen: ok=%v err=%v (recovery must not fail)", ok, err)
			}
			if msg.Queue != "real" {
				t.Fatalf("queue=%q want real", msg.Queue)
			}
		})
	}
}

// TestRecoveryToleratesStaleRecords proves the defense-in-depth layer: even if a
// record for a since-deleted subject reaches the WAL, recovery must not fail.
func TestRecoveryToleratesStaleRecords(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileWALStore(dir)
	if err != nil {
		t.Fatalf("NewFileWALStore: %v", err)
	}
	mustQueue(t, st, "q")
	mustAppend(t, st, "q", "m1", "tenant-a")

	// Bypass the public API to write a record whose subject will not exist at
	// replay time. This is the exact shape the old code produced for a typo.
	if err := st.write(walRecord{Op: "purge_queue", QueueName: "gone-before-replay"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := st.write(walRecord{Op: "update", Message: Message{ID: "never-existed"}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := st.write(walRecord{Op: "ack", Ack: AckRequest{MessageID: "also-missing"}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	re, err := NewFileWALStore(dir)
	if err != nil {
		t.Fatalf("recovery failed on stale records: %v", err)
	}
	defer func() { _ = re.Close() }()
	if _, ok, err := re.Get(context.Background(), "m1"); err != nil || !ok {
		t.Fatalf("Get after recovery: ok=%v err=%v", ok, err)
	}
}

// TestLeaseEnforcesPrincipalTenantScope is a regression test: Lease filtered only
// on the caller-supplied req.TenantID, so a credential scoped to tenant-a could
// lease tenant-b's messages by naming tenant-b in the request.
func TestLeaseEnforcesPrincipalTenantScope(t *testing.T) {
	st := NewMemoryStore()
	mustQueue(t, st, "q")
	mustAppend(t, st, "q", "from-a", "tenant-a")
	mustAppend(t, st, "q", "from-b", "tenant-b")

	ctxA := leaseTenantContext(t, "tenant-a")

	// Asking for the other tenant explicitly must be refused, not honoured.
	if _, err := st.Lease(ctxA, LeaseRequest{Queue: "q", TenantID: "tenant-b", Limit: 10}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-tenant Lease err=%v want ErrUnauthorized", err)
	}
	// Omitting tenant_id resolves to the credential's own tenant.
	got, err := st.Lease(ctxA, LeaseRequest{Queue: "q", Limit: 10})
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("leased=%d want 1", len(got))
	}
	if got[0].ID != "from-a" {
		t.Fatalf("leased %q, want only the caller's own tenant (from-a)", got[0].ID)
	}
	// The same rule must hold on the WAL-facing prepare path: the request is
	// refused outright rather than silently filtered.
	if _, err := st.prepareLease(ctxA, LeaseRequest{Queue: "q", TenantID: "tenant-b", Limit: 10}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("prepareLease cross-tenant err=%v want ErrUnauthorized", err)
	}
}

// TestUpdateBatchSkipsMissingMessages ensures UpdateBatch only WALs entries that
// can actually be applied, so replay never trips over a not-found.
func TestUpdateBatchSkipsMissingMessages(t *testing.T) {
	dir := t.TempDir()
	st, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatalf("NewBinaryFileWALStore: %v", err)
	}
	mustQueue(t, st, "q")
	mustAppend(t, st, "q", "m1", "tenant-a")

	errs := st.UpdateBatch(context.Background(), []Message{{ID: "m1", Queue: "q", TenantID: "tenant-a"}, {ID: "ghost"}})
	if errs == nil || len(errs) != 2 {
		t.Fatalf("UpdateBatch errs=%v want 2 entries", errs)
	}
	if errs[0] != nil {
		t.Fatalf("existing message err=%v want nil", errs[0])
	}
	if !errors.Is(errs[1], ErrMessageNotFound) {
		t.Fatalf("missing message err=%v want ErrMessageNotFound", errs[1])
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	re, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer func() { _ = re.Close() }()
	if _, ok, err := re.Get(context.Background(), "m1"); err != nil || !ok {
		t.Fatalf("Get after reopen: ok=%v err=%v", ok, err)
	}
}

// TestSchemaRegisterRejectsScopedCredentials: schemas are shared config, so a
// tenant-scoped credential must not be able to mutate them.
func TestSchemaRegisterRejectsScopedCredentials(t *testing.T) {
	r := NewMemorySchemaRegistry()
	ctx := leaseTenantContext(t, "tenant-a")
	def := SchemaDefinition{Name: "orders", Version: "v1"}
	if err := r.RegisterContext(ctx, def); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("scoped Register err=%v want ErrUnauthorized", err)
	}
	// Unrestricted setup is still allowed.
	if err := r.Register(def); err != nil {
		t.Fatalf("unrestricted Register: %v", err)
	}
	if _, ok := r.GetSchema("orders", "v1"); !ok {
		t.Fatal("schema should be registered")
	}
}

// TestUpdateRejectsMissingMessage: BinaryFileWALStore.Update used to append the
// WAL record before discovering the message did not exist.
func TestUpdateRejectsMissingMessage(t *testing.T) {
	dir := t.TempDir()
	st, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatalf("NewBinaryFileWALStore: %v", err)
	}
	if err := st.Update(context.Background(), Message{ID: "ghost"}); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("Update(ghost) err=%v want ErrMessageNotFound", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	re, err := NewBinaryFileWALStore(dir)
	if err != nil {
		t.Fatalf("recovery failed on empty WAL: %v", err)
	}
	defer func() { _ = re.Close() }()
}
