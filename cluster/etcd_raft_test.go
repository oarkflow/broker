package cluster

import "testing"

func TestEtcdRaftCoreUsesSafeDefaults(t *testing.T) {
	core, err := NewEtcdRaftCore(EtcdRaftCoreConfig{ID: 1, Peers: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	if core.RawNode == nil || core.Storage == nil || !core.RawNode.HasReady() {
		t.Fatalf("invalid etcd raft core: %+v", core)
	}
}

func TestEtcdRaftCoreRejectsInvalidMembership(t *testing.T) {
	if _, err := NewEtcdRaftCore(EtcdRaftCoreConfig{}); err == nil {
		t.Fatal("zero node ID accepted")
	}
	if _, err := NewEtcdRaftCore(EtcdRaftCoreConfig{ID: 1, Peers: []uint64{1, 1}}); err == nil {
		t.Fatal("duplicate peer ID accepted")
	}
}
