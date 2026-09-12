package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/oarkflow/broker"
)

func runDistributed(args []string) error {
	flags := flag.NewFlagSet("distributed", flag.ContinueOnError)
	dataDir := flags.String("data", "", "cluster data directory; empty uses a temporary directory")
	addressesFlag := flags.String("addresses", "", "comma-separated addresses for three Raft nodes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	addresses := strings.Split(*addressesFlag, ",")
	if *addressesFlag == "" {
		var err error
		addresses, err = freeLoopbackAddresses(3)
		if err != nil {
			return err
		}
	}
	if len(addresses) != 3 {
		return fmt.Errorf("distributed example requires exactly three addresses")
	}
	peers := make(map[uint64]string, len(addresses))
	for i, address := range addresses {
		peers[uint64(i+1)] = strings.TrimSpace(address)
	}

	root := *dataDir
	remove := false
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "broker-complete-cluster-")
		if err != nil {
			return err
		}
		remove = true
	}
	if remove {
		defer os.RemoveAll(root)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	nodes := make([]*broker.DistributedBroker, 0, 3)
	defer func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			stopCtx, stop := stopContext()
			_ = nodes[i].Stop(stopCtx)
			stop()
		}
	}()
	for i, address := range addresses {
		id := uint64(i + 1)
		node, err := broker.NewDistributed(
			broker.Config{Name: fmt.Sprintf("complete-node-%d", id), NodeID: address, Store: broker.NewMemoryStore(), PollInterval: 2 * time.Millisecond},
			broker.DistributedConfig{
				UseEtcdRaft: true, RaftNodeID: id, RaftPeers: peers,
				ListenAddr: address, WALDir: filepath.Join(root, fmt.Sprintf("node-%d", id)),
				SubmitTimeout: 5 * time.Second, AllowStaleReads: true,
				SnapshotEntries: 4, SnapshotCatchUpEntries: 2,
			},
		)
		if err != nil {
			return fmt.Errorf("create node %d: %w", id, err)
		}
		nodes = append(nodes, node)
		if err := node.Start(ctx); err != nil {
			return fmt.Errorf("start node %d: %w", id, err)
		}
	}

	var leader *broker.DistributedBroker
	for leader == nil {
		for _, node := range nodes {
			if node.IsLeader() {
				leader = node
				break
			}
		}
		if leader != nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("elect leader: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err := leader.DeclareQueue(ctx, broker.QueueConfig{Name: "cluster.jobs"}); err != nil {
		return fmt.Errorf("replicate queue declaration: %w", err)
	}
	result, err := leader.Publisher().Publish(ctx, "", map[string]string{"job": "replicated"}, broker.WithMessageID("cluster-message-1"), broker.WithQueue("cluster.jobs"), broker.WithConfirmLevel(broker.ConfirmReplicated))
	if err != nil {
		return fmt.Errorf("replicated publish: %w", err)
	}

	for {
		converged := 0
		for _, node := range nodes {
			message, ok, readErr := node.Store().Get(ctx, result.MessageID)
			if readErr == nil && ok && message.Queue == "cluster.jobs" {
				converged++
			}
		}
		if converged == len(nodes) {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for follower convergence: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}

	states := make([]string, 0, len(nodes))
	for i, node := range nodes {
		states = append(states, fmt.Sprintf("%s=%s", addresses[i], node.RaftState()))
	}
	sort.Strings(states)
	fmt.Printf("distributed complete: leader=%s message=%s replicas=%d states=%v data=%s\n", leader.Leader(), result.MessageID, len(nodes), states, root)
	return nil
}

func freeLoopbackAddresses(count int) ([]string, error) {
	listeners := make([]net.Listener, 0, count)
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	addresses := make([]string, 0, count)
	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("reserve loopback address: %w", err)
		}
		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
	}
	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			return nil, err
		}
	}
	listeners = nil
	return addresses, nil
}
