package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"math/big"
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
	peerTLS, err := localPeerTLS(peers)
	if err != nil {
		return fmt.Errorf("configure local Raft peer TLS: %w", err)
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
				ListenAddr: address, WALDir: filepath.Join(root, fmt.Sprintf("node-%d", id)), TLSConfig: peerTLS,
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

// localPeerTLS creates an ephemeral CA and a peer certificate whose SANs cover
// the configured addresses. The example uses the same certificate on each
// local node; production clusters should issue a distinct certificate per node.
func localPeerTLS(peers map[uint64]string) (*tls.Config, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate local CA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate local CA serial: %w", err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "broker complete example CA"},
		NotBefore:    now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create local CA certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("parse local CA certificate: %w", err)
	}

	peerKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate local peer key: %w", err)
	}
	serial, err = rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate local peer serial: %w", err)
	}
	peerTemplate := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "broker complete example peer"},
		NotBefore:    now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	for _, address := range peers {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("parse peer address %q: %w", address, err)
		}
		if ip := net.ParseIP(host); ip != nil {
			peerTemplate.IPAddresses = append(peerTemplate.IPAddresses, ip)
		} else {
			peerTemplate.DNSNames = append(peerTemplate.DNSNames, host)
		}
	}
	peerDER, err := x509.CreateCertificate(rand.Reader, peerTemplate, caCert, &peerKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create local peer certificate: %w", err)
	}
	cert := tls.Certificate{Certificate: [][]byte{peerDER, caDER}, PrivateKey: peerKey}
	cert.Leaf, err = x509.ParseCertificate(peerDER)
	if err != nil {
		return nil, fmt.Errorf("parse local peer certificate: %w", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(caCert)
	return &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: roots, ClientCAs: roots,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
	}, nil
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
