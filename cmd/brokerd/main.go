package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/amqp"
	"github.com/oarkflow/broker/cluster"
	"github.com/oarkflow/broker/contrib/sqlitestore"
	"github.com/oarkflow/broker/pkg/logger"
	"github.com/oarkflow/zlog"
)

// version is set for release builds with -ldflags -X main.version=<tag>.
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print brokerd build version and exit")
	addr := flag.String("addr", env("BROKER_ADDR", ":8090"), "HTTP listen address")
	profileName := flag.String("profile", env("BROKER_PROFILE", string(broker.ProfileDurable)), "performance profile: durable, latency, throughput, lightweight")
	nativeAddr := flag.String("native-addr", env("BROKER_NATIVE_ADDR", ""), "native BST1 stream listen address; empty disables; requires -token or mapped mTLS identity unless --insecure-dev")
	nativeRPCAddr := flag.String("native-rpc-addr", env("BROKER_NATIVE_RPC_ADDR", ""), "native JSON/TCP RPC listen address; empty disables")
	grpcAddr := flag.String("grpc-addr", env("BROKER_GRPC_ADDR", ""), "protobuf/HTTP2 gRPC listen address; empty disables")
	mqttAddr := flag.String("mqtt-addr", env("BROKER_MQTT_ADDR", ""), "MQTT 3.1.1 listen address; empty disables")
	amqpAddr := flag.String("amqp-addr", env("BROKER_AMQP_ADDR", ""), "AMQP 0-9-1 listen address; empty disables")
	amqpUsername := flag.String("amqp-username", env("BROKER_AMQP_USERNAME", ""), "AMQP PLAIN username")
	amqpPassword := flag.String("amqp-password", env("BROKER_AMQP_PASSWORD", ""), "AMQP PLAIN password")
	sseAddr := flag.String("sse-addr", env("BROKER_SSE_ADDR", ""), "SSE data-plane listen address; empty disables")
	ssePath := flag.String("sse-path", env("BROKER_SSE_PATH", "/events"), "SSE endpoint path")
	webSocketAddr := flag.String("websocket-addr", env("BROKER_WEBSOCKET_ADDR", ""), "RFC 6455 WebSocket listen address; empty disables")
	webSocketPath := flag.String("websocket-path", env("BROKER_WEBSOCKET_PATH", "/ws"), "RFC 6455 WebSocket endpoint path")
	nativeDir := flag.String("native-dir", env("BROKER_NATIVE_DIR", ""), "native segmented stream directory; defaults to <wal>/native-stream")
	nativePartitions := flag.Int("native-partitions", int(envInt64("BROKER_NATIVE_PARTITIONS", 8)), "native stream partitions")
	nativeSegmentBytes := flag.Int64("native-segment-bytes", envInt64("BROKER_NATIVE_SEGMENT_BYTES", 1<<30), "native stream segment size")
	nativeCommitDelay := flag.Duration("native-commit-delay", 750*time.Microsecond, "native stream group commit delay")
	nativeCommitBytes := flag.Int("native-commit-bytes", int(envInt64("BROKER_NATIVE_COMMIT_BYTES", 4<<20)), "native stream group commit byte target")
	nativeSync := flag.Bool("native-sync", !envBool("BROKER_NATIVE_NO_SYNC"), "fdatasync grouped native stream appends before acknowledgement")
	storeKind := flag.String("store", env("BROKER_STORE", ""), "message store: memory, wal, sqlite; empty auto-selects wal when -wal is set")
	sqliteDriver := flag.String("sqlite-driver", env("BROKER_SQLITE_DRIVER", "sqlite"), "database/sql driver for sqlite message store")
	sqliteDSN := flag.String("sqlite-dsn", env("BROKER_SQLITE_DSN", "./.broker-data/broker-messages.sqlite"), "SQLite DSN for message store")
	wal := flag.String("wal", env("BROKER_WAL", ""), "WAL directory; empty uses in-memory store")
	walFormat := flag.String("wal-format", env("BROKER_WAL_FORMAT", "binary"), "WAL format: binary or json")
	walShards := flag.Int("wal-shards", int(envInt64("BROKER_WAL_SHARDS", 1)), "independent binary WAL shards; use separate volumes for best scaling")
	walSegmentSize := flag.Int64("wal-segment-size", envInt64("BROKER_WAL_SEGMENT_SIZE", 64<<20), "maximum BWL2 segment size in bytes before rotating")
	walMaxSegments := flag.Int("wal-max-segments", int(envInt64("BROKER_WAL_MAX_SEGMENTS", 10)), "maximum active BWL2 segments before compaction")
	walRetention := flag.Duration("wal-retention", time.Duration(envInt64("BROKER_WAL_RETENTION_SEC", 0))*time.Second, "BWL2: age after which a closed, checkpointed segment is eligible for deletion; 0 disables age-based retention")
	walCompactionInterval := flag.Duration("wal-compaction-interval", time.Duration(envInt64("BROKER_WAL_COMPACTION_INTERVAL_SEC", 0))*time.Second, "BWL2: how often the background compactor runs; 0 uses the default (5m)")
	frameSigningKey := flag.String("frame-signing-key", env("BROKER_FRAME_SIGNING_KEY", ""), "optional FWB frame and BWL1 WAL HMAC signing key")
	frameEncryptionKey := flag.String("frame-encryption-key", env("BROKER_FRAME_ENCRYPTION_KEY", ""), "optional FWB frame and BWL1 WAL AES-GCM encryption key")
	walDecryptionKeys := flag.String("wal-decryption-keys", env("BROKER_WAL_DECRYPTION_KEYS", ""), "comma-separated previous raw WAL encryption keys accepted for BWL1/BWL2 recovery; new records use -frame-encryption-key")
	frameCompress := flag.Int("frame-compress-threshold", int(envInt64("BROKER_FRAME_COMPRESS_THRESHOLD", 0)), "gzip FWB payloads and BWL1 records at or above this size; 0 disables")
	codecType := flag.String("codec", env("BROKER_CODEC", "fwb1"), "frame codec: fwb1 (string keys) or fwb2 (compact numeric tags)")
	token := flag.String("token", env("BROKER_TOKEN", ""), "required bearer token for admin/API unless --insecure-dev is set")
	readToken := flag.String("read-token", env("BROKER_READ_TOKEN", ""), "optional read-only bearer token")
	workerToken := flag.String("worker-token", env("BROKER_WORKER_TOKEN", ""), "optional remote worker bearer token with consumer/message/task permissions")
	publisherToken := flag.String("publisher-token", env("BROKER_PUBLISHER_TOKEN", ""), "optional publisher-only bearer token")
	queue := flag.String("queue", env("BROKER_DEFAULT_QUEUE", "default"), "default queue")
	insecureDev := flag.Bool("insecure-dev", envBool("BROKER_INSECURE_DEV"), "disable authentication for local development only")
	tlsCert := flag.String("tls-cert", env("BROKER_TLS_CERT", ""), "TLS certificate file")
	tlsKey := flag.String("tls-key", env("BROKER_TLS_KEY", ""), "TLS private key file")
	clientCA := flag.String("client-ca", env("BROKER_CLIENT_CA", ""), "client CA PEM for mTLS")
	requireClientCert := flag.Bool("require-client-cert", envBool("BROKER_REQUIRE_CLIENT_CERT"), "require and verify client certificates")
	clientCertIdentities := flag.String("client-cert-identities", env("BROKER_CLIENT_CERT_IDENTITIES", ""), "comma-separated verified certificate identities to map to an ACL principal (cn:, dns:, uri:, email:, ip:)")
	clientCertPermissions := flag.String("client-cert-permissions", env("BROKER_CLIENT_CERT_PERMISSIONS", broker.PermissionBrokerRead), "comma-separated ACL permissions for mapped client certificates")
	maxBody := flag.Int64("max-body", envInt64("BROKER_MAX_BODY", 1<<20), "maximum HTTP request body size in bytes")
	quiet := flag.Bool("quiet", envBool("BROKER_QUIET"), "disable live broker event logs")
	debugEvents := flag.Bool("debug-events", envBool("BROKER_DEBUG_EVENTS"), "include debug heartbeat/lease-extension events in live logs")
	messageHistory := flag.Int("message-history", int(envInt64("BROKER_MESSAGE_HISTORY", 0)), "message history entries retained per message; 0 disables hot-path history")
	notificationHistory := flag.Int("notification-history", int(envInt64("BROKER_NOTIFICATION_HISTORY", 0)), "notification events retained in memory; 0 disables retention")
	compactBacklog := flag.Bool("compact-backlog", envBool("BROKER_COMPACT_BACKLOG"), "hold queued, never-leased messages in a compact encoded form: about half the memory and a fraction of the GC cost for large backlogs")
	retainCompleted := flag.Bool("retain-completed", envBool("BROKER_RETAIN_COMPLETED"), "retain completed message bodies in memory after durable acknowledgement")
	publisherTracking := flag.Bool("publisher-tracking", envBool("BROKER_PUBLISHER_TRACKING"), "retain per-publisher counters/history")
	consumerEventTracking := flag.Bool("consumer-event-tracking", envBool("BROKER_CONSUMER_EVENT_TRACKING"), "emit per-message consumer events")
	otlpEndpoint := flag.String("otlp-endpoint", env("BROKER_OTLP_ENDPOINT", ""), "OpenTelemetry Protocol collector endpoint (e.g. http://localhost:4318)")
	otlpInterval := flag.Duration("otlp-interval", time.Duration(envInt64("BROKER_OTLP_INTERVAL_SEC", 15))*time.Second, "OpenTelemetry metrics export interval")
	clusterEnabled := flag.Bool("cluster", envBool("BROKER_CLUSTER"), "enable experimental etcd/raft distributed mode; requires a durable WAL and peer mTLS")
	clusterListen := flag.String("cluster-listen", env("BROKER_CLUSTER_LISTEN", ""), "Raft peer transport bind address; required with -cluster")
	clusterPeers := flag.String("cluster-peers", env("BROKER_CLUSTER_PEERS", ""), "Raft peers as comma-separated nodeID=host:port entries, including this node")
	clusterNodeID := flag.Uint64("cluster-node-id", uint64(envInt64("BROKER_CLUSTER_NODE_ID", 1)), "this node's numeric Raft ID")
	clusterJoinExisting := flag.Bool("cluster-join-existing", envBool("BROKER_CLUSTER_JOIN_EXISTING"), "join an existing etcd/raft cluster after its leader adds this node; do not bootstrap a separate cluster")
	clusterWAL := flag.String("cluster-wal", env("BROKER_CLUSTER_WAL", ""), "separate durable directory for etcd/raft state; defaults to <wal>-raft")
	clusterPartitionWAL := flag.String("cluster-partition-wal", env("BROKER_CLUSTER_PARTITION_WAL", ""), "separate durable root for per-partition BWL2 stores; defaults to <wal>-partitions")
	clusterReconcileInterval := flag.Duration("cluster-reconcile-interval", envDuration("BROKER_CLUSTER_RECONCILE_INTERVAL", 5*time.Second), "interval for reconciling locally assigned partition Raft groups")
	clusterAutoReassignInterval := flag.Duration("cluster-auto-reassign-interval", envDuration("BROKER_CLUSTER_AUTO_REASSIGN_INTERVAL", 0), "opt-in interval for replacing unavailable replicas and balancing healthy replicas by count, workload, and disk; 0 disables")
	clusterReadIndexTimeout := flag.Duration("cluster-read-index-timeout", envDuration("BROKER_CLUSTER_READ_INDEX_TIMEOUT", 5*time.Second), "maximum wait for quorum-confirmed controller reads; caller deadlines may shorten this")
	clusterReplicationFactor := flag.Int("cluster-replication-factor", int(envInt64("BROKER_CLUSTER_REPLICATION_FACTOR", 3)), "replicas required for each partition assignment")
	clusterMinISR := flag.Int("cluster-min-isr", int(envInt64("BROKER_CLUSTER_MIN_ISR", 0)), "minimum live in-sync replicas required for partition writes; 0 uses a majority")
	clusterMaxReplicationLag := flag.Int64("cluster-max-replication-lag", envInt64("BROKER_CLUSTER_MAX_REPLICATION_LAG", 1000), "maximum Raft log entries behind the leader for a replica to remain in ISR")
	clusterReplicationTimeout := flag.Duration("cluster-replication-timeout", envDuration("BROKER_CLUSTER_REPLICATION_TIMEOUT", 5*time.Second), "maximum inactivity before a partition replica leaves the ISR")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if *clusterEnabled && *clusterNodeID == 0 {
		podName := env("POD_NAME", "")
		dash := strings.LastIndexByte(podName, '-')
		if dash < 0 || dash == len(podName)-1 {
			logger.Log.Fatal("-cluster-node-id=0 requires POD_NAME to end with a StatefulSet ordinal")
		}
		ordinal, err := strconv.ParseUint(podName[dash+1:], 10, 64)
		if err != nil || ordinal == ^uint64(0) {
			logger.Log.Fatal("POD_NAME must end with a valid StatefulSet ordinal when -cluster-node-id=0 is used", zlog.String("pod_name", podName))
		}
		*clusterNodeID = ordinal + 1
	}

	certificateIdentities := splitCommaSeparated(*clientCertIdentities)
	for _, identity := range certificateIdentities {
		if !validCertificateIdentity(identity) {
			logger.Log.Fatal("invalid client certificate identity; expected cn:, dns:, uri:, email:, or ip: followed by a non-empty value", zlog.String("identity", identity))
		}
	}
	if len(certificateIdentities) > 0 && !*requireClientCert {
		logger.Log.Fatal("-client-cert-identities requires -require-client-cert so mapped certificates are requested and verified")
	}
	hasMappedMTLSIdentity := *requireClientCert && len(certificateIdentities) > 0
	if !*insecureDev && *token == "" && !hasMappedMTLSIdentity {
		logger.Log.Fatal("BROKER_TOKEN/-token or required mTLS with mapped client certificate identities is required; use --insecure-dev only for local development")
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		logger.Log.Fatal("both -tls-cert and -tls-key must be provided together")
	}
	if *requireClientCert {
		if *tlsCert == "" || *clientCA == "" {
			logger.Log.Fatal("-require-client-cert requires -tls-cert, -tls-key, and -client-ca")
		}
		if _, err := listenerTLSConfig(*tlsCert, *tlsKey, *clientCA, true); err != nil {
			logger.Log.Fatal("configure HTTP client certificate verification", zlog.Err(err))
		}
	}
	if *clusterJoinExisting && !*clusterEnabled {
		logger.Log.Fatal("-cluster-join-existing requires -cluster")
	}
	if *clusterEnabled && (*clusterReplicationFactor <= 0 || *clusterMinISR < 0 || *clusterMaxReplicationLag < 0) {
		logger.Log.Fatal("cluster replication factor must be positive; minimum ISR and max replication lag must be non-negative")
	}
	if *clusterAutoReassignInterval < 0 {
		logger.Log.Fatal("-cluster-auto-reassign-interval must be non-negative")
	}
	if !*insecureDev && *tlsCert == "" {
		logger.Log.Fatal("production mode requires TLS. Configure -tls-cert and -tls-key, or use --insecure-dev only for local development.")
	}
	if *nativeAddr != "" && !*insecureDev && *token == "" && !hasMappedMTLSIdentity {
		logger.Log.Fatal("native BST1 requires a bearer token (-token) or required mTLS with mapped client certificate identities in production mode, or --insecure-dev for local development")
	}
	if *amqpAddr != "" && !*insecureDev && *amqpUsername == "" && *amqpPassword == "" && !hasMappedMTLSIdentity {
		logger.Log.Fatal("AMQP requires PLAIN credentials or required mTLS with mapped client certificate identities in production mode")
	}

	frameCodec := broker.NewFrameCodec(broker.FrameCodecConfig{
		Type:              broker.CodecType(*codecType),
		CompressThreshold: *frameCompress,
		MaxFrameSize:      int(*maxBody) + (1 << 20),
		SigningKey:        []byte(*frameSigningKey),
		EncryptionKey:     []byte(*frameEncryptionKey),
		WALDecryptionKeys: rawKeyList(*walDecryptionKeys),
	})
	var store broker.Store
	var stateStore broker.StateStore
	profile, profileErr := broker.DefaultsForProfile(broker.PerformanceProfile(*profileName))
	if profileErr != nil {
		logger.Log.Fatal("invalid performance profile", zlog.Err(profileErr))
	}
	kind := strings.ToLower(strings.TrimSpace(*storeKind))
	if kind == "" && *wal != "" {
		kind = "wal"
	}
	var raftPeers map[uint64]string
	var raftTLS *tls.Config
	if *clusterEnabled {
		if *wal == "" {
			logger.Log.Fatal("-wal is required when -cluster is enabled")
		}
		if kind != "wal" && kind != "filewal" && kind != "file-wal" && kind != "wal2" && kind != "segmented" && kind != "segmented-wal" {
			logger.Log.Fatal("-cluster requires a durable WAL store (-store=wal or -store=wal2)")
		}
		if *clusterListen == "" {
			logger.Log.Fatal("-cluster-listen is required when -cluster is enabled")
		}
		if *clusterNodeID == 0 {
			logger.Log.Fatal("-cluster-node-id must be non-zero")
		}
		if !*requireClientCert || *clientCA == "" || *tlsCert == "" || *tlsKey == "" {
			logger.Log.Fatal("distributed peer transport requires -tls-cert, -tls-key, -client-ca, and -require-client-cert")
		}
		var err error
		raftPeers, err = parseRaftPeers(*clusterPeers)
		if err != nil {
			logger.Log.Fatal("invalid -cluster-peers", zlog.Err(err))
		}
		if _, ok := raftPeers[*clusterNodeID]; !ok {
			logger.Log.Fatal("-cluster-peers must include this -cluster-node-id")
		}
		if *clusterJoinExisting {
			if len(raftPeers) < 4 {
				logger.Log.Fatal("-cluster-peers must include the joining node and at least three existing cluster nodes when -cluster-join-existing is enabled")
			}
		} else if len(raftPeers) < 3 || len(raftPeers)%2 == 0 {
			logger.Log.Fatal("-cluster-peers must contain an odd number of at least three nodes")
		}
		tlsCfg, tlsErr := listenerTLSConfig(*tlsCert, *tlsKey, *clientCA, true)
		if tlsErr != nil {
			logger.Log.Fatal("configure cluster peer mTLS", zlog.Err(tlsErr))
		}
		raftTLS = tlsCfg.Clone()
		// TCPTransport uses one TLS config for dialing and accepting peers.
		// Trust the configured peer CA on outbound connections as well.
		raftTLS.RootCAs = raftTLS.ClientCAs
		if *clusterWAL == "" {
			*clusterWAL = *wal + "-raft"
		}
		if *clusterPartitionWAL == "" {
			*clusterPartitionWAL = *wal + "-partitions"
		}
		if *clusterReconcileInterval <= 0 {
			logger.Log.Fatal("-cluster-reconcile-interval must be positive")
		}
		if pathsOverlap(*wal, *clusterWAL) || pathsOverlap(*wal, *clusterPartitionWAL) || pathsOverlap(*clusterWAL, *clusterPartitionWAL) {
			logger.Log.Fatal("-wal, -cluster-wal, and -cluster-partition-wal must use separate, non-overlapping directories")
		}
	}
	switch kind {
	case "", "memory":
		store = broker.NewMemoryStore()
	case "sqlite":
		st, state, err := sqlitestore.OpenStores(*sqliteDriver, *sqliteDSN)
		if err != nil {
			logger.Log.Fatal("open sqlite message store", zlog.String("driver", *sqliteDriver), zlog.String("dsn", *sqliteDSN), zlog.Err(err))
		}
		store = st
		stateStore = state
	case "wal", "filewal", "file-wal":
		if *wal == "" {
			logger.Log.Fatal("-wal is required when -store=wal")
		}
		if strings.EqualFold(*walFormat, "json") || strings.EqualFold(*walFormat, "jsonl") {
			walCfg := broker.FileWALStoreConfig{Dir: *wal}
			if *frameEncryptionKey != "" {
				// Try hex decoding first, fall back to raw bytes
				keyBytes, err := hex.DecodeString(*frameEncryptionKey)
				if err != nil {
					keyBytes = []byte(*frameEncryptionKey)
				}
				walCfg.EncryptionKey = keyBytes
				walCfg.KeyName = "default"
			}
			st, err := broker.NewFileWALStoreWithConfig(walCfg)
			if err != nil {
				logger.Log.Fatal("open JSON WAL store", zlog.Err(err))
			}
			store = st
		} else {
			if *walShards <= 0 {
				logger.Log.Fatal("-wal-shards must be positive")
			}
			if *walShards == 1 {
				st, err := broker.NewBinaryFileWALStoreWithCodec(*wal, frameCodec)
				if err != nil {
					logger.Log.Fatal("open binary WAL store", zlog.Err(err))
				}
				store = st
			} else {
				st, err := broker.NewPartitionedBinaryWALStore(*wal, *walShards, frameCodec)
				if err != nil {
					logger.Log.Fatal("open partitioned binary WAL store", zlog.Err(err))
				}
				store = st
			}
		}
	case "wal2", "segmented", "segmented-wal":
		if *wal == "" {
			logger.Log.Fatal("-wal is required when -store=wal2")
		}
		st, err := broker.NewSegmentedWALStore(broker.SegmentedWALConfig{
			Dir:                *wal,
			MaxSegmentSize:     *walSegmentSize,
			MaxSegments:        *walMaxSegments,
			Retention:          *walRetention,
			CompactionInterval: *walCompactionInterval,
			Codec:              frameCodec,
		})
		if err != nil {
			logger.Log.Fatal("open segmented WAL store", zlog.Err(err))
		}
		store = st
	default:
		logger.Log.Fatal("unsupported store", zlog.String("store", *storeKind))
	}
	if !*insecureDev && profile.Profile != broker.ProfileLightweight && kind == "memory" {
		logger.Log.Fatal("production durable, latency, and throughput profiles require a durable store; configure -store=wal or -store=sqlite")
	}

	if configurable, ok := store.(interface{ SetHistoryLimit(int) }); ok {
		configurable.SetHistoryLimit(*messageHistory)
	}
	if configurable, ok := store.(interface{ SetCompactBacklog(bool) }); ok {
		configurable.SetCompactBacklog(*compactBacklog)
	}
	if configurable, ok := store.(interface{ SetRetainCompleted(bool) }); ok {
		configurable.SetRetainCompleted(*retainCompleted)
	}

	brokerConfig := broker.Config{
		Mode:                        broker.ModeStandalone,
		PerformanceProfile:          profile.Profile,
		Store:                       store,
		StateStore:                  stateStore,
		DefaultQueue:                *queue,
		FrameCodec:                  frameCodec,
		NotificationHistoryCapacity: *notificationHistory,
		EnablePublisherTracking:     *publisherTracking,
		EnableConsumerEventTracking: *consumerEventTracking,
		OTLPEndpoint:                *otlpEndpoint,
		OTLPInterval:                *otlpInterval,
	}
	var b *broker.Broker
	var distributed *broker.DistributedBroker
	var partitionStoreFactory broker.PartitionStoreFactory
	var err error
	if *clusterEnabled {
		partitionStoreFactory, err = broker.NewSegmentedWALPartitionStoreFactory(*clusterPartitionWAL, broker.SegmentedWALConfig{
			MaxSegmentSize:     *walSegmentSize,
			MaxSegments:        *walMaxSegments,
			Retention:          *walRetention,
			CompactionInterval: *walCompactionInterval,
			Codec:              frameCodec,
		})
		if err != nil {
			logger.Log.Fatal("configure partition WAL stores", zlog.Err(err))
		}
		brokerConfig.NodeID = strconv.FormatUint(*clusterNodeID, 10)
		distributed, err = broker.NewDistributed(brokerConfig, broker.DistributedConfig{
			UseEtcdRaft:           true,
			RaftNodeID:            *clusterNodeID,
			RaftPeers:             raftPeers,
			JoinExisting:          *clusterJoinExisting,
			ListenAddr:            *clusterListen,
			WALDir:                *clusterWAL,
			TLSConfig:             raftTLS,
			ReplicationFactor:     *clusterReplicationFactor,
			MinInSyncReplicas:     *clusterMinISR,
			MaxReplicationLag:     uint64(*clusterMaxReplicationLag),
			ReplicationTimeout:    *clusterReplicationTimeout,
			ReadIndexTimeout:      *clusterReadIndexTimeout,
			PartitionStoreFactory: partitionStoreFactory,
			PartitionStoragePath:  *clusterPartitionWAL,
		})
		if err != nil {
			logger.Log.Fatal("create distributed broker", zlog.Err(err))
		}
		b = distributed.Broker
	} else {
		b, err = broker.New(brokerConfig)
		if err != nil {
			logger.Log.Fatal("create broker", zlog.Err(err))
		}
	}
	if !*quiet {
		broker.LogNotifications(b.Notifications(), "brokerd-stdout", *debugEvents)
	}
	var startErr error
	if distributed != nil {
		startErr = distributed.Start(context.Background())
	} else {
		startErr = b.Start(context.Background())
	}
	if startErr != nil {
		logger.Log.Fatal("start broker", zlog.Err(startErr))
	}
	partitionReconcileCtx, stopPartitionReconcile := context.WithCancel(context.Background())
	var partitionReconcileWG sync.WaitGroup
	if distributed != nil {
		partitionReconcileWG.Add(1)
		go func() {
			defer partitionReconcileWG.Done()
			reconcilePartitionGroups(partitionReconcileCtx, distributed, *clusterNodeID, *clusterReconcileInterval)
		}()
		if *clusterAutoReassignInterval > 0 {
			partitionReconcileWG.Add(1)
			go func() {
				defer partitionReconcileWG.Done()
				err := distributed.RunAutomaticReassignment(partitionReconcileCtx, *clusterAutoReassignInterval, func(err error) {
					logger.Log.Warn("automatic partition reassignment", zlog.Err(err))
				})
				if err != nil && !errors.Is(err, context.Canceled) {
					logger.Log.Warn("automatic partition reassignment stopped", zlog.Err(err))
				}
			}()
		}
	}

	tokens := []broker.AdminToken{}
	if *token != "" {
		tokens = append(tokens, broker.AdminToken{Name: "admin", Token: *token, Permissions: []string{broker.PermissionAdminAll}})
	}
	if *readToken != "" {
		tokens = append(tokens, broker.AdminToken{Name: "reader", Token: *readToken, Permissions: []string{broker.PermissionBrokerRead, broker.PermissionStatsRead, broker.PermissionMetricsRead, broker.PermissionClusterRead, broker.PermissionQueueRead, broker.PermissionTopicRead, broker.PermissionStreamRead, broker.PermissionMessageRead, broker.PermissionDLQRead, broker.PermissionRouteRead, broker.PermissionNotification, broker.PermissionConsumerRead, broker.PermissionTaskRead}})
	}
	if *workerToken != "" {
		tokens = append(tokens, broker.AdminToken{Name: "worker", Token: *workerToken, Permissions: []string{broker.PermissionBrokerRead, broker.PermissionConsumerRegister, broker.PermissionConsumerRead, broker.PermissionConsumerControl, broker.PermissionConsumerPoll, broker.PermissionMessageRead, broker.PermissionMessageAck, broker.PermissionMessageNack, broker.PermissionTaskRead, broker.PermissionTaskWrite, broker.PermissionDLQRead, broker.PermissionNotification}})
	}
	if *publisherToken != "" {
		tokens = append(tokens, broker.AdminToken{Name: "publisher", Token: *publisherToken, Permissions: []string{broker.PermissionPublish, broker.PermissionTaskWrite, broker.PermissionTaskRead, broker.PermissionBrokerRead}})
	}
	if len(certificateIdentities) > 0 {
		permissions := splitCommaSeparated(*clientCertPermissions)
		if len(permissions) == 0 {
			logger.Log.Fatal("client certificate identities require at least one permission")
		}
		tokens = append(tokens, broker.AdminToken{Name: "client-certificate", CertificateIdentities: certificateIdentities, Permissions: permissions})
	}
	acl := broker.NewACL(tokens...)
	b.SetACL(acl)
	var stopACLSync context.CancelFunc
	var aclSyncDone chan struct{}
	if distributed != nil {
		acl.SetControllerQuorum(distributed.ControllerQuorum())
		aclSyncCtx, cancel := context.WithCancel(context.Background())
		stopACLSync = cancel
		aclSyncDone = make(chan struct{})
		go func() {
			defer close(aclSyncDone)
			for {
				if err := acl.SyncControllerQuorum(aclSyncCtx); err == nil {
					return
				}
				timer := time.NewTimer(time.Second)
				select {
				case <-aclSyncCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	}
	admin := broker.AdminServer{Broker: b, Distributed: distributed, ACL: acl, InsecureDev: *insecureDev, MaxBodyBytes: *maxBody, TLSCertFile: *tlsCert, TLSKeyFile: *tlsKey, ClientCAFile: *clientCA, RequireClientCert: *requireClientCert}
	srv := admin.HTTPServer(*addr)
	type listenerFailure struct {
		name string
		err  error
	}
	serverErr := make(chan listenerFailure, 8)
	go func() {
		if *tlsCert != "" || *tlsKey != "" {
			serverErr <- listenerFailure{"http", srv.ListenAndServeTLS(*tlsCert, *tlsKey)}
			return
		}
		serverErr <- listenerFailure{"http", srv.ListenAndServe()}
	}()
	select {
	case failure := <-serverErr:
		if failure.err != nil && failure.err != http.ErrServerClosed {
			logger.Log.Fatal("broker listen failed", zlog.String("listener", failure.name), zlog.Err(failure.err))
		}
	case <-time.After(200 * time.Millisecond):
	}
	var native *broker.NativeStreamServer
	if *nativeAddr != "" {
		dir := *nativeDir
		if dir == "" {
			if *wal != "" {
				dir = filepath.Join(*wal, "native-stream")
			} else {
				dir = filepath.Join(".broker-data", "native-stream")
			}
		}
		tlsCfg, tlsErr := listenerTLSConfig(*tlsCert, *tlsKey, *clientCA, *requireClientCert)
		if tlsErr != nil {
			logger.Log.Fatal("configure native stream TLS", zlog.Err(tlsErr))
		}
		native, err = broker.NewNativeStreamServer(broker.NativeStreamConfig{Dir: dir, Partitions: *nativePartitions, SegmentBytes: *nativeSegmentBytes, GroupCommitDelay: *nativeCommitDelay, GroupCommitBytes: *nativeCommitBytes, MaxFrameBytes: 128 << 20, Sync: *nativeSync, ACL: admin.ACL, InsecureDev: *insecureDev, TLSConfig: tlsCfg})
		if err != nil {
			logger.Log.Fatal("open native stream", zlog.Err(err))
		}
		go func() { serverErr <- listenerFailure{"native-stream", native.ListenAndServe(*nativeAddr)} }()
	}
	var nativeRPC *broker.NativeRPCServer
	if *nativeRPCAddr != "" {
		nativeRPC = broker.NewNativeRPCServer(b, broker.NativeRPCConfig{ListenAddr: *nativeRPCAddr, TLSCertFile: *tlsCert, TLSKeyFile: *tlsKey, ClientCAFile: *clientCA, RequireClientCert: *requireClientCert, BrokerToken: *token, ACL: admin.ACL, MaxMessageSize: int(*maxBody), AllowInsecure: *insecureDev})
		go func() { serverErr <- listenerFailure{"native-rpc", nativeRPC.ListenAndServe()} }()
	}
	var grpcServer *broker.GRPCV1Server
	if *grpcAddr != "" {
		tlsCfg, tlsErr := listenerTLSConfig(*tlsCert, *tlsKey, *clientCA, *requireClientCert)
		if tlsErr != nil {
			logger.Log.Fatal("configure gRPC TLS", zlog.Err(tlsErr))
		}
		grpcServer, err = broker.NewGRPCV1Server(b, broker.GRPCV1Config{ListenAddr: *grpcAddr, TLSConfig: tlsCfg, ACL: admin.ACL, InsecureDev: *insecureDev, MaxMessageSize: int(*maxBody)})
		if err != nil {
			logger.Log.Fatal("configure gRPC", zlog.Err(err))
		}
		go func() { serverErr <- listenerFailure{"grpc", grpcServer.ListenAndServe()} }()
	}
	var mqtt *broker.MQTTServer
	if *mqttAddr != "" {
		mqtt = broker.NewMQTTServer(b, broker.MQTTConfig{ListenAddr: *mqttAddr, TLSCertFile: *tlsCert, TLSKeyFile: *tlsKey, ClientCAFile: *clientCA, RequireClientCert: *requireClientCert, RequireAuth: true, BrokerToken: *token, MaxPacketSize: int(*maxBody), ACL: admin.ACL, AllowInsecure: *insecureDev})
		go func() { serverErr <- listenerFailure{"mqtt", mqtt.ListenAndServe()} }()
	}
	var amqpServer *amqp.Server
	if *amqpAddr != "" {
		tlsCfg, tlsErr := listenerTLSConfig(*tlsCert, *tlsKey, *clientCA, *requireClientCert)
		if tlsErr != nil {
			logger.Log.Fatal("configure AMQP TLS", zlog.Err(tlsErr))
		}
		amqpServer = amqp.NewServer(amqp.ServerConfig{ListenAddr: *amqpAddr, Broker: b, TLSConfig: tlsCfg, Username: *amqpUsername, Password: *amqpPassword, AllowInsecure: *insecureDev, FrameMaxSize: uint32(*maxBody), ACL: admin.ACL})
		if tlsCfg != nil {
			err = amqpServer.ListenTLS(*amqpAddr)
		} else {
			err = amqpServer.Listen()
		}
		if err != nil {
			logger.Log.Fatal("AMQP listen failed", zlog.Err(err))
		}
		go func() { serverErr <- listenerFailure{"amqp", amqpServer.Serve()} }()
	}
	var sse *broker.SSEServer
	var sseHTTP *http.Server
	if *sseAddr != "" {
		sse = broker.NewSSEServer(b, broker.SSEConfig{Path: *ssePath, ACL: admin.ACL, InsecureDev: *insecureDev})
		sse.SetToken(*token)
		sseTLS, tlsErr := listenerTLSConfig(*tlsCert, *tlsKey, *clientCA, *requireClientCert)
		if tlsErr != nil {
			logger.Log.Fatal("configure SSE TLS", zlog.Err(tlsErr))
		}
		sseHTTP = &http.Server{Addr: *sseAddr, Handler: sse.Handler(), ReadHeaderTimeout: 5 * time.Second, TLSConfig: sseTLS}
		go func() {
			if *tlsCert != "" {
				serverErr <- listenerFailure{"sse", sseHTTP.ListenAndServeTLS(*tlsCert, *tlsKey)}
				return
			}
			serverErr <- listenerFailure{"sse", sseHTTP.ListenAndServe()}
		}()
	}
	var webSocket *broker.RFC6455Server
	var webSocketHTTP *http.Server
	if *webSocketAddr != "" {
		webSocket = broker.NewRFC6455Server(b, broker.RFC6455Config{Path: *webSocketPath, ACL: admin.ACL, InsecureDev: *insecureDev, MaxMessageSize: *maxBody})
		webSocketTLS, tlsErr := listenerTLSConfig(*tlsCert, *tlsKey, *clientCA, *requireClientCert)
		if tlsErr != nil {
			logger.Log.Fatal("configure WebSocket TLS", zlog.Err(tlsErr))
		}
		webSocketHTTP = &http.Server{Addr: *webSocketAddr, Handler: webSocket.Handler(), ReadHeaderTimeout: 5 * time.Second, TLSConfig: webSocketTLS}
		go func() {
			if *tlsCert != "" {
				serverErr <- listenerFailure{"websocket", webSocketHTTP.ListenAndServeTLS(*tlsCert, *tlsKey)}
				return
			}
			serverErr <- listenerFailure{"websocket", webSocketHTTP.ListenAndServe()}
		}()
	}
	fmt.Printf("fastworker broker listening on %s\n", *addr)
	if native != nil {
		fmt.Printf("native BST1 stream listening on %s partitions=%d sync=%t\n", *nativeAddr, *nativePartitions, *nativeSync)
	}
	if nativeRPC != nil {
		fmt.Printf("native JSON/TCP RPC listening on %s\n", *nativeRPCAddr)
	}
	if grpcServer != nil {
		fmt.Printf("protobuf/HTTP2 gRPC listening on %s\n", *grpcAddr)
	}
	if mqtt != nil {
		fmt.Printf("MQTT 3.1.1 listening on %s\n", *mqttAddr)
	}
	if amqpServer != nil {
		fmt.Printf("AMQP 0-9-1 listening on %s\n", *amqpAddr)
	}
	if sse != nil {
		fmt.Printf("SSE listening on %s%s\n", *sseAddr, *ssePath)
	}
	if webSocket != nil {
		fmt.Printf("RFC 6455 WebSocket listening on %s%s\n", *webSocketAddr, *webSocketPath)
	}
	if *insecureDev {
		fmt.Println("WARNING: authentication disabled by --insecure-dev")
	}
	if *tlsCert != "" {
		fmt.Println("TLS enabled")
	}
	if *requireClientCert {
		fmt.Println("mTLS client certificate verification enabled")
	}
	if kind == "sqlite" {
		fmt.Printf("durable sqlite message store: driver=%s dsn=%s\n", *sqliteDriver, *sqliteDSN)
	}
	if kind == "wal" || kind == "filewal" || kind == "file-wal" {
		fmt.Printf("durable WAL: %s format=%s shards=%d history=%d retain_completed=%t\n", *wal, *walFormat, *walShards, *messageHistory, *retainCompleted)
	}
	if distributed != nil {
		fmt.Printf("experimental etcd/raft cluster: node=%d peers=%d listen=%s join_existing=%t\n", *clusterNodeID, len(raftPeers), *clusterListen, *clusterJoinExisting)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sig:
	case failure := <-serverErr:
		if failure.err != nil && failure.err != http.ErrServerClosed {
			logger.Log.Error("listener stopped", zlog.String("listener", failure.name), zlog.Err(failure.err))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if stopACLSync != nil {
		stopACLSync()
		<-aclSyncDone
	}
	stopPartitionReconcile()
	partitionReconcileWG.Wait()
	_ = srv.Shutdown(ctx)
	if native != nil {
		_ = native.Close()
	}
	if nativeRPC != nil {
		nativeRPC.Stop()
	}
	if grpcServer != nil {
		_ = grpcServer.Stop(ctx)
	}
	if mqtt != nil {
		mqtt.Stop()
	}
	if amqpServer != nil {
		_ = amqpServer.Stop(ctx)
	}
	if sseHTTP != nil {
		_ = sseHTTP.Shutdown(ctx)
		sse.Stop()
	}
	if webSocketHTTP != nil {
		_ = webSocketHTTP.Shutdown(ctx)
		webSocket.Stop()
	}
	if distributed != nil {
		_ = distributed.Stop(ctx)
	} else {
		_ = b.Stop(ctx)
	}
	if srvErr := srv.Close(); srvErr != nil && srvErr != http.ErrServerClosed {
		logger.Log.Warn("server close", zlog.Err(srvErr))
	}
	select {
	case failure := <-serverErr:
		if failure.err != nil && failure.err != http.ErrServerClosed {
			logger.Log.Warn("server error", zlog.String("listener", failure.name), zlog.Err(failure.err))
		}
	default:
	}
}

func parseRaftPeers(raw string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	addresses := make(map[string]uint64)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		idText, address, ok := strings.Cut(entry, "=")
		if !ok || strings.TrimSpace(address) == "" {
			return nil, fmt.Errorf("peer %q must use nodeID=host:port", entry)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(idText), 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("peer %q has an invalid non-zero node ID", entry)
		}
		if _, exists := peers[id]; exists {
			return nil, fmt.Errorf("duplicate node ID %d", id)
		}
		address = strings.TrimSpace(address)
		if _, _, err := net.SplitHostPort(address); err != nil {
			return nil, fmt.Errorf("peer %q has invalid host:port address: %w", entry, err)
		}
		if previous, exists := addresses[address]; exists {
			return nil, fmt.Errorf("nodes %d and %d share peer address %q", previous, id, address)
		}
		peers[id] = address
		addresses[address] = id
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("at least one nodeID=host:port peer is required")
	}
	return peers, nil
}

func reconcilePartitionGroups(ctx context.Context, distributed *broker.DistributedBroker, localPeerID uint64, interval time.Duration) {
	peers := distributed.ControllerPeerAddresses()
	localAddress := peers[localPeerID]
	if distributed == nil || localAddress == "" || localPeerID == 0 || interval <= 0 {
		return
	}
	reconcile := func() {
		quorum := distributed.ControllerQuorum()
		if quorum == nil {
			return
		}
		peers = distributed.ControllerPeerAddresses()
		localAddress = peers[localPeerID]
		if localAddress == "" {
			return
		}
		snapshot, err := quorum.TopologySnapshotLinearizable(ctx)
		if err != nil {
			if ctx.Err() == nil {
				logger.Log.Warn("read partition assignments for local reconciliation", zlog.Err(err))
			}
			return
		}
		if quorum.IsLeader() {
			peerIDs := make([]uint64, 0, len(peers))
			for peerID := range peers {
				peerIDs = append(peerIDs, peerID)
			}
			sort.Slice(peerIDs, func(i, j int) bool { return peerIDs[i] < peerIDs[j] })
			registered := false
			for _, peerID := range peerIDs {
				if distributed.RaftPeerRole(peerID) == "" {
					continue // Do not recreate registry entries for removed seed peers.
				}
				_, changed, registerErr := ensureClusterNode(ctx, quorum, snapshot.Nodes, peers[peerID], strconv.FormatUint(peerID, 10))
				if registerErr != nil {
					if ctx.Err() == nil {
						logger.Log.Warn("register cluster peer in node registry", zlog.String("peer_id", strconv.FormatUint(peerID, 10)), zlog.Err(registerErr))
					}
					return
				}
				registered = registered || changed
			}
			if registered {
				return // Re-read one committed topology snapshot on the next pass.
			}
		}
		localNodeID := ""
		for _, node := range snapshot.Nodes {
			if node != nil && node.Address == localAddress {
				localNodeID = node.ID
				if node.State != cluster.NodeLeft && node.Metadata[cluster.NodeMetadataBrokerVersion] != version {
					if err := quorum.ReportNodeBrokerVersion(ctx, node.ID, version); err != nil {
						if ctx.Err() == nil {
							logger.Log.Warn("report local broker version to cluster registry", zlog.String("node_id", node.ID), zlog.Err(err))
						}
						return
					}
					return // Re-read after this node's version is committed.
				}
				break
			}
		}
		localAssignment := false
		if localNodeID != "" {
			for _, partition := range snapshot.Partitions {
				if partition == nil {
					continue
				}
				replicas := append([]string(nil), partition.Replicas...)
				if pending := partition.Reassignment; pending != nil {
					replicas = append(replicas, pending.ExpectedReplicas...)
					replicas = append(replicas, pending.DesiredReplicas...)
				}
				for _, replica := range replicas {
					if replica == localNodeID {
						localAssignment = true
						break
					}
				}
				if localAssignment {
					break
				}
			}
		}
		hasLocalGroups := false
		for _, groupID := range distributed.RaftGroupIDs() {
			if strings.HasPrefix(groupID, "partition/") {
				hasLocalGroups = true
				break
			}
		}
		if !localAssignment && !hasLocalGroups {
			return
		}
		if err := distributed.ReconcilePartitionGroups(ctx, nil); err != nil && ctx.Err() == nil {
			logger.Log.Warn("reconcile local partition Raft groups", zlog.Err(err))
		}
	}
	reconcile()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcile()
		}
	}
}

func ensureClusterNode(ctx context.Context, quorum *cluster.ControllerQuorum, nodes []*cluster.NodeInfo, address, defaultID string) (string, bool, error) {
	var byAddress *cluster.NodeInfo
	var byID *cluster.NodeInfo
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if node.Address == address {
			if byAddress != nil && byAddress.ID != node.ID {
				return "", false, fmt.Errorf("Raft address %q is registered to multiple logical nodes", address)
			}
			byAddress = node
		}
		if node.ID == defaultID {
			byID = node
		}
	}
	if byAddress != nil {
		return byAddress.ID, false, nil
	}
	if byID != nil {
		for _, node := range nodes {
			if node != nil && node.ID != byID.ID && node.Address == address {
				return "", false, fmt.Errorf("Raft address %q is already registered to logical node %q", address, node.ID)
			}
		}
		updated := *byID
		updated.Address = address
		if err := quorum.RegisterNode(ctx, &updated); err != nil {
			return "", false, err
		}
		return byID.ID, true, nil
	}
	node := &cluster.NodeInfo{ID: defaultID, Address: address, State: cluster.NodeJoining}
	if err := quorum.RegisterNode(ctx, node); err != nil {
		return "", false, err
	}
	return defaultID, true, nil
}

func pathsOverlap(first, second string) bool {
	firstAbs, err := filepath.Abs(filepath.Clean(first))
	if err != nil {
		return true
	}
	secondAbs, err := filepath.Abs(filepath.Clean(second))
	if err != nil {
		return true
	}
	within := func(base, path string) bool {
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return true
		}
		return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
	}
	return within(firstAbs, secondAbs) || within(secondAbs, firstAbs)
}

func listenerTLSConfig(certFile, keyFile, clientCAFile string, requireClientCert bool) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if clientCAFile != "" {
		pem, err := os.ReadFile(clientCAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no valid certificates in client CA file")
		}
		cfg.ClientCAs = pool
	}
	if requireClientCert {
		if cfg.ClientCAs == nil {
			return nil, fmt.Errorf("client CA is required when client certificates are mandatory")
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func splitCommaSeparated(raw string) []string {
	var values []string
	for _, value := range strings.Split(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func rawKeyList(raw string) [][]byte {
	values := splitCommaSeparated(raw)
	keys := make([][]byte, len(values))
	for i, value := range values {
		keys[i] = []byte(value)
	}
	return keys
}

func validCertificateIdentity(identity string) bool {
	prefix, value, ok := strings.Cut(strings.TrimSpace(identity), ":")
	if !ok || value == "" || value != strings.TrimSpace(value) {
		return false
	}
	switch prefix {
	case "cn", "dns", "uri", "email":
		return true
	case "ip":
		return net.ParseIP(value) != nil
	default:
		return false
	}
}

func envBool(k string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func envInt64(k string, d int64) int64 {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		var out int64
		if _, err := fmt.Sscanf(v, "%d", &out); err == nil && out > 0 {
			return out
		}
	}
	return d
}

func envDuration(k string, d time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if out, err := time.ParseDuration(v); err == nil && out > 0 {
			return out
		}
	}
	return d
}

func frameCodecOptions(signingKey, encryptionKey string, compressThreshold int, maxFrame int) []broker.BinaryMessageCodecOption {
	var opts []broker.BinaryMessageCodecOption
	if maxFrame > 0 {
		opts = append(opts, broker.WithFrameMaxSize(maxFrame))
	}
	if compressThreshold > 0 {
		opts = append(opts, broker.WithFrameCompressionThreshold(compressThreshold))
	}
	if signingKey != "" {
		opts = append(opts, broker.WithFrameSigningKey([]byte(signingKey)))
	}
	if encryptionKey != "" {
		opts = append(opts, broker.WithFrameEncryptionKey([]byte(encryptionKey)))
	}
	return opts
}
