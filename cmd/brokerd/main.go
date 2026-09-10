package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/amqp"
	"github.com/oarkflow/broker/contrib/sqlitestore"
	"github.com/oarkflow/broker/pkg/logger"
	"github.com/oarkflow/zlog"
)

func main() {
	addr := flag.String("addr", env("BROKER_ADDR", ":8090"), "HTTP listen address")
	profileName := flag.String("profile", env("BROKER_PROFILE", string(broker.ProfileDurable)), "performance profile: durable, latency, throughput, lightweight")
	nativeAddr := flag.String("native-addr", env("BROKER_NATIVE_ADDR", ""), "unauthenticated native BST1 stream listen address; empty disables; development only")
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
	frameSigningKey := flag.String("frame-signing-key", env("BROKER_FRAME_SIGNING_KEY", ""), "optional FWB frame and BWL1 WAL HMAC signing key")
	frameEncryptionKey := flag.String("frame-encryption-key", env("BROKER_FRAME_ENCRYPTION_KEY", ""), "optional FWB frame and BWL1 WAL AES-GCM encryption key")
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
	maxBody := flag.Int64("max-body", envInt64("BROKER_MAX_BODY", 1<<20), "maximum HTTP request body size in bytes")
	quiet := flag.Bool("quiet", envBool("BROKER_QUIET"), "disable live broker event logs")
	debugEvents := flag.Bool("debug-events", envBool("BROKER_DEBUG_EVENTS"), "include debug heartbeat/lease-extension events in live logs")
	messageHistory := flag.Int("message-history", int(envInt64("BROKER_MESSAGE_HISTORY", 0)), "message history entries retained per message; 0 disables hot-path history")
	notificationHistory := flag.Int("notification-history", int(envInt64("BROKER_NOTIFICATION_HISTORY", 0)), "notification events retained in memory; 0 disables retention")
	retainCompleted := flag.Bool("retain-completed", envBool("BROKER_RETAIN_COMPLETED"), "retain completed message bodies in memory after durable acknowledgement")
	publisherTracking := flag.Bool("publisher-tracking", envBool("BROKER_PUBLISHER_TRACKING"), "retain per-publisher counters/history")
	consumerEventTracking := flag.Bool("consumer-event-tracking", envBool("BROKER_CONSUMER_EVENT_TRACKING"), "emit per-message consumer events")
	flag.Parse()

	if !*insecureDev && *token == "" {
		logger.Log.Fatal("BROKER_TOKEN or -token is required. Use --insecure-dev only for local development.")
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		logger.Log.Fatal("both -tls-cert and -tls-key must be provided together")
	}
	if !*insecureDev && *tlsCert == "" {
		logger.Log.Fatal("production mode requires TLS. Configure -tls-cert and -tls-key, or use --insecure-dev only for local development.")
	}
	if !*insecureDev && *nativeAddr != "" {
		logger.Log.Fatal("native BST1 has no authentication layer and may only be enabled with --insecure-dev")
	}
	if *amqpAddr != "" && !*insecureDev && *amqpUsername == "" && *amqpPassword == "" && !*requireClientCert {
		logger.Log.Fatal("AMQP requires PLAIN credentials or mTLS in production mode")
	}

	frameCodec := broker.NewFrameCodec(broker.FrameCodecConfig{
		Type:              broker.CodecType(*codecType),
		CompressThreshold: *frameCompress,
		MaxFrameSize:      int(*maxBody) + (1 << 20),
		SigningKey:        []byte(*frameSigningKey),
		EncryptionKey:     []byte(*frameEncryptionKey),
	})
	var store broker.Store
	profile, profileErr := broker.DefaultsForProfile(broker.PerformanceProfile(*profileName))
	if profileErr != nil {
		logger.Log.Fatal("invalid performance profile", zlog.Err(profileErr))
	}
	kind := strings.ToLower(strings.TrimSpace(*storeKind))
	if kind == "" && *wal != "" {
		kind = "wal"
	}
	switch kind {
	case "", "memory":
		store = broker.NewMemoryStore()
	case "sqlite":
		st, err := sqlitestore.OpenMessageStore(*sqliteDriver, *sqliteDSN)
		if err != nil {
			logger.Log.Fatal("open sqlite message store", zlog.String("driver", *sqliteDriver), zlog.String("dsn", *sqliteDSN), zlog.Err(err))
		}
		store = st
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
	default:
		logger.Log.Fatal("unsupported store", zlog.String("store", *storeKind))
	}
	if !*insecureDev && profile.Profile != broker.ProfileLightweight && kind == "memory" {
		logger.Log.Fatal("production durable, latency, and throughput profiles require a durable store; configure -store=wal or -store=sqlite")
	}

	if configurable, ok := store.(interface{ SetHistoryLimit(int) }); ok {
		configurable.SetHistoryLimit(*messageHistory)
	}
	if configurable, ok := store.(interface{ SetRetainCompleted(bool) }); ok {
		configurable.SetRetainCompleted(*retainCompleted)
	}

	b, err := broker.New(broker.Config{
		Mode:                        broker.ModeStandalone,
		PerformanceProfile:          profile.Profile,
		Store:                       store,
		DefaultQueue:                *queue,
		FrameCodec:                  frameCodec,
		NotificationHistoryCapacity: *notificationHistory,
		EnablePublisherTracking:     *publisherTracking,
		EnableConsumerEventTracking: *consumerEventTracking,
	})
	if err != nil {
		logger.Log.Fatal("create broker", zlog.Err(err))
	}
	if !*quiet {
		broker.LogNotifications(b.Notifications(), "brokerd-stdout", *debugEvents)
	}
	if err := b.Start(context.Background()); err != nil {
		logger.Log.Fatal("start broker", zlog.Err(err))
	}

	tokens := []broker.AdminToken{}
	if *token != "" {
		tokens = append(tokens, broker.AdminToken{Name: "admin", Token: *token, Permissions: []string{broker.PermissionAdminAll}})
	}
	if *readToken != "" {
		tokens = append(tokens, broker.AdminToken{Name: "reader", Token: *readToken, Permissions: []string{broker.PermissionBrokerRead, broker.PermissionQueueRead, broker.PermissionTopicRead, broker.PermissionStreamRead, broker.PermissionMessageRead, broker.PermissionDLQRead, broker.PermissionRouteRead, broker.PermissionNotification, broker.PermissionConsumerRead, broker.PermissionTaskRead}})
	}
	if *workerToken != "" {
		tokens = append(tokens, broker.AdminToken{Name: "worker", Token: *workerToken, Permissions: []string{broker.PermissionBrokerRead, broker.PermissionConsumerRegister, broker.PermissionConsumerRead, broker.PermissionConsumerControl, broker.PermissionConsumerPoll, broker.PermissionMessageRead, broker.PermissionMessageAck, broker.PermissionMessageNack, broker.PermissionTaskRead, broker.PermissionTaskWrite, broker.PermissionDLQRead, broker.PermissionNotification}})
	}
	if *publisherToken != "" {
		tokens = append(tokens, broker.AdminToken{Name: "publisher", Token: *publisherToken, Permissions: []string{broker.PermissionPublish, broker.PermissionTaskWrite, broker.PermissionTaskRead, broker.PermissionBrokerRead}})
	}
	admin := broker.AdminServer{Broker: b, ACL: broker.NewACL(tokens...), InsecureDev: *insecureDev, MaxBodyBytes: *maxBody, TLSCertFile: *tlsCert, TLSKeyFile: *tlsKey, ClientCAFile: *clientCA, RequireClientCert: *requireClientCert}
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
		nativeRPC = broker.NewNativeRPCServer(b, broker.NativeRPCConfig{ListenAddr: *nativeRPCAddr, TLSCertFile: *tlsCert, TLSKeyFile: *tlsKey, ClientCAFile: *clientCA, RequireClientCert: *requireClientCert, BrokerToken: *token, MaxMessageSize: int(*maxBody)})
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
		mqtt = broker.NewMQTTServer(b, broker.MQTTConfig{ListenAddr: *mqttAddr, TLSCertFile: *tlsCert, TLSKeyFile: *tlsKey, ClientCAFile: *clientCA, RequireClientCert: *requireClientCert, RequireAuth: true, BrokerToken: *token, MaxPacketSize: int(*maxBody), ACL: admin.ACL})
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
	_ = b.Stop(ctx)
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
