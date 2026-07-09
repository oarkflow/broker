package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/broker"
	"github.com/oarkflow/broker/contrib/sqlitestore"
	"github.com/oarkflow/broker/pkg/logger"
	"github.com/oarkflow/zlog"
)

func main() {
	addr := flag.String("addr", env("BROKER_ADDR", ":8090"), "HTTP listen address")
	storeKind := flag.String("store", env("BROKER_STORE", ""), "message store: memory, wal, sqlite; empty auto-selects wal when -wal is set")
	sqliteDriver := flag.String("sqlite-driver", env("BROKER_SQLITE_DRIVER", "sqlite"), "database/sql driver for sqlite message store")
	sqliteDSN := flag.String("sqlite-dsn", env("BROKER_SQLITE_DSN", "./.broker-data/broker-messages.sqlite"), "SQLite DSN for message store")
	wal := flag.String("wal", env("BROKER_WAL", ""), "WAL directory; empty uses in-memory store")
	walFormat := flag.String("wal-format", env("BROKER_WAL_FORMAT", "binary"), "WAL format: binary or json")
	frameSigningKey := flag.String("frame-signing-key", env("BROKER_FRAME_SIGNING_KEY", ""), "optional FWB1 frame HMAC signing key")
	frameEncryptionKey := flag.String("frame-encryption-key", env("BROKER_FRAME_ENCRYPTION_KEY", ""), "optional FWB1 frame AES-GCM payload encryption key")
	frameCompress := flag.Int("frame-compress-threshold", int(envInt64("BROKER_FRAME_COMPRESS_THRESHOLD", 0)), "gzip payloads in FWB1 frames at or above this size; 0 disables")
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
	flag.Parse()

	if !*insecureDev && *token == "" {
		logger.Log.Fatal("BROKER_TOKEN or -token is required. Use --insecure-dev only for local development.")
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		logger.Log.Fatal("both -tls-cert and -tls-key must be provided together")
	}

	frameCodec := broker.NewBinaryMessageCodec(frameCodecOptions(*frameSigningKey, *frameEncryptionKey, *frameCompress, int(*maxBody)+1<<20)...)
	var store broker.Store
	kind := strings.ToLower(strings.TrimSpace(*storeKind))
	if kind == "" && *wal != "" {
		kind = "wal"
	}
	switch kind {
	case "", "memory":
		// broker.New will install the default in-memory message store.
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
			st, err := broker.NewFileWALStore(*wal)
			if err != nil {
				logger.Log.Fatal("open JSON WAL store", zlog.Err(err))
			}
			store = st
		} else {
			st, err := broker.NewBinaryFileWALStore(*wal, frameCodecOptions(*frameSigningKey, *frameEncryptionKey, *frameCompress, int(*maxBody)+1<<20)...)
			if err != nil {
				logger.Log.Fatal("open binary WAL store", zlog.Err(err))
			}
			store = st
		}
	default:
		logger.Log.Fatal("unsupported store", zlog.String("store", *storeKind))
	}

	b, err := broker.New(broker.Config{Mode: broker.ModeStandalone, Store: store, DefaultQueue: *queue, FrameCodec: frameCodec})
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
	serverErr := make(chan error, 1)
	go func() {
		if *tlsCert != "" || *tlsKey != "" {
			serverErr <- srv.ListenAndServeTLS(*tlsCert, *tlsKey)
			return
		}
		serverErr <- srv.ListenAndServe()
	}()
	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal("broker listen failed", zlog.String("addr", *addr), zlog.Err(err))
		}
	case <-time.After(200 * time.Millisecond):
	}
	fmt.Printf("fastworker broker listening on %s\n", *addr)
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
		fmt.Printf("durable WAL: %s format=%s\n", *wal, *walFormat)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = b.Stop(ctx)
	if srvErr := srv.Close(); srvErr != nil && srvErr != http.ErrServerClosed {
		logger.Log.Warn("server close", zlog.Err(srvErr))
	}
	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			logger.Log.Warn("server error", zlog.Err(err))
		}
	default:
	}
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
