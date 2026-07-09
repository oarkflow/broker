package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/fastworker/broker"
)

func main() {
	addr := flag.String("addr", env("BROKER_ADDR", ":8090"), "HTTP listen address")
	wal := flag.String("wal", env("BROKER_WAL", ""), "WAL directory; empty uses in-memory store")
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
		log.Fatal("BROKER_TOKEN or -token is required. Use --insecure-dev only for local development.")
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		log.Fatal("both -tls-cert and -tls-key must be provided together")
	}

	var store broker.Store
	if *wal != "" {
		st, err := broker.NewFileWALStore(*wal)
		if err != nil {
			log.Fatalf("open WAL store: %v", err)
		}
		store = st
	}

	b, err := broker.New(broker.Config{Mode: broker.ModeStandalone, Store: store, DefaultQueue: *queue})
	if err != nil {
		log.Fatalf("create broker: %v", err)
	}
	if !*quiet {
		broker.LogNotifications(b.Notifications(), "brokerd-stdout", *debugEvents, log.Printf)
	}
	if err := b.Start(context.Background()); err != nil {
		log.Fatalf("start broker: %v", err)
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
			log.Fatalf("broker listen failed on %s: %v", *addr, err)
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
	if *wal != "" {
		fmt.Printf("durable WAL: %s\n", *wal)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = b.Stop(ctx)
	if srvErr := srv.Close(); srvErr != nil && srvErr != http.ErrServerClosed {
		log.Printf("server close: %v", srvErr)
	}
	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			log.Printf("server error: %v", err)
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
