package broker

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	brokerv1 "github.com/oarkflow/broker/api/broker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCV1ProtobufRoundTrip(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	server, err := NewGRPCV1Server(b, GRPCV1Config{InsecureDev: true})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	go server.Serve(listener)
	defer server.Stop(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := brokerv1.NewBrokerServiceClient(conn)
	if health, err := client.Health(ctx, &brokerv1.HealthRequest{}); err != nil || health.Version != Version {
		t.Fatalf("health=%+v err=%v", health, err)
	}
	published, err := client.Publish(ctx, &brokerv1.PublishRequest{Topic: "events", Queue: "jobs", Payload: []byte("payload")})
	if err != nil || published.MessageId == "" || published.Acknowledgement != string(ConfirmEnqueued) {
		t.Fatalf("publish=%+v err=%v", published, err)
	}
	polled, err := client.Poll(ctx, &brokerv1.PollRequest{Queue: "jobs", ConsumerId: "worker", Limit: 1, VisibilityTimeoutMillis: 1000})
	if err != nil || len(polled.Messages) != 1 || polled.Messages[0].Id != published.MessageId {
		t.Fatalf("poll=%+v err=%v", polled, err)
	}
}

func TestGRPCV1AuthorizationInterceptor(t *testing.T) {
	b := MustNew(Config{})
	acl := NewACL(AdminToken{Name: "reader", Token: "secret", Permissions: []string{PermissionBrokerRead}})
	server, err := NewGRPCV1Server(b, GRPCV1Config{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}, ACL: acl})
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	called := false
	_, err = server.authorizeUnary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: brokerv1.BrokerService_Health_FullMethodName}, func(ctx context.Context, req any) (any, error) {
		called = true
		if principalFromContext(ctx).Name != "reader" {
			t.Fatal("authenticated principal was not propagated")
		}
		return nil, nil
	})
	if err != nil || !called {
		t.Fatalf("authorized request err=%v called=%t", err, called)
	}
	_, err = server.authorizeUnary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: brokerv1.BrokerService_Publish_FullMethodName}, func(context.Context, any) (any, error) { return nil, nil })
	if err == nil {
		t.Fatal("reader credential was allowed to publish")
	}
}
