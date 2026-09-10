package broker

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"time"

	brokerv1 "github.com/oarkflow/broker/api/broker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type GRPCV1Config struct {
	ListenAddr     string
	TLSConfig      *tls.Config
	ACL            *ACL
	InsecureDev    bool
	MaxMessageSize int
}

// GRPCV1Server is the protobuf/HTTP2 gRPC data plane.
type GRPCV1Server struct {
	brokerv1.UnimplementedBrokerServiceServer
	broker *Broker
	cfg    GRPCV1Config
	server *grpc.Server
	ln     net.Listener
}

func NewGRPCV1Server(b *Broker, cfg GRPCV1Config) (*GRPCV1Server, error) {
	if b == nil {
		return nil, errf(ErrInvalidMessage, "broker is required")
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":50051"
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 4 << 20
	}
	if !cfg.InsecureDev && cfg.TLSConfig == nil {
		return nil, errf(ErrUnauthorized, "gRPC production listener requires TLS")
	}
	if !cfg.InsecureDev && cfg.ACL == nil {
		return nil, errf(ErrUnauthorized, "gRPC production listener requires an ACL")
	}
	s := &GRPCV1Server{broker: b, cfg: cfg}
	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.MaxMessageSize),
		grpc.MaxSendMsgSize(cfg.MaxMessageSize),
		grpc.UnaryInterceptor(s.authorizeUnary),
	}
	if cfg.TLSConfig != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(cfg.TLSConfig.Clone())))
	}
	s.server = grpc.NewServer(options...)
	brokerv1.RegisterBrokerServiceServer(s.server, s)
	return s, nil
}

func (s *GRPCV1Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return err
	}
	s.ln = ln
	return s.server.Serve(ln)
}

func (s *GRPCV1Server) Serve(ln net.Listener) error {
	s.ln = ln
	return s.server.Serve(ln)
}

func (s *GRPCV1Server) Stop(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.server.Stop()
		return ctx.Err()
	}
}

func (s *GRPCV1Server) authorizeUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	principal := AdminPrincipal{Name: "development", Permissions: []string{PermissionAdminAll}}
	if !s.cfg.InsecureDev {
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		raw := ""
		if len(values) > 0 {
			raw = strings.TrimSpace(values[0])
			if len(raw) >= 7 && strings.EqualFold(raw[:7], "Bearer ") {
				raw = strings.TrimSpace(raw[7:])
			}
		}
		var ok bool
		principal, ok = s.cfg.ACL.Authenticate(raw)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "invalid credential")
		}
		permission := grpcMethodPermission(info.FullMethod)
		if !s.cfg.ACL.Authorize(principal, permission) {
			return nil, status.Error(codes.PermissionDenied, "credential lacks required permission")
		}
	}
	return handler(context.WithValue(ctx, adminPrincipalKey{}, principal), req)
}

func grpcMethodPermission(method string) string {
	switch {
	case strings.HasSuffix(method, "/Publish"):
		return PermissionPublish
	case strings.HasSuffix(method, "/Poll"):
		return PermissionConsumerPoll
	case strings.HasSuffix(method, "/Ack"):
		return PermissionMessageAck
	case strings.HasSuffix(method, "/Nack"):
		return PermissionMessageNack
	case strings.HasSuffix(method, "/ReadStream"):
		return PermissionStreamRead
	case strings.HasSuffix(method, "/CommitOffset"):
		return PermissionStreamCommit
	default:
		return PermissionBrokerRead
	}
}

func (s *GRPCV1Server) Health(context.Context, *brokerv1.HealthRequest) (*brokerv1.HealthResponse, error) {
	health := s.broker.Health()
	return &brokerv1.HealthResponse{Status: health.Status, Version: health.Version}, nil
}

func (s *GRPCV1Server) Publish(ctx context.Context, in *brokerv1.PublishRequest) (*brokerv1.PublishResponse, error) {
	result, err := s.broker.Publisher().Publish(ctx, in.Topic, in.Payload, WithQueue(in.Queue), WithStream(in.Stream), WithHeaders(in.Headers), WithTenantID(in.TenantId), WithIdempotencyKey(in.IdempotencyKey), WithProducerID(in.ProducerId))
	if err != nil {
		return nil, grpcStatus(err)
	}
	return &brokerv1.PublishResponse{MessageId: result.MessageID, Acknowledgement: string(result.Confirmed), Partition: int32(result.Partition), Offset: result.Offset, LeaderEpoch: result.LeaderEpoch, ReplicationCount: int32(result.Replicas), Durability: string(result.Durability)}, nil
}

func (s *GRPCV1Server) Poll(ctx context.Context, in *brokerv1.PollRequest) (*brokerv1.PollResponse, error) {
	messages, err := s.broker.Store().Lease(ctx, LeaseRequest{Queue: in.Queue, ConsumerID: in.ConsumerId, ConsumerGroup: in.ConsumerGroup, Limit: int(in.Limit), VisibilityTimeout: time.Duration(in.VisibilityTimeoutMillis) * time.Millisecond, TenantID: in.TenantId})
	if err != nil {
		return nil, grpcStatus(err)
	}
	return &brokerv1.PollResponse{Messages: grpcMessages(messages)}, nil
}

func (s *GRPCV1Server) Ack(ctx context.Context, in *brokerv1.AckRequest) (*brokerv1.AckResponse, error) {
	err := s.broker.Store().Ack(ctx, AckRequest{MessageID: in.MessageId, Queue: in.Queue, LeaseToken: in.LeaseToken, ConsumerID: in.ConsumerId})
	return &brokerv1.AckResponse{}, grpcStatus(err)
}

func (s *GRPCV1Server) Nack(ctx context.Context, in *brokerv1.NackRequest) (*brokerv1.AckResponse, error) {
	err := s.broker.Store().Nack(ctx, NackRequest{MessageID: in.MessageId, Queue: in.Queue, LeaseToken: in.LeaseToken, ConsumerID: in.ConsumerId, Action: NackAction(in.Action), Reason: in.Reason, Error: in.Error, Delay: time.Duration(in.DelayMillis) * time.Millisecond, DLQ: in.Dlq})
	return &brokerv1.AckResponse{}, grpcStatus(err)
}

func (s *GRPCV1Server) ReadStream(ctx context.Context, in *brokerv1.ReadStreamRequest) (*brokerv1.ReadStreamResponse, error) {
	messages, err := s.broker.ReadStream(ctx, StreamReadRequest{Stream: in.Stream, ConsumerGroup: in.ConsumerGroup, AfterOffset: in.AfterOffset, Limit: int(in.Limit), TenantID: in.TenantId})
	if err != nil {
		return nil, grpcStatus(err)
	}
	return &brokerv1.ReadStreamResponse{Messages: grpcMessages(messages)}, nil
}

func (s *GRPCV1Server) CommitOffset(ctx context.Context, in *brokerv1.CommitOffsetRequest) (*brokerv1.AckResponse, error) {
	err := s.broker.CommitOffset(ctx, OffsetCommit{Stream: in.Stream, ConsumerGroup: in.ConsumerGroup, Offset: in.Offset, TenantID: in.TenantId})
	return &brokerv1.AckResponse{}, grpcStatus(err)
}

func grpcMessages(messages []Message) []*brokerv1.BrokerMessage {
	out := make([]*brokerv1.BrokerMessage, 0, len(messages))
	for _, message := range messages {
		out = append(out, &brokerv1.BrokerMessage{Id: message.ID, Topic: message.Topic, Queue: message.Queue, Stream: message.Stream, Partition: int32(message.Partition), Offset: message.Offset, Payload: message.Payload, Headers: message.Headers, TenantId: message.TenantID, LeaseToken: message.LeaseToken, Attempts: int32(message.Attempts)})
	}
	return out
}

func grpcStatus(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrUnauthorized):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, ErrMessageNotFound), errors.Is(err, ErrQueueNotFound), errors.Is(err, ErrStreamNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrBackpressure), errors.Is(err, ErrRateLimited), errors.Is(err, ErrThrottled):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, ErrInvalidMessage), errors.Is(err, ErrMessageTooLarge):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrNotLeader), errors.Is(err, ErrQuorumUnavailable), errors.Is(err, ErrStorageUnavailable), errors.Is(err, ErrBrokerUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, ErrLeaseLost), errors.Is(err, ErrDuplicateMessage), errors.Is(err, ErrTransactionClosed), errors.Is(err, ErrTransactionAborted):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
