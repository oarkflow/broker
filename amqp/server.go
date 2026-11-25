package amqp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/broker"
)

// ServerConfig holds AMQP server configuration.
type ServerConfig struct {
	ListenAddr     string
	TLSConfig      *tls.Config
	MaxConnections int
	FrameMaxSize   uint32
	Heartbeat      time.Duration
	// Username and Password enable AMQP PLAIN authentication. PLAIN is accepted
	// only over TLS unless AllowInsecure is explicitly enabled for development.
	Username string
	Password string
	// AllowInsecure permits an unauthenticated/plaintext development listener.
	// It must never be enabled on a production listener.
	AllowInsecure bool
	// Broker is the underlying fastworker broker instance.
	Broker *broker.Broker
	// ACL provides shared authentication and admission control across protocols.
	ACL *broker.ACL
}

// DefaultServerConfig returns a sensible default configuration.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		ListenAddr:     ":5672",
		MaxConnections: 10000,
		FrameMaxSize:   FrameMaxSize,
		Heartbeat:      HeartbeatDefault * time.Second,
	}
}

// Server is the AMQP protocol server.
type Server struct {
	cfg           ServerConfig
	listener      net.Listener
	conns         map[*Connection]struct{}
	connsMu       sync.RWMutex
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	nextID        atomic.Uint64
	started       atomic.Bool
	exchangesMu   sync.RWMutex
	exchanges     map[string]broker.AMQPExchangeRecord
	exchangeOpsMu sync.Mutex
}

// NewServer creates a new AMQP server.
func NewServer(cfg ServerConfig) *Server {
	if cfg.Broker == nil {
		panic("amqp: broker is required")
	}
	if cfg.FrameMaxSize == 0 {
		cfg.FrameMaxSize = FrameMaxSize
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = HeartbeatDefault * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cfg:    cfg,
		conns:  make(map[*Connection]struct{}),
		ctx:    ctx,
		cancel: cancel,
		exchanges: map[string]broker.AMQPExchangeRecord{
			"amq.direct":  {Name: "amq.direct", Type: "direct"},
			"amq.topic":   {Name: "amq.topic", Type: "topic"},
			"amq.fanout":  {Name: "amq.fanout", Type: "fanout"},
			"amq.match":   {Name: "amq.match", Type: "headers"},
			"amq.headers": {Name: "amq.headers", Type: "headers"},
		},
	}
}

func (s *Server) exchangeType(name string) (string, bool) {
	exchange, ok := s.exchangeRecord(name)
	return exchange.Type, ok
}

func (s *Server) exchangeRecord(name string) (broker.AMQPExchangeRecord, bool) {
	s.exchangesMu.RLock()
	exchange, ok := s.exchanges[name]
	s.exchangesMu.RUnlock()
	return exchange, ok
}

func (s *Server) declareExchange(ctx context.Context, exchange broker.AMQPExchangeRecord) error {
	s.exchangesMu.Lock()
	defer s.exchangesMu.Unlock()
	if existing, ok := s.exchanges[exchange.Name]; ok {
		if existing.Type != exchange.Type || existing.Durable != exchange.Durable {
			return broker.ErrInvalidMessage
		}
		return nil
	}
	if exchange.Durable {
		// The in-memory implementation cannot provide durability across process
		// restarts. Require an explicitly persistent StateStore for durable AMQP
		// declarations.
		if _, memoryOnly := s.cfg.Broker.StateStore().(*broker.MemoryStateStore); memoryOnly {
			return broker.ErrUnsupportedMode
		}
		store, ok := s.cfg.Broker.StateStore().(broker.AMQPExchangeStore)
		if !ok {
			return broker.ErrUnsupportedMode
		}
		if err := store.PutAMQPExchange(ctx, exchange); err != nil {
			return err
		}
	}
	s.exchanges[exchange.Name] = exchange
	return nil
}

func (s *Server) deleteExchange(ctx context.Context, name string) error {
	s.exchangesMu.Lock()
	defer s.exchangesMu.Unlock()
	exchange, ok := s.exchanges[name]
	if !ok {
		return broker.ErrTopicNotFound
	}
	if exchange.Durable {
		store, ok := s.cfg.Broker.StateStore().(broker.AMQPExchangeStore)
		if !ok {
			return broker.ErrUnsupportedMode
		}
		if err := store.DeleteAMQPExchange(ctx, name); err != nil {
			return err
		}
	}
	delete(s.exchanges, name)
	return nil
}

func (s *Server) loadDurableExchanges(ctx context.Context) error {
	store, ok := s.cfg.Broker.StateStore().(broker.AMQPExchangeStore)
	if !ok {
		return nil
	}
	exchanges, err := store.ListAMQPExchanges(ctx)
	if err != nil {
		return err
	}
	s.exchangesMu.Lock()
	for _, exchange := range exchanges {
		if existing, exists := s.exchanges[exchange.Name]; exists && existing.Type != exchange.Type {
			s.exchangesMu.Unlock()
			return fmt.Errorf("amqp: durable exchange %q conflicts with built-in exchange type", exchange.Name)
		}
		s.exchanges[exchange.Name] = exchange
	}
	s.exchangesMu.Unlock()

	bindingStore, ok := s.cfg.Broker.StateStore().(broker.AMQPExchangeBindingStore)
	if !ok {
		return nil
	}
	bindings, err := bindingStore.ListAMQPExchangeBindings(ctx)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		source, sourceOK := s.exchangeRecord(binding.Source)
		if !sourceOK || !source.Durable {
			return fmt.Errorf("amqp: durable binding %q references a missing or non-durable exchange", binding.ID)
		}
		switch binding.DestinationKind {
		case broker.DestinationExchange:
			destination, destinationOK := s.exchangeRecord(binding.Destination)
			if !destinationOK || !destination.Durable {
				return fmt.Errorf("amqp: durable binding %q references a missing or non-durable destination exchange", binding.ID)
			}
		case broker.DestinationQueue:
			lookup, ok := s.cfg.Broker.Store().(broker.QueueConfigLookup)
			if !ok {
				return fmt.Errorf("amqp: durable binding %q requires queue configuration lookup support", binding.ID)
			}
			queue, found, err := lookup.LookupQueueConfig(ctx, binding.Destination)
			if err != nil {
				return err
			}
			if !found || !queue.Durable {
				return fmt.Errorf("amqp: durable binding %q references a missing or non-durable queue", binding.ID)
			}
		default:
			return fmt.Errorf("amqp: durable binding %q has unsupported destination kind %q", binding.ID, binding.DestinationKind)
		}
		var matches []broker.AMQPHeaderMatch
		if binding.HeadersJSON != "" {
			if err := json.Unmarshal([]byte(binding.HeadersJSON), &matches); err != nil {
				return fmt.Errorf("amqp: durable binding %q has invalid header criteria: %w", binding.ID, err)
			}
		}
		if binding.HeadersJSON == "" {
			binding.HeadersJSON = "[]"
		}
		wantID := amqpExchangeLinkRouteNameWithHeaders(binding.Source, binding.Destination, binding.RoutingKey, binding.TenantID, binding.HeadersJSON, binding.HeadersMatchAll)
		if binding.DestinationKind == broker.DestinationQueue {
			wantID = amqpBindingRouteNameWithHeaders(binding.Source, binding.Destination, binding.RoutingKey, binding.TenantID, binding.HeadersJSON, binding.HeadersMatchAll)
		}
		if binding.ID != wantID {
			return fmt.Errorf("amqp: durable binding %q has an invalid identity", binding.ID)
		}
		addAMQPExchangeBindingRoute(s.cfg.Broker.Router(), source.Type, binding)
	}
	return nil
}

// Listen starts the TCP listener.
func (s *Server) Listen() error {
	if err := s.loadDurableExchanges(s.ctx); err != nil {
		return fmt.Errorf("amqp: load durable exchanges: %w", err)
	}
	if err := s.validateAuthenticationConfig(); err != nil {
		return err
	}
	if !s.cfg.AllowInsecure {
		return errors.New("amqp: plaintext listener disabled; configure TLS or explicitly enable AllowInsecure for development")
	}
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("amqp: listen %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = ln
	s.started.Store(true)
	return nil
}

// ListenTLS starts a TLS listener.
func (s *Server) ListenTLS(addr string) error {
	if err := s.loadDurableExchanges(s.ctx); err != nil {
		return fmt.Errorf("amqp: load durable exchanges: %w", err)
	}
	if err := s.validateAuthenticationConfig(); err != nil {
		return err
	}
	if s.cfg.TLSConfig == nil {
		return errors.New("amqp: TLS configuration is required")
	}
	hasPlainAuth := (s.cfg.Username != "" && s.cfg.Password != "") || s.cfg.ACL != nil
	hasMappedCertificateAuth := s.cfg.ACL != nil && s.cfg.TLSConfig.ClientAuth >= tls.VerifyClientCertIfGiven
	if !hasPlainAuth && !hasMappedCertificateAuth {
		return errors.New("amqp: TLS listener requires PLAIN credentials or ACL-mapped verified client certificates")
	}
	ln, err := tls.Listen("tcp", addr, s.cfg.TLSConfig)
	if err != nil {
		return fmt.Errorf("amqp: listen tls %s: %w", addr, err)
	}
	s.listener = ln
	s.started.Store(true)
	return nil
}

func (s *Server) validateAuthenticationConfig() error {
	if (s.cfg.Username == "") != (s.cfg.Password == "") {
		return errors.New("amqp: PLAIN username and password must be configured together")
	}
	return nil
}

// Serve accepts connections until the server is stopped.
func (s *Server) Serve() error {
	if s.listener == nil {
		if err := s.Listen(); err != nil {
			return err
		}
	}

	log.Printf("[amqp] listening on %s", s.listener.Addr().String())

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return nil
			default:
				log.Printf("[amqp] accept error: %v", err)
				continue
			}
		}

		s.connsMu.RLock()
		count := len(s.conns)
		s.connsMu.RUnlock()

		if s.cfg.MaxConnections > 0 && count >= s.cfg.MaxConnections {
			log.Printf("[amqp] max connections reached, rejecting %s", conn.RemoteAddr())
			conn.Close()
			continue
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// handleConn processes a single AMQP connection.
func (s *Server) handleConn(netConn net.Conn) {
	defer s.wg.Done()

	conn := NewConnection(s, netConn, s.cfg.FrameMaxSize, s.cfg.Heartbeat)
	s.addConn(conn)
	defer s.removeConn(conn)

	if err := conn.Handle(); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			log.Printf("[amqp] connection %s error: %v", conn.RemoteAddr(), err)
		}
	}
}

func (s *Server) addConn(conn *Connection) {
	s.connsMu.Lock()
	s.conns[conn] = struct{}{}
	s.connsMu.Unlock()
}

func (s *Server) removeConn(conn *Connection) {
	s.connsMu.Lock()
	delete(s.conns, conn)
	s.connsMu.Unlock()
}

func (s *Server) queueConsumerCount(ctx context.Context, queue string) int {
	s.connsMu.RLock()
	connections := make([]*Connection, 0, len(s.conns))
	for conn := range s.conns {
		connections = append(connections, conn)
	}
	s.connsMu.RUnlock()

	count := 0
	for _, conn := range connections {
		conn.channelsMu.RLock()
		channels := make([]*Channel, 0, len(conn.channels))
		for _, channel := range conn.channels {
			channels = append(channels, channel)
		}
		conn.channelsMu.RUnlock()
		for _, channel := range channels {
			channel.consumersMu.RLock()
			for _, consumer := range channel.consumers {
				if consumer.queue == queue && broker.TenantAllowed(ctx, consumer.tenantID) {
					count++
				}
			}
			channel.consumersMu.RUnlock()
		}
	}
	return count
}

func amqpCountUint32(count int) uint32 {
	if count <= 0 {
		return 0
	}
	if uint64(count) > uint64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(count)
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	if s.listener != nil {
		s.listener.Close()
	}

	s.cancel()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}

	return nil
}

// Addr returns the listener address.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// ConnectionCount returns the number of active connections.
func (s *Server) ConnectionCount() int {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()
	return len(s.conns)
}
