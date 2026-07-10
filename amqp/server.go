package amqp

import (
	"context"
	"crypto/tls"
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
	// Broker is the underlying fastworker broker instance.
	Broker *broker.Broker
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
	cfg       ServerConfig
	listener  net.Listener
	conns     map[*Connection]struct{}
	connsMu   sync.RWMutex
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	nextID    atomic.Uint64
	started   atomic.Bool
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
		cfg:   cfg,
		conns: make(map[*Connection]struct{}),
		ctx:   ctx,
		cancel: cancel,
	}
}

// Listen starts the TCP listener.
func (s *Server) Listen() error {
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
	ln, err := tls.Listen("tcp", addr, s.cfg.TLSConfig)
	if err != nil {
		return fmt.Errorf("amqp: listen tls %s: %w", addr, err)
	}
	s.listener = ln
	s.started.Store(true)
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
