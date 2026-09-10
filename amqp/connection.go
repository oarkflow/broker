package amqp

import (
	"bufio"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	broker "github.com/oarkflow/broker"
)

// Connection represents an AMQP client connection.
type Connection struct {
	server          *Server
	netConn         net.Conn
	codec           *Codec
	reader          *bufio.Reader
	writer          *bufio.Writer
	rwc             *connReadWriteCloser
	channels        map[uint16]*Channel
	channelsMu      sync.RWMutex
	state           atomic.Int32
	frameMax        uint32
	heartbeat       time.Duration
	clientHeartbeat uint16
	vhost           string
	username        string
	closed          chan struct{}
	closeOnce       sync.Once
	release         func()
}

// Connection states.
const (
	connStateCreated int32 = iota
	connStateStart
	connStateTune
	connStateOpen
	connStateClosed
)

// connReadWriteCloser wraps net.Conn with buffered reader/writer and supports flushing.
type connReadWriteCloser struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
}

func newConnReadWriteCloser(conn net.Conn, size int) *connReadWriteCloser {
	return &connReadWriteCloser{
		conn: conn,
		br:   bufio.NewReaderSize(conn, size),
		bw:   bufio.NewWriterSize(conn, size),
	}
}

func (c *connReadWriteCloser) Read(p []byte) (int, error)  { return c.br.Read(p) }
func (c *connReadWriteCloser) Write(p []byte) (int, error) { return c.bw.Write(p) }
func (c *connReadWriteCloser) Flush() error                { return c.bw.Flush() }
func (c *connReadWriteCloser) Close() error                { return c.conn.Close() }

// NewConnection creates a new AMQP connection.
func NewConnection(s *Server, netConn net.Conn, frameMax uint32, heartbeat time.Duration) *Connection {
	rwc := newConnReadWriteCloser(netConn, int(frameMax))
	codec := NewCodec(rwc)

	return &Connection{
		server:    s,
		netConn:   netConn,
		codec:     codec,
		reader:    rwc.br,
		writer:    rwc.bw,
		rwc:       rwc,
		channels:  make(map[uint16]*Channel),
		frameMax:  frameMax,
		heartbeat: heartbeat,
		closed:    make(chan struct{}),
	}
}

// Handle runs the AMQP connection handshake and message loop.
func (c *Connection) Handle() error {
	defer c.Close()

	// AMQP 0-9-1 protocol header is exactly eight octets.
	header := make([]byte, 8)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return fmt.Errorf("failed to read protocol header: %w", err)
	}
	if string(header) != "AMQP\x00\x00\x09\x01" {
		return fmt.Errorf("unsupported protocol header: %x", header)
	}

	// AMQP handshake: send Connection.Start
	if err := c.sendStart(); err != nil {
		return err
	}
	c.state.Store(connStateStart)

	// Wait for Connection.StartOk
	for {
		frame, err := c.codec.ReadFrame()
		if err != nil {
			return err
		}

		switch frame.Type {
		case FrameMethod:
			if err := c.handleMethodFrame(frame); err != nil {
				return err
			}
			if c.state.Load() == connStateClosed {
				return nil
			}
		case FrameHeartbeat:
			// Respond with heartbeat
		default:
			return fmt.Errorf("unexpected frame type: %d", frame.Type)
		}

		if c.state.Load() == connStateOpen {
			break
		}
	}

	// Start heartbeat ticker
	if c.heartbeat > 0 {
		go c.heartbeatLoop()
	}

	// Main message loop
	return c.readLoop()
}

// handleMethodFrame processes a method frame.
func (c *Connection) handleMethodFrame(frame *Frame) error {
	method, err := ParseMethodFrame(frame.Payload)
	if err != nil {
		return err
	}

	switch method.ClassID {
	case ClassConnection:
		return c.handleConnectionMethod(method)
	case ClassChannel:
		ch, err := c.getChannel(frame.ChannelID)
		if err != nil {
			return err
		}
		return ch.handleMethod(method)
	default:
		ch, err := c.getChannel(frame.ChannelID)
		if err != nil {
			return err
		}
		return ch.handleMethod(method)
	}
}

// handleConnectionMethod handles connection-level methods.
func (c *Connection) handleConnectionMethod(method *MethodFrame) error {
	switch method.MethodID {
	case MethodConnectionStartOk:
		return c.handleStartOk(method.Args)
	case MethodConnectionTuneOk:
		return c.handleTuneOk(method.Args)
	case MethodConnectionOpen:
		return c.handleConnectionOpen(method.Args)
	case MethodConnectionClose:
		return c.handleConnectionClose(method.Args)
	case MethodConnectionCloseOk:
		return nil
	default:
		return fmt.Errorf("unknown connection method: %d", method.MethodID)
	}
}

// sendStart sends Connection.Start to the client.
func (c *Connection) sendStart() error {
	serverProps := table{
		"product":   "fastworker-broker",
		"version":   broker.Version,
		"platform":  "Go",
		"copyright": "Fastworker",
		"capabilities": table{
			"publisher_confirms":     true,
			"consumer_cancel_notify": true,
			"basic.nack":             true,
			"connection.blocked":     true,
		},
	}

	args := EncodeMethodArgs(
		uint8(ProtocolMajor),     // version-major
		uint8(ProtocolMinor),     // version-minor
		table(serverProps),       // server-properties
		c.advertisedMechanisms(), // mechanisms
		"en_US",                  // locales
	)

	return c.codec.WriteMethodFrame(0, ClassConnection, MethodConnectionStart, args)
}

// handleStartOk processes Connection.StartOk from client.
func (c *Connection) handleStartOk(args []byte) error {
	r := &argsReader{data: args}

	// Client properties
	if _, err := r.ReadTable(); err != nil {
		return fmt.Errorf("connection.start-ok client properties: %w", err)
	}
	// Mechanism
	mechanism, err := r.ReadString()
	if err != nil {
		return fmt.Errorf("connection.start-ok mechanism: %w", err)
	}
	// Response
	response, err := r.ReadBytes()
	if err != nil {
		return fmt.Errorf("connection.start-ok response: %w", err)
	}
	// Locale
	if _, err := r.ReadString(); err != nil {
		return fmt.Errorf("connection.start-ok locale: %w", err)
	}
	if err := c.authenticate(mechanism, response); err != nil {
		return err
	}
	c.state.Store(connStateStart)

	// Send Connection.Tune
	return c.sendTune()
}

func (c *Connection) advertisedMechanisms() string {
	mechanisms := ""
	if c.server.cfg.Username != "" || c.server.cfg.Password != "" || c.server.cfg.AllowInsecure {
		mechanisms = "PLAIN"
	}
	if tc, ok := c.netConn.(*tls.Conn); ok {
		state := tc.ConnectionState()
		if len(state.PeerCertificates) > 0 && len(state.VerifiedChains) > 0 {
			if mechanisms != "" {
				mechanisms += " "
			}
			mechanisms += "EXTERNAL"
		}
	}
	return mechanisms
}

func (c *Connection) authenticate(mechanism string, response []byte) error {
	switch mechanism {
	case "PLAIN":
		if _, ok := c.netConn.(*tls.Conn); !ok && !c.server.cfg.AllowInsecure {
			return errors.New("PLAIN authentication requires TLS")
		}
		username, password, ok := parsePlainResponse(response)
		if !ok {
			return errors.New("invalid PLAIN authentication response")
		}
		if c.server.cfg.ACL != nil {
			if principal, ok := c.server.cfg.ACL.Authenticate(password); ok {
				release, err := c.server.cfg.ACL.AdmitConnection(principal)
				if err != nil {
					return err
				}
				c.release = release
				c.username = principal.Name
				return nil
			}
		}
		if c.server.cfg.Username == "" && c.server.cfg.Password == "" {
			if !c.server.cfg.AllowInsecure && c.server.cfg.ACL == nil {
				return errors.New("PLAIN authentication is not configured")
			}
		} else if !secureEqual(username, c.server.cfg.Username) || !secureEqual(password, c.server.cfg.Password) {
			return errors.New("invalid AMQP credentials")
		}
		c.username = username
		return nil
	case "EXTERNAL":
		tc, ok := c.netConn.(*tls.Conn)
		if !ok {
			return errors.New("EXTERNAL authentication requires TLS")
		}
		state := tc.ConnectionState()
		if len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
			return errors.New("EXTERNAL authentication requires a verified client certificate")
		}
		if c.server.cfg.ACL != nil {
			if principal, ok := c.server.cfg.ACL.AuthenticateCertificate(state.VerifiedChains[0][0]); ok {
				release, err := c.server.cfg.ACL.AdmitConnection(principal)
				if err != nil {
					return err
				}
				c.release = release
				c.username = principal.Name
				return nil
			}
		}
		c.username = state.PeerCertificates[0].Subject.String()
		return nil
	default:
		return fmt.Errorf("unsupported AMQP authentication mechanism %q", mechanism)
	}
}

func parsePlainResponse(response []byte) (string, string, bool) {
	first := -1
	second := -1
	for i, b := range response {
		if b != 0 {
			continue
		}
		if first < 0 {
			first = i
		} else {
			second = i
			break
		}
	}
	if first < 0 || second < 0 || second == len(response)-1 {
		return "", "", false
	}
	return string(response[first+1 : second]), string(response[second+1:]), true
}

func secureEqual(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// sendTune sends Connection.Tune to the client.
func (c *Connection) sendTune() error {
	args := EncodeMethodArgs(
		uint16(ChannelMax),            // channel-max
		uint32(c.frameMax),            // frame-max
		uint16(c.heartbeat.Seconds()), // heartbeat
	)
	return c.codec.WriteMethodFrame(0, ClassConnection, MethodConnectionTune, args)
}

// handleTuneOk processes Connection.TuneOk from client.
func (c *Connection) handleTuneOk(args []byte) error {
	r := &argsReader{data: args}

	// Channel-max
	chanMax, _ := r.ReadUint16()
	// Frame-max
	frameMax, _ := r.ReadUint32()
	// Heartbeat
	heartbeat, _ := r.ReadUint16()

	if frameMax > 0 && frameMax < c.frameMax {
		c.frameMax = frameMax
		c.codec.SetFrameMax(frameMax)
	}

	c.clientHeartbeat = heartbeat

	// Calculate effective heartbeat
	if c.clientHeartbeat == 0 || c.heartbeat.Seconds() == 0 {
		c.heartbeat = 0
	} else {
		if c.clientHeartbeat < uint16(c.heartbeat.Seconds()) {
			c.heartbeat = time.Duration(c.clientHeartbeat) * time.Second
		}
	}

	_ = chanMax
	c.state.Store(connStateTune)

	// Send Connection.Open
	return c.sendConnectionOpen()
}

// sendConnectionOpen sends Connection.Open to the client.
func (c *Connection) sendConnectionOpen() error {
	args := EncodeMethodArgs(
		string(c.vhost), // vhost
		uint8(0),        // insist
	)
	return c.codec.WriteMethodFrame(0, ClassConnection, MethodConnectionOpen, args)
}

// handleConnectionOpen processes Connection.Open from client.
func (c *Connection) handleConnectionOpen(args []byte) error {
	r := &argsReader{data: args}
	vhost, _ := r.ReadString()
	c.vhost = vhost

	// Send Connection.OpenOk
	openOkArgs := EncodeMethodArgs(
		string(""), // known-hosts
		uint8(0),   // reserved-1
	)
	if err := c.codec.WriteMethodFrame(0, ClassConnection, MethodConnectionOpenOk, openOkArgs); err != nil {
		return err
	}

	c.state.Store(connStateOpen)
	log.Printf("[amqp] connection opened from %s (vhost=%s)", c.netConn.RemoteAddr(), vhost)

	return nil
}

// handleConnectionClose processes Connection.Close from client.
func (c *Connection) handleConnectionClose(args []byte) error {
	r := &argsReader{data: args}
	replyCode, _ := r.ReadUint16()
	replyText, _ := r.ReadString()
	_, _ = r.ReadUint16() // class-id
	_, _ = r.ReadUint16() // method-id

	log.Printf("[amqp] connection close: %d %s", replyCode, replyText)

	// Close all channels
	c.channelsMu.Lock()
	for _, ch := range c.channels {
		ch.close()
	}
	c.channels = make(map[uint16]*Channel)
	c.channelsMu.Unlock()

	// Send Connection.CloseOk
	return c.codec.WriteMethodFrame(0, ClassConnection, MethodConnectionCloseOk, nil)
}

// readLoop is the main connection read loop.
func (c *Connection) readLoop() error {
	deadline := time.Now().Add(c.heartbeat * 3)
	_ = c.netConn.SetReadDeadline(deadline)

	for {
		select {
		case <-c.closed:
			return nil
		default:
		}

		frame, err := c.codec.ReadFrame()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return err
			}
			// Check if it's a timeout
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return fmt.Errorf("heartbeat timeout")
			}
			return err
		}

		// Reset read deadline
		deadline = time.Now().Add(c.heartbeat * 3)
		_ = c.netConn.SetReadDeadline(deadline)

		switch frame.Type {
		case FrameMethod:
			if err := c.handleMethodFrame(frame); err != nil {
				return err
			}
		case FrameHeartbeat:
			// Heartbeat received, connection alive
		case FrameHeader, FrameBody:
			// Content frames are handled by channels
			ch, err := c.getChannel(frame.ChannelID)
			if err != nil {
				continue
			}
			ch.handleContentFrame(frame)
		}
	}
}

// heartbeatLoop sends periodic heartbeats.
func (c *Connection) heartbeatLoop() {
	if c.heartbeat <= 0 {
		return
	}

	ticker := time.NewTicker(c.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.codec.WriteHeartbeat(); err != nil {
				return
			}
		case <-c.closed:
			return
		}
	}
}

// getChannel returns a channel by ID, or an error if not found.
func (c *Connection) getChannel(id uint16) (*Channel, error) {
	c.channelsMu.RLock()
	ch, ok := c.channels[id]
	c.channelsMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("channel %d not found", id)
	}
	return ch, nil
}

// createChannel creates a new channel.
func (c *Connection) createChannel(id uint16) *Channel {
	ch := NewChannel(c, id)
	c.channelsMu.Lock()
	c.channels[id] = ch
	c.channelsMu.Unlock()
	return ch
}

// removeChannel removes a channel.
func (c *Connection) removeChannel(id uint16) {
	c.channelsMu.Lock()
	delete(c.channels, id)
	c.channelsMu.Unlock()
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		if c.release != nil {
			c.release()
		}
		c.state.Store(connStateClosed)
		close(c.closed)
		c.netConn.Close()
	})
	return nil
}

// RemoteAddr returns the remote network address.
func (c *Connection) RemoteAddr() net.Addr {
	return c.netConn.RemoteAddr()
}

// VHost returns the virtual host.
func (c *Connection) VHost() string {
	return c.vhost
}

// argsReader is a helper for reading AMQP method arguments.
type argsReader struct {
	data []byte
	pos  int
}

func (r *argsReader) ReadUint8() (uint8, error) {
	if r.pos >= len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := r.data[r.pos]
	r.pos++
	return v, nil
}

func (r *argsReader) ReadUint16() (uint16, error) {
	if r.pos+2 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := uint16(r.data[r.pos])<<8 | uint16(r.data[r.pos+1])
	r.pos += 2
	return v, nil
}

func (r *argsReader) ReadUint32() (uint32, error) {
	if r.pos+4 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := uint32(r.data[r.pos])<<24 | uint32(r.data[r.pos+1])<<16 | uint32(r.data[r.pos+2])<<8 | uint32(r.data[r.pos+3])
	r.pos += 4
	return v, nil
}

func (r *argsReader) ReadUint64() (uint64, error) {
	if r.pos+8 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := uint64(r.data[r.pos])<<56 | uint64(r.data[r.pos+1])<<48 | uint64(r.data[r.pos+2])<<40 | uint64(r.data[r.pos+3])<<32 |
		uint64(r.data[r.pos+4])<<24 | uint64(r.data[r.pos+5])<<16 | uint64(r.data[r.pos+6])<<8 | uint64(r.data[r.pos+7])
	r.pos += 8
	return v, nil
}

func (r *argsReader) ReadBool() (bool, error) {
	v, err := r.ReadUint8()
	return v != 0, err
}

func (r *argsReader) ReadString() (string, error) {
	length, err := r.ReadUint16()
	if err != nil {
		return "", err
	}
	if r.pos+int(length) > len(r.data) {
		return "", io.ErrUnexpectedEOF
	}
	s := string(r.data[r.pos : r.pos+int(length)])
	r.pos += int(length)
	return s, nil
}

func (r *argsReader) ReadLongString() (string, error) {
	length, err := r.ReadUint32()
	if err != nil {
		return "", err
	}
	if r.pos+int(length) > len(r.data) {
		return "", io.ErrUnexpectedEOF
	}
	s := string(r.data[r.pos : r.pos+int(length)])
	r.pos += int(length)
	return s, nil
}

func (r *argsReader) ReadBytes() ([]byte, error) {
	length, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}
	if r.pos+int(length) > len(r.data) {
		return nil, io.ErrUnexpectedEOF
	}
	b := make([]byte, length)
	copy(b, r.data[r.pos:r.pos+int(length)])
	r.pos += int(length)
	return b, nil
}

func (r *argsReader) ReadTable() (table, error) {
	data, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}
	return DecodeTable(data)
}
