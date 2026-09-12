package cluster

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultHandshakeTimeout = 5 * time.Second
	defaultReadTimeout      = 60 * time.Second
	defaultWriteTimeout     = 5 * time.Second
	defaultWriteBufLimit    = 10 * 1024 * 1024 // 10 MB
	healthCheckInterval     = 30 * time.Second
)

// TCPTransport implements the Transport interface using TCP.
type TCPTransport struct {
	mu              sync.RWMutex
	addr            string
	nodeID          string
	listener        net.Listener
	conns           map[string]*nodeConn
	connsMu         sync.RWMutex
	handler         func(from string, msg *RaftMessage)
	stopCh          chan struct{}
	wg              sync.WaitGroup
	TLSConfig       *tls.Config
	requestID       uint64 // atomic counter
	dialMu          sync.Mutex
	pendingMu       sync.Mutex
	pendingRequests map[uint64]chan *RaftMessage
	closed          atomic.Bool

	HandshakeTimeout time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	WriteBufLimit    int64
}

type nodeConn struct {
	conn         net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	mu           sync.Mutex
	stopCh       chan struct{}
	dead         atomic.Bool
	lastActivity time.Time
	writeBytes   int64
	bytesSent    atomic.Uint64
	bytesRecv    atomic.Uint64
}

// NewTCPTransport creates a new TCP transport.
func NewTCPTransport() *TCPTransport {
	return &TCPTransport{
		conns:            make(map[string]*nodeConn),
		stopCh:           make(chan struct{}),
		pendingRequests:  make(map[uint64]chan *RaftMessage),
		HandshakeTimeout: defaultHandshakeTimeout,
		ReadTimeout:      defaultReadTimeout,
		WriteTimeout:     defaultWriteTimeout,
		WriteBufLimit:    defaultWriteBufLimit,
	}
}

// Listen starts listening for incoming connections.
func (t *TCPTransport) Listen(addr string, handler func(from string, msg *RaftMessage)) error {
	if t.closed.Load() {
		return ErrClosed
	}
	var ln net.Listener
	var err error

	if t.TLSConfig != nil {
		ln, err = tls.Listen("tcp", addr, t.TLSConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	t.addr = addr
	t.listener = ln
	t.handler = handler

	t.wg.Add(1)
	go t.acceptLoop()

	t.wg.Add(1)
	go t.healthCheckLoop()

	return nil
}

// acceptLoop accepts incoming connections.
func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				log.Printf("[transport] accept error: %v", err)
				continue
			}
		}

		t.wg.Add(1)
		go t.handleConn(conn)
	}
}

// handleConn handles an incoming connection (server side - reads handshake first).
func (t *TCPTransport) handleConn(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	// First message should be a handshake with node ID
	conn.SetReadDeadline(time.Now().Add(t.HandshakeTimeout))
	nodeIDBytes, err := reader.ReadBytes('\n')
	if err != nil {
		log.Printf("[transport] handshake read error: %v", err)
		return
	}
	conn.SetReadDeadline(time.Time{})

	nodeID := string(nodeIDBytes)
	if len(nodeID) > 0 && nodeID[len(nodeID)-1] == '\n' {
		nodeID = nodeID[:len(nodeID)-1]
	}

	nc := &nodeConn{
		conn:         conn,
		reader:       reader,
		writer:       writer,
		stopCh:       make(chan struct{}),
		lastActivity: time.Now(),
	}

	t.connsMu.Lock()
	if t.closed.Load() {
		t.connsMu.Unlock()
		return
	}
	// Close existing connection to this node
	if old, ok := t.conns[nodeID]; ok {
		close(old.stopCh)
		old.conn.Close()
	}
	t.conns[nodeID] = nc
	t.connsMu.Unlock()

	// Send our ID back
	id := t.nodeID
	if id == "" {
		id = t.addr
	}
	writer.WriteString(id + "\n")
	writer.Flush()

	log.Printf("[transport] connected to node %s at %s", nodeID, conn.RemoteAddr())

	t.readLoop(nc, nodeID)
}

// readLoop reads messages from a connection and dispatches them.
// It routes responses (with a CorrelationID) to pending request channels,
// and passes all other messages to the handler.
func (t *TCPTransport) readLoop(nc *nodeConn, nodeID string) {
	for {
		select {
		case <-nc.stopCh:
			return
		default:
		}

		nc.conn.SetReadDeadline(time.Now().Add(t.ReadTimeout))
		line, err := nc.reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("[transport] read error from %s: %v", nodeID, err)
			}
			return
		}
		nc.conn.SetReadDeadline(time.Time{})

		nc.mu.Lock()
		nc.lastActivity = time.Now()
		nc.mu.Unlock()
		nc.bytesRecv.Add(uint64(len(line)))

		msg := &RaftMessage{}
		if err := json.Unmarshal(line, msg); err != nil {
			log.Printf("[transport] unmarshal error from %s: %v", nodeID, err)
			continue
		}

		// Internal heartbeat messages are not dispatched to the handler
		if msg.Type == MsgHeartbeat || msg.Type == MsgHeartbeatResp {
			continue
		}

		// Route response messages to pending request channels
		if msg.CorrelationID != 0 {
			t.pendingMu.Lock()
			ch, ok := t.pendingRequests[msg.CorrelationID]
			t.pendingMu.Unlock()
			if ok {
				select {
				case ch <- msg:
				default:
				}
				continue
			}
		}

		if t.handler != nil {
			t.handler(nodeID, msg)
		}
	}
}

// requestResponse sends a request message and waits for the correlated response.
func (t *TCPTransport) requestResponse(nodeID string, msg *RaftMessage) (*RaftMessage, error) {
	id := atomic.AddUint64(&t.requestID, 1)
	msg.CorrelationID = id

	ch := make(chan *RaftMessage, 1)
	t.pendingMu.Lock()
	t.pendingRequests[id] = ch
	t.pendingMu.Unlock()

	defer func() {
		t.pendingMu.Lock()
		delete(t.pendingRequests, id)
		t.pendingMu.Unlock()
	}()

	if err := t.Send(nodeID, msg); err != nil {
		return nil, err
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-t.stopCh:
		return nil, fmt.Errorf("transport closed")
	case <-time.After(t.ReadTimeout):
		return nil, fmt.Errorf("request to %s timed out", nodeID)
	}
}

// Send sends a message to a specific node.
func (t *TCPTransport) Send(nodeID string, msg *RaftMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	nc, err := t.getConn(nodeID)
	if err != nil {
		return err
	}

	// Send via writeTo; if the connection is broken, attempt to reconnect and retry once.
	err = t.writeTo(nc, data)
	if err == nil {
		return nil
	}

	if rerr := t.reconnect(nodeID); rerr != nil {
		return fmt.Errorf("send to %s failed: %w (reconnect: %v)", nodeID, err, rerr)
	}

	nc, err = t.getConn(nodeID)
	if err != nil {
		return fmt.Errorf("send to %s failed after reconnect: %w", nodeID, err)
	}

	return t.writeTo(nc, data)
}

// writeTo serializes and writes data to a connection.
func (t *TCPTransport) writeTo(nc *nodeConn, data []byte) error {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	if nc.dead.Load() {
		return fmt.Errorf("connection dead")
	}

	writeSize := int64(len(data)) + 1 // +1 for '\n'
	if nc.writeBytes+writeSize > t.WriteBufLimit {
		return fmt.Errorf("write buffer limit exceeded (%d >= %d)", nc.writeBytes+writeSize, t.WriteBufLimit)
	}

	nc.writeBytes += writeSize
	nc.conn.SetWriteDeadline(time.Now().Add(t.WriteTimeout))

	_, err := nc.writer.Write(data)
	if err != nil {
		nc.writeBytes -= writeSize
		return err
	}
	nc.writer.WriteByte('\n')
	nc.conn.SetWriteDeadline(time.Time{})

	err = nc.writer.Flush()
	if err != nil {
		nc.writeBytes -= writeSize
		return err
	}

	nc.writeBytes -= writeSize
	nc.lastActivity = time.Now()
	nc.bytesSent.Add(uint64(writeSize))
	return nil
}

// reconnect closes the broken connection and redials up to 3 times with
// exponential backoff (100ms, 200ms, 400ms).
func (t *TCPTransport) reconnect(nodeID string) error {
	t.removeConn(nodeID)

	delays := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	var lastErr error

	for _, delay := range delays {
		time.Sleep(delay)
		_, err := t.dial(nodeID)
		if err == nil {
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("reconnect to %s failed: %w", nodeID, lastErr)
}

// removeConn removes a connection from the map and closes it.
func (t *TCPTransport) removeConn(nodeID string) {
	t.connsMu.Lock()
	if nc, ok := t.conns[nodeID]; ok {
		close(nc.stopCh)
		nc.conn.Close()
		delete(t.conns, nodeID)
	}
	t.connsMu.Unlock()
}

// healthCheckLoop periodically pings connections and marks dead ones.
func (t *TCPTransport) healthCheckLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.checkConnections()
		}
	}
}

// checkConnections iterates over all connections and pings idle ones.
func (t *TCPTransport) checkConnections() {
	t.connsMu.RLock()
	nodeIDs := make([]string, 0, len(t.conns))
	conns := make(map[string]*nodeConn, len(t.conns))
	for id, nc := range t.conns {
		nodeIDs = append(nodeIDs, id)
		conns[id] = nc
	}
	t.connsMu.RUnlock()

	for _, nodeID := range nodeIDs {
		select {
		case <-t.stopCh:
			return
		default:
		}

		nc := conns[nodeID]

		nc.mu.Lock()
		idle := time.Since(nc.lastActivity)
		nc.mu.Unlock()

		// Only ping connections idle for more than half the check interval
		if idle < healthCheckInterval/2 {
			continue
		}

		heartbeat := &RaftMessage{Type: MsgHeartbeat}
		if err := t.Send(nodeID, heartbeat); err != nil {
			log.Printf("[transport] health check failed for %s: %v", nodeID, err)
			nc.dead.Store(true)
		}
	}
}

// SendAppendEntries sends AppendEntries to a node and waits for the response.
func (t *TCPTransport) SendAppendEntries(nodeID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	msg := &RaftMessage{
		Type:          MsgAppendEntries,
		AppendEntries: req,
	}

	resp, err := t.requestResponse(nodeID, msg)
	if err != nil {
		return nil, err
	}
	return resp.AppendEntriesResp, nil
}

// SendRequestVote sends RequestVote to a node and waits for the response.
func (t *TCPTransport) SendRequestVote(nodeID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	msg := &RaftMessage{
		Type:        MsgRequestVote,
		RequestVote: req,
	}

	resp, err := t.requestResponse(nodeID, msg)
	if err != nil {
		return nil, err
	}
	return resp.RequestVoteResp, nil
}

// SendInstallSnapshot sends a snapshot to a node and waits for the response.
func (t *TCPTransport) SendInstallSnapshot(nodeID string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	msg := &RaftMessage{
		Type:            MsgInstallSnapshot,
		InstallSnapshot: req,
	}

	resp, err := t.requestResponse(nodeID, msg)
	if err != nil {
		return nil, err
	}
	return resp.InstallSnapshotResp, nil
}

// getConn returns a connection to a node, creating one if necessary.
func (t *TCPTransport) getConn(nodeID string) (*nodeConn, error) {
	if t.closed.Load() {
		return nil, ErrClosed
	}
	t.connsMu.RLock()
	nc, ok := t.conns[nodeID]
	t.connsMu.RUnlock()

	if ok {
		if nc.dead.Load() {
			t.removeConn(nodeID)
		} else {
			return nc, nil
		}
	}

	t.dialMu.Lock()
	defer t.dialMu.Unlock()

	// Double-check after acquiring lock
	t.connsMu.RLock()
	nc, ok = t.conns[nodeID]
	t.connsMu.RUnlock()
	if ok {
		if nc.dead.Load() {
			t.removeConn(nodeID)
		} else {
			return nc, nil
		}
	}

	return t.dial(nodeID)
}

// dial creates a new connection to a node.
func (t *TCPTransport) dial(nodeID string) (*nodeConn, error) {
	dialer := &net.Dialer{Timeout: t.HandshakeTimeout}

	var conn net.Conn
	var err error

	if t.TLSConfig != nil {
		conn, err = tls.DialWithDialer(dialer, "tcp", nodeID, t.TLSConfig)
	} else {
		conn, err = dialer.Dial("tcp", nodeID)
	}
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", nodeID, err)
	}

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	// Send handshake
	id := t.nodeID
	if id == "" {
		id = t.addr
	}
	writer.WriteString(id + "\n")
	writer.Flush()

	// Read response
	conn.SetReadDeadline(time.Now().Add(t.HandshakeTimeout))
	_, err = reader.ReadBytes('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("handshake failed: %w", err)
	}
	conn.SetReadDeadline(time.Time{})

	nc := &nodeConn{
		conn:         conn,
		reader:       reader,
		writer:       writer,
		stopCh:       make(chan struct{}),
		lastActivity: time.Now(),
	}

	t.connsMu.Lock()
	if t.closed.Load() {
		t.connsMu.Unlock()
		_ = conn.Close()
		return nil, ErrClosed
	}
	// Close existing connection to this node if any
	if old, ok := t.conns[nodeID]; ok {
		close(old.stopCh)
		old.conn.Close()
	}
	t.conns[nodeID] = nc
	t.wg.Add(1)
	t.connsMu.Unlock()

	log.Printf("[transport] dialed node %s", nodeID)

	// Start read loop (handshake already done in dial)
	go func() {
		defer t.wg.Done()
		t.readLoop(nc, nodeID)
	}()

	return nc, nil
}

// Close stops the transport.
func (t *TCPTransport) Close() error {
	if !t.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(t.stopCh)
	// Stop accepting before draining connections. A connection accepted during
	// the drain could otherwise be added after the map snapshot and keep its
	// read loop alive forever.
	if t.listener != nil {
		_ = t.listener.Close()
	}

	// Drain pending requests before closing connections
	t.pendingMu.Lock()
	for id, ch := range t.pendingRequests {
		close(ch)
		delete(t.pendingRequests, id)
	}
	t.pendingMu.Unlock()

	t.connsMu.Lock()
	for _, nc := range t.conns {
		close(nc.stopCh)
		nc.conn.Close()
	}
	t.conns = make(map[string]*nodeConn)
	t.connsMu.Unlock()

	t.wg.Wait()
	return nil
}

// Addr returns the local address.
func (t *TCPTransport) Addr() string {
	return t.addr
}

// ConnectedNodes returns the list of connected node IDs.
func (t *TCPTransport) ConnectedNodes() []string {
	t.connsMu.RLock()
	defer t.connsMu.RUnlock()

	nodes := make([]string, 0, len(t.conns))
	for id := range t.conns {
		nodes = append(nodes, id)
	}
	return nodes
}

// Stats returns a snapshot of transport statistics.
func (t *TCPTransport) Stats() map[string]any {
	t.connsMu.RLock()
	connCount := len(t.conns)
	var totalSent, totalRecv uint64
	connectedNodes := make([]string, 0, connCount)
	for id, nc := range t.conns {
		connectedNodes = append(connectedNodes, id)
		totalSent += nc.bytesSent.Load()
		totalRecv += nc.bytesRecv.Load()
	}
	t.connsMu.RUnlock()

	t.pendingMu.Lock()
	pendingCount := len(t.pendingRequests)
	t.pendingMu.Unlock()

	return map[string]any{
		"connection_count": connCount,
		"connected_nodes":  connectedNodes,
		"pending_requests": pendingCount,
		"total_bytes_sent": totalSent,
		"total_bytes_recv": totalRecv,
	}
}

// MemoryTransport is a transport for testing (in-process).
type MemoryTransport struct {
	mu      sync.RWMutex
	nodes   map[string]*MemoryTransport
	blocked map[string]bool
	handler func(from string, msg *RaftMessage)
	id      string
	stopCh  chan struct{}

	// Per-type response channels (fan-out safe)
	aeRespCh chan *AppendEntriesResponse
	rvRespCh chan *RequestVoteResponse
	isRespCh chan *InstallSnapshotResponse
}

// NewMemoryTransport creates a new in-memory transport for testing.
func NewMemoryTransport(id string) *MemoryTransport {
	return &MemoryTransport{
		nodes:    make(map[string]*MemoryTransport),
		blocked:  make(map[string]bool),
		id:       id,
		stopCh:   make(chan struct{}),
		aeRespCh: make(chan *AppendEntriesResponse, 100),
		rvRespCh: make(chan *RequestVoteResponse, 100),
		isRespCh: make(chan *InstallSnapshotResponse, 100),
	}
}

// Register registers this transport with a cluster registry.
func (t *MemoryTransport) Register(registry map[string]*MemoryTransport) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Register self
	registry[t.id] = t

	// Update our nodes map with all registered nodes
	for id, node := range registry {
		if id != t.id {
			t.nodes[id] = node
			// Also register us with the other node
			node.mu.Lock()
			node.nodes[t.id] = t
			node.mu.Unlock()
		}
	}
}

// Listen starts handling incoming messages.
func (t *MemoryTransport) Listen(addr string, handler func(from string, msg *RaftMessage)) error {
	t.mu.Lock()
	t.handler = handler
	t.mu.Unlock()
	return nil
}

// Send sends a message to a node.
func (t *MemoryTransport) Send(nodeID string, msg *RaftMessage) error {
	t.mu.RLock()
	target, ok := t.nodes[nodeID]
	blocked := t.blocked[nodeID]
	t.mu.RUnlock()

	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}
	target.mu.RLock()
	blocked = blocked || target.blocked[t.id]
	handler := target.handler
	target.mu.RUnlock()
	if blocked {
		return ErrPartitioned
	}

	// Route responses to dedicated channels instead of handler
	switch msg.Type {
	case MsgAppendEntriesResp:
		select {
		case target.aeRespCh <- msg.AppendEntriesResp:
		default:
		}
		return nil
	case MsgRequestVoteResp:
		select {
		case target.rvRespCh <- msg.RequestVoteResp:
		default:
		}
		return nil
	case MsgInstallSnapshotResp:
		select {
		case target.isRespCh <- msg.InstallSnapshotResp:
		default:
		}
		return nil
	}

	// Non-response messages go to handler
	if handler != nil {
		go handler(t.id, msg)
	}

	return nil
}

// SetPartition toggles a bidirectional test-network partition between this
// transport and nodeID. Send checks both endpoints, so changing either side is
// sufficient to isolate the link.
func (t *MemoryTransport) SetPartition(nodeID string, partitioned bool) {
	t.mu.Lock()
	if partitioned {
		t.blocked[nodeID] = true
	} else {
		delete(t.blocked, nodeID)
	}
	t.mu.Unlock()
}

// SendAppendEntries sends AppendEntries to a node.
func (t *MemoryTransport) SendAppendEntries(nodeID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	msg := &RaftMessage{
		Type:          MsgAppendEntries,
		AppendEntries: req,
	}

	t.mu.RLock()
	_, ok := t.nodes[nodeID]
	t.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	err := t.Send(nodeID, msg)
	if err != nil {
		return nil, err
	}

	// Response arrives on our own channel
	select {
	case resp := <-t.aeRespCh:
		return resp, nil
	case <-time.After(500 * time.Millisecond):
		return &AppendEntriesResponse{Term: req.Term, Success: false}, nil
	}
}

// SendRequestVote sends RequestVote to a node.
func (t *MemoryTransport) SendRequestVote(nodeID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	msg := &RaftMessage{
		Type:        MsgRequestVote,
		RequestVote: req,
	}

	t.mu.RLock()
	_, ok := t.nodes[nodeID]
	t.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	err := t.Send(nodeID, msg)
	if err != nil {
		return nil, err
	}

	// Response arrives on our own channel
	select {
	case resp := <-t.rvRespCh:
		return resp, nil
	case <-time.After(500 * time.Millisecond):
		return &RequestVoteResponse{Term: req.Term, VoteGranted: false}, nil
	}
}

// SendInstallSnapshot sends a snapshot to a node.
func (t *MemoryTransport) SendInstallSnapshot(nodeID string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	msg := &RaftMessage{Type: MsgInstallSnapshot, InstallSnapshot: req}
	t.mu.RLock()
	_, ok := t.nodes[nodeID]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	if err := t.Send(nodeID, msg); err != nil {
		return nil, err
	}
	select {
	case resp := <-t.isRespCh:
		return resp, nil
	case <-t.stopCh:
		return nil, ErrClosed
	case <-time.After(500 * time.Millisecond):
		return nil, ErrSnapshotFailed
	}
}

// Close stops the transport.
func (t *MemoryTransport) Close() error {
	close(t.stopCh)
	return nil
}
