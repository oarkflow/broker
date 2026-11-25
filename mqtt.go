package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// MQTTConfig configures the MQTT protocol server.
type MQTTConfig struct {
	// ListenAddr is the TCP address to listen on. Default: ":1883".
	// For TLS, use ":8883" or set TLSCertFile/TLSKeyFile.
	ListenAddr string
	// MaxConnections limits concurrent MQTT connections. Default: 1000.
	MaxConnections int
	// MaxInflight limits inbound QoS 2 and outbound QoS 1/2 messages per client. Default: 20.
	MaxInflight int
	// KeepAlive is the max keep-alive seconds. Default: 60.
	KeepAlive uint16
	// MaxPacketSize is the max MQTT packet size. Default: 1MB.
	MaxPacketSize int
	// ConnectTimeout limits how long a client may take to send and complete CONNECT. Default: 5s.
	ConnectTimeout time.Duration
	// WriteTimeout limits how long a packet write may block. Default: 10s.
	WriteTimeout time.Duration
	// RequireAuth requires username/password or ACL authentication. Public
	// production listeners reject startup when authentication is not enabled.
	RequireAuth bool
	// Username/Password for basic auth.
	Username string
	Password string
	// TLSCertFile is the path to the TLS certificate file.
	TLSCertFile string
	// TLSKeyFile is the path to the TLS private key file.
	TLSKeyFile string
	// ClientCAFile is the path to the CA certificate for client verification (mTLS).
	ClientCAFile string
	// RequireClientCert requires client certificate verification (mTLS).
	RequireClientCert bool
	// AllowInsecure permits plaintext or unauthenticated development listeners.
	// Production listeners require TLS and an authentication method.
	AllowInsecure bool
	// BrokerToken is the bearer token for MQTT CONNECT authentication.
	// If set, the MQTT password field is validated against this token.
	BrokerToken string
	// ACL provides shared authentication and admission control across protocols.
	ACL *ACL
}

// MQTT 3.1.1 packet types.
const (
	MQTTConnect     byte = 0x10
	MQTTConnack     byte = 0x20
	MQTTPublish     byte = 0x30
	MQTTPuback      byte = 0x40
	MQTTPubrec      byte = 0x50
	MQTTPubrel      byte = 0x60
	MQTTPubcomp     byte = 0x70
	MQTTSubscribe   byte = 0x82
	MQTTSuback      byte = 0x90
	MQTTUnsubscribe byte = 0xA2
	MQTTUnsuback    byte = 0xB0
	MQTTPingreq     byte = 0xC0
	MQTTPingresp    byte = 0xD0
	MQTTDisconnect  byte = 0xE0
)

// MQTTServer is a lightweight MQTT 3.1.1 server backed by the broker.
type MQTTSession struct {
	Key              string                             `json:"key"`
	SessionID        string                             `json:"session_id"`
	ClientID         string                             `json:"client_id"`
	Subscriptions    map[string]uint8                   `json:"subscriptions,omitempty"`
	QoS1InFlight     map[uint16]MQTTQoS1Pending         `json:"qos1_in_flight,omitempty"`
	QoS2OutInFlight  map[uint16]MQTTQoS2OutboundPending `json:"qos2_out_in_flight,omitempty"`
	QoS2InFlight     map[uint16]MQTTQoS2Pending         `json:"qos2_in_flight,omitempty"`
	NextQoS2Sequence uint64                             `json:"next_qos2_sequence"`
	UpdatedAt        time.Time                          `json:"updated_at"`
}

// MQTTQoS1Pending is a server-to-client PUBLISH awaiting PUBACK. The message
// remains in the session record until the client confirms delivery.
type MQTTQoS1Pending struct {
	MessageID string `json:"message_id"`
	Topic     string `json:"topic"`
	Payload   []byte `json:"payload"`
	Retain    bool   `json:"retain,omitempty"`
}

// MQTTQoS2OutboundPending is a server-to-client PUBLISH/PUBREL exchange. Phase
// is "publish" until PUBREC and "pubrel" until PUBCOMP.
type MQTTQoS2OutboundPending struct {
	MessageID string `json:"message_id"`
	Topic     string `json:"topic"`
	Payload   []byte `json:"payload"`
	Retain    bool   `json:"retain,omitempty"`
	Phase     string `json:"phase"`
}

// MQTTQoS2Pending is the durable part of an inbound QoS 2 publish exchange.
type MQTTQoS2Pending struct {
	Topic    string `json:"topic"`
	Payload  []byte `json:"payload"`
	QoS      byte   `json:"qos"`
	Retain   bool   `json:"retain,omitempty"`
	Sequence uint64 `json:"sequence"`
}

type MQTTServer struct {
	broker     *Broker
	cfg        MQTTConfig
	ln         net.Listener
	conns      map[string]*mqttConn
	clients    map[string]*mqttConn
	mu         sync.RWMutex
	sessions   map[string]*MQTTSession
	sessionsMu sync.Mutex
	retainedMu sync.RWMutex
	retained   map[string]MQTTRetainedMessage
	wg         sync.WaitGroup
	connSeq    atomic.Uint64
	ctx        context.Context
	cancel     context.CancelFunc
}

type mqttConn struct {
	id               string
	conn             net.Conn
	clientID         string
	sessionKey       string
	sessionID        string
	username         string
	principal        AdminPrincipal
	cleanSession     bool
	keepAlive        uint16
	will             *mqttWill
	connected        atomic.Bool
	gracefulClose    atomic.Bool
	suppressWill     atomic.Bool
	willHandled      atomic.Bool
	willMu           sync.Mutex
	willRecord       *MQTTWillRecord
	subs             map[string]uint8 // topic -> qos
	consumers        map[string]*Subscription
	incomingQoS2     map[uint16]mqttPendingPublish
	nextQoS2Sequence uint64
	closed           chan struct{}
	closeOnce        sync.Once
	release          func()
	ready            chan struct{}
	stopping         chan struct{}
	stopOnce         sync.Once
	sessionMu        sync.Mutex
	outgoingQoS1     map[uint16]MQTTQoS1Pending
	outgoingQoS2     map[uint16]MQTTQoS2OutboundPending
	nextOutgoingID   uint16
	outChanged       chan struct{}
	writeMu          sync.Mutex
}

type mqttWill struct {
	id      string
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

func ValidateMQTTWillRecord(record MQTTWillRecord) error {
	if record.ID == "" || len(record.ID) > 128 || record.SessionKey == "" ||
		record.ClientID == "" || !validMQTTUTF8(record.ClientID) ||
		!validMQTTTopicName(record.Topic) || len(record.Payload) > int(^uint16(0)) ||
		record.QoS > 2 || record.CreatedAt.IsZero() {
		return ErrInvalidMessage
	}
	for _, tenant := range record.Tenants {
		if tenant == "" || len(tenant) > 256 || !validMQTTUTF8(tenant) {
			return ErrInvalidMessage
		}
	}
	return nil
}

type mqttSubscriptionRequest struct {
	filter string
	qos    uint8
}

func (mc *mqttConn) signalClosed() {
	mc.closeOnce.Do(func() { close(mc.closed) })
}

func (ms *MQTTServer) principalContext(mc *mqttConn) context.Context {
	return WithAdminPrincipal(ms.ctx, mc.principal)
}

func (ms *MQTTServer) putRetained(ctx context.Context, message MQTTRetainedMessage) error {
	if store, ok := ms.broker.StateStore().(MQTTRetainedStore); ok {
		if len(message.Payload) == 0 {
			return store.DeleteMQTTRetained(ctx, message.TenantID, message.Topic)
		}
		return store.PutMQTTRetained(ctx, message)
	}
	tenant, err := resolveTenant(ctx, message.TenantID)
	if err != nil {
		return err
	}
	key := mqttRetainedKey(tenant, message.Topic)
	ms.retainedMu.Lock()
	defer ms.retainedMu.Unlock()
	if len(message.Payload) == 0 {
		delete(ms.retained, key)
		return nil
	}
	message.TenantID = tenant
	message.Payload = append([]byte(nil), message.Payload...)
	ms.retained[key] = message
	return nil
}

func (ms *MQTTServer) updateRetained(ctx context.Context, topic string, payload []byte, qos byte) error {
	tenant, err := resolveTenant(ctx, "")
	if err != nil {
		return err
	}
	return ms.putRetained(ctx, MQTTRetainedMessage{
		TenantID:  tenant,
		Topic:     topic,
		Payload:   append([]byte(nil), payload...),
		QoS:       qos,
		UpdatedAt: time.Now(),
	})
}

func (ms *MQTTServer) listRetained(ctx context.Context, tenant string) ([]MQTTRetainedMessage, error) {
	if store, ok := ms.broker.StateStore().(MQTTRetainedStore); ok {
		return store.ListMQTTRetained(ctx, tenant)
	}
	resolved, err := resolveTenant(ctx, tenant)
	if err != nil {
		return nil, err
	}
	ms.retainedMu.RLock()
	defer ms.retainedMu.RUnlock()
	out := make([]MQTTRetainedMessage, 0, len(ms.retained))
	for _, message := range ms.retained {
		if tenant != "" && message.TenantID != resolved {
			continue
		}
		if tenantAllowed(ctx, message.TenantID) {
			out = append(out, cloneMQTTRetained(message))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TenantID == out[j].TenantID {
			return out[i].Topic < out[j].Topic
		}
		return out[i].TenantID < out[j].TenantID
	})
	return out, nil
}

type mqttPendingPublish struct {
	MQTTQoS2Pending
}

// NewMQTTServer creates a new MQTT server.
func NewMQTTServer(b *Broker, cfg MQTTConfig) *MQTTServer {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":1883"
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 1000
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 20
	}
	if cfg.KeepAlive == 0 {
		cfg.KeepAlive = 60
	}
	if cfg.MaxPacketSize <= 0 {
		cfg.MaxPacketSize = 1 << 20
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &MQTTServer{
		broker:   b,
		cfg:      cfg,
		conns:    make(map[string]*mqttConn),
		clients:  make(map[string]*mqttConn),
		sessions: make(map[string]*MQTTSession),
		retained: make(map[string]MQTTRetainedMessage),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// ListenAndServe starts the MQTT server.
func (ms *MQTTServer) ListenAndServe() error {
	if ms.ctx.Err() != nil {
		return nil
	}
	if (ms.cfg.Username == "") != (ms.cfg.Password == "") {
		return errors.New("mqtt: username and password must be configured together")
	}
	if (ms.cfg.TLSCertFile == "") != (ms.cfg.TLSKeyFile == "") {
		return errors.New("mqtt: TLS certificate and key must be configured together")
	}
	hasTLS := ms.cfg.TLSCertFile != "" && ms.cfg.TLSKeyFile != ""
	if ms.cfg.RequireClientCert && ms.cfg.ClientCAFile == "" {
		return errors.New("mqtt: client CA is required when client certificates are mandatory")
	}
	certificateAuth := hasTLS && ms.cfg.RequireClientCert && ms.cfg.ClientCAFile != ""
	hasAuthentication := ms.cfg.ACL.hasUsableCredential(certificateAuth) || ms.cfg.BrokerToken != "" ||
		(ms.cfg.RequireAuth && ms.cfg.Username != "" && ms.cfg.Password != "")
	if err := validateListenerSecurity("mqtt", ms.cfg.AllowInsecure, hasTLS, hasAuthentication, ""); err != nil {
		return err
	}
	if err := ms.recoverPendingWills(ms.ctx); err != nil {
		return fmt.Errorf("mqtt: recover pending Wills: %w", err)
	}
	var err error
	var ln net.Listener
	// Set up TLS if configured
	if ms.cfg.TLSCertFile != "" && ms.cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(ms.cfg.TLSCertFile, ms.cfg.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("mqtt: load TLS keypair: %w", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		if ms.cfg.ClientCAFile != "" {
			pem, err := os.ReadFile(ms.cfg.ClientCAFile)
			if err != nil {
				return fmt.Errorf("mqtt: load client CA: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return fmt.Errorf("mqtt: no valid certs in client CA")
			}
			tlsCfg.ClientCAs = pool
		}
		if ms.cfg.RequireClientCert {
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
		ln, err = tls.Listen("tcp", ms.cfg.ListenAddr, tlsCfg)
		if err != nil {
			return err
		}
		log.Printf("[mqtt] listening on %s (TLS)", ms.cfg.ListenAddr)
	} else {
		ln, err = net.Listen("tcp", ms.cfg.ListenAddr)
		if err != nil {
			return err
		}
		log.Printf("[mqtt] listening on %s", ms.cfg.ListenAddr)
	}
	ms.mu.Lock()
	if ms.ctx.Err() != nil {
		ms.mu.Unlock()
		_ = ln.Close()
		return nil
	}
	ms.ln = ln
	ms.mu.Unlock()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ms.ctx.Done():
				return nil
			default:
				log.Printf("[mqtt] accept error: %v", err)
				continue
			}
		}
		ms.mu.Lock()
		if ms.ctx.Err() != nil {
			ms.mu.Unlock()
			_ = conn.Close()
			return nil
		}
		ms.wg.Add(1)
		ms.mu.Unlock()
		go ms.handleConn(conn)
	}
}

// Stop shuts down the MQTT server.
func (ms *MQTTServer) Stop() {
	ms.mu.Lock()
	if ms.ln != nil {
		_ = ms.ln.Close()
	}
	connections := make([]*mqttConn, 0, len(ms.conns))
	for id, c := range ms.conns {
		c.suppressWill.Store(true)
		connections = append(connections, c)
		c.conn.Close()
		delete(ms.conns, id)
	}
	ms.mu.Unlock()

	// A server shutdown closes each active network connection without receiving
	// a client DISCONNECT. MQTT 3.1.1 therefore requires its Will to be
	// published. Use a bounded context detached from the listener's lifetime so
	// cancellation below cannot discard these publications.
	willCtx, cancelWills := context.WithTimeout(context.Background(), 10*time.Second)
	var willWG sync.WaitGroup
	willSlots := make(chan struct{}, 16)
	for _, c := range connections {
		if !c.connected.Load() || c.will == nil || c.gracefulClose.Load() {
			continue
		}
		select {
		case willSlots <- struct{}{}:
		case <-willCtx.Done():
			log.Printf("[mqtt] shutdown timed out before publishing last will for %s: %v", c.clientID, willCtx.Err())
			continue
		}
		willWG.Add(1)
		go func(conn *mqttConn) {
			defer willWG.Done()
			defer func() { <-willSlots }()
			if err := ms.publishWill(WithAdminPrincipal(willCtx, conn.principal), conn); err != nil {
				log.Printf("[mqtt] publish last will during shutdown for %s: %v", conn.clientID, err)
			}
		}(c)
	}
	willWG.Wait()
	cancelWills()
	ms.cancel()
	ms.wg.Wait()
}

func (ms *MQTTServer) handleConn(conn net.Conn) {
	defer ms.wg.Done()
	id := fmt.Sprintf("mqtt-%d", ms.connSeq.Add(1))
	mc := &mqttConn{
		id: id, conn: conn, subs: make(map[string]uint8), consumers: make(map[string]*Subscription),
		incomingQoS2: make(map[uint16]mqttPendingPublish), outgoingQoS1: make(map[uint16]MQTTQoS1Pending), outgoingQoS2: make(map[uint16]MQTTQoS2OutboundPending),
		closed: make(chan struct{}), ready: make(chan struct{}), stopping: make(chan struct{}), outChanged: make(chan struct{}),
	}
	ms.mu.Lock()
	if ms.ctx.Err() != nil {
		ms.mu.Unlock()
		_ = conn.Close()
		return
	}
	if len(ms.conns) >= ms.cfg.MaxConnections {
		ms.mu.Unlock()
		conn.Close()
		return
	}
	ms.conns[id] = mc
	ms.mu.Unlock()
	defer func() {
		mc.stopOnce.Do(func() { close(mc.stopping) })
		ms.detachSubscriptions(mc)
		if mc.connected.Load() && mc.will != nil && !mc.gracefulClose.Load() && !mc.suppressWill.Load() && ms.ctx.Err() == nil {
			if err := ms.publishWill(ms.principalContext(mc), mc); err != nil {
				log.Printf("[mqtt] publish last will for %s: %v", mc.clientID, err)
			}
		}
		if mc.release != nil {
			mc.release()
		}
		ms.mu.Lock()
		delete(ms.conns, id)
		if ms.clients[mc.clientID] == mc {
			delete(ms.clients, mc.clientID)
		}
		ms.mu.Unlock()
		conn.Close()
		mc.signalClosed()
	}()

	// Read first packet (must be CONNECT)
	if err := conn.SetReadDeadline(time.Now().Add(ms.cfg.ConnectTimeout)); err != nil {
		return
	}
	fixedHeader, remaining, err := ms.readPacketHeader(conn)
	if err != nil {
		return
	}
	if fixedHeader != MQTTConnect {
		return
	}
	if remaining > ms.cfg.MaxPacketSize {
		return
	}
	payload := make([]byte, remaining)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return
	}
	if !ms.handleConnect(mc, payload) {
		return
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	mc.connected.Store(true)
	// Start keep-alive timer
	var keepAliveTimer *time.Timer
	if mc.keepAlive > 0 {
		timeout := time.Duration(mc.keepAlive) * time.Second * 3 / 2
		keepAliveTimer = time.AfterFunc(timeout, func() {
			conn.Close()
		})
	}
	defer func() {
		if keepAliveTimer != nil {
			keepAliveTimer.Stop()
		}
	}()
	if err := ms.resendOutbound(mc); err != nil {
		log.Printf("[mqtt] resume outbound deliveries for %s: %v", mc.clientID, err)
		return
	}
	close(mc.ready)

	// Read loop
	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
		}
		fixedHeader, remaining, err := ms.readPacketHeader(conn)
		if err != nil {
			return
		}
		if remaining > ms.cfg.MaxPacketSize {
			return
		}
		payload := make([]byte, remaining)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}
		if keepAliveTimer != nil {
			keepAliveTimer.Reset(time.Duration(mc.keepAlive) * time.Second * 3 / 2)
		}
		if ms.cfg.ACL != nil {
			switch fixedHeader & 0xF0 {
			case MQTTPublish, MQTTSubscribe & 0xF0, MQTTUnsubscribe & 0xF0:
				if err := ms.cfg.ACL.AdmitRate(mc.principal); err != nil {
					// Disconnect without acknowledging a throttled publish so QoS
					// clients can retry after the rate limit clears.
					return
				}
			}
		}
		switch fixedHeader & 0xF0 {
		case MQTTPublish:
			ms.handlePublish(mc, fixedHeader, payload)
		case MQTTPuback:
			if fixedHeader&0x0F != 0 || !ms.handleOutgoingPuback(mc, payload) {
				return
			}
		case MQTTPubrec:
			if fixedHeader&0x0F != 0 || !ms.handleOutgoingPubrec(mc, payload) {
				return
			}
		case MQTTPubrel:
			if fixedHeader&0x0F != 0x02 || len(payload) != 2 {
				return
			}
			pktID := binary.BigEndian.Uint16(payload)
			if pktID == 0 {
				return
			}
			if err := ms.completeIncomingQoS2(mc, pktID); err != nil {
				log.Printf("[mqtt] qos2 publish error: %v", err)
				continue
			}
			ms.sendPubcomp(mc, pktID)
		case MQTTPubcomp:
			if fixedHeader&0x0F != 0 || !ms.handleOutgoingPubcomp(mc, payload) {
				return
			}
		case MQTTSubscribe & 0xF0:
			if fixedHeader&0x0F != 0x02 {
				return
			}
			if !ms.handleSubscribe(mc, payload) {
				return
			}
		case MQTTUnsubscribe & 0xF0:
			if fixedHeader&0x0F != 0x02 {
				return
			}
			if !ms.handleUnsubscribe(mc, payload) {
				return
			}
		case MQTTPingreq:
			if fixedHeader&0x0F != 0 || len(payload) != 0 {
				return
			}
			ms.sendPacket(mc, MQTTPingresp, nil)
		case MQTTDisconnect:
			if fixedHeader&0x0F != 0 || len(payload) != 0 {
				return
			}
			ms.clearWill(mc)
			return
		default:
			// This direction accepts only client-originated MQTT 3.1.1
			// control packets. Reserved and server-originated types are malformed.
			return
		}
	}
}

func (ms *MQTTServer) handleConnect(mc *mqttConn, payload []byte) bool {
	if len(payload) < 10 {
		return false
	}
	// Protocol name
	protoLen := int(binary.BigEndian.Uint16(payload[0:2]))
	if len(payload) < 2+protoLen+4 {
		return false
	}
	protoName := string(payload[2 : 2+protoLen])
	if protoName != "MQTT" && protoName != "MQIsdp" {
		return false
	}
	offset := 2 + protoLen
	protocolLevel := payload[offset]
	if (protoName == "MQTT" && protocolLevel != 4) || (protoName == "MQIsdp" && protocolLevel != 3) {
		return false
	}
	offset++
	connectFlags := payload[offset]
	if connectFlags&0x01 != 0 ||
		(connectFlags&0x04 == 0 && (connectFlags&0x38 != 0 || connectFlags&0x20 != 0)) ||
		(connectFlags&0x04 != 0 && (connectFlags>>3)&0x03 == 3) ||
		(connectFlags&0x40 != 0 && connectFlags&0x80 == 0) {
		return false
	}
	cleanSession := (connectFlags & 0x02) != 0
	mc.cleanSession = cleanSession
	offset++
	keepAlive := binary.BigEndian.Uint16(payload[offset:])
	if keepAlive > 0 && keepAlive > ms.cfg.KeepAlive {
		keepAlive = ms.cfg.KeepAlive
	}
	mc.keepAlive = keepAlive
	offset += 2

	// Client ID
	if offset+2 > len(payload) {
		return false
	}
	clientIDLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2
	if offset+clientIDLen > len(payload) {
		return false
	}
	mc.clientID = string(payload[offset : offset+clientIDLen])
	if !validMQTTUTF8(mc.clientID) {
		return false
	}
	offset += clientIDLen

	// Will topic/payload (if will flag set)
	var will *mqttWill
	if connectFlags&0x04 != 0 {
		if offset+2 > len(payload) {
			return false
		}
		willTopicLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if willTopicLen == 0 || offset+willTopicLen > len(payload) {
			return false
		}
		willTopic := string(payload[offset : offset+willTopicLen])
		if !validMQTTTopicName(willTopic) {
			return false
		}
		offset += willTopicLen
		if offset+2 > len(payload) {
			return false
		}
		willPayloadLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if offset+willPayloadLen > len(payload) {
			return false
		}
		willQoS := (connectFlags >> 3) & 0x03
		will = &mqttWill{topic: willTopic, payload: append([]byte(nil), payload[offset:offset+willPayloadLen]...), qos: willQoS, retain: connectFlags&0x20 != 0}
		offset += willPayloadLen
	}

	// Username (if username flag set)
	password := ""
	if connectFlags&0x80 != 0 {
		if offset+2 > len(payload) {
			return false
		}
		userLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if offset+userLen > len(payload) {
			return false
		}
		mc.username = string(payload[offset : offset+userLen])
		if !validMQTTUTF8(mc.username) {
			return false
		}
		offset += userLen
	}
	// Password (if password flag set)
	if connectFlags&0x40 != 0 {
		if offset+2 > len(payload) {
			return false
		}
		passLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if offset+passLen > len(payload) {
			return false
		}
		password = string(payload[offset : offset+passLen])
		offset += passLen
	}
	if offset != len(payload) {
		return false
	}

	// Auth check
	if ms.cfg.ACL != nil {
		var principal AdminPrincipal
		authenticated := false
		if tc, isTLS := mc.conn.(*tls.Conn); isTLS {
			state := tc.ConnectionState()
			if len(state.VerifiedChains) > 0 && len(state.VerifiedChains[0]) > 0 {
				if p, ok := ms.cfg.ACL.AuthenticateCertificate(state.VerifiedChains[0][0]); ok {
					principal = p
					authenticated = true
				}
			}
		}
		if !authenticated && password != "" {
			if p, ok := ms.cfg.ACL.Authenticate(password); ok {
				principal = p
				authenticated = true
			}
		}
		if authenticated {
			mc.principal = principal
			release, err := ms.cfg.ACL.AdmitConnection(principal)
			if err != nil {
				ms.sendConnack(mc, 0x03, false) // Server unavailable
				return false
			}
			mc.release = release
		} else if ms.cfg.RequireAuth || !ms.cfg.AllowInsecure {
			ms.sendConnack(mc, 0x05, false) // Connection Refused: not authorized
			return false
		}
	} else if ms.cfg.BrokerToken != "" {
		// Use broker token for authentication
		if !constantTimeEqual(password, ms.cfg.BrokerToken) {
			ms.sendConnack(mc, 0x05, false) // Connection Refused: not authorized
			return false
		}
	} else if ms.cfg.RequireAuth && (!constantTimeEqual(mc.username, ms.cfg.Username) || !constantTimeEqual(password, ms.cfg.Password)) {
		ms.sendConnack(mc, 0x05, false) // Connection Refused: not authorized
		return false
	}
	if will != nil && ms.cfg.ACL != nil && !ms.cfg.ACL.Authorize(mc.principal, PermissionPublish) {
		ms.sendConnack(mc, 0x05, false)
		return false
	}
	if mc.clientID == "" && !cleanSession {
		ms.sendConnack(mc, 0x02, false) // Identifier rejected.
		return false
	}
	if mc.clientID == "" {
		mc.clientID = "mqtt-" + randomHex(8)
	}
	if _, err := resolveTenant(ms.principalContext(mc), ""); err != nil {
		// MQTT 3.1.1 has no tenant selector. A credential scoped to multiple
		// tenants must use a credential scoped to one tenant for this listener.
		ms.sendConnack(mc, 0x05, false)
		return false
	}
	mc.will = will
	mc.sessionID = randomHex(16)
	identity := mc.principal.Name
	if identity == "" {
		identity = mc.username
	}
	if identity == "" && ms.cfg.BrokerToken != "" {
		identity = "broker-token"
	}
	if identity == "" {
		identity = "anonymous"
	}
	mc.sessionKey = mqttSessionKey(identity, mc.clientID)
	// MQTT permits only one active connection for a client identifier. Close
	// and fully detach the old connection before attaching this one, so stale
	// cleanup cannot remove the replacement connection's routes.
	ms.mu.Lock()
	previous := ms.clients[mc.clientID]
	if previous != nil && previous != mc {
		_ = previous.conn.Close()
	}
	ms.mu.Unlock()
	if previous != nil && previous != mc {
		<-previous.closed
	}
	ms.mu.Lock()
	ms.clients[mc.clientID] = mc
	ms.mu.Unlock()

	sessionPresent := false
	if cleanSession {
		old, found, err := ms.getSession(mc.sessionKey)
		if err != nil {
			ms.sendConnack(mc, 0x03, false)
			return false
		}
		if found {
			for topicFilter := range old.Subscriptions {
				if _, err := ms.broker.Store().PurgeQueue(ms.ctx, mqttSubscriptionQueue(mc.sessionKey, topicFilter)); err != nil {
					log.Printf("[mqtt] purge clean-session queue %s: %v", topicFilter, err)
					ms.sendConnack(mc, 0x03, false)
					return false
				}
			}
			if err := ms.deleteSession(mc.sessionKey); err != nil {
				ms.sendConnack(mc, 0x03, false)
				return false
			}
		}
	} else {
		sess, exists, err := ms.getSession(mc.sessionKey)
		if err != nil {
			ms.sendConnack(mc, 0x03, false)
			return false
		}
		if !exists {
			sess = MQTTSession{
				Key:           mc.sessionKey,
				SessionID:     mc.sessionID,
				ClientID:      mc.clientID,
				Subscriptions: make(map[string]uint8),
				UpdatedAt:     time.Now(),
			}
			if err := ms.putSession(sess); err != nil {
				ms.sendConnack(mc, 0x03, false)
				return false
			}
		} else {
			if sess.SessionID == "" {
				// Migrate prior persisted sessions while keeping their old QoS 2
				// idempotency namespace for any handshake already in flight.
				sess.SessionID = sess.Key
				if err := ms.putSession(sess); err != nil {
					ms.sendConnack(mc, 0x03, false)
					return false
				}
			}
			mc.sessionID = sess.SessionID
			sessionPresent = true
			mc.nextQoS2Sequence = sess.NextQoS2Sequence
			for packetID, pending := range sess.QoS2InFlight {
				pending.Payload = append([]byte(nil), pending.Payload...)
				mc.incomingQoS2[packetID] = mqttPendingPublish{MQTTQoS2Pending: pending}
			}
			for packetID, pending := range sess.QoS1InFlight {
				pending.Payload = append([]byte(nil), pending.Payload...)
				mc.outgoingQoS1[packetID] = pending
			}
			for packetID, pending := range sess.QoS2OutInFlight {
				pending.Payload = append([]byte(nil), pending.Payload...)
				mc.outgoingQoS2[packetID] = pending
			}
			for topicFilter, qos := range sess.Subscriptions {
				if err := ms.attachSubscription(mc, topicFilter, qos); err != nil {
					mc.stopOnce.Do(func() { close(mc.stopping) })
					ms.detachSubscriptions(mc)
					ms.sendConnack(mc, 0x03, false)
					return false
				}
			}
		}
	}

	if mc.will != nil {
		mc.will.id = randomHex(16)
		record := MQTTWillRecord{
			ID:                mc.will.id,
			SessionKey:        mc.sessionKey,
			ClientID:          mc.clientID,
			PrincipalName:     mc.principal.Name,
			Tenants:           append([]string(nil), mc.principal.Tenants...),
			RequestsPerSecond: mc.principal.RequestsPerSecond,
			Burst:             mc.principal.Burst,
			MaxConcurrent:     mc.principal.MaxConcurrent,
			Topic:             mc.will.topic,
			Payload:           append([]byte(nil), mc.will.payload...),
			QoS:               mc.will.qos,
			Retain:            mc.will.retain,
			CreatedAt:         time.Now().UTC(),
		}
		if err := ValidateMQTTWillRecord(record); err != nil {
			ms.sendConnack(mc, 0x03, false)
			return false
		}
		if store, ok := ms.broker.StateStore().(MQTTWillStore); ok {
			if err := store.PutMQTTWill(ms.ctx, record); err != nil {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ms.ctx), 5*time.Second)
				_ = store.DeleteMQTTWill(cleanupCtx, record.ID)
				cleanupCancel()
				ms.sendConnack(mc, 0x03, false)
				return false
			}
		}
		mc.willRecord = &record
	}
	// Mark the accepted connection before writing CONNACK so Stop can observe
	// and publish its Will if the connection is closed during the handshake.
	mc.connected.Store(true)
	ms.sendConnack(mc, 0x00, sessionPresent) // Connection Accepted
	return true
}

func (ms *MQTTServer) handlePublish(mc *mqttConn, fixedHeader byte, payload []byte) {
	reject := func() {
		if mc.conn != nil {
			_ = mc.conn.Close()
		}
	}
	if ms.cfg.ACL != nil && !ms.cfg.ACL.Authorize(mc.principal, PermissionPublish) {
		// MQTT 3.1.1 has no negative acknowledgement for an incoming PUBLISH.
		// Closing without PUBACK/PUBREC lets a QoS client retry after policy is
		// corrected and avoids accepting unauthorized QoS 2 state.
		reject()
		return
	}
	if len(payload) < 2 {
		reject()
		return
	}
	topicLen := int(binary.BigEndian.Uint16(payload[0:2]))
	if 2+topicLen > len(payload) {
		reject()
		return
	}
	topic := string(payload[2 : 2+topicLen])
	if !validMQTTTopicName(topic) {
		reject()
		return
	}
	offset := 2 + topicLen
	var pktID uint16
	qos := (fixedHeader >> 1) & 0x03
	if qos == 3 || (qos == 0 && fixedHeader&0x08 != 0) {
		reject()
		return
	}
	if qos > 0 {
		if offset+2 > len(payload) {
			reject()
			return
		}
		pktID = binary.BigEndian.Uint16(payload[offset:])
		if pktID == 0 {
			reject()
			return
		}
		offset += 2
	}
	msgPayload := append([]byte(nil), payload[offset:]...)
	retain := fixedHeader&0x01 != 0
	if qos == 2 {
		pending, exists := mc.incomingQoS2[pktID]
		if exists {
			// A packet identifier stays occupied until PUBCOMP. Only an
			// identical retransmission with DUP set belongs to the existing
			// exchange; acknowledging a different PUBLISH here would silently
			// discard its payload in favor of the old staged one.
			if fixedHeader&0x08 == 0 || pending.Topic != topic || pending.Retain != retain || !bytes.Equal(pending.Payload, msgPayload) {
				reject()
				return
			}
		} else {
			if len(mc.incomingQoS2) >= ms.cfg.MaxInflight {
				// MQTT 3.1.1 has no receive-maximum negotiation. Leaving the
				// connection open without PUBREC would strand this packet while
				// the client waits for a response. Close it so a persistent
				// session can retry after capacity becomes available.
				reject()
				return
			}
			pending, err := ms.beginQoS2(mc, pktID, topic, msgPayload, retain)
			if err != nil {
				log.Printf("[mqtt] persist qos2 publish: %v", err)
				// Do not acknowledge a QoS 2 packet until its in-flight state
				// is durable. Closing lets the client retry it on reconnect.
				reject()
				return
			}
			mc.incomingQoS2[pktID] = pending
		}
		ms.sendPubrec(mc, pktID)
		return
	}
	if err := ms.publishMQTT(mc, topic, msgPayload, qos, retain); err != nil {
		log.Printf("[mqtt] publish error: %v", err)
		return
	}
	if qos == 1 {
		ms.sendPuback(mc, pktID)
	}
}

func (ms *MQTTServer) publishMQTT(mc *mqttConn, topic string, msgPayload []byte, qos byte, retain bool) error {
	return ms.publishMQTTWithContext(ms.principalContext(mc), topic, msgPayload, qos, retain, "")
}

func (ms *MQTTServer) publishWill(ctx context.Context, mc *mqttConn) error {
	if mc == nil {
		return nil
	}
	mc.willMu.Lock()
	if mc.will == nil || mc.gracefulClose.Load() || !mc.willHandled.CompareAndSwap(false, true) {
		mc.willMu.Unlock()
		return nil
	}
	var record MQTTWillRecord
	if mc.willRecord != nil {
		record = cloneMQTTWillRecord(*mc.willRecord)
	} else {
		record = MQTTWillRecord{
			ID:                mc.will.id,
			SessionKey:        mc.sessionKey,
			ClientID:          mc.clientID,
			PrincipalName:     mc.principal.Name,
			Tenants:           append([]string(nil), mc.principal.Tenants...),
			RequestsPerSecond: mc.principal.RequestsPerSecond,
			Burst:             mc.principal.Burst,
			MaxConcurrent:     mc.principal.MaxConcurrent,
			Topic:             mc.will.topic,
			Payload:           append([]byte(nil), mc.will.payload...),
			QoS:               mc.will.qos,
			Retain:            mc.will.retain,
			CreatedAt:         time.Now().UTC(),
		}
	}
	mc.willMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	base := context.WithoutCancel(ctx)
	var willCtx context.Context
	var cancel context.CancelFunc
	if deadline, ok := ctx.Deadline(); ok {
		willCtx, cancel = context.WithDeadline(base, deadline)
	} else {
		willCtx, cancel = context.WithTimeout(base, 10*time.Second)
	}
	defer cancel()
	store, _ := ms.broker.StateStore().(MQTTWillStore)
	return ms.publishStoredWill(willCtx, record, store)
}

func (ms *MQTTServer) clearWill(mc *mqttConn) {
	if mc == nil {
		return
	}
	mc.willMu.Lock()
	mc.gracefulClose.Store(true)
	mc.willHandled.Store(true)
	var recordID string
	if mc.willRecord != nil {
		recordID = mc.willRecord.ID
	}
	mc.willMu.Unlock()
	if recordID == "" {
		return
	}
	store, ok := ms.broker.StateStore().(MQTTWillStore)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ms.ctx), 5*time.Second)
	defer cancel()
	if err := store.DeleteMQTTWill(ctx, recordID); err != nil {
		log.Printf("[mqtt] remove Will after DISCONNECT for %s: %v", mc.clientID, err)
	}
}

func (ms *MQTTServer) recoverPendingWills(ctx context.Context) error {
	store, ok := ms.broker.StateStore().(MQTTWillStore)
	if !ok {
		return nil
	}
	after := ""
	for {
		records, next, err := store.ListMQTTWills(ctx, after, 128)
		if err != nil {
			return err
		}
		if len(records) > 128 {
			return errors.New("MQTT Will store returned an oversized page")
		}
		previousID := after
		for _, record := range records {
			if err := ValidateMQTTWillRecord(record); err != nil || record.ID <= previousID {
				return fmt.Errorf("MQTT Will store returned invalid or unordered record %q: %w", record.ID, errors.Join(err, ErrInvalidMessage))
			}
			previousID = record.ID
			principal := AdminPrincipal{
				Name: record.PrincipalName, Tenants: append([]string(nil), record.Tenants...),
				RequestsPerSecond: record.RequestsPerSecond, Burst: record.Burst,
				MaxConcurrent: record.MaxConcurrent,
			}
			publishCtx, cancel := context.WithTimeout(WithAdminPrincipal(ctx, principal), 10*time.Second)
			err := ms.publishStoredWill(publishCtx, record, store)
			cancel()
			if err != nil {
				return fmt.Errorf("recover MQTT Will %s for client %s: %w", record.ID, record.ClientID, err)
			}
		}
		if next == "" {
			return nil
		}
		if next <= after || len(records) == 0 || next != records[len(records)-1].ID {
			return errors.New("MQTT Will store returned a non-advancing page cursor")
		}
		after = next
	}
}

func (ms *MQTTServer) publishStoredWill(ctx context.Context, record MQTTWillRecord, store MQTTWillStore) error {
	if err := retryMQTTWillOperation(ctx, func() error {
		return ms.publishMQTTWithContext(ctx, record.Topic, record.Payload, record.QoS, record.Retain, record.ID)
	}); err != nil {
		return fmt.Errorf("publish MQTT Will %s: %w", record.ID, err)
	}
	if store == nil {
		return nil
	}
	if err := retryMQTTWillOperation(ctx, func() error {
		return store.DeleteMQTTWill(ctx, record.ID)
	}); err != nil {
		return fmt.Errorf("remove published MQTT Will %s: %w", record.ID, err)
	}
	return nil
}

func retryMQTTWillOperation(ctx context.Context, operation func() error) error {
	delay := 100 * time.Millisecond
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(lastErr, err)
		}
		lastErr = operation()
		if lastErr == nil {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return errors.Join(lastErr, ctx.Err())
		}
		if delay < time.Second {
			delay *= 2
		}
	}
}

func (ms *MQTTServer) publishMQTTWithContext(ctx context.Context, topic string, msgPayload []byte, qos byte, retain bool, willID string) error {
	// Map MQTT topic to broker topic/queue
	brokerTopic := strings.ReplaceAll(topic, "/", ".")
	opts := []PublishOption{
		WithType("mqtt"),
		WithHeaders(map[string]string{"mqtt_topic": topic, "mqtt_qos": fmt.Sprintf("%d", qos), "mqtt_retain": fmt.Sprintf("%t", retain)}),
	}
	if qos > 0 {
		opts = append(opts, WithQueue("mqtt."+brokerTopic))
	}

	if willID != "" {
		idempotencyID := "mqtt-will-" + willID
		opts = append(opts, WithMessageID(idempotencyID), WithIdempotencyKey(idempotencyID))
	}
	if _, err := ms.broker.Publisher().Publish(ctx, brokerTopic, msgPayload, opts...); err != nil && !errors.Is(err, ErrDuplicateMessage) {
		return err
	}
	if retain {
		return ms.updateRetained(ctx, topic, msgPayload, qos)
	}
	return nil
}

func (ms *MQTTServer) completeIncomingQoS2(mc *mqttConn, packetID uint16) error {
	pending, ok := mc.incomingQoS2[packetID]
	if !ok {
		// PUBREL is idempotent; a duplicate receives PUBCOMP without publishing
		// the application message a second time.
		return nil
	}
	if err := ms.publishMQTTQoS2(mc, pending.MQTTQoS2Pending); err != nil {
		return err
	}
	if err := ms.finishQoS2(mc, packetID); err != nil {
		return err
	}
	delete(mc.incomingQoS2, packetID)
	return nil
}

func (ms *MQTTServer) publishMQTTQoS2(mc *mqttConn, pending MQTTQoS2Pending) error {
	brokerTopic := strings.ReplaceAll(pending.Topic, "/", ".")
	idempotencyKey := fmt.Sprintf("mqtt-qos2-%s-%d", mc.sessionID, pending.Sequence)
	messageID := idempotencyKey
	_, err := ms.broker.Publisher().Publish(ms.principalContext(mc), brokerTopic, pending.Payload,
		WithMessageID(messageID),
		WithIdempotencyKey(idempotencyKey),
		WithType("mqtt"),
		WithHeaders(map[string]string{"mqtt_topic": pending.Topic, "mqtt_qos": "2", "mqtt_retain": fmt.Sprintf("%t", pending.Retain)}),
		WithQueue("mqtt."+brokerTopic),
	)
	if errors.Is(err, ErrDuplicateMessage) {
		// The QoS 2 packet id and sequence are persisted before PUBREC. A
		// duplicate means this exchange already appended its output before a
		// crash interrupted the handshake.
		err = nil
	}
	if err != nil {
		return err
	}
	if pending.Retain {
		return ms.updateRetained(ms.principalContext(mc), pending.Topic, pending.Payload, pending.QoS)
	}
	return nil
}

func (ms *MQTTServer) attachSubscription(mc *mqttConn, topicFilter string, qos uint8) error {
	if ms.cfg.ACL != nil && !ms.cfg.ACL.Authorize(mc.principal, PermissionMessageRead) {
		return ErrUnauthorized
	}
	tenant, err := resolveTenant(ms.principalContext(mc), "")
	if err != nil {
		return err
	}
	if mc.subs == nil {
		mc.subs = make(map[string]uint8)
	}
	if mc.consumers == nil {
		mc.consumers = make(map[string]*Subscription)
	}
	if existing := mc.consumers[topicFilter]; existing != nil {
		existing.Stop()
		delete(mc.consumers, topicFilter)
		ms.broker.RemoveRoute(mqttSubscriptionRoute(mc.sessionKey, topicFilter))
	}
	mc.subs[topicFilter] = qos

	// Create broker subscription.
	brokerTopic := strings.NewReplacer("/", ".", "+", "*").Replace(topicFilter)
	queue := mqttSubscriptionQueue(mc.sessionKey, topicFilter)
	if err := ms.broker.DeclareQueue(ms.ctx, QueueConfig{Name: queue, VisibilityTimeout: 30 * time.Second}); err != nil {
		delete(mc.subs, topicFilter)
		return err
	}
	routeName := mqttSubscriptionRoute(mc.sessionKey, topicFilter)
	ms.broker.AddRoute(Route{
		Name:     routeName,
		Mode:     RouteTopic,
		Match:    brokerTopic,
		TenantID: tenant,
		To:       []Destination{{Kind: DestinationQueue, Name: queue}},
	})
	subscription, err := ms.broker.Subscribe(ms.principalContext(mc), ConsumerConfig{
		Name:    "mqtt-" + mc.sessionKey + "-" + queue,
		Queue:   queue,
		AckMode: AckManual,
	}, func(ctx context.Context, d Delivery) error {
		mqttTopic := topicFilter
		if v, ok := d.Message.Headers["mqtt_topic"]; ok {
			mqttTopic = v
		}
		if !mqttTopicMatches(topicFilter, mqttTopic) {
			return d.Ack(ctx)
		}
		messageQoS := uint8(0)
		if value, ok := d.Message.Headers["mqtt_qos"]; ok {
			if parsed, parseErr := strconv.ParseUint(value, 10, 8); parseErr == nil && parsed <= 2 {
				messageQoS = uint8(parsed)
			}
		}
		outQoS := minMQTTQoS(messageQoS, qos)
		return ms.sendSubscriptionDelivery(mc, ctx, d, mqttTopic, outQoS)
	})
	if err != nil {
		delete(mc.subs, topicFilter)
		ms.broker.RemoveRoute(routeName)
		return err
	}
	mc.consumers[topicFilter] = subscription
	return nil
}

func mqttSubscriptionQueue(sessionKey, topicFilter string) string {
	filterHash := sha256.Sum256([]byte(topicFilter))
	return "mqtt.sub." + sessionKey + "." + hex.EncodeToString(filterHash[:12])
}

func mqttSubscriptionRoute(sessionKey, topicFilter string) string {
	filterHash := sha256.Sum256([]byte(topicFilter))
	return "mqtt." + sessionKey + "." + hex.EncodeToString(filterHash[:12])
}

func mqttSessionKey(identity, clientID string) string {
	sum := sha256.Sum256([]byte(identity + "\x00" + clientID))
	return hex.EncodeToString(sum[:])
}

func (ms *MQTTServer) getSession(key string) (MQTTSession, bool, error) {
	ms.sessionsMu.Lock()
	if session, ok := ms.sessions[key]; ok {
		ms.sessionsMu.Unlock()
		return cloneMQTTSession(*session), true, nil
	}
	ms.sessionsMu.Unlock()
	store, ok := ms.broker.StateStore().(MQTTSessionStore)
	if !ok {
		return MQTTSession{}, false, nil
	}
	session, found, err := store.GetMQTTSession(ms.ctx, key)
	if err != nil || !found {
		return session, found, err
	}
	ms.sessionsMu.Lock()
	ms.sessions[key] = &session
	ms.sessionsMu.Unlock()
	return cloneMQTTSession(session), true, nil
}

func (ms *MQTTServer) putSession(session MQTTSession) error {
	session.UpdatedAt = time.Now()
	if store, ok := ms.broker.StateStore().(MQTTSessionStore); ok {
		if err := store.PutMQTTSession(ms.ctx, session); err != nil {
			return err
		}
	}
	copy := cloneMQTTSession(session)
	ms.sessionsMu.Lock()
	ms.sessions[session.Key] = &copy
	ms.sessionsMu.Unlock()
	return nil
}

func (ms *MQTTServer) deleteSession(key string) error {
	if store, ok := ms.broker.StateStore().(MQTTSessionStore); ok {
		if err := store.DeleteMQTTSession(ms.ctx, key); err != nil {
			return err
		}
	}
	ms.sessionsMu.Lock()
	delete(ms.sessions, key)
	ms.sessionsMu.Unlock()
	return nil
}

func (ms *MQTTServer) updateSessionSubscription(mc *mqttConn, filter string, qos *uint8) error {
	// Serialize session read-modify-write with outbound QoS 1 and inbound QoS 2
	// state updates made by concurrent subscription delivery workers.
	mc.sessionMu.Lock()
	defer mc.sessionMu.Unlock()
	session, found, err := ms.getSession(mc.sessionKey)
	if err != nil {
		return err
	}
	if !found {
		return ErrInvalidMessage
	}
	if session.Subscriptions == nil {
		session.Subscriptions = make(map[string]uint8)
	}
	if qos == nil {
		delete(session.Subscriptions, filter)
	} else {
		session.Subscriptions[filter] = *qos
	}
	return ms.putSession(session)
}

func (ms *MQTTServer) beginQoS2(mc *mqttConn, packetID uint16, topic string, payload []byte, retain bool) (mqttPendingPublish, error) {
	mc.sessionMu.Lock()
	defer mc.sessionMu.Unlock()
	pending := mqttPendingPublish{MQTTQoS2Pending: MQTTQoS2Pending{Topic: topic, Payload: append([]byte(nil), payload...), QoS: 2, Retain: retain}}
	if mc.cleanSession {
		pending.Sequence = mc.nextQoS2Sequence
		mc.nextQoS2Sequence++
		return pending, nil
	}
	session, found, err := ms.getSession(mc.sessionKey)
	if err != nil {
		return mqttPendingPublish{}, err
	}
	if !found {
		return mqttPendingPublish{}, ErrInvalidMessage
	}
	pending.Sequence = session.NextQoS2Sequence
	session.NextQoS2Sequence++
	if session.QoS2InFlight == nil {
		session.QoS2InFlight = make(map[uint16]MQTTQoS2Pending)
	}
	session.QoS2InFlight[packetID] = pending.MQTTQoS2Pending
	if err := ms.putSession(session); err != nil {
		return mqttPendingPublish{}, err
	}
	mc.nextQoS2Sequence = session.NextQoS2Sequence
	return pending, nil
}

func (ms *MQTTServer) finishQoS2(mc *mqttConn, packetID uint16) error {
	mc.sessionMu.Lock()
	defer mc.sessionMu.Unlock()
	if mc.cleanSession {
		return nil
	}
	session, found, err := ms.getSession(mc.sessionKey)
	if err != nil {
		return err
	}
	if !found {
		return ErrInvalidMessage
	}
	delete(session.QoS2InFlight, packetID)
	return ms.putSession(session)
}

func (ms *MQTTServer) detachSubscription(mc *mqttConn, topicFilter string) {
	if subscription := mc.consumers[topicFilter]; subscription != nil {
		subscription.Stop()
		delete(mc.consumers, topicFilter)
	}
	ms.broker.RemoveRoute(mqttSubscriptionRoute(mc.sessionKey, topicFilter))
}

func (ms *MQTTServer) detachSubscriptions(mc *mqttConn) {
	for topicFilter := range mc.consumers {
		ms.detachSubscription(mc, topicFilter)
	}
}

func (ms *MQTTServer) handleSubscribe(mc *mqttConn, payload []byte) bool {
	if len(payload) < 5 {
		return false
	}
	pktID := binary.BigEndian.Uint16(payload[0:2])
	if pktID == 0 {
		return false
	}
	offset := 2
	var requests []mqttSubscriptionRequest
	for offset < len(payload) {
		if offset+2 > len(payload) {
			return false
		}
		topicLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if topicLen == 0 || offset+topicLen+1 > len(payload) {
			return false
		}
		topicFilter := string(payload[offset : offset+topicLen])
		offset += topicLen
		qos := payload[offset]
		offset++
		if qos > 2 || !validMQTTTopicFilter(topicFilter) {
			return false
		}
		requests = append(requests, mqttSubscriptionRequest{filter: topicFilter, qos: qos})
	}
	if len(requests) == 0 {
		return false
	}
	grantedQoS := make([]byte, 0, len(requests))
	var accepted []mqttSubscriptionRequest
	for _, request := range requests {
		qos := request.qos
		if err := ms.attachSubscription(mc, request.filter, qos); err != nil {
			grantedQoS = append(grantedQoS, 0x80)
			continue
		}
		grantedQoS = append(grantedQoS, qos)
		if !mc.cleanSession {
			if err := ms.updateSessionSubscription(mc, request.filter, &qos); err != nil {
				delete(mc.subs, request.filter)
				ms.detachSubscription(mc, request.filter)
				grantedQoS[len(grantedQoS)-1] = 0x80
			}
		}
		if grantedQoS[len(grantedQoS)-1] != 0x80 {
			accepted = append(accepted, request)
		}
	}
	// Send SUBACK
	data := make([]byte, 2+len(grantedQoS))
	binary.BigEndian.PutUint16(data[:2], pktID)
	copy(data[2:], grantedQoS)
	ms.sendPacket(mc, MQTTSuback, data)
	if len(accepted) > 0 {
		accepted = append([]mqttSubscriptionRequest(nil), accepted...)
		ms.wg.Add(1)
		go func() {
			defer ms.wg.Done()
			if err := ms.sendRetained(mc, accepted); err != nil {
				log.Printf("[mqtt] replay retained messages: %v", err)
				_ = mc.conn.Close()
			}
		}()
	}
	return true
}

func (ms *MQTTServer) sendRetained(mc *mqttConn, subscriptions []mqttSubscriptionRequest) error {
	if len(subscriptions) == 0 {
		return nil
	}
	ctx := ms.principalContext(mc)
	tenant, err := resolveTenant(ctx, "")
	if err != nil {
		return err
	}
	if store, ok := ms.broker.StateStore().(MQTTRetainedPagedStore); ok {
		const pageSize = 128
		afterTenant, afterTopic := "", ""
		for {
			page, err := store.ListMQTTRetainedPage(ctx, tenant, afterTenant, afterTopic, pageSize)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				return nil
			}
			if err := ms.sendRetainedMessages(mc, subscriptions, page); err != nil {
				return err
			}
			last := page[len(page)-1]
			afterTenant, afterTopic = last.TenantID, last.Topic
			if len(page) < pageSize {
				return nil
			}
		}
	}
	retained, err := ms.listRetained(ms.principalContext(mc), tenant)
	if err != nil {
		return err
	}
	return ms.sendRetainedMessages(mc, subscriptions, retained)
}

func (ms *MQTTServer) sendRetainedMessages(mc *mqttConn, subscriptions []mqttSubscriptionRequest, retained []MQTTRetainedMessage) error {
	for _, message := range retained {
		outQoS := uint8(0)
		matched := false
		for _, subscription := range subscriptions {
			if mqttTopicMatches(subscription.filter, message.Topic) {
				matched = true
				candidate := minMQTTQoS(message.QoS, subscription.qos)
				if candidate > outQoS {
					outQoS = candidate
				}
			}
		}
		if matched {
			if outQoS == 0 {
				if err := ms.sendMqttPublishRetained(mc, message.Topic, message.Payload); err != nil {
					return err
				}
				continue
			}
			messageID := mqttRetainedMessageID(message)
			if outQoS == 1 {
				packetID, duplicate, err := ms.registerQoS1(ms.principalContext(mc), mc, messageID, message.Topic, message.Payload, true)
				if err != nil {
					return err
				}
				select {
				case <-mc.stopping:
					return nil
				default:
				}
				if err := ms.sendMqttPublishPacket(mc, message.Topic, message.Payload, 1, packetID, duplicate, true); err != nil {
					return err
				}
			} else {
				packetID, duplicate, phase, err := ms.registerQoS2(ms.principalContext(mc), mc, messageID, message.Topic, message.Payload, true)
				if err != nil {
					return err
				}
				select {
				case <-mc.stopping:
					return nil
				default:
				}
				if phase == "pubrel" {
					if err := ms.sendPubrel(mc, packetID); err != nil {
						return err
					}
				} else if err := ms.sendMqttPublishPacket(mc, message.Topic, message.Payload, 2, packetID, duplicate, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func mqttRetainedMessageID(message MQTTRetainedMessage) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(mqttRetainedKey(message.TenantID, message.Topic)))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(strconv.FormatInt(message.UpdatedAt.UnixNano(), 10)))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(message.Payload)
	return "mqtt-retained-" + hex.EncodeToString(hash.Sum(nil))
}

func minMQTTQoS(a, b uint8) uint8 {
	if a < b {
		return a
	}
	return b
}

func (ms *MQTTServer) sendSubscriptionDelivery(mc *mqttConn, ctx context.Context, delivery Delivery, topic string, qos uint8) error {
	select {
	case <-mc.ready:
	case <-mc.stopping:
		return nil
	case <-ctx.Done():
		return nil
	}
	if qos == 0 {
		if err := ms.sendMqttPublishPacket(mc, topic, delivery.Message.Payload, 0, 0, false, false); err != nil {
			_ = mc.conn.Close()
			return nil
		}
		return delivery.Ack(ctx)
	}
	if qos == 2 {
		packetID, duplicate, phase, err := ms.registerQoS2(ctx, mc, delivery.Message.ID, topic, delivery.Message.Payload, false)
		if err != nil {
			select {
			case <-mc.stopping:
				return nil
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		select {
		case <-mc.stopping:
			return nil
		default:
		}
		if phase == "pubrel" {
			if err := delivery.Ack(ctx); err != nil {
				log.Printf("[mqtt] acknowledge broker delivery %s after QoS 2 recovery: %v", delivery.Message.ID, err)
			}
			if err := ms.sendPubrel(mc, packetID); err != nil {
				_ = mc.conn.Close()
			}
			return nil
		}
		if err := ms.sendMqttPublishPacket(mc, topic, delivery.Message.Payload, 2, packetID, duplicate, false); err != nil {
			_ = mc.conn.Close()
			return nil
		}
		if err := delivery.Ack(ctx); err != nil {
			log.Printf("[mqtt] acknowledge broker delivery %s after QoS 2 PUBLISH: %v", delivery.Message.ID, err)
		}
		return nil
	}
	packetID, duplicate, err := ms.registerQoS1(ctx, mc, delivery.Message.ID, topic, delivery.Message.Payload, false)
	if err != nil {
		select {
		case <-mc.stopping:
			return nil
		case <-ctx.Done():
			return nil
		default:
			return err
		}
	}
	select {
	case <-mc.stopping:
		return nil
	default:
	}
	if err := ms.sendMqttPublishPacket(mc, topic, delivery.Message.Payload, 1, packetID, duplicate, false); err != nil {
		_ = mc.conn.Close()
		return nil
	}
	// Session state is persisted before the broker queue is acknowledged. A
	// crash between these operations can redeliver the same message; registerQoS1
	// finds the message ID and reuses its packet identifier.
	if err := delivery.Ack(ctx); err != nil {
		log.Printf("[mqtt] acknowledge broker delivery %s after PUBLISH: %v", delivery.Message.ID, err)
	}
	return nil
}

func (ms *MQTTServer) registerQoS1(ctx context.Context, mc *mqttConn, messageID, topic string, payload []byte, retain bool) (uint16, bool, error) {
	for {
		select {
		case <-mc.stopping:
			return 0, false, context.Canceled
		case <-ctx.Done():
			return 0, false, ctx.Err()
		default:
		}
		mc.sessionMu.Lock()
		var session MQTTSession
		if !mc.cleanSession {
			var found bool
			var err error
			session, found, err = ms.getSession(mc.sessionKey)
			if err != nil {
				mc.sessionMu.Unlock()
				return 0, false, err
			}
			if !found {
				mc.sessionMu.Unlock()
				return 0, false, ErrInvalidMessage
			}
		}
		for packetID, pending := range mc.outgoingQoS1 {
			if pending.MessageID == messageID {
				mc.sessionMu.Unlock()
				return packetID, true, nil
			}
		}
		if outgoingInFlightLocked(mc) < ms.cfg.MaxInflight {
			packetID, ok := ms.nextOutgoingPacketIDLocked(mc)
			if !ok {
				mc.sessionMu.Unlock()
				return 0, false, ErrBackpressure
			}
			pending := MQTTQoS1Pending{MessageID: messageID, Topic: topic, Payload: append([]byte(nil), payload...), Retain: retain}
			if !mc.cleanSession {
				if session.QoS1InFlight == nil {
					session.QoS1InFlight = make(map[uint16]MQTTQoS1Pending)
				}
				session.QoS1InFlight[packetID] = pending
				if err := ms.putSession(session); err != nil {
					mc.sessionMu.Unlock()
					return 0, false, err
				}
			}
			mc.outgoingQoS1[packetID] = pending
			mc.sessionMu.Unlock()
			return packetID, false, nil
		}
		changed := mc.outChanged
		mc.sessionMu.Unlock()
		select {
		case <-changed:
		case <-mc.stopping:
			return 0, false, context.Canceled
		case <-ctx.Done():
			return 0, false, ctx.Err()
		}
	}
}

func (ms *MQTTServer) registerQoS2(ctx context.Context, mc *mqttConn, messageID, topic string, payload []byte, retain bool) (uint16, bool, string, error) {
	for {
		select {
		case <-mc.stopping:
			return 0, false, "", context.Canceled
		case <-ctx.Done():
			return 0, false, "", ctx.Err()
		default:
		}
		mc.sessionMu.Lock()
		var session MQTTSession
		if !mc.cleanSession {
			var found bool
			var err error
			session, found, err = ms.getSession(mc.sessionKey)
			if err != nil {
				mc.sessionMu.Unlock()
				return 0, false, "", err
			}
			if !found {
				mc.sessionMu.Unlock()
				return 0, false, "", ErrInvalidMessage
			}
		}
		for packetID, pending := range mc.outgoingQoS2 {
			if pending.MessageID == messageID {
				mc.sessionMu.Unlock()
				return packetID, true, pending.Phase, nil
			}
		}
		if outgoingInFlightLocked(mc) < ms.cfg.MaxInflight {
			packetID, ok := ms.nextOutgoingPacketIDLocked(mc)
			if !ok {
				mc.sessionMu.Unlock()
				return 0, false, "", ErrBackpressure
			}
			pending := MQTTQoS2OutboundPending{MessageID: messageID, Topic: topic, Payload: append([]byte(nil), payload...), Retain: retain, Phase: "publish"}
			if !mc.cleanSession {
				if session.QoS2OutInFlight == nil {
					session.QoS2OutInFlight = make(map[uint16]MQTTQoS2OutboundPending)
				}
				session.QoS2OutInFlight[packetID] = pending
				if err := ms.putSession(session); err != nil {
					mc.sessionMu.Unlock()
					return 0, false, "", err
				}
			}
			mc.outgoingQoS2[packetID] = pending
			mc.sessionMu.Unlock()
			return packetID, false, pending.Phase, nil
		}
		changed := mc.outChanged
		mc.sessionMu.Unlock()
		select {
		case <-changed:
		case <-mc.stopping:
			return 0, false, "", context.Canceled
		case <-ctx.Done():
			return 0, false, "", ctx.Err()
		}
	}
}

func outgoingInFlightLocked(mc *mqttConn) int {
	return len(mc.outgoingQoS1) + len(mc.outgoingQoS2)
}

func (ms *MQTTServer) nextOutgoingPacketIDLocked(mc *mqttConn) (uint16, bool) {
	for i := 0; i < 65535; i++ {
		mc.nextOutgoingID++
		if mc.nextOutgoingID == 0 {
			mc.nextOutgoingID = 1
		}
		if _, exists := mc.outgoingQoS1[mc.nextOutgoingID]; exists {
			continue
		}
		if _, exists := mc.outgoingQoS2[mc.nextOutgoingID]; !exists {
			return mc.nextOutgoingID, true
		}
	}
	return 0, false
}

func (ms *MQTTServer) resendOutbound(mc *mqttConn) error {
	mc.sessionMu.Lock()
	pendingQoS1 := make(map[uint16]MQTTQoS1Pending, len(mc.outgoingQoS1))
	for packetID, message := range mc.outgoingQoS1 {
		message.Payload = append([]byte(nil), message.Payload...)
		pendingQoS1[packetID] = message
	}
	pendingQoS2 := make(map[uint16]MQTTQoS2OutboundPending, len(mc.outgoingQoS2))
	for packetID, message := range mc.outgoingQoS2 {
		message.Payload = append([]byte(nil), message.Payload...)
		pendingQoS2[packetID] = message
	}
	mc.sessionMu.Unlock()
	for packetID, message := range pendingQoS1 {
		if err := ms.sendMqttPublishPacket(mc, message.Topic, message.Payload, 1, packetID, true, message.Retain); err != nil {
			return err
		}
	}
	for packetID, message := range pendingQoS2 {
		if message.Phase == "pubrel" {
			if err := ms.sendPubrel(mc, packetID); err != nil {
				return err
			}
			continue
		}
		if message.Phase != "publish" {
			return ErrInvalidMessage
		}
		if err := ms.sendMqttPublishPacket(mc, message.Topic, message.Payload, 2, packetID, true, message.Retain); err != nil {
			return err
		}
	}
	return nil
}

func (ms *MQTTServer) handleOutgoingPuback(mc *mqttConn, payload []byte) bool {
	if len(payload) != 2 {
		return false
	}
	packetID := binary.BigEndian.Uint16(payload)
	if packetID == 0 {
		return false
	}
	mc.sessionMu.Lock()
	if _, exists := mc.outgoingQoS1[packetID]; !exists {
		mc.sessionMu.Unlock()
		return true
	}
	if !mc.cleanSession {
		session, found, err := ms.getSession(mc.sessionKey)
		if err != nil || !found {
			mc.sessionMu.Unlock()
			log.Printf("[mqtt] load session after PUBACK %d: %v", packetID, err)
			return false
		}
		delete(session.QoS1InFlight, packetID)
		if err := ms.putSession(session); err != nil {
			mc.sessionMu.Unlock()
			log.Printf("[mqtt] persist PUBACK %d: %v", packetID, err)
			return false
		}
	}
	delete(mc.outgoingQoS1, packetID)
	close(mc.outChanged)
	mc.outChanged = make(chan struct{})
	mc.sessionMu.Unlock()
	return true
}

func (ms *MQTTServer) handleOutgoingPubrec(mc *mqttConn, payload []byte) bool {
	if len(payload) != 2 {
		return false
	}
	packetID := binary.BigEndian.Uint16(payload)
	if packetID == 0 {
		return false
	}
	mc.sessionMu.Lock()
	pending, exists := mc.outgoingQoS2[packetID]
	if !exists {
		mc.sessionMu.Unlock()
		return true
	}
	if pending.Phase != "publish" && pending.Phase != "pubrel" {
		mc.sessionMu.Unlock()
		return false
	}
	if pending.Phase != "pubrel" {
		pending.Phase = "pubrel"
		if !mc.cleanSession {
			session, found, err := ms.getSession(mc.sessionKey)
			if err != nil || !found {
				mc.sessionMu.Unlock()
				log.Printf("[mqtt] load session after PUBREC %d: %v", packetID, err)
				return false
			}
			if session.QoS2OutInFlight == nil {
				session.QoS2OutInFlight = make(map[uint16]MQTTQoS2OutboundPending)
			}
			session.QoS2OutInFlight[packetID] = pending
			if err := ms.putSession(session); err != nil {
				mc.sessionMu.Unlock()
				log.Printf("[mqtt] persist PUBREC %d: %v", packetID, err)
				return false
			}
		}
		mc.outgoingQoS2[packetID] = pending
	}
	mc.sessionMu.Unlock()
	if err := ms.sendPubrel(mc, packetID); err != nil {
		log.Printf("[mqtt] send PUBREL %d: %v", packetID, err)
		return false
	}
	return true
}

func (ms *MQTTServer) handleOutgoingPubcomp(mc *mqttConn, payload []byte) bool {
	if len(payload) != 2 {
		return false
	}
	packetID := binary.BigEndian.Uint16(payload)
	if packetID == 0 {
		return false
	}
	mc.sessionMu.Lock()
	if _, exists := mc.outgoingQoS2[packetID]; !exists {
		mc.sessionMu.Unlock()
		return true
	}
	if mc.outgoingQoS2[packetID].Phase != "pubrel" {
		mc.sessionMu.Unlock()
		return false
	}
	if !mc.cleanSession {
		session, found, err := ms.getSession(mc.sessionKey)
		if err != nil || !found {
			mc.sessionMu.Unlock()
			log.Printf("[mqtt] load session after PUBCOMP %d: %v", packetID, err)
			return false
		}
		delete(session.QoS2OutInFlight, packetID)
		if err := ms.putSession(session); err != nil {
			mc.sessionMu.Unlock()
			log.Printf("[mqtt] persist PUBCOMP %d: %v", packetID, err)
			return false
		}
	}
	delete(mc.outgoingQoS2, packetID)
	close(mc.outChanged)
	mc.outChanged = make(chan struct{})
	mc.sessionMu.Unlock()
	return true
}

func mqttTopicMatches(filter, topic string) bool {
	if filter == "" || topic == "" {
		return false
	}
	if strings.HasPrefix(topic, "$") && (strings.HasPrefix(filter, "+") || strings.HasPrefix(filter, "#")) {
		return false
	}
	filters := strings.Split(filter, "/")
	topics := strings.Split(topic, "/")
	for i, level := range filters {
		if level == "#" {
			return i == len(filters)-1
		}
		if i >= len(topics) {
			return false
		}
		if level != "+" && level != topics[i] {
			return false
		}
	}
	return len(filters) == len(topics)
}

func validMQTTTopicName(topic string) bool {
	return topic != "" && validMQTTUTF8(topic) && !strings.ContainsAny(topic, "+#")
}

func validMQTTTopicFilter(filter string) bool {
	if filter == "" || !validMQTTUTF8(filter) {
		return false
	}
	levels := strings.Split(filter, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || i != len(levels)-1) {
			return false
		}
		if strings.Contains(level, "+") && level != "+" {
			return false
		}
	}
	return true
}

func validMQTTUTF8(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r == 0 || (r >= 0x01 && r <= 0x1f) || (r >= 0x7f && r <= 0x9f) {
			return false
		}
		// MQTT 3.1.1 recommends excluding Unicode noncharacters from encoded
		// strings. Reject both the contiguous BMP range and the FFFE/FFFF code
		// points at the end of every Unicode plane.
		if (r >= 0xfdd0 && r <= 0xfdef) || r&0xffff == 0xfffe || r&0xffff == 0xffff {
			return false
		}
	}
	return true
}

func (ms *MQTTServer) handleUnsubscribe(mc *mqttConn, payload []byte) bool {
	if len(payload) < 4 {
		return false
	}
	pktID := binary.BigEndian.Uint16(payload[0:2])
	if pktID == 0 {
		return false
	}
	offset := 2
	filters := make([]string, 0, 1)
	for offset < len(payload) {
		if offset+2 > len(payload) {
			return false
		}
		topicLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2 + topicLen
		if topicLen == 0 || offset > len(payload) {
			return false
		}
		filter := string(payload[offset-topicLen : offset])
		if !validMQTTTopicFilter(filter) {
			return false
		}
		filters = append(filters, filter)
	}
	if len(filters) == 0 {
		return false
	}
	for _, topic := range filters {
		delete(mc.subs, topic)
		ms.detachSubscription(mc, topic)
		if !mc.cleanSession {
			if err := ms.updateSessionSubscription(mc, topic, nil); err != nil {
				return false
			}
		}
	}
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, pktID)
	ms.sendPacket(mc, MQTTUnsuback, data)
	return true
}

func (ms *MQTTServer) sendConnack(mc *mqttConn, returnCode byte, sessionPresent bool) {
	var flags byte
	if sessionPresent && returnCode == 0 {
		flags = 0x01
	}
	ms.sendPacket(mc, MQTTConnack, []byte{flags, returnCode})
}

func (ms *MQTTServer) sendPuback(mc *mqttConn, pktID uint16) {
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, pktID)
	ms.sendPacket(mc, MQTTPuback, data)
}

func (ms *MQTTServer) sendPubrec(mc *mqttConn, pktID uint16) {
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, pktID)
	ms.sendPacket(mc, MQTTPubrec, data)
}

func (ms *MQTTServer) sendPubrel(mc *mqttConn, pktID uint16) error {
	if pktID == 0 {
		return ErrInvalidMessage
	}
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, pktID)
	pkt := []byte{MQTTPubrel | 0x02, 0x02, data[0], data[1]}
	return ms.writePacket(mc, pkt)
}

func (ms *MQTTServer) sendPubcomp(mc *mqttConn, pktID uint16) {
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, pktID)
	ms.sendPacket(mc, MQTTPubcomp, data)
}

func (ms *MQTTServer) sendMqttPublishRetained(mc *mqttConn, topic string, payload []byte) error {
	return ms.sendMqttPublishPacket(mc, topic, payload, 0, 0, false, true)
}

func (ms *MQTTServer) sendMqttPublishPacket(mc *mqttConn, topic string, payload []byte, qos byte, packetID uint16, duplicate, retain bool) error {
	if qos > 2 || (qos == 0 && packetID != 0) || (qos > 0 && packetID == 0) {
		return ErrInvalidMessage
	}
	var pkt []byte
	topicBytes := []byte(topic)
	if !validMQTTTopicName(topic) || len(topicBytes) > int(^uint16(0)) {
		return ErrInvalidMessage
	}
	flags := qos << 1
	if duplicate && qos > 0 {
		flags |= 0x08
	}
	if retain {
		flags |= 0x01
	}
	pkt = append(pkt, byte(MQTTPublish|flags))
	remaining := 2 + len(topicBytes) + len(payload)
	if qos > 0 {
		remaining += 2
	}
	if remaining > ms.cfg.MaxPacketSize {
		return ErrMessageTooLarge
	}
	pkt = ms.encodeRemainingLength(pkt, remaining)
	pkt = append(pkt, byte(len(topicBytes)>>8), byte(len(topicBytes)))
	pkt = append(pkt, topicBytes...)
	if qos > 0 {
		pkt = append(pkt, byte(packetID>>8), byte(packetID))
	}
	pkt = append(pkt, payload...)
	return ms.writePacket(mc, pkt)
}

func (ms *MQTTServer) sendPacket(mc *mqttConn, pktType byte, payload []byte) {
	pkt := []byte{pktType}
	pkt = ms.encodeRemainingLength(pkt, len(payload))
	pkt = append(pkt, payload...)
	_ = ms.writePacket(mc, pkt)
}

func (ms *MQTTServer) writePacket(mc *mqttConn, pkt []byte) error {
	mc.writeMu.Lock()
	defer mc.writeMu.Unlock()
	if err := mc.conn.SetWriteDeadline(time.Now().Add(ms.cfg.WriteTimeout)); err != nil {
		_ = mc.conn.Close()
		return err
	}
	n, err := mc.conn.Write(pkt)
	clearErr := mc.conn.SetWriteDeadline(time.Time{})
	if err == nil && n != len(pkt) {
		err = io.ErrShortWrite
	}
	if err == nil && clearErr != nil {
		err = clearErr
	}
	if err != nil {
		// A partial MQTT control packet cannot be recovered on this stream.
		_ = mc.conn.Close()
	}
	return err
}

func (ms *MQTTServer) encodeRemainingLength(pkt []byte, length int) []byte {
	for {
		encodedByte := byte(length % 128)
		length /= 128
		if length > 0 {
			encodedByte |= 0x80
		}
		pkt = append(pkt, encodedByte)
		if length == 0 {
			break
		}
	}
	return pkt
}

func (ms *MQTTServer) readPacketHeader(conn net.Conn) (byte, int, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return 0, 0, err
	}
	fixedHeader := buf[0]
	// Read remaining length
	multiplier := 1
	remaining := 0
	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(conn, buf); err != nil {
			return 0, 0, err
		}
		remaining += int(buf[0]&0x7F) * multiplier
		if buf[0]&0x80 == 0 {
			if i > 0 && remaining < 1<<(7*i) {
				return 0, 0, ErrInvalidMessage
			}
			return fixedHeader, remaining, nil
		}
		if i == 3 {
			return 0, 0, ErrInvalidMessage
		}
		multiplier *= 128
	}
	return 0, 0, ErrInvalidMessage
}
