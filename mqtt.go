package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// MQTTConfig configures the MQTT protocol server.
type MQTTConfig struct {
	// ListenAddr is the TCP address to listen on. Default: ":1883".
	// For TLS, use ":8883" or set TLSCertFile/TLSKeyFile.
	ListenAddr string
	// MaxConnections limits concurrent MQTT connections. Default: 1000.
	MaxConnections int
	// MaxInflight limits in-flight QoS 1/2 messages per client. Default: 20.
	MaxInflight int
	// KeepAlive is the max keep-alive seconds. Default: 60.
	KeepAlive uint16
	// MaxPacketSize is the max MQTT packet size. Default: 1MB.
	MaxPacketSize int
	// RequireAuth requires username/password authentication. Default: false.
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
type MQTTServer struct {
	broker *Broker
	cfg    MQTTConfig
	ln     net.Listener
	conns  map[string]*mqttConn
	mu     sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

type mqttConn struct {
	id           string
	conn         net.Conn
	clientID     string
	username     string
	keepAlive    uint16
	subs         map[string]uint8 // topic -> qos
	incomingQoS2 map[uint16]mqttPendingPublish
	closed       chan struct{}
	release      func()
}

type mqttPendingPublish struct {
	topic   string
	payload []byte
	qos     byte
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
	ctx, cancel := context.WithCancel(context.Background())
	return &MQTTServer{
		broker: b,
		cfg:    cfg,
		conns:  make(map[string]*mqttConn),
		ctx:    ctx,
		cancel: cancel,
	}
}

// ListenAndServe starts the MQTT server.
func (ms *MQTTServer) ListenAndServe() error {
	var err error
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
		ln, err := tls.Listen("tcp", ms.cfg.ListenAddr, tlsCfg)
		if err != nil {
			return err
		}
		ms.ln = ln
		log.Printf("[mqtt] listening on %s (TLS)", ms.cfg.ListenAddr)
	} else {
		ms.ln, err = net.Listen("tcp", ms.cfg.ListenAddr)
		if err != nil {
			return err
		}
		log.Printf("[mqtt] listening on %s", ms.cfg.ListenAddr)
	}
	for {
		conn, err := ms.ln.Accept()
		if err != nil {
			select {
			case <-ms.ctx.Done():
				return nil
			default:
				log.Printf("[mqtt] accept error: %v", err)
				continue
			}
		}
		ms.wg.Add(1)
		go ms.handleConn(conn)
	}
}

// Stop shuts down the MQTT server.
func (ms *MQTTServer) Stop() {
	ms.cancel()
	if ms.ln != nil {
		ms.ln.Close()
	}
	ms.mu.Lock()
	for id, c := range ms.conns {
		c.conn.Close()
		close(c.closed)
		delete(ms.conns, id)
	}
	ms.mu.Unlock()
	ms.wg.Wait()
}

func (ms *MQTTServer) handleConn(conn net.Conn) {
	defer ms.wg.Done()
	id := fmt.Sprintf("mqtt-%d", time.Now().UnixNano())
	mc := &mqttConn{id: id, conn: conn, subs: make(map[string]uint8), incomingQoS2: make(map[uint16]mqttPendingPublish), closed: make(chan struct{})}
	ms.mu.Lock()
	if len(ms.conns) >= ms.cfg.MaxConnections {
		ms.mu.Unlock()
		conn.Close()
		return
	}
	ms.conns[id] = mc
	ms.mu.Unlock()
	defer func() {
		if mc.release != nil {
			mc.release()
		}
		ms.mu.Lock()
		delete(ms.conns, id)
		ms.mu.Unlock()
		conn.Close()
		close(mc.closed)
	}()

	// Read first packet (must be CONNECT)
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
		switch fixedHeader & 0xF0 {
		case MQTTPublish:
			ms.handlePublish(mc, fixedHeader, payload)
		case MQTTPuback:
			// QoS 1 ack - no action needed for now
		case MQTTPubrec:
			// QoS 2 step 1 - send pubrel
			if len(payload) >= 2 {
				pktID := binary.BigEndian.Uint16(payload[:2])
				ms.sendPubrel(conn, pktID)
			}
		case MQTTPubrel:
			if len(payload) >= 2 {
				pktID := binary.BigEndian.Uint16(payload[:2])
				if err := ms.completeIncomingQoS2(mc, pktID); err != nil {
					log.Printf("[mqtt] qos2 publish error: %v", err)
					continue
				}
				ms.sendPubcomp(conn, pktID)
			}
		case MQTTPubcomp:
			// QoS 2 complete
		case MQTTSubscribe & 0xF0:
			if fixedHeader&0x0F != 0x02 {
				return
			}
			ms.handleSubscribe(mc, payload)
		case MQTTUnsubscribe & 0xF0:
			if fixedHeader&0x0F != 0x02 {
				return
			}
			ms.handleUnsubscribe(mc, payload)
		case MQTTPingreq:
			ms.sendPacket(conn, MQTTPingresp, nil)
		case MQTTDisconnect:
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
	_ = payload[offset] // protocol level
	offset++
	connectFlags := payload[offset]
	offset++
	keepAlive := binary.BigEndian.Uint16(payload[offset:])
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
	offset += clientIDLen

	// Will topic/payload (if will flag set)
	if connectFlags&0x04 != 0 {
		if offset+2 > len(payload) {
			return false
		}
		willTopicLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2 + willTopicLen
		if offset+2 > len(payload) {
			return false
		}
		willPayloadLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2 + willPayloadLen
	}

	// Username (if username flag set)
	password := ""
	if connectFlags&0x80 != 0 {
		if offset+2 > len(payload) {
			return false
		}
		userLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if offset+userLen <= len(payload) {
			mc.username = string(payload[offset : offset+userLen])
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
		if offset+passLen <= len(payload) {
			password = string(payload[offset : offset+passLen])
		}
		offset += passLen
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
			release, err := ms.cfg.ACL.AdmitConnection(principal)
			if err != nil {
				ms.sendConnack(mc.conn, 0x03) // Server unavailable
				return false
			}
			mc.release = release
		} else if ms.cfg.RequireAuth {
			ms.sendConnack(mc.conn, 0x05) // Connection Refused: not authorized
			return false
		}
	} else if ms.cfg.BrokerToken != "" {
		// Use broker token for authentication
		if !constantTimeEqual(password, ms.cfg.BrokerToken) {
			ms.sendConnack(mc.conn, 0x05) // Connection Refused: not authorized
			return false
		}
	} else if ms.cfg.RequireAuth && (!constantTimeEqual(mc.username, ms.cfg.Username) || !constantTimeEqual(password, ms.cfg.Password)) {
		ms.sendConnack(mc.conn, 0x05) // Connection Refused: not authorized
		return false
	}

	ms.sendConnack(mc.conn, 0x00) // Connection Accepted
	return true
}

func (ms *MQTTServer) handlePublish(mc *mqttConn, fixedHeader byte, payload []byte) {
	if len(payload) < 4 {
		return
	}
	topicLen := int(binary.BigEndian.Uint16(payload[0:2]))
	if 2+topicLen > len(payload) {
		return
	}
	topic := string(payload[2 : 2+topicLen])
	offset := 2 + topicLen
	var pktID uint16
	qos := (fixedHeader >> 1) & 0x03
	if qos == 3 {
		return
	}
	if qos > 0 {
		if offset+2 > len(payload) {
			return
		}
		pktID = binary.BigEndian.Uint16(payload[offset:])
		offset += 2
	}
	msgPayload := append([]byte(nil), payload[offset:]...)
	if qos == 2 {
		if _, exists := mc.incomingQoS2[pktID]; !exists {
			if len(mc.incomingQoS2) >= ms.cfg.MaxInflight {
				return
			}
			mc.incomingQoS2[pktID] = mqttPendingPublish{topic: topic, payload: msgPayload, qos: qos}
		}
		ms.sendPubrec(mc.conn, pktID)
		return
	}
	if err := ms.publishMQTT(mc, topic, msgPayload, qos); err != nil {
		log.Printf("[mqtt] publish error: %v", err)
		return
	}
	if qos == 1 {
		ms.sendPuback(mc.conn, pktID)
	}
}

func (ms *MQTTServer) publishMQTT(mc *mqttConn, topic string, msgPayload []byte, qos byte) error {
	// Map MQTT topic to broker topic/queue
	brokerTopic := strings.ReplaceAll(topic, "/", ".")
	opts := []PublishOption{
		WithType("mqtt"),
		WithHeaders(map[string]string{"mqtt_topic": topic, "mqtt_qos": fmt.Sprintf("%d", qos)}),
	}
	if qos > 0 {
		opts = append(opts, WithQueue("mqtt."+brokerTopic))
	}

	_, err := ms.broker.Publisher().Publish(ms.ctx, brokerTopic, msgPayload, opts...)
	return err
}

func (ms *MQTTServer) completeIncomingQoS2(mc *mqttConn, packetID uint16) error {
	pending, ok := mc.incomingQoS2[packetID]
	if !ok {
		// PUBREL is idempotent; a duplicate receives PUBCOMP without publishing
		// the application message a second time.
		return nil
	}
	if err := ms.publishMQTT(mc, pending.topic, pending.payload, pending.qos); err != nil {
		return err
	}
	delete(mc.incomingQoS2, packetID)
	return nil
}

func (ms *MQTTServer) handleSubscribe(mc *mqttConn, payload []byte) {
	if len(payload) < 5 {
		return
	}
	pktID := binary.BigEndian.Uint16(payload[0:2])
	offset := 2
	var grantedQoS []byte
	for offset+2 < len(payload) {
		topicLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if offset+topicLen+1 > len(payload) {
			break
		}
		topicFilter := string(payload[offset : offset+topicLen])
		offset += topicLen
		qos := payload[offset]
		offset++
		grantedQoS = append(grantedQoS, qos)
		mc.subs[topicFilter] = qos

		// Create broker subscription
		brokerTopic := strings.ReplaceAll(topicFilter, "/", ".")
		queue := "mqtt.sub." + mc.clientID + "." + strings.ReplaceAll(topicFilter, "*", "star")
		_ = ms.broker.DeclareQueue(ms.ctx, QueueConfig{Name: queue, VisibilityTimeout: 30 * time.Second})
		ms.broker.AddRoute(Route{
			Name:  "mqtt." + mc.clientID + "." + topicFilter,
			Mode:  RouteTopic,
			Match: brokerTopic,
			To:    []Destination{{Kind: DestinationQueue, Name: queue}},
		})
		ms.broker.Subscribe(ms.ctx, ConsumerConfig{
			Name:    "mqtt-" + mc.clientID + "-" + queue,
			Queue:   queue,
			AckMode: AckAuto,
		}, func(ctx context.Context, d Delivery) error {
			mqttTopic := topicFilter
			if v, ok := d.Message.Headers["mqtt_topic"]; ok {
				mqttTopic = v
			}
			ms.sendMqttPublish(mc.conn, mqttTopic, d.Message.Payload, 0)
			return nil
		})
	}
	// Send SUBACK
	data := make([]byte, 2+len(grantedQoS))
	binary.BigEndian.PutUint16(data[:2], pktID)
	copy(data[2:], grantedQoS)
	ms.sendPacket(mc.conn, MQTTSuback, data)
}

func (ms *MQTTServer) handleUnsubscribe(mc *mqttConn, payload []byte) {
	if len(payload) < 4 {
		return
	}
	pktID := binary.BigEndian.Uint16(payload[0:2])
	offset := 2
	for offset+2 < len(payload) {
		topicLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2 + topicLen
		delete(mc.subs, string(payload[offset-topicLen:offset]))
	}
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, pktID)
	ms.sendPacket(mc.conn, MQTTUnsuback, data)
}

func (ms *MQTTServer) sendConnack(conn net.Conn, returnCode byte) {
	ms.sendPacket(conn, MQTTConnack, []byte{0x00, returnCode})
}

func (ms *MQTTServer) sendPuback(conn net.Conn, pktID uint16) {
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, pktID)
	ms.sendPacket(conn, MQTTPuback, data)
}

func (ms *MQTTServer) sendPubrec(conn net.Conn, pktID uint16) {
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, pktID)
	ms.sendPacket(conn, MQTTPubrec, data)
}

func (ms *MQTTServer) sendPubrel(conn net.Conn, pktID uint16) {
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, pktID)
	ms.sendPacket(conn, MQTTPubrel, data)
}

func (ms *MQTTServer) sendPubcomp(conn net.Conn, pktID uint16) {
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, pktID)
	ms.sendPacket(conn, MQTTPubcomp, data)
}

func (ms *MQTTServer) sendMqttPublish(conn net.Conn, topic string, payload []byte, qos byte) {
	var pkt []byte
	topicBytes := []byte(topic)
	pkt = append(pkt, byte(MQTTPublish|(qos<<1)))
	pkt = ms.encodeRemainingLength(pkt, 2+len(topicBytes)+len(payload))
	pkt = append(pkt, byte(len(topicBytes)>>8), byte(len(topicBytes)))
	pkt = append(pkt, topicBytes...)
	pkt = append(pkt, payload...)
	conn.Write(pkt)
}

func (ms *MQTTServer) sendPacket(conn net.Conn, pktType byte, payload []byte) {
	pkt := []byte{pktType}
	pkt = ms.encodeRemainingLength(pkt, len(payload))
	pkt = append(pkt, payload...)
	conn.Write(pkt)
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
	for {
		if _, err := io.ReadFull(conn, buf); err != nil {
			return 0, 0, err
		}
		remaining += int(buf[0]&0x7F) * multiplier
		if buf[0]&0x80 == 0 {
			break
		}
		multiplier *= 128
	}
	return fixedHeader, remaining, nil
}
