package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
)

func encodeConnectPayload(clientID string, cleanSession bool) []byte {
	// Protocol Name: "MQTT"
	var buf bytes.Buffer
	proto := "MQTT"
	binary.Write(&buf, binary.BigEndian, uint16(len(proto)))
	buf.WriteString(proto)
	buf.WriteByte(4) // Protocol Level 4 (3.1.1)

	var flags byte
	if cleanSession {
		flags |= 0x02
	}
	buf.WriteByte(flags)
	binary.Write(&buf, binary.BigEndian, uint16(60)) // KeepAlive: 60s

	// Client ID
	binary.Write(&buf, binary.BigEndian, uint16(len(clientID)))
	buf.WriteString(clientID)

	return buf.Bytes()
}

func encodeSubscribePayload(packetID uint16, topic string, qos byte) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, packetID)
	binary.Write(&buf, binary.BigEndian, uint16(len(topic)))
	buf.WriteString(topic)
	buf.WriteByte(qos)
	return buf.Bytes()
}

func TestMQTTPersistentSessionLifecycle(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())

	server := NewMQTTServer(b, MQTTConfig{})

	// 1. Initial connect with CleanSession = false -> SessionPresent should be false (0x00)
	conn1 := &mqttBufferConn{}
	mc1 := &mqttConn{conn: conn1, incomingQoS2: make(map[uint16]mqttPendingPublish)}
	payload1 := encodeConnectPayload("client-p1", false)
	if !server.handleConnect(mc1, payload1) {
		t.Fatal("handleConnect failed on conn1")
	}

	// Verify CONNACK: byte 0 is MQTTConnack (0x20), byte 1 is len 2, byte 2 is flags, byte 3 is returnCode
	resp1 := conn1.Bytes()
	if len(resp1) < 4 || resp1[0] != MQTTConnack || resp1[2] != 0x00 || resp1[3] != 0x00 {
		t.Fatalf("expected CONNACK with sessionPresent=0, got %x", resp1)
	}

	// 2. Subscribe to "sensors/temp"
	subPayload := encodeSubscribePayload(1, "sensors/temp", 1)
	server.handleSubscribe(mc1, subPayload)
	if _, ok := mc1.subs["sensors/temp"]; !ok {
		t.Fatal("expected sensors/temp in mc1 subscriptions")
	}

	// Check server session state
	server.sessionsMu.Lock()
	sess, ok := server.sessions["client-p1"]
	server.sessionsMu.Unlock()
	if !ok || sess.Subscriptions["sensors/temp"] != 1 {
		t.Fatalf("session not properly saved in server: %+v", sess)
	}

	// 3. Reconnect with CleanSession = false -> SessionPresent should be true (0x01)
	conn2 := &mqttBufferConn{}
	mc2 := &mqttConn{conn: conn2, incomingQoS2: make(map[uint16]mqttPendingPublish)}
	payload2 := encodeConnectPayload("client-p1", false)
	if !server.handleConnect(mc2, payload2) {
		t.Fatal("handleConnect failed on conn2")
	}

	resp2 := conn2.Bytes()
	if len(resp2) < 4 || resp2[0] != MQTTConnack || resp2[2] != 0x01 || resp2[3] != 0x00 {
		t.Fatalf("expected CONNACK with sessionPresent=1, got %x", resp2)
	}

	// Subscriptions should be re-attached
	if qos, exists := mc2.subs["sensors/temp"]; !exists || qos != 1 {
		t.Fatalf("expected reattached subscription for sensors/temp, got subs=%+v", mc2.subs)
	}

	// 4. Connect with CleanSession = true -> SessionPresent should be false (0x00) and session wiped
	conn3 := &mqttBufferConn{}
	mc3 := &mqttConn{conn: conn3, incomingQoS2: make(map[uint16]mqttPendingPublish)}
	payload3 := encodeConnectPayload("client-p1", true)
	if !server.handleConnect(mc3, payload3) {
		t.Fatal("handleConnect failed on conn3")
	}

	resp3 := conn3.Bytes()
	if len(resp3) < 4 || resp3[0] != MQTTConnack || resp3[2] != 0x00 || resp3[3] != 0x00 {
		t.Fatalf("expected CONNACK with sessionPresent=0 on cleanSession, got %x", resp3)
	}

	server.sessionsMu.Lock()
	_, ok = server.sessions["client-p1"]
	server.sessionsMu.Unlock()
	if ok {
		t.Fatal("session was not deleted on CleanSession=true")
	}
}
