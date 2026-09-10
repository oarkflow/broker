package broker

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"
)

type mqttBufferConn struct{ bytes.Buffer }

func (c *mqttBufferConn) Read(p []byte) (int, error)       { return 0, io.EOF }
func (c *mqttBufferConn) Close() error                     { return nil }
func (c *mqttBufferConn) LocalAddr() net.Addr              { return mqttTestAddr("local") }
func (c *mqttBufferConn) RemoteAddr() net.Addr             { return mqttTestAddr("remote") }
func (c *mqttBufferConn) SetDeadline(time.Time) error      { return nil }
func (c *mqttBufferConn) SetReadDeadline(time.Time) error  { return nil }
func (c *mqttBufferConn) SetWriteDeadline(time.Time) error { return nil }

type mqttTestAddr string

func (a mqttTestAddr) Network() string { return "memory" }
func (a mqttTestAddr) String() string  { return string(a) }

func mqttPublishPayload(topic string, packetID uint16, payload string) []byte {
	out := []byte{byte(len(topic) >> 8), byte(len(topic))}
	out = append(out, topic...)
	if packetID != 0 {
		out = append(out, byte(packetID>>8), byte(packetID))
	}
	return append(out, payload...)
}

func TestMQTTPublishUsesFixedHeaderQoS(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	server := NewMQTTServer(b, MQTTConfig{})
	conn := &mqttBufferConn{}
	client := &mqttConn{conn: conn, incomingQoS2: make(map[uint16]mqttPendingPublish)}

	server.handlePublish(client, MQTTPublish|0x02, mqttPublishPayload("a/b", 7, "one"))
	if got := conn.Bytes(); !bytes.Equal(got, []byte{MQTTPuback, 2, 0, 7}) {
		t.Fatalf("PUBACK=%x", got)
	}
	msgs, err := b.Store().ListQueue(context.Background(), "mqtt.a.b", 10)
	if err != nil || len(msgs) != 1 || string(msgs[0].Payload) != "one" {
		t.Fatalf("qos1 publish messages=%+v err=%v", msgs, err)
	}
}

func TestMQTTQoS2PublishesOnlyAfterPUBRELAndIsIdempotent(t *testing.T) {
	b := MustNew(Config{Store: NewMemoryStore()})
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Stop(context.Background())
	server := NewMQTTServer(b, MQTTConfig{MaxInflight: 2})
	conn := &mqttBufferConn{}
	client := &mqttConn{conn: conn, incomingQoS2: make(map[uint16]mqttPendingPublish)}

	server.handlePublish(client, MQTTPublish|0x04, mqttPublishPayload("qos/two", 9, "two"))
	if got := conn.Bytes(); !bytes.Equal(got, []byte{MQTTPubrec, 2, 0, 9}) {
		t.Fatalf("PUBREC=%x", got)
	}
	if _, err := b.Store().ListQueue(context.Background(), "mqtt.qos.two", 10); err != ErrQueueNotFound {
		t.Fatalf("qos2 message became visible before PUBREL: %v", err)
	}
	if err := server.completeIncomingQoS2(client, 9); err != nil {
		t.Fatal(err)
	}
	if err := server.completeIncomingQoS2(client, 9); err != nil {
		t.Fatal(err)
	}
	msgs, err := b.Store().ListQueue(context.Background(), "mqtt.qos.two", 10)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("qos2 messages=%d err=%v", len(msgs), err)
	}
}
