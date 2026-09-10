package amqp

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/oarkflow/broker"
)

func setupTestAMQPChannel(t *testing.T) (*Channel, *Codec, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	b, err := broker.New(broker.Config{Mode: broker.ModeEmbedded, DefaultQueue: "amqp-test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	server := NewServer(ServerConfig{Broker: b, AllowInsecure: true})

	var serverConn net.Conn
	var acceptErr error
	acceptDone := make(chan struct{})
	go func() {
		serverConn, acceptErr = ln.Accept()
		close(acceptDone)
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	<-acceptDone
	if acceptErr != nil {
		t.Fatal(acceptErr)
	}

	conn := NewConnection(server, serverConn, FrameMaxSize, time.Second)
	ch := NewChannel(conn, 1)
	conn.channels[1] = ch

	clientCodec := NewCodec(clientConn)

	cleanup := func() {
		ch.close()
		_ = clientConn.Close()
		_ = serverConn.Close()
		_ = ln.Close()
		_ = b.Stop(context.Background())
	}

	return ch, clientCodec, cleanup
}

func TestAMQPChannelFlowControl(t *testing.T) {
	ch, clientCodec, cleanup := setupTestAMQPChannel(t)
	defer cleanup()

	if !ch.FlowActive() {
		t.Fatal("expected channel flow to be active initially")
	}

	// 1. Client pauses flow: channel.flow(false)
	err := ch.handleMethod(&MethodFrame{
		ClassID:  ClassChannel,
		MethodID: MethodChannelFlow,
		Args:     EncodeMethodArgs(false),
	})
	if err != nil {
		t.Fatalf("handleMethod error: %v", err)
	}

	frame, err := clientCodec.ReadFrame()
	if err != nil {
		t.Fatalf("failed to read response frame: %v", err)
	}
	if frame.Type != FrameMethod || frame.ChannelID != 1 {
		t.Fatalf("unexpected frame: %+v", frame)
	}
	method, err := ParseMethodFrame(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if method.ClassID != ClassChannel || method.MethodID != MethodChannelFlowOk {
		t.Fatalf("expected Channel.FlowOk, got class=%d method=%d", method.ClassID, method.MethodID)
	}
	r := &argsReader{data: method.Args}
	active, _ := r.ReadBool()
	if active {
		t.Fatal("expected flow active false in flow-ok")
	}
	if ch.FlowActive() {
		t.Fatal("expected channel flow to be inactive")
	}

	// 2. Client resumes flow: channel.flow(true)
	err = ch.handleMethod(&MethodFrame{
		ClassID:  ClassChannel,
		MethodID: MethodChannelFlow,
		Args:     EncodeMethodArgs(true),
	})
	if err != nil {
		t.Fatalf("handleMethod error: %v", err)
	}

	frame, err = clientCodec.ReadFrame()
	if err != nil {
		t.Fatalf("failed to read response frame: %v", err)
	}
	method, err = ParseMethodFrame(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if method.ClassID != ClassChannel || method.MethodID != MethodChannelFlowOk {
		t.Fatalf("expected Channel.FlowOk, got class=%d method=%d", method.ClassID, method.MethodID)
	}
	r = &argsReader{data: method.Args}
	active, _ = r.ReadBool()
	if !active {
		t.Fatal("expected flow active true in flow-ok")
	}
	if !ch.FlowActive() {
		t.Fatal("expected channel flow to be active")
	}

	// 3. Server initiates flow pause: SetFlow(false)
	if err := ch.SetFlow(false); err != nil {
		t.Fatalf("SetFlow error: %v", err)
	}

	frame, err = clientCodec.ReadFrame()
	if err != nil {
		t.Fatalf("failed to read frame from SetFlow: %v", err)
	}
	method, err = ParseMethodFrame(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if method.ClassID != ClassChannel || method.MethodID != MethodChannelFlow {
		t.Fatalf("expected Channel.Flow, got class=%d method=%d", method.ClassID, method.MethodID)
	}
	r = &argsReader{data: method.Args}
	active, _ = r.ReadBool()
	if active {
		t.Fatal("expected flow active false in server initiated flow")
	}
	if ch.FlowActive() {
		t.Fatal("expected channel flow to be inactive after SetFlow(false)")
	}
}

func TestAMQPPublisherConfirmsAndFlowNack(t *testing.T) {
	ch, clientCodec, cleanup := setupTestAMQPChannel(t)
	defer cleanup()

	if ch.Confirmed() {
		t.Fatal("expected confirms to be disabled initially")
	}

	// 1. Enable confirms: confirm.select(nowait=false)
	err := ch.handleMethod(&MethodFrame{
		ClassID:  ClassConfirm,
		MethodID: MethodConfirmSelect,
		Args:     EncodeMethodArgs(false),
	})
	if err != nil {
		t.Fatalf("handleMethod confirm.select error: %v", err)
	}

	frame, err := clientCodec.ReadFrame()
	if err != nil {
		t.Fatalf("failed to read confirm.select-ok: %v", err)
	}
	method, err := ParseMethodFrame(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if method.ClassID != ClassConfirm || method.MethodID != MethodConfirmSelectOk {
		t.Fatalf("expected Confirm.SelectOk, got class=%d method=%d", method.ClassID, method.MethodID)
	}
	if !ch.Confirmed() {
		t.Fatal("expected confirms to be enabled")
	}

	// 2. Publish message while flow is active -> should receive Basic.Ack with deliveryTag=1
	publishArgs := EncodeMethodArgs(
		uint16(0),           // reserved-1
		string("amqp-test"), // exchange
		string("key.1"),     // routing-key
		false,               // mandatory
		false,               // immediate
	)
	hdrPayload := make([]byte, 12)
	hdrPayload[1] = 60
	hdrPayload[9] = 5
	if err := clientCodec.WriteFrame(&Frame{Type: FrameHeader, ChannelID: 1, Payload: hdrPayload}); err != nil {
		t.Fatal(err)
	}
	if err := clientCodec.WriteFrame(&Frame{Type: FrameBody, ChannelID: 1, Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}

	if err := ch.handleBasicPublish(publishArgs); err != nil {
		t.Fatalf("handleBasicPublish error: %v", err)
	}

	frame, err = clientCodec.ReadFrame()
	if err != nil {
		t.Fatalf("failed to read basic.ack frame: %v", err)
	}
	method, err = ParseMethodFrame(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if method.ClassID != ClassBasic || method.MethodID != MethodBasicAck {
		t.Fatalf("expected Basic.Ack (class 60, method 80), got class=%d method=%d", method.ClassID, method.MethodID)
	}
	r := &argsReader{data: method.Args}
	tag, _ := r.ReadUint64()
	multiple, _ := r.ReadBool()
	if tag != 1 || multiple {
		t.Fatalf("expected deliveryTag=1, multiple=false; got tag=%d multiple=%v", tag, multiple)
	}

	// 3. Pause flow, then publish message -> should receive Basic.Nack with deliveryTag=2
	ch.flowActive.Store(false)

	publishArgs2 := EncodeMethodArgs(
		uint16(0),
		string("amqp-test"),
		string("key.2"),
		false,
		false,
	)
	hdrPayload2 := make([]byte, 12)
	hdrPayload2[1] = 60
	hdrPayload2[9] = 5
	if err := clientCodec.WriteFrame(&Frame{Type: FrameHeader, ChannelID: 1, Payload: hdrPayload2}); err != nil {
		t.Fatal(err)
	}
	if err := clientCodec.WriteFrame(&Frame{Type: FrameBody, ChannelID: 1, Payload: []byte("world")}); err != nil {
		t.Fatal(err)
	}

	if err := ch.handleBasicPublish(publishArgs2); err != nil {
		t.Fatalf("handleBasicPublish on paused flow error: %v", err)
	}

	frame, err = clientCodec.ReadFrame()
	if err != nil {
		t.Fatalf("failed to read basic.nack frame: %v", err)
	}
	method, err = ParseMethodFrame(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if method.ClassID != ClassBasic || method.MethodID != MethodBasicNack {
		t.Fatalf("expected Basic.Nack (class 60, method 120), got class=%d method=%d", method.ClassID, method.MethodID)
	}
	r = &argsReader{data: method.Args}
	tag, _ = r.ReadUint64()
	multiple, _ = r.ReadBool()
	requeue, _ := r.ReadBool()
	if tag != 2 || multiple || requeue {
		t.Fatalf("expected deliveryTag=2, multiple=false, requeue=false; got tag=%d multiple=%v requeue=%v", tag, multiple, requeue)
	}
}
