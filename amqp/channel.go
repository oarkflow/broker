package amqp

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/broker"
)

// Channel represents an AMQP channel within a connection.
type Channel struct {
	conn          *Connection
	id            uint16
	confirmed     atomic.Bool
	unacked       map[uint64]*pendingDelivery
	unackedMu     sync.Mutex
	deliverySeq   atomic.Uint64
	prefetchCount uint16
	prefetchSize  uint32
	consumers     map[string]*amqpConsumer
	consumersMu   sync.RWMutex
	txActive      bool
	txBuffer      []*txOperation
}

type pendingDelivery struct {
	deliveryTag uint64
	messageID   string
	queue       string
}

type txOperation struct {
	typ  string
	data any
}

// amqpConsumer represents an active consumer.
type amqpConsumer struct {
	consumerTag string
	queue       string
	autoAck     bool
	exclusive   bool
	noLocal     bool
	noWait      bool
	handler     func(Delivery)
}

// NewChannel creates a new AMQP channel.
func NewChannel(conn *Connection, id uint16) *Channel {
	return &Channel{
		conn:      conn,
		id:        id,
		unacked:   make(map[uint64]*pendingDelivery),
		consumers: make(map[string]*amqpConsumer),
	}
}

// handleMethod processes a method frame for this channel.
func (ch *Channel) handleMethod(method *MethodFrame) error {
	switch method.ClassID {
	case ClassChannel:
		return ch.handleChannelMethod(method)
	case ClassExchange:
		return ch.handleExchangeMethod(method)
	case ClassQueue:
		return ch.handleQueueMethod(method)
	case ClassBasic:
		return ch.handleBasicMethod(method)
	case ClassConfirm:
		return ch.handleConfirmMethod(method)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown class %d", method.ClassID))
	}
}

// handleChannelMethod handles channel-level methods.
func (ch *Channel) handleChannelMethod(method *MethodFrame) error {
	switch method.MethodID {
	case MethodChannelOpen:
		return ch.handleChannelOpen()
	case MethodChannelClose:
		return ch.handleChannelClose(method.Args)
	case MethodChannelFlow:
		return ch.handleChannelFlow(method.Args)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown channel method %d", method.MethodID))
	}
}

func (ch *Channel) handleChannelOpen() error {
	return ch.codec().WriteMethodFrame(ch.id, ClassChannel, MethodChannelOpenOk, nil)
}

func (ch *Channel) handleChannelClose(args []byte) error {
	ch.close()
	return ch.codec().WriteMethodFrame(ch.id, ClassChannel, MethodChannelCloseOk, nil)
}

func (ch *Channel) handleChannelFlow(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadBool() // flow active
	return ch.codec().WriteMethodFrame(ch.id, ClassChannel, MethodChannelFlowOk, EncodeMethodArgs(true))
}

// handleExchangeMethod handles exchange methods.
func (ch *Channel) handleExchangeMethod(method *MethodFrame) error {
	switch method.MethodID {
	case MethodExchangeDeclare:
		return ch.handleExchangeDeclare(method.Args)
	case MethodExchangeDelete:
		return ch.handleExchangeDelete(method.Args)
	case MethodExchangeBind:
		return ch.codec().WriteMethodFrame(ch.id, ClassExchange, MethodExchangeBindOk, nil)
	case MethodExchangeUnbind:
		return ch.codec().WriteMethodFrame(ch.id, ClassExchange, MethodExchangeUnbindOk, nil)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown exchange method %d", method.MethodID))
	}
}

func (ch *Channel) handleExchangeDeclare(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16() // reserved
	exchange, _ := r.ReadString()
	exchangeType, _ := r.ReadString()
	durable, _ := r.ReadBool()
	autoDelete, _ := r.ReadBool()
	noWait, _ := r.ReadBool()

	_ = durable
	_ = autoDelete
	_ = exchangeType

	// Declare the exchange as a topic in the broker
	if exchange != "" {
		err := ch.conn.server.cfg.Broker.DeclareTopic(ch.conn.server.ctx, broker.TopicConfig{
			Name: exchange,
		})
		if err != nil {
			return ch.sendChannelError(ReplyInternalError, err.Error())
		}
	}

	if !noWait {
		return ch.codec().WriteMethodFrame(ch.id, ClassExchange, MethodExchangeDeclareOk, nil)
	}
	return nil
}

func (ch *Channel) handleExchangeDelete(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16()  // reserved
	_, _ = r.ReadString()  // exchange
	ifUnused, _ := r.ReadBool()
	noWait, _ := r.ReadBool()

	_ = ifUnused
	_ = noWait

	// exchanges are topics in the broker; deletion is a no-op for now
	if !noWait {
		return ch.codec().WriteMethodFrame(ch.id, ClassExchange, MethodExchangeDeleteOk, nil)
	}
	return nil
}

// handleQueueMethod handles queue methods.
func (ch *Channel) handleQueueMethod(method *MethodFrame) error {
	switch method.MethodID {
	case MethodQueueDeclare:
		return ch.handleQueueDeclare(method.Args)
	case MethodQueueBind:
		return ch.handleQueueBind(method.Args)
	case MethodQueueUnbind:
		return ch.codec().WriteMethodFrame(ch.id, ClassQueue, MethodQueueUnbindOk, nil)
	case MethodQueuePurge:
		return ch.handleQueuePurge(method.Args)
	case MethodQueueDelete:
		return ch.handleQueueDelete(method.Args)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown queue method %d", method.MethodID))
	}
}

func (ch *Channel) handleQueueDeclare(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16() // reserved
	queue, _ := r.ReadString()
	autoDelete, _ := r.ReadBool()
	durable, _ := r.ReadBool()
	exclusive, _ := r.ReadBool()
	noWait, _ := r.ReadBool()

	_ = autoDelete
	_ = durable
	_ = exclusive
	_ = noWait

	queueName := queue
	if queueName == "" {
		queueName = fmt.Sprintf("amq.gen-%s", randomHex(8))
	}

	// Declare queue in broker
	err := ch.conn.server.cfg.Broker.DeclareQueue(ch.conn.server.ctx, broker.QueueConfig{
		Name:              queueName,
		AckMode:           broker.AckManual,
		VisibilityTimeout: 30 * time.Second,
		RetryPolicy:       broker.DefaultRetryPolicy(),
	})
	if err != nil {
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}

	// Send Queue.DeclareOk
	args2 := EncodeMethodArgs(
		uint16(0),         // reserved
		string(queueName), // queue
		uint32(0),         // message-count
		uint32(0),         // consumer-count
	)
	return ch.codec().WriteMethodFrame(ch.id, ClassQueue, MethodQueueDeclareOk, args2)
}

func (ch *Channel) handleQueueBind(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16() // reserved
	queue, _ := r.ReadString()
	exchange, _ := r.ReadString()
	routingKey, _ := r.ReadString()
	noWait, _ := r.ReadBool()
	_, _ = r.ReadTable() // arguments

	_ = noWait

	// Bind queue to exchange (topic) by adding a route
	if exchange != "" {
		ch.conn.server.cfg.Broker.AddRoute(broker.Route{
			Mode: broker.RouteTopic,
			Match: routingKey,
			To: []broker.Destination{
				{Kind: broker.DestinationQueue, Name: queue},
			},
			Enabled: true,
		})
	}

	return ch.codec().WriteMethodFrame(ch.id, ClassQueue, MethodQueueBindOk, nil)
}

func (ch *Channel) handleQueuePurge(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16() // reserved
	queue, _ := r.ReadString()
	_, _ = r.ReadBool() // no-wait

	n, err := ch.conn.server.cfg.Broker.PurgeQueue(ch.conn.server.ctx, queue)
	if err != nil {
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}

	return ch.codec().WriteMethodFrame(ch.id, ClassQueue, MethodQueuePurgeOk, EncodeMethodArgs(uint32(n)))
}

func (ch *Channel) handleQueueDelete(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16() // reserved
	_, _ = r.ReadString() // queue
	ifUnused, _ := r.ReadBool()
	ifEmpty, _ := r.ReadBool()
	noWait, _ := r.ReadBool()

	_ = ifUnused
	_ = ifEmpty
	_ = noWait

	// Queue deletion is not fully supported yet; send ok with count 0
	return ch.codec().WriteMethodFrame(ch.id, ClassQueue, MethodQueueDeleteOk, EncodeMethodArgs(uint32(0)))
}

// handleBasicMethod handles basic methods.
func (ch *Channel) handleBasicMethod(method *MethodFrame) error {
	switch method.MethodID {
	case MethodBasicQos:
		return ch.handleBasicQos(method.Args)
	case MethodBasicConsume:
		return ch.handleBasicConsume(method.Args)
	case MethodBasicCancel:
		return ch.handleBasicCancel(method.Args)
	case MethodBasicPublish:
		return ch.handleBasicPublish(method.Args)
	case MethodBasicAck:
		return ch.handleBasicAck(method.Args)
	case 90: // MethodBasicNack/MethodBasicReject - AMQP uses same ID for both
		// Distinguish by checking args length: nack has multiple+requeue (9 bytes), reject has just delivery-tag+requeue (9 bytes)
		// Actually both are same length, so we'll treat both as nack
		if len(method.Args) >= 9 {
			return ch.handleBasicNack(method.Args)
		}
		return ch.handleBasicReject(method.Args)
	case MethodBasicRecover:
		return ch.handleBasicRecover(method.Args)
	case MethodBasicGet:
		return ch.handleBasicGet(method.Args)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown basic method %d", method.MethodID))
	}
}

func (ch *Channel) handleBasicQos(args []byte) error {
	r := &argsReader{data: args}
	ch.prefetchSize, _ = r.ReadUint32()
	ch.prefetchCount, _ = r.ReadUint16()
	_, _ = r.ReadBool() // global
	return ch.codec().WriteMethodFrame(ch.id, ClassBasic, MethodBasicQosOk, nil)
}

func (ch *Channel) handleBasicConsume(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16() // reserved
	queue, _ := r.ReadString()
	consumerTag, _ := r.ReadString()
	noLocal, _ := r.ReadBool()
	autoAck, _ := r.ReadBool()
	exclusive, _ := r.ReadBool()
	noWait, _ := r.ReadBool()
	_, _ = r.ReadTable() // arguments

	_ = noLocal
	_ = exclusive
	_ = noWait

	if consumerTag == "" {
		consumerTag = fmt.Sprintf("amq.ctag-%s", randomHex(8))
	}

	// Create internal consumer
	consumer := &amqpConsumer{
		consumerTag: consumerTag,
		queue:       queue,
		autoAck:     autoAck,
		exclusive:   exclusive,
		noLocal:     noLocal,
	}

	ch.consumersMu.Lock()
	ch.consumers[consumerTag] = consumer
	ch.consumersMu.Unlock()

	// Start consuming from the queue
	go ch.consumeMessages(queue, consumerTag, autoAck)

	// Send Basic.ConsumeOk
	return ch.codec().WriteMethodFrame(ch.id, ClassBasic, MethodBasicConsumeOk, EncodeMethodArgs(string(consumerTag)))
}

func (ch *Channel) consumeMessages(queue, consumerTag string, autoAck bool) {
	ctx := ch.conn.server.ctx
	b := ch.conn.server.cfg.Broker

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Lease a message from the queue
		messages, err := b.Store().Lease(ctx, broker.LeaseRequest{
			Queue:      queue,
			ConsumerID: consumerTag,
			Limit:      int(ch.prefetchCount),
		})
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range messages {
			delivery := ch.convertMessage(msg, queue, autoAck)
			ch.sendDelivery(delivery)
		}

		if len(messages) == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (ch *Channel) convertMessage(msg broker.Message, queue string, autoAck bool) Delivery {
	deliveryTag := ch.deliverySeq.Add(1)

	if !autoAck {
		ch.unackedMu.Lock()
		ch.unacked[deliveryTag] = &pendingDelivery{
			deliveryTag: deliveryTag,
			messageID:   msg.ID,
			queue:       queue,
		}
		ch.unackedMu.Unlock()
	}

	headers := make(map[string]any)
	for k, v := range msg.Headers {
		headers[k] = v
	}

	return Delivery{
		DeliveryTag: deliveryTag,
		Redeliver:   msg.Redeliveries > 0,
		Exchange:    msg.Topic,
		RoutingKey:  msg.RoutingKey,
		Body:        msg.Payload,
		Headers:     headers,
		Properties: &Properties{
			ContentType:   msg.ContentType,
			CorrelationID: msg.CorrelationID,
			ReplyTo:       msg.ReplyTo,
			MessageID:     msg.ID,
			Type:          msg.Type,
		},
	}
}

func (ch *Channel) sendDelivery(delivery Delivery) {
	// Build properties
	props := delivery.Properties
	if props == nil {
		props = &Properties{}
	}

	// Encode properties
	propFlags, propBuf := ch.encodeProperties(props)

	// Send Basic.Deliver
	deliverArgs := EncodeMethodArgs(
		string(""),                // consumer-tag (empty for pushed deliveries)
		delivery.DeliveryTag,      // delivery-tag
		delivery.Redeliver,        // redeliver
		delivery.Exchange,         // exchange
		delivery.RoutingKey,       // routing-key
	)

	if err := ch.codec().WriteMethodFrame(ch.id, ClassBasic, MethodBasicDeliver, deliverArgs); err != nil {
		return
	}

	// Send content header
	if err := ch.codec().WriteHeaderFrame(ch.id, ClassBasic, uint64(len(delivery.Body)), propFlags, propBuf); err != nil {
		return
	}

	// Send content body(s)
	body := delivery.Body
	for len(body) > 0 {
		end := len(body)
		if end > int(ch.conn.frameMax) {
			end = int(ch.conn.frameMax)
		}
		if err := ch.codec().WriteBodyFrame(ch.id, body[:end]); err != nil {
			return
		}
		body = body[end:]
	}
}

func (ch *Channel) encodeProperties(props *Properties) (uint16, []byte) {
	var flags uint16
	var buf []byte

	if props.ContentType != "" {
		flags |= PropContentType
		buf = appendShortString(buf, props.ContentType)
	}
	if props.ContentEncoding != "" {
		flags |= PropContentEncoding
		buf = appendShortString(buf, props.ContentEncoding)
	}
	if len(props.Headers) > 0 {
		flags |= PropHeaders
		tableData := encodeTableToBytes(props.Headers)
		buf = appendLongBytes(buf, tableData)
	}
	if props.DeliveryMode != 0 {
		flags |= PropDeliveryMode
		buf = append(buf, props.DeliveryMode)
	}
	if props.Priority != 0 {
		flags |= PropPriority
		buf = append(buf, props.Priority)
	}
	if props.CorrelationID != "" {
		flags |= PropCorrelationID
		buf = appendShortString(buf, props.CorrelationID)
	}
	if props.ReplyTo != "" {
		flags |= PropReplyTo
		buf = appendShortString(buf, props.ReplyTo)
	}
	if props.Expiration != "" {
		flags |= PropExpiration
		buf = appendShortString(buf, props.Expiration)
	}
	if props.MessageID != "" {
		flags |= PropMessageID
		buf = appendShortString(buf, props.MessageID)
	}
	if props.Timestamp != 0 {
		flags |= PropTimestamp
		buf = append(buf,
			byte(props.Timestamp>>56), byte(props.Timestamp>>48),
			byte(props.Timestamp>>40), byte(props.Timestamp>>32),
			byte(props.Timestamp>>24), byte(props.Timestamp>>16),
			byte(props.Timestamp>>8), byte(props.Timestamp))
	}
	if props.Type != "" {
		flags |= PropType
		buf = appendShortString(buf, props.Type)
	}
	if props.UserID != "" {
		flags |= PropUserID
		buf = appendShortString(buf, props.UserID)
	}
	if props.AppID != "" {
		flags |= PropAppID
		buf = appendShortString(buf, props.AppID)
	}
	if props.ClusterID != "" {
		flags |= PropClusterID
		buf = appendShortString(buf, props.ClusterID)
	}

	return flags, buf
}

func appendShortString(buf []byte, s string) []byte {
	buf = append(buf, byte(len(s)>>8), byte(len(s)))
	buf = append(buf, s...)
	return buf
}

func appendLongBytes(buf []byte, data []byte) []byte {
	buf = append(buf, byte(len(data)>>24), byte(len(data)>>16), byte(len(data)>>8), byte(len(data)))
	buf = append(buf, data...)
	return buf
}

func encodeTableToBytes(t table) []byte {
	var buf []byte
	start := len(buf)
	buf = append(buf, 0, 0, 0, 0) // placeholder for length

	for key, value := range t {
		buf = append(buf, byte(len(key)))
		buf = append(buf, key...)
		buf = encodeFieldValueToBytes(buf, value)
	}

	// Write length
	length := uint32(len(buf) - start - 4)
	buf[start] = byte(length >> 24)
	buf[start+1] = byte(length >> 16)
	buf[start+2] = byte(length >> 8)
	buf[start+3] = byte(length)

	return buf
}

func encodeFieldValueToBytes(buf []byte, v any) []byte {
	switch val := v.(type) {
	case string:
		buf = append(buf, 'S')
		buf = append(buf, byte(len(val)>>24), byte(len(val)>>16), byte(len(val)>>8), byte(len(val)))
		buf = append(buf, val...)
	case int:
		buf = append(buf, 'I')
		buf = append(buf, byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
	case int32:
		buf = append(buf, 'I')
		buf = append(buf, byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
	case int64:
		buf = append(buf, 'L')
		buf = append(buf, byte(val>>56), byte(val>>48), byte(val>>40), byte(val>>32),
			byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
	case uint8:
		buf = append(buf, 'B', val)
	case bool:
		buf = append(buf, 't')
		if val {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	case float64:
		buf = append(buf, 'f')
		bits := uint64(val)
		buf = append(buf, byte(bits>>56), byte(bits>>48), byte(bits>>40), byte(bits>>32),
			byte(bits>>24), byte(bits>>16), byte(bits>>8), byte(bits))
	case table:
		buf = append(buf, 'F')
		tableBytes := encodeTableToBytes(val)
		buf = appendLongBytes(buf, tableBytes)
	}
	return buf
}

func (ch *Channel) handleBasicCancel(args []byte) error {
	r := &argsReader{data: args}
	consumerTag, _ := r.ReadString()
	_, _ = r.ReadBool() // no-wait

	ch.consumersMu.Lock()
	delete(ch.consumers, consumerTag)
	ch.consumersMu.Unlock()

	return ch.codec().WriteMethodFrame(ch.id, ClassBasic, MethodBasicCancelOk, EncodeMethodArgs(string(consumerTag)))
}

func (ch *Channel) handleBasicPublish(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16() // reserved
	exchange, _ := r.ReadString()
	routingKey, _ := r.ReadString()
	mandatory, _ := r.ReadBool()
	immediate, _ := r.ReadBool()

	_ = mandatory
	_ = immediate

	// Read content header and body
	props, body, err := ch.readContent()
	if err != nil {
		return err
	}

	// Convert headers to map[string]string
	headers := make(map[string]string)
	if props.Headers != nil {
		for k, v := range props.Headers {
			if s, ok := v.(string); ok {
				headers[k] = s
			}
		}
	}

	// Publish to broker
	opts := []broker.PublishOption{
		broker.WithRoutingKey(routingKey),
		broker.WithContentType(props.ContentType),
		broker.WithCorrelationID(props.CorrelationID),
	}
	if props.MessageID != "" {
		opts = append(opts, broker.WithMessageID(props.MessageID))
	}
	if props.Type != "" {
		opts = append(opts, broker.WithType(props.Type))
	}
	if len(headers) > 0 {
		opts = append(opts, broker.WithHeaders(headers))
	}

	_, err = ch.conn.server.cfg.Broker.Publisher().Publish(ch.conn.server.ctx, exchange, body, opts...)
	if err != nil {
		log.Printf("[amqp] publish error: %v", err)
	}

	// If in confirm mode, send ack
	if ch.confirmed.Load() {
		deliveryTag := ch.deliverySeq.Add(1)
		return ch.codec().WriteMethodFrame(ch.id, ClassConfirm, 80, EncodeMethodArgs(deliveryTag, true))
	}

	return nil
}

func (ch *Channel) readContent() (*Properties, []byte, error) {
	// Read content header
	headerFrame, err := ch.conn.codec.ReadFrame()
	if err != nil {
		return nil, nil, err
	}
	if headerFrame.Type != FrameHeader {
		return nil, nil, fmt.Errorf("expected header frame, got %d", headerFrame.Type)
	}

	hdr, err := ParseHeaderFrame(headerFrame.Payload)
	if err != nil {
		return nil, nil, err
	}

	// Parse properties
	props := &Properties{}
	if hdr.PropertyFlags != 0 {
		r := &argsReader{data: hdr.PropertyBuf}
		if hdr.PropertyFlags&PropContentType != 0 {
			props.ContentType, _ = r.ReadString()
		}
		if hdr.PropertyFlags&PropContentEncoding != 0 {
			props.ContentEncoding, _ = r.ReadString()
		}
		if hdr.PropertyFlags&PropHeaders != 0 {
			data, _ := r.ReadBytes()
			t, _ := DecodeTable(data)
			props.Headers = t
		}
		if hdr.PropertyFlags&PropDeliveryMode != 0 {
			props.DeliveryMode, _ = r.ReadUint8()
		}
		if hdr.PropertyFlags&PropPriority != 0 {
			props.Priority, _ = r.ReadUint8()
		}
		if hdr.PropertyFlags&PropCorrelationID != 0 {
			props.CorrelationID, _ = r.ReadString()
		}
		if hdr.PropertyFlags&PropReplyTo != 0 {
			props.ReplyTo, _ = r.ReadString()
		}
		if hdr.PropertyFlags&PropExpiration != 0 {
			props.Expiration, _ = r.ReadString()
		}
		if hdr.PropertyFlags&PropMessageID != 0 {
			props.MessageID, _ = r.ReadString()
		}
		if hdr.PropertyFlags&PropTimestamp != 0 {
			ts, _ := r.ReadUint32()
			props.Timestamp = uint64(ts)
		}
		if hdr.PropertyFlags&PropType != 0 {
			props.Type, _ = r.ReadString()
		}
		if hdr.PropertyFlags&PropUserID != 0 {
			props.UserID, _ = r.ReadString()
		}
		if hdr.PropertyFlags&PropAppID != 0 {
			props.AppID, _ = r.ReadString()
		}
		if hdr.PropertyFlags&PropClusterID != 0 {
			props.ClusterID, _ = r.ReadString()
		}
	}

	// Read body frame(s)
	var body []byte
	remaining := hdr.BodySize
	for remaining > 0 {
		bodyFrame, err := ch.conn.codec.ReadFrame()
		if err != nil {
			return nil, nil, err
		}
		if bodyFrame.Type != FrameBody {
			return nil, nil, fmt.Errorf("expected body frame, got %d", bodyFrame.Type)
		}
		body = append(body, bodyFrame.Payload...)
		remaining -= uint64(len(bodyFrame.Payload))
	}

	return props, body, nil
}

func (ch *Channel) handleBasicAck(args []byte) error {
	r := &argsReader{data: args}
	deliveryTag, _ := r.ReadUint64()
	multiple, _ := r.ReadBool()

	b := ch.conn.server.cfg.Broker

	ch.unackedMu.Lock()
	if multiple {
		// Ack all up to and including this tag
		for tag, pending := range ch.unacked {
			if tag <= deliveryTag {
				_ = b.Store().Ack(ch.conn.server.ctx, broker.AckRequest{
					MessageID:  pending.messageID,
					Queue:      pending.queue,
					ConsumerID: "",
				})
				delete(ch.unacked, tag)
			}
		}
	} else if pending, ok := ch.unacked[deliveryTag]; ok {
		_ = b.Store().Ack(ch.conn.server.ctx, broker.AckRequest{
			MessageID:  pending.messageID,
			Queue:      pending.queue,
			ConsumerID: "",
		})
		delete(ch.unacked, deliveryTag)
	}
	ch.unackedMu.Unlock()

	return nil
}

func (ch *Channel) handleBasicNack(args []byte) error {
	r := &argsReader{data: args}
	deliveryTag, _ := r.ReadUint64()
	multiple, _ := r.ReadBool()
	requeue, _ := r.ReadBool()

	return ch.handleNackOrReject(deliveryTag, multiple, requeue, "nack")
}

func (ch *Channel) handleBasicReject(args []byte) error {
	r := &argsReader{data: args}
	deliveryTag, _ := r.ReadUint64()
	requeue, _ := r.ReadBool()

	return ch.handleNackOrReject(deliveryTag, false, requeue, "reject")
}

func (ch *Channel) handleNackOrReject(deliveryTag uint64, multiple, requeue bool, action string) error {
	b := ch.conn.server.cfg.Broker

	ch.unackedMu.Lock()
	if multiple {
		for tag, pending := range ch.unacked {
			if tag <= deliveryTag {
				nackAction := broker.NackRequeue
				if !requeue {
					nackAction = broker.NackReject
				}
				_ = b.Store().Nack(ch.conn.server.ctx, broker.NackRequest{
					MessageID:  pending.messageID,
					Queue:      pending.queue,
					ConsumerID: "",
					Action:     nackAction,
				})
				delete(ch.unacked, tag)
			}
		}
	} else if pending, ok := ch.unacked[deliveryTag]; ok {
		nackAction := broker.NackRequeue
		if !requeue {
			nackAction = broker.NackReject
		}
		_ = b.Store().Nack(ch.conn.server.ctx, broker.NackRequest{
			MessageID:  pending.messageID,
			Queue:      pending.queue,
			ConsumerID: "",
			Action:     nackAction,
		})
		delete(ch.unacked, deliveryTag)
	}
	ch.unackedMu.Unlock()

	return nil
}

func (ch *Channel) handleBasicRecover(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadBool() // requeue
	// Recovery is handled by the lease sweeper
	return ch.codec().WriteMethodFrame(ch.id, ClassBasic, MethodBasicRecoverOk, nil)
}

func (ch *Channel) handleBasicGet(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16() // reserved
	queue, _ := r.ReadString()
	autoAck, _ := r.ReadBool()

	b := ch.conn.server.cfg.Broker

	// Try to get a single message
	messages, err := b.Store().Lease(ch.conn.server.ctx, broker.LeaseRequest{
		Queue:      queue,
		ConsumerID: fmt.Sprintf("amqp-get-%d", ch.id),
		Limit:      1,
	})
	if err != nil || len(messages) == 0 {
		// Send Basic.GetEmpty
		return ch.codec().WriteMethodFrame(ch.id, ClassBasic, 72, EncodeMethodArgs(uint32(0)))
	}

	msg := messages[0]
	deliveryTag := ch.deliverySeq.Add(1)

	if !autoAck {
		ch.unackedMu.Lock()
		ch.unacked[deliveryTag] = &pendingDelivery{
			deliveryTag: deliveryTag,
			messageID:   msg.ID,
			queue:       queue,
		}
		ch.unackedMu.Unlock()
	}

	// Send Basic.GetOk
	getOkArgs := EncodeMethodArgs(
		deliveryTag,
		msg.RoutingKey,
		msg.Topic,
		uint32(0), // message-count
	)
	if err := ch.codec().WriteMethodFrame(ch.id, ClassBasic, 71, getOkArgs); err != nil {
		return err
	}

	// Send content header and body
	props := &Properties{
		ContentType:   msg.ContentType,
		CorrelationID: msg.CorrelationID,
		ReplyTo:       msg.ReplyTo,
		MessageID:     msg.ID,
		Type:          msg.Type,
	}
	propFlags, propBuf := ch.encodeProperties(props)

	if err := ch.codec().WriteHeaderFrame(ch.id, ClassBasic, uint64(len(msg.Payload)), propFlags, propBuf); err != nil {
		return err
	}

	return ch.codec().WriteBodyFrame(ch.id, msg.Payload)
}

// handleConfirmMethod handles confirm methods.
func (ch *Channel) handleConfirmMethod(method *MethodFrame) error {
	switch method.MethodID {
	case MethodConfirmSelect:
		ch.confirmed.Store(true)
		return ch.codec().WriteMethodFrame(ch.id, ClassConfirm, MethodConfirmSelectOk, nil)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown confirm method %d", method.MethodID))
	}
}

// handleContentFrame handles content frames (header/body) for this channel.
func (ch *Channel) handleContentFrame(frame *Frame) {
	// Content frames are processed inline by readContent()
}

func (ch *Channel) sendChannelError(code uint16, text string) error {
	args := EncodeMethodArgs(
		uint16(code),
		string(text),
		uint16(0), // class-id
		uint16(0), // method-id
	)
	return ch.codec().WriteMethodFrame(ch.id, ClassChannel, MethodChannelClose, args)
}

func (ch *Channel) close() {
	ch.consumersMu.Lock()
	ch.consumers = make(map[string]*amqpConsumer)
	ch.consumersMu.Unlock()

	ch.unackedMu.Lock()
	ch.unacked = make(map[uint64]*pendingDelivery)
	ch.unackedMu.Unlock()
}

func (ch *Channel) codec() *Codec {
	return ch.conn.codec
}

func randomHex(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = "0123456789abcdef"[time.Now().UnixNano()%16]
		time.Sleep(time.Nanosecond)
	}
	return string(b)
}
