package amqp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/broker"
)

// Channel represents an AMQP channel within a connection.
type Channel struct {
	conn                *Connection
	id                  uint16
	confirmed           atomic.Bool
	flowActive          atomic.Bool
	closed              atomic.Bool
	unacked             map[uint64]*pendingDelivery
	unackedMu           sync.Mutex
	deliverySeq         atomic.Uint64
	prefetchCount       atomic.Uint32
	prefetchGlobal      atomic.Bool
	prefetchSize        uint32
	prefetchMu          sync.Mutex
	prefetchOutstanding map[string]uint32
	prefetchReserved    map[string]uint32
	globalOutstanding   uint32
	globalReserved      uint32
	consumers           map[string]*amqpConsumer
	consumersMu         sync.RWMutex
	writeMu             sync.Mutex
	txActive            bool
	txBuffer            []*txOperation
	closeOnce           sync.Once
}

type pendingDelivery struct {
	deliveryTag uint64
	messageID   string
	queue       string
	leaseToken  string
	consumerTag string
}

type txOperation struct {
	kind        string
	messageID   string
	queue       string
	leaseToken  string
	nackAction  broker.NackAction
	consumerTag string
	topic       string
	body        []byte
	opts        []broker.PublishOption
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
	cancel      context.CancelFunc
}

var errChannelFlowInactive = errors.New("amqp: channel flow inactive")
var errChannelClosed = errors.New("amqp: channel closed")

// NewChannel creates a new AMQP channel.
func NewChannel(conn *Connection, id uint16) *Channel {
	ch := &Channel{
		conn:                conn,
		id:                  id,
		unacked:             make(map[uint64]*pendingDelivery),
		consumers:           make(map[string]*amqpConsumer),
		prefetchOutstanding: make(map[string]uint32),
		prefetchReserved:    make(map[string]uint32),
	}
	ch.flowActive.Store(true)
	return ch
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
	case ClassTx:
		return ch.handleTxMethod(method)
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
	case MethodChannelCloseOk:
		ch.close()
		ch.conn.removeChannel(ch.id)
		return nil
	case MethodChannelFlow:
		return ch.handleChannelFlow(method.Args)
	case MethodChannelFlowOk:
		return ch.handleChannelFlowOk(method.Args)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown channel method %d", method.MethodID))
	}
}

func (ch *Channel) handleChannelOpen() error {
	return ch.writeMethodFrame(ClassChannel, MethodChannelOpenOk, nil)
}

func (ch *Channel) handleChannelClose(args []byte) error {
	ch.close()
	err := ch.writeMethodFrame(ClassChannel, MethodChannelCloseOk, nil)
	ch.conn.removeChannel(ch.id)
	return err
}

func (ch *Channel) handleChannelFlow(args []byte) error {
	r := &argsReader{data: args}
	active, _ := r.ReadBool()
	ch.flowActive.Store(active)
	return ch.writeMethodFrame(ClassChannel, MethodChannelFlowOk, EncodeMethodArgs(active))
}

func (ch *Channel) handleChannelFlowOk(args []byte) error {
	r := &argsReader{data: args}
	active, _ := r.ReadBool()
	ch.flowActive.Store(active)
	return nil
}

// FlowActive returns true if flow is enabled on this channel.
func (ch *Channel) FlowActive() bool {
	return ch.flowActive.Load()
}

// SetFlow enables or disables flow control on the channel and notifies the client.
func (ch *Channel) SetFlow(active bool) error {
	ch.flowActive.Store(active)
	return ch.writeMethodFrame(ClassChannel, MethodChannelFlow, EncodeMethodArgs(active))
}

// handleExchangeMethod handles exchange methods.
func (ch *Channel) handleExchangeMethod(method *MethodFrame) error {
	switch method.MethodID {
	case MethodExchangeDeclare:
		return ch.handleExchangeDeclare(method.Args)
	case MethodExchangeDelete:
		return ch.handleExchangeDelete(method.Args)
	case MethodExchangeBind:
		return ch.writeMethodFrame(ClassExchange, MethodExchangeBindOk, nil)
	case MethodExchangeUnbind:
		return ch.writeMethodFrame(ClassExchange, MethodExchangeUnbindOk, nil)
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
		return ch.writeMethodFrame(ClassExchange, MethodExchangeDeclareOk, nil)
	}
	return nil
}

func (ch *Channel) handleExchangeDelete(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16() // reserved
	_, _ = r.ReadString() // exchange
	ifUnused, _ := r.ReadBool()
	noWait, _ := r.ReadBool()

	_ = ifUnused
	_ = noWait

	// exchanges are topics in the broker; deletion is a no-op for now
	if !noWait {
		return ch.writeMethodFrame(ClassExchange, MethodExchangeDeleteOk, nil)
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
		return ch.writeMethodFrame(ClassQueue, MethodQueueUnbindOk, nil)
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
	return ch.writeMethodFrame(ClassQueue, MethodQueueDeclareOk, args2)
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
			Mode:  broker.RouteTopic,
			Match: routingKey,
			To: []broker.Destination{
				{Kind: broker.DestinationQueue, Name: queue},
			},
			Enabled: true,
		})
	}

	return ch.writeMethodFrame(ClassQueue, MethodQueueBindOk, nil)
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

	return ch.writeMethodFrame(ClassQueue, MethodQueuePurgeOk, EncodeMethodArgs(uint32(n)))
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
	return ch.writeMethodFrame(ClassQueue, MethodQueueDeleteOk, EncodeMethodArgs(uint32(0)))
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
	case MethodBasicReject:
		return ch.handleBasicReject(method.Args)
	case MethodBasicNack:
		return ch.handleBasicNack(method.Args)
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
	prefetchCount, _ := r.ReadUint16()
	ch.prefetchCount.Store(uint32(prefetchCount))
	global, _ := r.ReadBool()
	ch.prefetchGlobal.Store(global)
	return ch.writeMethodFrame(ClassBasic, MethodBasicQosOk, nil)
}

func (ch *Channel) reservePrefetch(consumerTag string) int {
	ch.prefetchMu.Lock()
	defer ch.prefetchMu.Unlock()
	if ch.prefetchReserved == nil {
		ch.prefetchReserved = make(map[string]uint32)
	}
	if ch.prefetchOutstanding == nil {
		ch.prefetchOutstanding = make(map[string]uint32)
	}
	limit := ch.prefetchCount.Load()
	available := uint32(1)
	if limit > 0 {
		used := ch.globalOutstanding + ch.globalReserved
		if !ch.prefetchGlobal.Load() {
			used = ch.prefetchOutstanding[consumerTag] + ch.prefetchReserved[consumerTag]
		}
		if used >= limit {
			return 0
		}
		available = limit - used
	}
	ch.prefetchReserved[consumerTag] += available
	ch.globalReserved += available
	return int(available)
}

func (ch *Channel) releasePrefetchReservation(consumerTag string, count uint32) {
	ch.prefetchMu.Lock()
	reserved := ch.prefetchReserved[consumerTag]
	if count > reserved {
		count = reserved
	}
	ch.prefetchReserved[consumerTag] = reserved - count
	if count > ch.globalReserved {
		ch.globalReserved = 0
	} else {
		ch.globalReserved -= count
	}
	ch.prefetchMu.Unlock()
}

func (ch *Channel) reservationToInFlight(consumerTag string) {
	ch.prefetchMu.Lock()
	if ch.prefetchReserved[consumerTag] > 0 {
		ch.prefetchReserved[consumerTag]--
		if ch.globalReserved > 0 {
			ch.globalReserved--
		}
	}
	ch.prefetchOutstanding[consumerTag]++
	ch.globalOutstanding++
	ch.prefetchMu.Unlock()
}

func (ch *Channel) releasePrefetchInFlight(consumerTag string) {
	ch.prefetchMu.Lock()
	if ch.prefetchOutstanding[consumerTag] > 0 {
		ch.prefetchOutstanding[consumerTag]--
		if ch.globalOutstanding > 0 {
			ch.globalOutstanding--
		}
	}
	ch.prefetchMu.Unlock()
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
		noWait:      noWait,
	}
	consumerCtx, cancel := context.WithCancel(ch.conn.server.ctx)
	consumer.cancel = cancel

	ch.consumersMu.Lock()
	if _, exists := ch.consumers[consumerTag]; exists {
		ch.consumersMu.Unlock()
		cancel()
		return ch.sendChannelError(ReplyPreconditionFailed, "consumer tag already exists")
	}
	ch.consumers[consumerTag] = consumer
	ch.consumersMu.Unlock()

	// Start consuming from the queue
	go ch.consumeMessages(consumerCtx, queue, consumerTag, autoAck)

	// Send Basic.ConsumeOk
	if noWait {
		return nil
	}
	return ch.writeMethodFrame(ClassBasic, MethodBasicConsumeOk, EncodeMethodArgs(string(consumerTag)))
}

func (ch *Channel) consumeMessages(ctx context.Context, queue, consumerTag string, autoAck bool) {
	b := ch.conn.server.cfg.Broker

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !ch.flowActive.Load() {
			if !waitAMQP(ctx, 25*time.Millisecond) {
				return
			}
			continue
		}

		// Lease a message from the queue
		limit := ch.reservePrefetch(consumerTag)
		if limit == 0 {
			if !waitAMQP(ctx, 25*time.Millisecond) {
				return
			}
			continue
		}
		messages, err := b.Store().Lease(ctx, broker.LeaseRequest{
			Queue:      queue,
			ConsumerID: consumerTag,
			Limit:      limit,
		})
		if err != nil {
			ch.releasePrefetchReservation(consumerTag, uint32(limit))
			if !waitAMQP(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		if len(messages) < limit {
			ch.releasePrefetchReservation(consumerTag, uint32(limit-len(messages)))
		}

		for i, msg := range messages {
			select {
			case <-ctx.Done():
				for _, remaining := range messages[i:] {
					_ = b.Store().Nack(context.Background(), broker.NackRequest{MessageID: remaining.ID, Queue: queue, LeaseToken: remaining.LeaseToken, ConsumerID: consumerTag, Action: broker.NackRequeue})
					ch.releasePrefetchReservation(consumerTag, 1)
				}
				return
			default:
			}
			if !autoAck {
				ch.reservationToInFlight(consumerTag)
			}
			delivery := ch.convertMessage(msg, queue, consumerTag, autoAck)
			if err := ch.sendDelivery(delivery); err != nil {
				if !autoAck {
					ch.unackedMu.Lock()
					delete(ch.unacked, delivery.DeliveryTag)
					ch.unackedMu.Unlock()
					ch.releasePrefetchInFlight(consumerTag)
				} else {
					ch.releasePrefetchReservation(consumerTag, 1)
				}
				_ = b.Store().Nack(context.Background(), broker.NackRequest{MessageID: msg.ID, Queue: queue, LeaseToken: msg.LeaseToken, ConsumerID: consumerTag, Action: broker.NackRequeue})
				if errors.Is(err, errChannelFlowInactive) {
					for _, remaining := range messages[i+1:] {
						_ = b.Store().Nack(context.Background(), broker.NackRequest{MessageID: remaining.ID, Queue: queue, LeaseToken: remaining.LeaseToken, ConsumerID: consumerTag, Action: broker.NackRequeue})
						ch.releasePrefetchReservation(consumerTag, 1)
					}
					break
				}
				for _, remaining := range messages[i+1:] {
					_ = b.Store().Nack(context.Background(), broker.NackRequest{MessageID: remaining.ID, Queue: queue, LeaseToken: remaining.LeaseToken, ConsumerID: consumerTag, Action: broker.NackRequeue})
					ch.releasePrefetchReservation(consumerTag, 1)
				}
				return
			}
			if autoAck {
				if err := b.Store().Ack(ctx, broker.AckRequest{MessageID: msg.ID, Queue: queue, LeaseToken: msg.LeaseToken, ConsumerID: consumerTag}); err != nil {
					log.Printf("[amqp] auto-ack failed for message %s: %v", msg.ID, err)
					_ = b.Store().Nack(context.Background(), broker.NackRequest{MessageID: msg.ID, Queue: queue, LeaseToken: msg.LeaseToken, ConsumerID: consumerTag, Action: broker.NackRequeue})
				}
				ch.releasePrefetchReservation(consumerTag, 1)
			}
		}

		if len(messages) == 0 {
			if !waitAMQP(ctx, 50*time.Millisecond) {
				return
			}
		}
	}
}

func waitAMQP(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (ch *Channel) convertMessage(msg broker.Message, queue, consumerTag string, autoAck bool) Delivery {
	deliveryTag := ch.deliverySeq.Add(1)

	if !autoAck {
		ch.unackedMu.Lock()
		ch.unacked[deliveryTag] = &pendingDelivery{
			deliveryTag: deliveryTag,
			messageID:   msg.ID,
			queue:       queue,
			leaseToken:  msg.LeaseToken,
			consumerTag: consumerTag,
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

func (ch *Channel) sendDelivery(delivery Delivery) error {
	ch.writeMu.Lock()
	defer ch.writeMu.Unlock()
	if ch.closed.Load() {
		return errChannelClosed
	}
	if !ch.flowActive.Load() {
		return errChannelFlowInactive
	}
	// Build properties
	props := delivery.Properties
	if props == nil {
		props = &Properties{}
	}

	// Encode properties
	propFlags, propBuf := ch.encodeProperties(props)

	// Send Basic.Deliver
	deliverArgs := EncodeMethodArgs(
		string(""),           // consumer-tag (empty for pushed deliveries)
		delivery.DeliveryTag, // delivery-tag
		delivery.Redeliver,   // redeliver
		delivery.Exchange,    // exchange
		delivery.RoutingKey,  // routing-key
	)

	if err := ch.codec().WriteMethodFrame(ch.id, ClassBasic, MethodBasicDeliver, deliverArgs); err != nil {
		return err
	}

	// Send content header
	if err := ch.codec().WriteHeaderFrame(ch.id, ClassBasic, uint64(len(delivery.Body)), propFlags, propBuf); err != nil {
		return err
	}

	// Send content body(s)
	body := delivery.Body
	for len(body) > 0 {
		end := len(body)
		if end > int(ch.conn.frameMax) {
			end = int(ch.conn.frameMax)
		}
		if err := ch.codec().WriteBodyFrame(ch.id, body[:end]); err != nil {
			return err
		}
		body = body[end:]
	}
	return nil
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
	noWait, _ := r.ReadBool()

	ch.consumersMu.Lock()
	consumer := ch.consumers[consumerTag]
	delete(ch.consumers, consumerTag)
	ch.consumersMu.Unlock()
	if consumer == nil {
		return ch.sendChannelError(ReplyNotFound, "consumer not found")
	}
	if consumer != nil && consumer.cancel != nil {
		consumer.cancel()
	}

	if noWait {
		return nil
	}
	return ch.writeMethodFrame(ClassBasic, MethodBasicCancelOk, EncodeMethodArgs(string(consumerTag)))
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

	// If in transaction mode, buffer the operation.
	if ch.txActive {
		ch.txBuffer = append(ch.txBuffer, &txOperation{
			kind:  "publish",
			topic: exchange,
			body:  body,
			opts:  opts,
		})
	} else if !ch.flowActive.Load() {
		err = fmt.Errorf("channel flow inactive")
		log.Printf("[amqp] publish paused by flow control on channel %d", ch.id)
	} else {
		_, err = ch.conn.server.cfg.Broker.Publisher().Publish(ch.conn.server.ctx, exchange, body, opts...)
		if err != nil {
			log.Printf("[amqp] publish error: %v", err)
		}
	}

	// If in confirm mode, send ack or nack
	if ch.confirmed.Load() {
		deliveryTag := ch.deliverySeq.Add(1)
		if err != nil {
			return ch.writeMethodFrame(ClassBasic, MethodBasicNack, EncodeMethodArgs(deliveryTag, false, false))
		}
		return ch.writeMethodFrame(ClassBasic, MethodBasicAck, EncodeMethodArgs(deliveryTag, false))
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
				if ch.txActive {
					ch.txBuffer = append(ch.txBuffer, &txOperation{kind: "ack", messageID: pending.messageID, queue: pending.queue, leaseToken: pending.leaseToken, consumerTag: pending.consumerTag})
					delete(ch.unacked, tag)
				} else {
					err := b.Store().Ack(ch.conn.server.ctx, broker.AckRequest{
						MessageID:  pending.messageID,
						Queue:      pending.queue,
						LeaseToken: pending.leaseToken,
						ConsumerID: "",
					})
					if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
						delete(ch.unacked, tag)
						ch.releasePrefetchInFlight(pending.consumerTag)
					} else {
						log.Printf("[amqp] ack failed for message %s: %v", pending.messageID, err)
					}
				}
			}
		}
	} else if pending, ok := ch.unacked[deliveryTag]; ok {
		if ch.txActive {
			ch.txBuffer = append(ch.txBuffer, &txOperation{kind: "ack", messageID: pending.messageID, queue: pending.queue, leaseToken: pending.leaseToken, consumerTag: pending.consumerTag})
			delete(ch.unacked, deliveryTag)
		} else {
			err := b.Store().Ack(ch.conn.server.ctx, broker.AckRequest{
				MessageID:  pending.messageID,
				Queue:      pending.queue,
				LeaseToken: pending.leaseToken,
				ConsumerID: "",
			})
			if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
				delete(ch.unacked, deliveryTag)
				ch.releasePrefetchInFlight(pending.consumerTag)
			} else {
				log.Printf("[amqp] ack failed for message %s: %v", pending.messageID, err)
			}
		}
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
				if ch.txActive {
					ch.txBuffer = append(ch.txBuffer, &txOperation{kind: "nack", messageID: pending.messageID, queue: pending.queue, leaseToken: pending.leaseToken, nackAction: nackAction, consumerTag: pending.consumerTag})
					delete(ch.unacked, tag)
				} else {
					err := b.Store().Nack(ch.conn.server.ctx, broker.NackRequest{
						MessageID:  pending.messageID,
						Queue:      pending.queue,
						LeaseToken: pending.leaseToken,
						ConsumerID: "",
						Action:     nackAction,
					})
					if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
						delete(ch.unacked, tag)
						ch.releasePrefetchInFlight(pending.consumerTag)
					} else {
						log.Printf("[amqp] nack failed for message %s: %v", pending.messageID, err)
					}
				}
			}
		}
	} else if pending, ok := ch.unacked[deliveryTag]; ok {
		nackAction := broker.NackRequeue
		if !requeue {
			nackAction = broker.NackReject
		}
		if ch.txActive {
			ch.txBuffer = append(ch.txBuffer, &txOperation{kind: "nack", messageID: pending.messageID, queue: pending.queue, leaseToken: pending.leaseToken, nackAction: nackAction, consumerTag: pending.consumerTag})
			delete(ch.unacked, deliveryTag)
		} else {
			err := b.Store().Nack(ch.conn.server.ctx, broker.NackRequest{
				MessageID:  pending.messageID,
				Queue:      pending.queue,
				LeaseToken: pending.leaseToken,
				ConsumerID: "",
				Action:     nackAction,
			})
			if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
				delete(ch.unacked, deliveryTag)
				ch.releasePrefetchInFlight(pending.consumerTag)
			} else {
				log.Printf("[amqp] nack failed for message %s: %v", pending.messageID, err)
			}
		}
	}
	ch.unackedMu.Unlock()

	return nil
}

func (ch *Channel) handleBasicRecover(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadBool() // requeue
	// Recovery is handled by the lease sweeper
	return ch.writeMethodFrame(ClassBasic, MethodBasicRecoverOk, nil)
}

func (ch *Channel) handleBasicGet(args []byte) error {
	r := &argsReader{data: args}
	_, _ = r.ReadUint16() // reserved
	queue, _ := r.ReadString()
	autoAck, _ := r.ReadBool()

	b := ch.conn.server.cfg.Broker

	// Try to get a single message
	consumerID := fmt.Sprintf("amqp-get-%d", ch.id)
	messages, err := b.Store().Lease(ch.conn.server.ctx, broker.LeaseRequest{
		Queue:      queue,
		ConsumerID: consumerID,
		Limit:      1,
	})
	if err != nil || len(messages) == 0 {
		// Send Basic.GetEmpty
		return ch.writeMethodFrame(ClassBasic, 72, EncodeMethodArgs(uint32(0)))
	}

	msg := messages[0]
	deliveryTag := ch.deliverySeq.Add(1)

	if !autoAck {
		ch.unackedMu.Lock()
		ch.unacked[deliveryTag] = &pendingDelivery{
			deliveryTag: deliveryTag,
			messageID:   msg.ID,
			queue:       queue,
			leaseToken:  msg.LeaseToken,
		}
		ch.unackedMu.Unlock()
	}

	// Send Basic.GetOk and the content frames as one channel write sequence.
	ch.writeMu.Lock()
	getOkArgs := EncodeMethodArgs(
		deliveryTag,
		msg.RoutingKey,
		msg.Topic,
		uint32(0), // message-count
	)
	if err := ch.codec().WriteMethodFrame(ch.id, ClassBasic, 71, getOkArgs); err != nil {
		ch.writeMu.Unlock()
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
		ch.writeMu.Unlock()
		return err
	}

	if err := ch.codec().WriteBodyFrame(ch.id, msg.Payload); err != nil {
		ch.writeMu.Unlock()
		return err
	}
	ch.writeMu.Unlock()
	if autoAck {
		return b.Store().Ack(ch.conn.server.ctx, broker.AckRequest{MessageID: msg.ID, Queue: queue, LeaseToken: msg.LeaseToken, ConsumerID: consumerID})
	}
	return nil
}

// handleConfirmMethod handles confirm methods.
func (ch *Channel) handleConfirmMethod(method *MethodFrame) error {
	switch method.MethodID {
	case MethodConfirmSelect:
		ch.confirmed.Store(true)
		var noWait bool
		if len(method.Args) > 0 {
			r := &argsReader{data: method.Args}
			noWait, _ = r.ReadBool()
		}
		if !noWait {
			return ch.writeMethodFrame(ClassConfirm, MethodConfirmSelectOk, nil)
		}
		return nil
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown confirm method %d", method.MethodID))
	}
}

// Confirmed returns whether publisher confirms are enabled on this channel.
func (ch *Channel) Confirmed() bool {
	return ch.confirmed.Load()
}

// SelectConfirm enables publisher confirms on this channel.
func (ch *Channel) SelectConfirm(noWait bool) error {
	ch.confirmed.Store(true)
	if !noWait {
		return ch.writeMethodFrame(ClassConfirm, MethodConfirmSelectOk, nil)
	}
	return nil
}

// handleTxMethod handles transaction methods.
func (ch *Channel) handleTxMethod(method *MethodFrame) error {
	switch method.MethodID {
	case MethodTxSelect:
		ch.txActive = true
		ch.txBuffer = nil
		return ch.writeMethodFrame(ClassTx, MethodTxSelectOk, nil)
	case MethodTxCommit:
		// Execute all buffered operations.
		for i, op := range ch.txBuffer {
			if err := ch.executeTxOp(op); err != nil {
				for _, pending := range ch.txBuffer[i:] {
					if pending.kind != "ack" && pending.kind != "nack" {
						continue
					}
					requeueErr := ch.conn.server.cfg.Broker.Store().Nack(context.Background(), broker.NackRequest{MessageID: pending.messageID, Queue: pending.queue, LeaseToken: pending.leaseToken, Action: broker.NackRequeue})
					if requeueErr == nil || errors.Is(requeueErr, broker.ErrLeaseLost) || errors.Is(requeueErr, broker.ErrMessageNotFound) {
						ch.releasePrefetchInFlight(pending.consumerTag)
					}
				}
				ch.txActive = false
				ch.txBuffer = nil
				return ch.sendChannelError(ReplyInternalError, err.Error())
			}
		}
		ch.txActive = false
		ch.txBuffer = nil
		return ch.writeMethodFrame(ClassTx, MethodTxCommitOk, nil)
	case MethodTxRollback:
		for _, pending := range ch.txBuffer {
			if pending.kind != "ack" && pending.kind != "nack" {
				continue
			}
			err := ch.conn.server.cfg.Broker.Store().Nack(context.Background(), broker.NackRequest{MessageID: pending.messageID, Queue: pending.queue, LeaseToken: pending.leaseToken, Action: broker.NackRequeue})
			if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
				ch.releasePrefetchInFlight(pending.consumerTag)
			}
		}
		ch.txActive = false
		ch.txBuffer = nil
		return ch.writeMethodFrame(ClassTx, MethodTxRollbackOk, nil)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown tx method %d", method.MethodID))
	}
}

func (ch *Channel) executeTxOp(op *txOperation) error {
	ctx := ch.conn.server.ctx
	switch op.kind {
	case "publish":
		_, err := ch.conn.server.cfg.Broker.Publisher().Publish(ctx, op.topic, op.body, op.opts...)
		return err
	case "ack":
		err := ch.conn.server.cfg.Broker.Store().Ack(ctx, broker.AckRequest{
			MessageID:  op.messageID,
			Queue:      op.queue,
			LeaseToken: op.leaseToken,
		})
		if err == nil {
			ch.releasePrefetchInFlight(op.consumerTag)
		}
		return err
	case "nack":
		err := ch.conn.server.cfg.Broker.Store().Nack(ctx, broker.NackRequest{
			MessageID:  op.messageID,
			Queue:      op.queue,
			LeaseToken: op.leaseToken,
			Action:     op.nackAction,
		})
		if err == nil {
			ch.releasePrefetchInFlight(op.consumerTag)
		}
		return err
	}
	return nil
}

// handleContentFrame handles content frames (header/body) for this channel.
func (ch *Channel) handleContentFrame(frame *Frame) {
	// Content frames are processed inline by readContent()
}

func (ch *Channel) sendChannelError(code uint16, text string) error {
	ch.close()
	args := EncodeMethodArgs(
		uint16(code),
		string(text),
		uint16(0), // class-id
		uint16(0), // method-id
	)
	return ch.writeMethodFrame(ClassChannel, MethodChannelClose, args)
}

func (ch *Channel) close() {
	ch.closeOnce.Do(func() {
		ch.closed.Store(true)
		ch.consumersMu.Lock()
		consumers := make([]*amqpConsumer, 0, len(ch.consumers))
		for _, consumer := range ch.consumers {
			consumers = append(consumers, consumer)
		}
		ch.consumers = make(map[string]*amqpConsumer)
		ch.consumersMu.Unlock()
		for _, consumer := range consumers {
			if consumer.cancel != nil {
				consumer.cancel()
			}
		}

		ch.unackedMu.Lock()
		pending := make([]*pendingDelivery, 0, len(ch.unacked))
		for _, delivery := range ch.unacked {
			pending = append(pending, delivery)
		}
		ch.unacked = make(map[uint64]*pendingDelivery)
		ch.unackedMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		store := ch.conn.server.cfg.Broker.Store()
		for _, delivery := range pending {
			err := store.Nack(ctx, broker.NackRequest{MessageID: delivery.messageID, Queue: delivery.queue, LeaseToken: delivery.leaseToken, Action: broker.NackRequeue})
			if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
				ch.releasePrefetchInFlight(delivery.consumerTag)
			} else {
				log.Printf("[amqp] requeue on channel close failed for %s: %v", delivery.messageID, err)
			}
		}
	})
}

func (ch *Channel) codec() *Codec {
	return ch.conn.codec
}

func (ch *Channel) writeMethodFrame(classID, methodID uint16, args []byte) error {
	ch.writeMu.Lock()
	defer ch.writeMu.Unlock()
	return ch.codec().WriteMethodFrame(ch.id, classID, methodID, args)
}

func randomHex(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = "0123456789abcdef"[time.Now().UnixNano()%16]
		time.Sleep(time.Nanosecond)
	}
	return string(b)
}
