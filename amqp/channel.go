package amqp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
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
	confirmSeq          atomic.Uint64
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
	delivery    Delivery
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
	tenantID    string
	autoAck     bool
	exclusive   bool
	noLocal     bool
	noWait      bool
	handler     func(Delivery)
	cancel      context.CancelFunc
}

var errChannelFlowInactive = errors.New("amqp: channel flow inactive")
var errChannelClosed = errors.New("amqp: channel closed")
var errContentTooLarge = errors.New("amqp: content body exceeds broker message size limit")

const amqpTimestampMetadataKey = "_amqp_timestamp_seconds"
const amqpHeadersMetadataKey = "_amqp_headers_table"
const amqpOtherPropertiesMetadataKey = "_amqp_other_properties"

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
	permission := amqpMethodPermission(method)
	if permission != "" && ch.conn.server.cfg.ACL != nil && !ch.conn.server.cfg.AllowInsecure &&
		!ch.conn.server.cfg.ACL.Authorize(ch.conn.principal, permission) {
		return ch.sendChannelError(ReplyAccessRefused, "access refused")
	}
	if ch.conn.server.cfg.ACL != nil && !ch.conn.server.cfg.AllowInsecure && amqpAutoAcks(method) &&
		!ch.conn.server.cfg.ACL.Authorize(ch.conn.principal, broker.PermissionMessageAck) {
		return ch.sendChannelError(ReplyAccessRefused, "automatic acknowledgement is not permitted")
	}
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

func amqpAutoAcks(method *MethodFrame) bool {
	if method == nil || method.ClassID != ClassBasic {
		return false
	}
	r := &argsReader{data: method.Args}
	switch method.MethodID {
	case MethodBasicConsume:
		_, _ = r.ReadUint16()
		_, _ = r.ReadString()
		_, _ = r.ReadString()
		_, _ = r.ReadBool()
		autoAck, _ := r.ReadBool()
		return autoAck
	case MethodBasicGet:
		_, _ = r.ReadUint16()
		_, _ = r.ReadString()
		autoAck, _ := r.ReadBool()
		return autoAck
	default:
		return false
	}
}

func amqpMethodPermission(method *MethodFrame) string {
	if method == nil {
		return ""
	}
	switch method.ClassID {
	case ClassExchange:
		switch method.MethodID {
		case MethodExchangeDeclare, MethodExchangeDelete, MethodExchangeBind, MethodExchangeUnbind:
			return broker.PermissionTopicWrite
		}
	case ClassQueue:
		switch method.MethodID {
		case MethodQueueDeclare, MethodQueueBind, MethodQueueUnbind:
			return broker.PermissionQueueWrite
		case MethodQueuePurge:
			return broker.PermissionQueuePurge
		case MethodQueueDelete:
			return broker.PermissionQueueControl
		}
	case ClassBasic:
		switch method.MethodID {
		case MethodBasicConsume, MethodBasicGet:
			return broker.PermissionConsumerPoll
		case MethodBasicPublish:
			return broker.PermissionPublish
		case MethodBasicAck:
			return broker.PermissionMessageAck
		case MethodBasicReject, MethodBasicNack:
			return broker.PermissionMessageNack
		case MethodBasicRecover, MethodBasicRecoverAsync:
			return broker.PermissionMessageNack
		}
	}
	return ""
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
	return ch.writeMethodFrame(ClassChannel, MethodChannelOpenOk, EncodeMethodArgs([]byte{}))
}

func (ch *Channel) handleChannelClose(args []byte) error {
	ch.close()
	err := ch.writeMethodFrame(ClassChannel, MethodChannelCloseOk, nil)
	ch.conn.removeChannel(ch.id)
	return err
}

func (ch *Channel) handleChannelFlow(args []byte) error {
	r := &argsReader{data: args}
	active, err := r.ReadBool()
	if err != nil || r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid channel.flow flags")
	}
	ch.flowActive.Store(active)
	return ch.writeMethodFrame(ClassChannel, MethodChannelFlowOk, EncodeMethodArgs(active))
}

func (ch *Channel) handleChannelFlowOk(args []byte) error {
	r := &argsReader{data: args}
	active, err := r.ReadBool()
	if err != nil || r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid channel.flow-ok flags")
	}
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
		return ch.handleExchangeBind(method.Args, false)
	case MethodExchangeUnbind:
		return ch.handleExchangeBind(method.Args, true)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown exchange method %d", method.MethodID))
	}
}

func (ch *Channel) handleExchangeDeclare(args []byte) error {
	ch.conn.server.exchangeOpsMu.Lock()
	defer ch.conn.server.exchangeOpsMu.Unlock()
	r := &argsReader{data: args}
	if _, err := r.ReadUint16(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.declare ticket")
	}
	exchange, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.declare name")
	}
	exchangeType, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.declare type")
	}
	passive, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.declare flags")
	}
	durable, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.declare flags")
	}
	autoDelete, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.declare flags")
	}
	internal, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.declare flags")
	}
	noWait, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.declare flags")
	}
	arguments, err := r.ReadTable()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange arguments")
	}
	if passive {
		existing, ok := ch.conn.server.exchangeType(exchange)
		if !ok {
			return ch.sendChannelError(ReplyNotFound, "exchange does not exist")
		}
		if existing != exchangeType {
			return ch.sendChannelError(ReplyPreconditionFailed, "exchange redeclared with a different type")
		}
		if !noWait {
			return ch.writeMethodFrame(ClassExchange, MethodExchangeDeclareOk, nil)
		}
		return nil
	}
	if autoDelete || internal || len(arguments) > 0 {
		return ch.sendChannelError(ReplyNotImplemented, "auto-delete, internal, and argument-based exchanges are not supported")
	}
	if exchange == "" {
		return ch.sendChannelError(ReplyPreconditionFailed, "the default exchange cannot be declared")
	}
	switch exchangeType {
	case "direct", "topic", "fanout", "headers":
	default:
		return ch.sendChannelError(ReplyPreconditionFailed, "unsupported exchange type")
	}
	if existing, ok := ch.conn.server.exchangeRecord(exchange); ok && (existing.Type != exchangeType || existing.Durable != durable) {
		return ch.sendChannelError(ReplyPreconditionFailed, "exchange redeclared with different properties")
	}

	// Broker topics provide the durable namespace used by exchange publishes.
	err = ch.conn.server.cfg.Broker.DeclareTopic(ch.conn.requestContext(), broker.TopicConfig{Name: exchange})
	if err != nil {
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}
	if err := ch.conn.server.declareExchange(ch.conn.requestContext(), broker.AMQPExchangeRecord{
		Name: exchange, Type: exchangeType, Durable: durable,
	}); err != nil {
		if errors.Is(err, broker.ErrUnsupportedMode) {
			return ch.sendChannelError(ReplyNotImplemented, "durable exchanges require a persistent AMQP exchange store")
		}
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}

	if !noWait {
		return ch.writeMethodFrame(ClassExchange, MethodExchangeDeclareOk, nil)
	}
	return nil
}

func (ch *Channel) handleExchangeDelete(args []byte) error {
	ch.conn.server.exchangeOpsMu.Lock()
	defer ch.conn.server.exchangeOpsMu.Unlock()
	r := &argsReader{data: args}
	if _, err := r.ReadUint16(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.delete ticket")
	}
	exchange, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.delete name")
	}
	ifUnused, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.delete flags")
	}
	noWait, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange.delete flags")
	}

	if ifUnused {
		for _, route := range ch.conn.server.cfg.Broker.Router().Routes() {
			if amqpRouteReferencesExchange(route.Name, exchange) {
				return ch.sendChannelError(ReplyPreconditionFailed, "exchange is in use")
			}
		}
	}
	if exchange == "" || exchange == "amq.direct" || exchange == "amq.topic" || exchange == "amq.fanout" || exchange == "amq.match" || exchange == "amq.headers" {
		return ch.sendChannelError(ReplyPreconditionFailed, "the default and built-in exchanges cannot be deleted")
	}
	if _, ok := ch.conn.server.exchangeType(exchange); !ok {
		return ch.sendChannelError(ReplyNotFound, "exchange does not exist")
	}
	if err := ch.conn.server.deleteExchange(ch.conn.requestContext(), exchange); err != nil {
		if errors.Is(err, broker.ErrTopicNotFound) {
			return ch.sendChannelError(ReplyNotFound, "exchange does not exist")
		}
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}
	ch.conn.server.cfg.Broker.Router().RemovePrefix(amqpExchangeRoutePrefix(exchange))
	for _, route := range ch.conn.server.cfg.Broker.Router().Routes() {
		if amqpRouteReferencesExchange(route.Name, exchange) && strings.HasPrefix(route.Name, "amqp.xbind.") {
			ch.conn.server.cfg.Broker.Router().Remove(route.Name)
		}
	}
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
		return ch.handleQueueUnbind(method.Args)
	case MethodQueuePurge:
		return ch.handleQueuePurge(method.Args)
	case MethodQueueDelete:
		return ch.handleQueueDelete(method.Args)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown queue method %d", method.MethodID))
	}
}

func (ch *Channel) handleQueueDeclare(args []byte) error {
	ch.conn.server.exchangeOpsMu.Lock()
	defer ch.conn.server.exchangeOpsMu.Unlock()
	r := &argsReader{data: args}
	if _, err := r.ReadUint16(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.declare ticket")
	}
	queue, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.declare name")
	}
	passive, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.declare flags")
	}
	durable, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.declare flags")
	}
	exclusive, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.declare flags")
	}
	autoDelete, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.declare flags")
	}
	noWait, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.declare flags")
	}
	if _, err := r.ReadTable(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.declare arguments")
	}
	if exclusive || autoDelete {
		return ch.sendChannelError(ReplyNotImplemented, "exclusive and auto-delete queues are not supported")
	}
	if durable && !amqpStoreIsDurable(ch.conn.server.cfg.Broker.Store()) {
		return ch.sendChannelError(ReplyPreconditionFailed, "durable queue requires a persistent or replicated store")
	}
	if passive && queue == "" {
		return ch.sendChannelError(ReplyNotFound, "passive queue declaration requires a queue name")
	}
	if noWait && queue == "" {
		return ch.sendChannelError(ReplyPreconditionFailed, "no-wait declaration cannot request a generated queue name")
	}

	queueName := queue
	if queueName == "" && !passive {
		randomName, err := randomHex(8)
		if err != nil {
			return ch.sendChannelError(ReplyInternalError, "could not generate queue name")
		}
		queueName = fmt.Sprintf("amq.gen-%s", randomName)
	}

	ctx := ch.conn.requestContext()
	if passive {
		_, err = ch.conn.server.cfg.Broker.Store().ListQueue(ctx, queueName, 1)
		if errors.Is(err, broker.ErrQueueNotFound) {
			return ch.sendChannelError(ReplyNotFound, "queue does not exist")
		}
	} else {
		if lookup, ok := ch.conn.server.cfg.Broker.Store().(broker.QueueConfigLookup); ok {
			existing, found, lookupErr := lookup.LookupQueueConfig(ctx, queueName)
			if lookupErr != nil {
				return ch.sendChannelError(ReplyInternalError, lookupErr.Error())
			}
			if found && existing.Durable != durable {
				return ch.sendChannelError(ReplyPreconditionFailed, "queue redeclared with a different durability flag")
			}
		} else if durable {
			return ch.sendChannelError(ReplyNotImplemented, "durable queues require queue configuration lookup support")
		}
		err = ch.conn.server.cfg.Broker.DeclareQueue(ctx, broker.QueueConfig{
			Name:              queueName,
			Durable:           durable,
			AckMode:           broker.AckManual,
			VisibilityTimeout: 30 * time.Second,
			RetryPolicy:       broker.DefaultRetryPolicy(),
		})
	}
	if err != nil {
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}
	if noWait {
		return nil
	}

	// Send Queue.DeclareOk
	messageCount := 0
	if counter, ok := ch.conn.server.cfg.Broker.Store().(broker.QueueReadyCounter); ok {
		messageCount, err = counter.QueueReadyCount(ch.conn.requestContext(), queueName)
	} else {
		var stats broker.StoreStats
		stats, err = ch.conn.server.cfg.Broker.Store().Stats(ch.conn.requestContext())
		messageCount = stats.Queues[queueName]
	}
	if err != nil {
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}
	consumerCount := ch.conn.server.queueConsumerCount(ch.conn.requestContext(), queueName)
	args2 := EncodeMethodArgs(
		string(queueName),              // queue
		amqpCountUint32(messageCount),  // message-count
		amqpCountUint32(consumerCount), // consumer-count
	)
	return ch.writeMethodFrame(ClassQueue, MethodQueueDeclareOk, args2)
}

func (ch *Channel) handleQueueBind(args []byte) error {
	ch.conn.server.exchangeOpsMu.Lock()
	defer ch.conn.server.exchangeOpsMu.Unlock()
	r := &argsReader{data: args}
	if _, err := r.ReadUint16(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.bind ticket")
	}
	queue, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.bind queue")
	}
	exchange, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.bind exchange")
	}
	routingKey, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.bind routing key")
	}
	noWait, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.bind flags")
	}
	arguments, err := r.ReadTable()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.bind arguments")
	}
	if exchange == "" {
		return ch.sendChannelError(ReplyAccessRefused, "the default exchange does not support explicit queue bindings")
	}
	ctx := ch.conn.requestContext()
	lookup, ok := ch.conn.server.cfg.Broker.Store().(broker.QueueConfigLookup)
	if !ok {
		return ch.sendChannelError(ReplyNotImplemented, "queue binding requires queue configuration lookup support")
	}
	queueConfig, found, err := lookup.LookupQueueConfig(ctx, queue)
	if err != nil {
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}
	if !found {
		return ch.sendChannelError(ReplyNotFound, "queue does not exist")
	}

	if exchange != "" {
		exchangeRecord, ok := ch.conn.server.exchangeRecord(exchange)
		if !ok {
			return ch.sendChannelError(ReplyNotFound, "exchange does not exist")
		}
		headerJSON, headersMatchAll, err := amqpBindingHeaders(exchangeRecord.Type, arguments)
		if err != nil {
			return ch.sendChannelError(ReplyPreconditionFailed, err.Error())
		}
		if exchangeRecord.Type == "headers" {
			routingKey = ""
		}
		tenantID, err := ch.amqpTenantID()
		if err != nil {
			return ch.sendChannelError(ReplyAccessRefused, err.Error())
		}
		routeName := amqpBindingRouteNameWithHeaders(exchange, queue, routingKey, tenantID, headerJSON, headersMatchAll)
		b := ch.conn.server.cfg.Broker
		binding := broker.AMQPExchangeBindingRecord{
			ID: routeName, Source: exchange, Destination: queue,
			DestinationKind: broker.DestinationQueue, RoutingKey: routingKey, TenantID: tenantID,
			HeadersJSON: headerJSON, HeadersMatchAll: headersMatchAll,
		}
		if exchangeRecord.Durable && queueConfig.Durable {
			store, ok := b.StateStore().(broker.AMQPExchangeBindingStore)
			if !ok {
				return ch.sendChannelError(ReplyNotImplemented, "durable queue bindings require a persistent binding store")
			}
			if err := store.PutAMQPExchangeBinding(ctx, binding); err != nil {
				return ch.sendChannelError(ReplyInternalError, err.Error())
			}
		}
		b.RemoveRoute(routeName)
		addAMQPExchangeBindingRoute(b.Router(), exchangeRecord.Type, binding)
	}
	if noWait {
		return nil
	}

	return ch.writeMethodFrame(ClassQueue, MethodQueueBindOk, nil)
}

func (ch *Channel) handleQueueUnbind(args []byte) error {
	ch.conn.server.exchangeOpsMu.Lock()
	defer ch.conn.server.exchangeOpsMu.Unlock()
	r := &argsReader{data: args}
	if _, err := r.ReadUint16(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.unbind ticket")
	}
	queue, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.unbind queue")
	}
	exchange, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.unbind exchange")
	}
	routingKey, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.unbind routing key")
	}
	arguments, err := r.ReadTable()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.unbind arguments")
	}
	if exchange == "" {
		return ch.sendChannelError(ReplyAccessRefused, "the default exchange does not support explicit queue bindings")
	}
	if exchange != "" {
		exchangeRecord, ok := ch.conn.server.exchangeRecord(exchange)
		if !ok {
			return ch.sendChannelError(ReplyNotFound, "exchange does not exist")
		}
		lookup, ok := ch.conn.server.cfg.Broker.Store().(broker.QueueConfigLookup)
		if !ok {
			return ch.sendChannelError(ReplyNotImplemented, "queue unbinding requires queue configuration lookup support")
		}
		queueConfig, found, err := lookup.LookupQueueConfig(ch.conn.requestContext(), queue)
		if err != nil {
			return ch.sendChannelError(ReplyInternalError, err.Error())
		}
		if !found {
			return ch.sendChannelError(ReplyNotFound, "queue does not exist")
		}
		headerJSON, headersMatchAll, err := amqpBindingHeaders(exchangeRecord.Type, arguments)
		if err != nil {
			return ch.sendChannelError(ReplyPreconditionFailed, err.Error())
		}
		if exchangeRecord.Type == "headers" {
			routingKey = ""
		}
		tenantID, err := ch.amqpTenantID()
		if err != nil {
			return ch.sendChannelError(ReplyAccessRefused, err.Error())
		}
		name := amqpBindingRouteNameWithHeaders(exchange, queue, routingKey, tenantID, headerJSON, headersMatchAll)
		router := ch.conn.server.cfg.Broker.Router()
		if !router.Remove(name) {
			return ch.sendChannelError(ReplyNotFound, "queue binding does not exist")
		}
		if exchangeRecord.Durable && queueConfig.Durable {
			store, ok := ch.conn.server.cfg.Broker.StateStore().(broker.AMQPExchangeBindingStore)
			if !ok {
				addAMQPExchangeBindingRoute(router, exchangeRecord.Type, broker.AMQPExchangeBindingRecord{
					ID: name, Source: exchange, Destination: queue, DestinationKind: broker.DestinationQueue,
					RoutingKey: routingKey, TenantID: tenantID, HeadersJSON: headerJSON, HeadersMatchAll: headersMatchAll,
				})
				return ch.sendChannelError(ReplyNotImplemented, "durable queue bindings require a persistent binding store")
			}
			if err := store.DeleteAMQPExchangeBinding(ch.conn.requestContext(), name); err != nil {
				addAMQPExchangeBindingRoute(router, exchangeRecord.Type, broker.AMQPExchangeBindingRecord{
					ID: name, Source: exchange, Destination: queue, DestinationKind: broker.DestinationQueue,
					RoutingKey: routingKey, TenantID: tenantID, HeadersJSON: headerJSON, HeadersMatchAll: headersMatchAll,
				})
				return ch.sendChannelError(ReplyInternalError, err.Error())
			}
		}
	}
	return ch.writeMethodFrame(ClassQueue, MethodQueueUnbindOk, nil)
}

func amqpBindingRouteName(exchange, queue, routingKey, tenantID string) string {
	return amqpBindingRouteNameWithHeaders(exchange, queue, routingKey, tenantID, "[]", true)
}

func amqpBindingRouteNameWithHeaders(exchange, queue, routingKey, tenantID, headersJSON string, matchAll bool) string {
	identity := amqpBindingIdentity(exchange, queue, routingKey, tenantID, headersJSON, matchAll)
	sum := sha256.Sum256(identity)
	return amqpExchangeRoutePrefix(exchange) + hex.EncodeToString(sum[:16])
}

func amqpExchangeRoutePrefix(exchange string) string {
	sum := sha256.Sum256([]byte(exchange))
	return "amqp.binding." + hex.EncodeToString(sum[:8]) + "."
}

func (ch *Channel) handleExchangeBind(args []byte, unbind bool) error {
	ch.conn.server.exchangeOpsMu.Lock()
	defer ch.conn.server.exchangeOpsMu.Unlock()
	r := &argsReader{data: args}
	if _, err := r.ReadUint16(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange binding ticket")
	}
	destination, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid destination exchange")
	}
	source, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid source exchange")
	}
	routingKey, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange binding key")
	}
	noWait, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange binding flags")
	}
	arguments, err := r.ReadTable()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid exchange binding arguments")
	}
	if destination == "" || source == "" {
		return ch.sendChannelError(ReplyPreconditionFailed, "default exchange cannot participate in exchange bindings")
	}
	sourceRecord, ok := ch.conn.server.exchangeRecord(source)
	if !ok {
		return ch.sendChannelError(ReplyNotFound, "source exchange does not exist")
	}
	destinationRecord, ok := ch.conn.server.exchangeRecord(destination)
	if !ok {
		return ch.sendChannelError(ReplyNotFound, "destination exchange does not exist")
	}
	headerJSON, headersMatchAll, err := amqpBindingHeaders(sourceRecord.Type, arguments)
	if err != nil {
		return ch.sendChannelError(ReplyPreconditionFailed, err.Error())
	}
	if sourceRecord.Type == "headers" {
		routingKey = ""
	}
	tenantID, err := ch.amqpTenantID()
	if err != nil {
		return ch.sendChannelError(ReplyAccessRefused, err.Error())
	}
	name := amqpExchangeLinkRouteNameWithHeaders(source, destination, routingKey, tenantID, headerJSON, headersMatchAll)
	router := ch.conn.server.cfg.Broker.Router()
	binding := broker.AMQPExchangeBindingRecord{
		ID: name, Source: source, Destination: destination, DestinationKind: broker.DestinationExchange,
		RoutingKey: routingKey, TenantID: tenantID, HeadersJSON: headerJSON, HeadersMatchAll: headersMatchAll,
	}
	durable := sourceRecord.Durable && destinationRecord.Durable
	if unbind {
		if !router.Remove(name) {
			return ch.sendChannelError(ReplyNotFound, "exchange binding does not exist")
		}
		if durable {
			store, ok := ch.conn.server.cfg.Broker.StateStore().(broker.AMQPExchangeBindingStore)
			if !ok {
				addAMQPExchangeLinkRoute(router, sourceRecord.Type, binding)
				return ch.sendChannelError(ReplyNotImplemented, "durable exchange bindings require a persistent binding store")
			}
			if err := store.DeleteAMQPExchangeBinding(ch.conn.requestContext(), name); err != nil {
				addAMQPExchangeLinkRoute(router, sourceRecord.Type, binding)
				return ch.sendChannelError(ReplyInternalError, err.Error())
			}
		}
		if noWait {
			return nil
		}
		return ch.writeMethodFrame(ClassExchange, MethodExchangeUnbindOk, nil)
	}
	if durable {
		store, ok := ch.conn.server.cfg.Broker.StateStore().(broker.AMQPExchangeBindingStore)
		if !ok {
			return ch.sendChannelError(ReplyNotImplemented, "durable exchange bindings require a persistent binding store")
		}
		if err := store.PutAMQPExchangeBinding(ch.conn.requestContext(), binding); err != nil {
			return ch.sendChannelError(ReplyInternalError, err.Error())
		}
	}
	router.Remove(name)
	addAMQPExchangeLinkRoute(router, sourceRecord.Type, binding)
	if noWait {
		return nil
	}
	return ch.writeMethodFrame(ClassExchange, MethodExchangeBindOk, nil)
}

func addAMQPExchangeLinkRoute(router *broker.Router, sourceType string, binding broker.AMQPExchangeBindingRecord) {
	if binding.DestinationKind == "" {
		binding.DestinationKind = broker.DestinationExchange
	}
	addAMQPExchangeBindingRoute(router, sourceType, binding)
}

func addAMQPExchangeBindingRoute(router *broker.Router, sourceType string, binding broker.AMQPExchangeBindingRecord) {
	router.Add(broker.Route{
		Name:      binding.ID,
		Mode:      broker.RouteMode(sourceType),
		TenantID:  binding.TenantID,
		Condition: amqpExchangeCondition(sourceType, binding.Source, binding.RoutingKey, binding.HeadersJSON, binding.HeadersMatchAll),
		To:        []broker.Destination{{Kind: binding.DestinationKind, Name: binding.Destination}},
		Enabled:   true,
	})
}

func amqpExchangeCondition(exchangeType, exchange, routingKey, headersJSON string, matchAll bool) func(broker.Message) bool {
	var headerMatches []broker.AMQPHeaderMatch
	if headersJSON != "" {
		_ = json.Unmarshal([]byte(headersJSON), &headerMatches)
	}
	return func(message broker.Message) bool {
		if message.Topic != exchange {
			return false
		}
		switch exchangeType {
		case "headers":
			return amqpHeadersMatch(headerMatches, matchAll, message.Headers)
		case "direct":
			return message.RoutingKey == routingKey
		case "fanout":
			return true
		default:
			return broker.TopicMatch(routingKey, message.RoutingKey)
		}
	}
}

func amqpHeadersMatch(criteria []broker.AMQPHeaderMatch, matchAll bool, messageHeaders map[string]string) bool {
	matched := 0
	for _, criterion := range criteria {
		value, exists := messageHeaders[criterion.Name]
		ok := exists && (!criterion.HasValue || value == criterion.Value)
		if matchAll && !ok {
			return false
		}
		if !matchAll && ok {
			matched++
			break
		}
	}
	if matchAll {
		return true
	}
	return matched > 0
}

func amqpBindingHeaders(exchangeType string, arguments table) (string, bool, error) {
	matchAll := true
	includeX := false
	if exchangeType != "headers" {
		for key := range arguments {
			if !strings.HasPrefix(key, "x-") {
				return "", false, fmt.Errorf("binding arguments are only supported for headers exchanges")
			}
		}
		return "[]", true, nil
	}
	if mode, ok := arguments["x-match"]; ok {
		value, ok := mode.(string)
		if !ok {
			return "", false, fmt.Errorf("headers exchange x-match must be a string")
		}
		switch value {
		case "all":
			matchAll = true
		case "any":
			matchAll = false
		case "all-with-x":
			matchAll, includeX = true, true
		case "any-with-x":
			matchAll, includeX = false, true
		default:
			return "", false, fmt.Errorf("headers exchange x-match must be all, any, all-with-x, or any-with-x")
		}
	}
	keys := make([]string, 0, len(arguments))
	for key := range arguments {
		if key == "x-match" || (strings.HasPrefix(key, "x-") && !includeX) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	criteria := make([]broker.AMQPHeaderMatch, 0, len(keys))
	for _, key := range keys {
		value := arguments[key]
		criterion := broker.AMQPHeaderMatch{Name: key}
		if value != nil {
			text, ok := amqpHeaderValueString(value)
			if !ok {
				return "", false, fmt.Errorf("headers exchange binding value for %q is not a supported scalar", key)
			}
			criterion.Value, criterion.HasValue = text, true
		}
		criteria = append(criteria, criterion)
	}
	encoded, err := json.Marshal(criteria)
	if err != nil {
		return "", false, err
	}
	return string(encoded), matchAll, nil
}

func amqpHeaderValueString(value any) (string, bool) {
	switch value.(type) {
	case table, []any:
		return "", false
	case []byte:
		return string(value.([]byte)), true
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return fmt.Sprint(value), true
	default:
		return "", false
	}
}

func amqpExchangeLinkRouteName(source, destination, routingKey, tenantID string) string {
	return amqpExchangeLinkRouteNameWithHeaders(source, destination, routingKey, tenantID, "[]", true)
}

func amqpExchangeLinkRouteNameWithHeaders(source, destination, routingKey, tenantID, headersJSON string, matchAll bool) string {
	sourceHash := sha256.Sum256([]byte(source))
	destinationHash := sha256.Sum256([]byte(destination))
	identityHash := sha256.Sum256(amqpBindingIdentity(source, destination, routingKey, tenantID, headersJSON, matchAll))
	return "amqp.xbind." + hex.EncodeToString(sourceHash[:8]) + "." + hex.EncodeToString(destinationHash[:8]) + "." + hex.EncodeToString(identityHash[:16])
}

func amqpBindingIdentity(source, destination, routingKey, tenantID, headersJSON string, matchAll bool) []byte {
	if headersJSON == "" || headersJSON == "[]" {
		if matchAll {
			return []byte(source + "\x00" + destination + "\x00" + routingKey + "\x00" + tenantID)
		}
	}
	return []byte(source + "\x00" + destination + "\x00" + routingKey + "\x00" + tenantID + "\x00" + headersJSON + "\x00" + fmt.Sprint(matchAll))
}

func amqpRouteReferencesExchange(routeName, exchange string) bool {
	exchangeHash := sha256.Sum256([]byte(exchange))
	hash := hex.EncodeToString(exchangeHash[:8])
	parts := strings.Split(routeName, ".")
	if len(parts) >= 3 && parts[0] == "amqp" && parts[1] == "binding" && parts[2] == hash {
		return true
	}
	return len(parts) >= 4 && parts[0] == "amqp" && parts[1] == "xbind" && (parts[2] == hash || parts[3] == hash)
}

func (ch *Channel) amqpTenantID() (string, error) {
	tenants := ch.conn.principal.Tenants
	if len(tenants) == 0 {
		return "", nil
	}
	for _, tenant := range tenants {
		if tenant == "*" {
			return "", nil
		}
	}
	if len(tenants) == 1 {
		return tenants[0], nil
	}
	return "", broker.ErrUnauthorized
}

func (ch *Channel) handleQueuePurge(args []byte) error {
	r := &argsReader{data: args}
	if _, err := r.ReadUint16(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.purge ticket")
	}
	queue, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.purge queue")
	}
	noWait, err := r.ReadBool()
	if err != nil || r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid queue.purge flags")
	}

	n, err := ch.conn.server.cfg.Broker.PurgeQueue(ch.conn.requestContext(), queue)
	if errors.Is(err, broker.ErrQueueNotFound) {
		return ch.sendChannelError(ReplyNotFound, "queue does not exist")
	}
	if err != nil {
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}

	if noWait {
		return nil
	}
	return ch.writeMethodFrame(ClassQueue, MethodQueuePurgeOk, EncodeMethodArgs(amqpCountUint32(n)))
}

func (ch *Channel) handleQueueDelete(args []byte) error {
	return ch.sendChannelError(ReplyNotImplemented, "queue deletion is not supported")
}

func amqpStoreIsDurable(store broker.Store) bool {
	reporter, ok := store.(broker.ConfirmationStore)
	if !ok {
		return false
	}
	switch reporter.ConfirmationLevel() {
	case broker.ConfirmPersisted, broker.ConfirmReplicated, broker.ConfirmCommitted, broker.ConfirmDelivered, broker.ConfirmProcessed:
		return true
	default:
		return false
	}
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
		return ch.handleBasicRecover(method.Args, false)
	case MethodBasicRecoverAsync:
		return ch.handleBasicRecover(method.Args, true)
	case MethodBasicGet:
		return ch.handleBasicGet(method.Args)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown basic method %d", method.MethodID))
	}
}

func (ch *Channel) handleBasicQos(args []byte) error {
	r := &argsReader{data: args}
	prefetchSize, err := r.ReadUint32()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.qos prefetch size")
	}
	prefetchCount, err := r.ReadUint16()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.qos prefetch count")
	}
	global, err := r.ReadBool()
	if err != nil || r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.qos flags")
	}
	ch.prefetchSize = prefetchSize
	ch.prefetchCount.Store(uint32(prefetchCount))
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
	if _, err := r.ReadUint16(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.consume ticket")
	}
	queue, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.consume queue")
	}
	consumerTag, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.consume consumer tag")
	}
	noLocal, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.consume flags")
	}
	autoAck, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.consume flags")
	}
	exclusive, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.consume flags")
	}
	noWait, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.consume flags")
	}
	if _, err := r.ReadTable(); err != nil || r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.consume arguments")
	}

	_ = noLocal
	_ = exclusive
	_ = noWait

	if consumerTag == "" {
		randomTag, err := randomHex(8)
		if err != nil {
			return ch.sendChannelError(ReplyInternalError, "could not generate consumer tag")
		}
		consumerTag = fmt.Sprintf("amq.ctag-%s", randomTag)
	}
	queueExists, err := amqpQueueExists(ch.conn.requestContext(), ch.conn.server.cfg.Broker.Store(), queue)
	if err != nil {
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}
	if !queueExists {
		return ch.sendChannelError(ReplyNotFound, "queue does not exist")
	}
	tenantID, err := broker.ResolveTenant(ch.conn.requestContext(), "")
	if err != nil {
		return ch.sendChannelError(ReplyAccessRefused, "AMQP consumer must resolve to one tenant")
	}

	// Create internal consumer
	consumer := &amqpConsumer{
		consumerTag: consumerTag,
		queue:       queue,
		tenantID:    tenantID,
		autoAck:     autoAck,
		exclusive:   exclusive,
		noLocal:     noLocal,
		noWait:      noWait,
	}
	consumerCtx, cancel := context.WithCancel(ch.conn.requestContext())
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
					_ = b.Store().Nack(ch.conn.requestContext(), broker.NackRequest{MessageID: remaining.ID, Queue: queue, LeaseToken: remaining.LeaseToken, ConsumerID: consumerTag, Action: broker.NackRequeue})
					ch.releasePrefetchReservation(consumerTag, 1)
				}
				return
			default:
			}
			if !autoAck {
				ch.reservationToInFlight(consumerTag)
			}
			delivery := ch.convertMessage(msg, queue, consumerTag, autoAck)
			if err := ch.sendDelivery(delivery, consumerTag); err != nil {
				if !autoAck {
					ch.unackedMu.Lock()
					delete(ch.unacked, delivery.DeliveryTag)
					ch.unackedMu.Unlock()
					ch.releasePrefetchInFlight(consumerTag)
				} else {
					ch.releasePrefetchReservation(consumerTag, 1)
				}
				_ = b.Store().Nack(ch.conn.requestContext(), broker.NackRequest{MessageID: msg.ID, Queue: queue, LeaseToken: msg.LeaseToken, ConsumerID: consumerTag, Action: broker.NackRequeue})
				if errors.Is(err, errChannelFlowInactive) {
					for _, remaining := range messages[i+1:] {
						_ = b.Store().Nack(ch.conn.requestContext(), broker.NackRequest{MessageID: remaining.ID, Queue: queue, LeaseToken: remaining.LeaseToken, ConsumerID: consumerTag, Action: broker.NackRequeue})
						ch.releasePrefetchReservation(consumerTag, 1)
					}
					break
				}
				for _, remaining := range messages[i+1:] {
					_ = b.Store().Nack(ch.conn.requestContext(), broker.NackRequest{MessageID: remaining.ID, Queue: queue, LeaseToken: remaining.LeaseToken, ConsumerID: consumerTag, Action: broker.NackRequeue})
					ch.releasePrefetchReservation(consumerTag, 1)
				}
				return
			}
			if autoAck {
				if err := b.Store().Ack(ctx, broker.AckRequest{MessageID: msg.ID, Queue: queue, LeaseToken: msg.LeaseToken, ConsumerID: consumerTag}); err != nil {
					log.Printf("[amqp] auto-ack failed for message %s: %v", msg.ID, err)
					_ = b.Store().Nack(ch.conn.requestContext(), broker.NackRequest{MessageID: msg.ID, Queue: queue, LeaseToken: msg.LeaseToken, ConsumerID: consumerTag, Action: broker.NackRequeue})
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
	headers, hasHeaders := amqpHeadersFromMetadata(msg.Metadata, msg.Headers)

	delivery := Delivery{
		DeliveryTag: deliveryTag,
		Redeliver:   msg.Redeliveries > 0,
		Exchange:    msg.Topic,
		RoutingKey:  msg.RoutingKey,
		Body:        msg.Payload,
		Headers:     headers,
		Properties: &Properties{
			ContentType:   msg.ContentType,
			Priority:      uint8(min(max(msg.Priority, 0), 9)),
			Headers:       headers,
			HasHeaders:    hasHeaders,
			CorrelationID: msg.CorrelationID,
			ReplyTo:       msg.ReplyTo,
			MessageID:     msg.ID,
			Type:          msg.Type,
		},
	}
	restoreAMQPPropertiesFromMetadata(msg.Metadata, delivery.Properties)
	if timestamp, ok := amqpTimestampFromMetadata(msg.Metadata); ok {
		delivery.Properties.Timestamp = timestamp
		delivery.Properties.HasTimestamp = true
	}
	if !autoAck {
		ch.unackedMu.Lock()
		ch.unacked[deliveryTag] = &pendingDelivery{
			deliveryTag: deliveryTag,
			messageID:   msg.ID,
			queue:       queue,
			leaseToken:  msg.LeaseToken,
			consumerTag: consumerTag,
			delivery:    delivery,
		}
		ch.unackedMu.Unlock()
	}
	return delivery
}

func (ch *Channel) sendDelivery(delivery Delivery, consumerTag string) error {
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
		consumerTag,
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
	if props.HasContentEncoding || props.ContentEncoding != "" {
		flags |= PropContentEncoding
		buf = appendShortString(buf, props.ContentEncoding)
	}
	if props.HasHeaders || len(props.Headers) > 0 {
		flags |= PropHeaders
		tableData := encodeTableToBytes(props.Headers)
		buf = append(buf, tableData...)
	}
	if props.HasDeliveryMode || props.DeliveryMode != 0 {
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
	if props.HasExpiration || props.Expiration != "" {
		flags |= PropExpiration
		buf = appendShortString(buf, props.Expiration)
	}
	if props.MessageID != "" {
		flags |= PropMessageID
		buf = appendShortString(buf, props.MessageID)
	}
	if props.HasTimestamp || props.Timestamp != 0 {
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
	if props.HasUserID || props.UserID != "" {
		flags |= PropUserID
		buf = appendShortString(buf, props.UserID)
	}
	if props.HasAppID || props.AppID != "" {
		flags |= PropAppID
		buf = appendShortString(buf, props.AppID)
	}
	if props.HasClusterID || props.ClusterID != "" {
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
	case nil:
		buf = append(buf, 'V')
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
	consumerTag, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.cancel consumer tag")
	}
	noWait, err := r.ReadBool()
	if err != nil || r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.cancel flags")
	}

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
	if _, err := r.ReadUint16(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.publish ticket")
	}
	exchange, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.publish exchange")
	}
	routingKey, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.publish routing key")
	}
	mandatory, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.publish flags")
	}
	immediate, err := r.ReadBool()
	if err != nil || r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.publish flags")
	}

	// Read content header and body
	props, body, err := ch.readContent()
	if err != nil {
		if errors.Is(err, errContentTooLarge) {
			return ch.sendChannelError(ReplyContentTooLarge, err.Error())
		}
		return err
	}
	if immediate {
		return ch.sendChannelError(ReplyNotImplemented, "basic.publish immediate is not supported")
	}
	if props.Priority > 9 {
		return ch.sendChannelError(ReplyPreconditionFailed, "message priority must be between 0 and 9")
	}
	expirationTTL, hasExpiration, err := amqpExpirationTTL(props)
	if err != nil {
		return ch.sendChannelError(ReplyPreconditionFailed, err.Error())
	}
	tenantID, err := ch.amqpTenantID()
	if err != nil {
		return ch.sendChannelError(ReplyAccessRefused, err.Error())
	}

	// Convert headers to map[string]string
	headers := make(map[string]string)
	if props.Headers != nil {
		for k, v := range props.Headers {
			if v == nil {
				headers[k] = ""
				continue
			}
			if value, ok := amqpHeaderValueString(v); ok {
				headers[k] = value
			}
		}
	}
	// Build the broker publish options. The exchange name is carried as the
	// broker topic so binding conditions can scope routing without exposing an
	// internal header to consumers.
	opts := []broker.PublishOption{
		broker.WithRoutingKey(routingKey),
		broker.WithContentType(props.ContentType),
		broker.WithCorrelationID(props.CorrelationID),
		broker.WithHeaders(headers),
		broker.WithPriority(int(props.Priority)),
		broker.WithRequireDestination(true),
	}
	if hasExpiration {
		opts = append(opts, broker.WithTTL(expirationTTL))
	}
	if metadata := amqpPropertiesMetadata(props); len(metadata) > 0 {
		opts = append(opts, broker.WithMetadata(metadata))
	}
	if tenantID != "" {
		opts = append(opts, broker.WithTenantID(tenantID))
	}
	if props.MessageID != "" {
		opts = append(opts, broker.WithMessageID(props.MessageID))
	}
	if props.Type != "" {
		opts = append(opts, broker.WithType(props.Type))
	}
	b := ch.conn.server.cfg.Broker
	ctx := ch.conn.requestContext()
	routable := false
	if exchange == "" {
		routable, err = amqpQueueExists(ctx, b.Store(), routingKey)
		if err != nil {
			return ch.sendChannelError(ReplyInternalError, err.Error())
		}
		if routable {
			opts = append(opts, broker.WithQueue(routingKey))
		}
	} else {
		if _, ok := ch.conn.server.exchangeType(exchange); !ok {
			return ch.sendChannelError(ReplyNotFound, "exchange does not exist")
		}
		routeMsg := broker.Message{Topic: exchange, RoutingKey: routingKey, TenantID: tenantID, Headers: headers}
		destinations, _ := b.Router().Route(ctx, routeMsg)
		for _, destination := range destinations {
			if destination.Kind == broker.DestinationQueue {
				routable = true
				break
			}
		}
	}
	if !ch.flowActive.Load() {
		log.Printf("[amqp] publish paused by flow control on channel %d", ch.id)
		if ch.confirmed.Load() {
			deliveryTag := ch.confirmSeq.Add(1)
			return ch.writeMethodFrame(ClassBasic, MethodBasicNack, EncodeMethodArgs(deliveryTag, false, false))
		}
		return nil
	}
	if !routable {
		if mandatory {
			if err := ch.sendBasicReturn(exchange, routingKey, props, body); err != nil {
				return err
			}
		}
		if ch.confirmed.Load() {
			deliveryTag := ch.confirmSeq.Add(1)
			return ch.writeMethodFrame(ClassBasic, MethodBasicAck, EncodeMethodArgs(deliveryTag, false))
		}
		return nil
	}

	// If in transaction mode, buffer the operation.
	if ch.txActive {
		ch.txBuffer = append(ch.txBuffer, &txOperation{
			kind:  "publish",
			topic: exchange,
			body:  body,
			opts:  opts,
		})
	} else {
		_, err = b.Publisher().Publish(ctx, exchange, body, opts...)
		if mandatory && (errors.Is(err, broker.ErrNoDestination) || errors.Is(err, broker.ErrQueueNotFound)) {
			err = ch.sendBasicReturn(exchange, routingKey, props, body)
		}
		if err != nil {
			log.Printf("[amqp] publish error: %v", err)
		}
	}

	// If in confirm mode, send ack or nack
	if ch.confirmed.Load() {
		deliveryTag := ch.confirmSeq.Add(1)
		if err != nil {
			return ch.writeMethodFrame(ClassBasic, MethodBasicNack, EncodeMethodArgs(deliveryTag, false, false))
		}
		return ch.writeMethodFrame(ClassBasic, MethodBasicAck, EncodeMethodArgs(deliveryTag, false))
	}

	return nil
}

// AMQP expiration is an unsigned decimal millisecond count. A zero value is
// represented by a one-nanosecond TTL so the broker's existing expiration
// check treats it as expired before the next lease.
func amqpExpirationTTL(props *Properties) (time.Duration, bool, error) {
	if !props.HasExpiration && props.Expiration == "" {
		return 0, false, nil
	}
	if props.Expiration == "" {
		return 0, false, nil
	}
	milliseconds, err := strconv.ParseUint(props.Expiration, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("invalid AMQP expiration %q", props.Expiration)
	}
	const maxMilliseconds = uint64((1<<63 - 1) / int64(time.Millisecond))
	if milliseconds > maxMilliseconds {
		return 0, false, fmt.Errorf("AMQP expiration %q exceeds the supported range", props.Expiration)
	}
	if milliseconds == 0 {
		return time.Nanosecond, true, nil
	}
	return time.Duration(milliseconds) * time.Millisecond, true, nil
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
	if hdr.PropertyFlags&PropContinuation != 0 {
		return nil, nil, errors.New("extended AMQP content property flags are not supported")
	}
	if maxSize := ch.conn.server.cfg.Broker.MaxMessageSize(); maxSize > 0 && hdr.BodySize > uint64(maxSize) {
		return nil, nil, fmt.Errorf("%w: %d > %d", errContentTooLarge, hdr.BodySize, maxSize)
	}

	// Parse properties
	props := &Properties{}
	if hdr.PropertyFlags != 0 {
		r := &argsReader{data: hdr.PropertyBuf}
		if hdr.PropertyFlags&PropContentType != 0 {
			props.ContentType, err = r.ReadString()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid content-type property: %w", err)
			}
		}
		if hdr.PropertyFlags&PropContentEncoding != 0 {
			props.ContentEncoding, err = r.ReadString()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid content-encoding property: %w", err)
			}
			props.HasContentEncoding = true
		}
		if hdr.PropertyFlags&PropHeaders != 0 {
			t, readErr := r.ReadTable()
			if readErr != nil {
				return nil, nil, fmt.Errorf("invalid headers property: %w", readErr)
			}
			props.Headers = t
			props.HasHeaders = true
		}
		if hdr.PropertyFlags&PropDeliveryMode != 0 {
			props.DeliveryMode, err = r.ReadUint8()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid delivery-mode property: %w", err)
			}
			props.HasDeliveryMode = true
		}
		if hdr.PropertyFlags&PropPriority != 0 {
			props.Priority, err = r.ReadUint8()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid priority property: %w", err)
			}
		}
		if hdr.PropertyFlags&PropCorrelationID != 0 {
			props.CorrelationID, err = r.ReadString()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid correlation-id property: %w", err)
			}
		}
		if hdr.PropertyFlags&PropReplyTo != 0 {
			props.ReplyTo, err = r.ReadString()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid reply-to property: %w", err)
			}
		}
		if hdr.PropertyFlags&PropExpiration != 0 {
			props.Expiration, err = r.ReadString()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid expiration property: %w", err)
			}
			props.HasExpiration = true
		}
		if hdr.PropertyFlags&PropMessageID != 0 {
			props.MessageID, err = r.ReadString()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid message-id property: %w", err)
			}
		}
		if hdr.PropertyFlags&PropTimestamp != 0 {
			ts, readErr := r.ReadUint64()
			if readErr != nil {
				return nil, nil, fmt.Errorf("invalid timestamp property: %w", readErr)
			}
			props.Timestamp = ts
			props.HasTimestamp = true
		}
		if hdr.PropertyFlags&PropType != 0 {
			props.Type, err = r.ReadString()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid type property: %w", err)
			}
		}
		if hdr.PropertyFlags&PropUserID != 0 {
			props.UserID, err = r.ReadString()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid user-id property: %w", err)
			}
			props.HasUserID = true
		}
		if hdr.PropertyFlags&PropAppID != 0 {
			props.AppID, err = r.ReadString()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid app-id property: %w", err)
			}
			props.HasAppID = true
		}
		if hdr.PropertyFlags&PropClusterID != 0 {
			props.ClusterID, err = r.ReadString()
			if err != nil {
				return nil, nil, fmt.Errorf("invalid cluster-id property: %w", err)
			}
			props.HasClusterID = true
		}
		if r.pos != len(r.data) {
			return nil, nil, fmt.Errorf("invalid content properties: %d trailing bytes", len(r.data)-r.pos)
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
		if len(bodyFrame.Payload) == 0 || uint64(len(bodyFrame.Payload)) > remaining {
			return nil, nil, fmt.Errorf("body frame length %d exceeds remaining content size %d", len(bodyFrame.Payload), remaining)
		}
		body = append(body, bodyFrame.Payload...)
		remaining -= uint64(len(bodyFrame.Payload))
	}

	return props, body, nil
}

func (ch *Channel) handleBasicAck(args []byte) error {
	r := &argsReader{data: args}
	deliveryTag, err := r.ReadUint64()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.ack delivery tag")
	}
	multiple, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.ack multiple flag")
	}
	if r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.ack arguments")
	}
	maxTag := ch.deliverySeq.Load()
	if (multiple && deliveryTag > maxTag) || (!multiple && (deliveryTag == 0 || deliveryTag > maxTag)) {
		return ch.sendChannelError(ReplyPreconditionFailed, "unknown delivery tag")
	}

	b := ch.conn.server.cfg.Broker
	var settlementErr error
	matched := false

	ch.unackedMu.Lock()
	if multiple {
		// Ack all up to and including this tag
		for tag, pending := range ch.unacked {
			if deliveryTag == 0 || tag <= deliveryTag {
				matched = true
				if ch.txActive {
					ch.txBuffer = append(ch.txBuffer, &txOperation{kind: "ack", messageID: pending.messageID, queue: pending.queue, leaseToken: pending.leaseToken, consumerTag: pending.consumerTag})
					delete(ch.unacked, tag)
				} else {
					err := b.Store().Ack(ch.conn.requestContext(), broker.AckRequest{
						MessageID:  pending.messageID,
						Queue:      pending.queue,
						LeaseToken: pending.leaseToken,
						ConsumerID: "",
					})
					if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
						delete(ch.unacked, tag)
						ch.releasePrefetchInFlight(pending.consumerTag)
					} else {
						settlementErr = err
						break
					}
				}
			}
		}
	} else if pending, ok := ch.unacked[deliveryTag]; ok {
		matched = true
		if ch.txActive {
			ch.txBuffer = append(ch.txBuffer, &txOperation{kind: "ack", messageID: pending.messageID, queue: pending.queue, leaseToken: pending.leaseToken, consumerTag: pending.consumerTag})
			delete(ch.unacked, deliveryTag)
		} else {
			err := b.Store().Ack(ch.conn.requestContext(), broker.AckRequest{
				MessageID:  pending.messageID,
				Queue:      pending.queue,
				LeaseToken: pending.leaseToken,
				ConsumerID: "",
			})
			if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
				delete(ch.unacked, deliveryTag)
				ch.releasePrefetchInFlight(pending.consumerTag)
			} else {
				settlementErr = err
			}
		}
	}
	ch.unackedMu.Unlock()
	if settlementErr != nil {
		return ch.sendChannelError(ReplyInternalError, "message acknowledgement failed")
	}
	if !multiple && !matched {
		return ch.sendChannelError(ReplyPreconditionFailed, "unknown delivery tag")
	}

	return nil
}

func (ch *Channel) handleBasicNack(args []byte) error {
	r := &argsReader{data: args}
	deliveryTag, err := r.ReadUint64()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.nack delivery tag")
	}
	multiple, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.nack multiple flag")
	}
	requeue, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.nack requeue flag")
	}
	if r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.nack arguments")
	}

	return ch.handleNackOrReject(deliveryTag, multiple, requeue, "nack")
}

func (ch *Channel) handleBasicReject(args []byte) error {
	r := &argsReader{data: args}
	deliveryTag, err := r.ReadUint64()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.reject delivery tag")
	}
	requeue, err := r.ReadBool()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.reject requeue flag")
	}
	if r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.reject arguments")
	}

	return ch.handleNackOrReject(deliveryTag, false, requeue, "reject")
}

func (ch *Channel) handleNackOrReject(deliveryTag uint64, multiple, requeue bool, action string) error {
	maxTag := ch.deliverySeq.Load()
	if (multiple && deliveryTag > maxTag) || (!multiple && (deliveryTag == 0 || deliveryTag > maxTag)) {
		return ch.sendChannelError(ReplyPreconditionFailed, "unknown delivery tag")
	}
	b := ch.conn.server.cfg.Broker
	var settlementErr error
	matched := false

	ch.unackedMu.Lock()
	if multiple {
		for tag, pending := range ch.unacked {
			if deliveryTag == 0 || tag <= deliveryTag {
				matched = true
				nackAction := broker.NackRequeue
				if !requeue {
					nackAction = broker.NackReject
				}
				if ch.txActive {
					ch.txBuffer = append(ch.txBuffer, &txOperation{kind: "nack", messageID: pending.messageID, queue: pending.queue, leaseToken: pending.leaseToken, nackAction: nackAction, consumerTag: pending.consumerTag})
					delete(ch.unacked, tag)
				} else {
					err := b.Store().Nack(ch.conn.requestContext(), broker.NackRequest{
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
						settlementErr = err
						break
					}
				}
			}
		}
	} else if pending, ok := ch.unacked[deliveryTag]; ok {
		matched = true
		nackAction := broker.NackRequeue
		if !requeue {
			nackAction = broker.NackReject
		}
		if ch.txActive {
			ch.txBuffer = append(ch.txBuffer, &txOperation{kind: "nack", messageID: pending.messageID, queue: pending.queue, leaseToken: pending.leaseToken, nackAction: nackAction, consumerTag: pending.consumerTag})
			delete(ch.unacked, deliveryTag)
		} else {
			err := b.Store().Nack(ch.conn.requestContext(), broker.NackRequest{
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
				settlementErr = err
			}
		}
	}
	ch.unackedMu.Unlock()
	if settlementErr != nil {
		return ch.sendChannelError(ReplyInternalError, "message negative acknowledgement failed")
	}
	if !multiple && !matched {
		return ch.sendChannelError(ReplyPreconditionFailed, "unknown delivery tag")
	}

	return nil
}

func (ch *Channel) handleBasicRecover(args []byte, async bool) error {
	r := &argsReader{data: args}
	requeue, err := r.ReadBool()
	if err != nil || r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.recover requeue flag")
	}
	if !requeue {
		ch.unackedMu.Lock()
		pending := make([]*pendingDelivery, 0, len(ch.unacked))
		for _, delivery := range ch.unacked {
			pending = append(pending, delivery)
		}
		ch.unackedMu.Unlock()
		sort.Slice(pending, func(i, j int) bool { return pending[i].deliveryTag < pending[j].deliveryTag })
		for _, item := range pending {
			// Basic.Get deliveries have no consumer to receive a Basic.Deliver
			// frame. Follow the common broker behavior and recover active pushed
			// deliveries only.
			if item.consumerTag == "" {
				continue
			}
			ch.consumersMu.RLock()
			_, active := ch.consumers[item.consumerTag]
			ch.consumersMu.RUnlock()
			if !active {
				continue
			}
			delivery := item.delivery
			delivery.Redeliver = true
			if err := ch.sendDelivery(delivery, item.consumerTag); err != nil {
				return err
			}
		}
	} else if err := ch.handleNackOrReject(0, true, true, "recover"); err != nil {
		return err
	}
	if async {
		return nil
	}
	return ch.writeMethodFrame(ClassBasic, MethodBasicRecoverOk, nil)
}

func (ch *Channel) handleBasicGet(args []byte) error {
	r := &argsReader{data: args}
	if _, err := r.ReadUint16(); err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.get ticket")
	}
	queue, err := r.ReadString()
	if err != nil {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.get queue")
	}
	autoAck, err := r.ReadBool()
	if err != nil || r.pos != len(r.data) {
		return ch.sendChannelError(ReplySyntaxError, "invalid basic.get flags")
	}

	b := ch.conn.server.cfg.Broker

	// Try to get a single message
	consumerID := fmt.Sprintf("amqp-get-%d", ch.id)
	messages, err := b.Store().Lease(ch.conn.requestContext(), broker.LeaseRequest{
		Queue:      queue,
		ConsumerID: consumerID,
		Limit:      1,
	})
	if errors.Is(err, broker.ErrQueueNotFound) {
		return ch.sendChannelError(ReplyNotFound, "queue does not exist")
	}
	if err != nil {
		return ch.sendChannelError(ReplyInternalError, err.Error())
	}
	if len(messages) == 0 {
		// Send Basic.GetEmpty
		return ch.writeMethodFrame(ClassBasic, MethodBasicGetEmpty, EncodeMethodArgs(""))
	}

	msg := messages[0]
	messageCount := 0
	if counter, ok := b.Store().(broker.QueueReadyCounter); ok {
		messageCount, err = counter.QueueReadyCount(ch.conn.requestContext(), queue)
		if err != nil && !errors.Is(err, broker.ErrUnsupportedMode) {
			_ = b.Store().Nack(ch.conn.requestContext(), broker.NackRequest{
				MessageID: msg.ID, Queue: queue, LeaseToken: msg.LeaseToken,
				ConsumerID: consumerID, Action: broker.NackRequeue,
			})
			return ch.sendChannelError(ReplyInternalError, err.Error())
		}
	}
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
		amqpCountUint32(messageCount), // message-count
	)
	if err := ch.codec().WriteMethodFrame(ch.id, ClassBasic, 71, getOkArgs); err != nil {
		ch.writeMu.Unlock()
		return err
	}

	// Send content header and body
	props := &Properties{
		ContentType:   msg.ContentType,
		Priority:      uint8(min(max(msg.Priority, 0), 9)),
		Headers:       nil,
		CorrelationID: msg.CorrelationID,
		ReplyTo:       msg.ReplyTo,
		MessageID:     msg.ID,
		Type:          msg.Type,
	}
	props.Headers, props.HasHeaders = amqpHeadersFromMetadata(msg.Metadata, msg.Headers)
	restoreAMQPPropertiesFromMetadata(msg.Metadata, props)
	if timestamp, ok := amqpTimestampFromMetadata(msg.Metadata); ok {
		props.Timestamp = timestamp
		props.HasTimestamp = true
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
		return b.Store().Ack(ch.conn.requestContext(), broker.AckRequest{MessageID: msg.ID, Queue: queue, LeaseToken: msg.LeaseToken, ConsumerID: consumerID})
	}
	return nil
}

func amqpQueueExists(ctx context.Context, store broker.Store, queue string) (bool, error) {
	if lookup, ok := store.(broker.QueueConfigLookup); ok {
		_, found, err := lookup.LookupQueueConfig(ctx, queue)
		if err == nil {
			return found, nil
		}
		if !errors.Is(err, broker.ErrUnsupportedMode) {
			return false, err
		}
	}
	if counter, ok := store.(broker.QueueReadyCounter); ok {
		_, err := counter.QueueReadyCount(ctx, queue)
		if err == nil {
			return true, nil
		}
		if errors.Is(err, broker.ErrQueueNotFound) {
			return false, nil
		}
		if !errors.Is(err, broker.ErrUnsupportedMode) {
			return false, err
		}
	}
	_, err := store.ListQueue(ctx, queue, 1)
	if errors.Is(err, broker.ErrQueueNotFound) {
		return false, nil
	}
	return err == nil, err
}

func amqpTimestampFromMetadata(metadata map[string]string) (uint64, bool) {
	value, ok := metadata[amqpTimestampMetadataKey]
	if !ok {
		return 0, false
	}
	timestamp, err := strconv.ParseUint(value, 10, 64)
	return timestamp, err == nil
}

func amqpPropertiesMetadata(props *Properties) map[string]string {
	metadata := make(map[string]string, 3)
	if props.HasTimestamp || props.Timestamp != 0 {
		metadata[amqpTimestampMetadataKey] = strconv.FormatUint(props.Timestamp, 10)
	}
	if props.HasHeaders || len(props.Headers) > 0 {
		metadata[amqpHeadersMetadataKey] = base64.StdEncoding.EncodeToString(encodeTableToBytes(props.Headers))
	}
	other := persistedAMQPProperties{
		ContentEncoding:    props.ContentEncoding,
		HasContentEncoding: props.HasContentEncoding || props.ContentEncoding != "",
		DeliveryMode:       props.DeliveryMode,
		HasDeliveryMode:    props.HasDeliveryMode || props.DeliveryMode != 0,
		Expiration:         props.Expiration,
		HasExpiration:      props.HasExpiration || props.Expiration != "",
		UserID:             props.UserID,
		HasUserID:          props.HasUserID || props.UserID != "",
		AppID:              props.AppID,
		HasAppID:           props.HasAppID || props.AppID != "",
		ClusterID:          props.ClusterID,
		HasClusterID:       props.HasClusterID || props.ClusterID != "",
	}
	if other.HasContentEncoding || other.HasDeliveryMode || other.HasExpiration ||
		other.HasUserID || other.HasAppID || other.HasClusterID {
		if encoded, err := json.Marshal(other); err == nil {
			metadata[amqpOtherPropertiesMetadataKey] = string(encoded)
		}
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

type persistedAMQPProperties struct {
	ContentEncoding    string `json:"content_encoding,omitempty"`
	HasContentEncoding bool   `json:"has_content_encoding,omitempty"`
	DeliveryMode       uint8  `json:"delivery_mode,omitempty"`
	HasDeliveryMode    bool   `json:"has_delivery_mode,omitempty"`
	Expiration         string `json:"expiration,omitempty"`
	HasExpiration      bool   `json:"has_expiration,omitempty"`
	UserID             string `json:"user_id,omitempty"`
	HasUserID          bool   `json:"has_user_id,omitempty"`
	AppID              string `json:"app_id,omitempty"`
	HasAppID           bool   `json:"has_app_id,omitempty"`
	ClusterID          string `json:"cluster_id,omitempty"`
	HasClusterID       bool   `json:"has_cluster_id,omitempty"`
}

func restoreAMQPPropertiesFromMetadata(metadata map[string]string, props *Properties) {
	encoded, ok := metadata[amqpOtherPropertiesMetadataKey]
	if !ok {
		return
	}
	var stored persistedAMQPProperties
	if err := json.Unmarshal([]byte(encoded), &stored); err != nil {
		return
	}
	props.ContentEncoding = stored.ContentEncoding
	props.HasContentEncoding = stored.HasContentEncoding
	props.DeliveryMode = stored.DeliveryMode
	props.HasDeliveryMode = stored.HasDeliveryMode
	props.Expiration = stored.Expiration
	props.HasExpiration = stored.HasExpiration
	props.UserID = stored.UserID
	props.HasUserID = stored.HasUserID
	props.AppID = stored.AppID
	props.HasAppID = stored.HasAppID
	props.ClusterID = stored.ClusterID
	props.HasClusterID = stored.HasClusterID
}

func amqpHeadersFromMetadata(metadata, fallback map[string]string) (map[string]any, bool) {
	if encoded, ok := metadata[amqpHeadersMetadataKey]; ok {
		if data, err := base64.StdEncoding.DecodeString(encoded); err == nil {
			if headers, err := DecodeTable(data); err == nil {
				return headers, true
			}
		}
	}
	if len(fallback) == 0 {
		return nil, false
	}
	headers := make(map[string]any, len(fallback))
	for key, value := range fallback {
		headers[key] = value
	}
	return headers, true
}

func (ch *Channel) sendBasicReturn(exchange, routingKey string, props *Properties, body []byte) error {
	ch.writeMu.Lock()
	defer ch.writeMu.Unlock()
	args := EncodeMethodArgs(uint16(ReplyNoRoute), "NO_ROUTE", exchange, routingKey)
	if err := ch.codec().WriteMethodFrame(ch.id, ClassBasic, MethodBasicReturn, args); err != nil {
		return err
	}
	flags, encoded := ch.encodeProperties(props)
	if err := ch.codec().WriteHeaderFrame(ch.id, ClassBasic, uint64(len(body)), flags, encoded); err != nil {
		return err
	}
	frameMax := int(ch.conn.frameMax)
	if frameMax <= 0 {
		frameMax = FrameMaxSize
	}
	for len(body) > 0 {
		end := min(len(body), frameMax)
		if err := ch.codec().WriteBodyFrame(ch.id, body[:end]); err != nil {
			return err
		}
		body = body[end:]
	}
	return nil
}

// handleConfirmMethod handles confirm methods.
func (ch *Channel) handleConfirmMethod(method *MethodFrame) error {
	switch method.MethodID {
	case MethodConfirmSelect:
		r := &argsReader{data: method.Args}
		noWait, err := r.ReadBool()
		if err != nil || r.pos != len(r.data) {
			return ch.sendChannelError(ReplySyntaxError, "invalid confirm.select flags")
		}
		ch.confirmed.Store(true)
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
				ch.txActive = false
				// Keep the failed operation and everything after it available to
				// close(), which retries requeueing with a bounded context.
				ch.txBuffer = append([]*txOperation(nil), ch.txBuffer[i:]...)
				return ch.sendChannelError(ReplyInternalError, err.Error())
			}
		}
		ch.txActive = false
		ch.txBuffer = nil
		return ch.writeMethodFrame(ClassTx, MethodTxCommitOk, nil)
	case MethodTxRollback:
		var rollbackErr error
		for _, pending := range ch.txBuffer {
			if pending.kind != "ack" && pending.kind != "nack" {
				continue
			}
			err := ch.conn.server.cfg.Broker.Store().Nack(ch.conn.requestContext(), broker.NackRequest{MessageID: pending.messageID, Queue: pending.queue, LeaseToken: pending.leaseToken, ConsumerID: pending.consumerTag, Action: broker.NackRequeue})
			if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
				ch.releasePrefetchInFlight(pending.consumerTag)
			} else {
				rollbackErr = err
				break
			}
		}
		if rollbackErr != nil {
			ch.txActive = false
			return ch.sendChannelError(ReplyInternalError, "transaction rollback failed")
		}
		ch.txActive = false
		ch.txBuffer = nil
		return ch.writeMethodFrame(ClassTx, MethodTxRollbackOk, nil)
	default:
		return ch.sendChannelError(ReplySyntaxError, fmt.Sprintf("unknown tx method %d", method.MethodID))
	}
}

func (ch *Channel) executeTxOp(op *txOperation) error {
	ctx := ch.conn.requestContext()
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
			ConsumerID: op.consumerTag,
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

		// Transactional acknowledgments have already been removed from unacked,
		// but remain leased until the transaction commits. Roll them back when
		// the channel closes so they do not wait for lease expiry.
		transactional := ch.txBuffer
		ch.txBuffer = nil
		ch.txActive = false

		ctx, cancel := context.WithTimeout(ch.conn.requestContext(), 2*time.Second)
		defer cancel()
		store := ch.conn.server.cfg.Broker.Store()
		for _, delivery := range pending {
			err := store.Nack(ctx, broker.NackRequest{MessageID: delivery.messageID, Queue: delivery.queue, LeaseToken: delivery.leaseToken, ConsumerID: delivery.consumerTag, Action: broker.NackRequeue})
			if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
				ch.releasePrefetchInFlight(delivery.consumerTag)
			} else {
				log.Printf("[amqp] requeue on channel close failed for %s: %v", delivery.messageID, err)
			}
		}
		for _, op := range transactional {
			if op.kind != "ack" && op.kind != "nack" {
				continue
			}
			err := store.Nack(ctx, broker.NackRequest{MessageID: op.messageID, Queue: op.queue, LeaseToken: op.leaseToken, ConsumerID: op.consumerTag, Action: broker.NackRequeue})
			if err == nil || errors.Is(err, broker.ErrLeaseLost) || errors.Is(err, broker.ErrMessageNotFound) {
				ch.releasePrefetchInFlight(op.consumerTag)
			} else {
				log.Printf("[amqp] rollback on channel close failed for %s: %v", op.messageID, err)
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

func randomHex(n int) (string, error) {
	if n <= 0 {
		return "", fmt.Errorf("random byte count must be positive")
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
