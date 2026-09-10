package amqp

// AMQP 0-9-1 protocol constants.
const (
	ProtocolHeader   = "AMQP"
	ProtocolVersion  = "0.9.1"
	ProtocolMajor    = 0
	ProtocolMinor    = 9
	ProtocolRevision = 1

	FrameMethod    = 1
	FrameHeader    = 2
	FrameBody      = 3
	FrameHeartbeat = 8

	FrameEnd = 0xCE

	// Default port for AMQP connections.
	DefaultPort = 5672

	// Frame sizes.
	FrameMinSize = 4096
	FrameMaxSize = 131072

	// Heartbeat interval in seconds.
	HeartbeatDefault = 60
	HeartbeatMin     = 5
	HeartbeatMax     = 120

	// Channel max.
	ChannelMax = 65535

	// Exchange types.
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"

	// Delivery modes.
	DeliveryModeTransient  = 1
	DeliveryModePersistent = 2

	// Message properties flags (bitmask).
	PropContentType     = 0x8000
	PropContentEncoding = 0x4000
	PropHeaders         = 0x2000
	PropDeliveryMode    = 0x1000
	PropPriority        = 0x0800
	PropCorrelationID   = 0x0400
	PropReplyTo         = 0x0200
	PropExpiration      = 0x0100
	PropMessageID       = 0x0080
	PropTimestamp       = 0x0040
	PropType            = 0x0020
	PropUserID          = 0x0010
	PropAppID           = 0x0008
	PropReserved        = 0x0004
	PropClusterID       = 0x0001
)

// AMQP method class IDs.
const (
	ClassConnection = 10
	ClassChannel    = 20
	ClassExchange   = 40
	ClassQueue      = 50
	ClassBasic      = 60
	ClassTx         = 90
	ClassConfirm    = 85
)

// Connection method IDs.
const (
	MethodConnectionStart     = 10
	MethodConnectionStartOk   = 11
	MethodConnectionTune      = 30
	MethodConnectionTuneOk    = 31
	MethodConnectionOpen      = 40
	MethodConnectionOpenOk    = 41
	MethodConnectionClose     = 50
	MethodConnectionCloseOk   = 51
	MethodConnectionBlocked   = 60
	MethodConnectionUnblocked = 61
)

// Channel method IDs.
const (
	MethodChannelOpen    = 10
	MethodChannelOpenOk  = 11
	MethodChannelFlow    = 20
	MethodChannelFlowOk  = 21
	MethodChannelClose   = 40
	MethodChannelCloseOk = 41
	MethodChannelReturn  = 50
	MethodChannelAck     = 60
)

// Exchange method IDs.
const (
	MethodExchangeDeclare   = 10
	MethodExchangeDeclareOk = 11
	MethodExchangeDelete    = 20
	MethodExchangeDeleteOk  = 21
	MethodExchangeBind      = 30
	MethodExchangeBindOk    = 31
	MethodExchangeUnbind    = 40
	MethodExchangeUnbindOk  = 41
)

// Queue method IDs.
const (
	MethodQueueDeclare   = 10
	MethodQueueDeclareOk = 11
	MethodQueueBind      = 20
	MethodQueueBindOk    = 21
	MethodQueueUnbind    = 30
	MethodQueueUnbindOk  = 31
	MethodQueuePurge     = 40
	MethodQueuePurgeOk   = 41
	MethodQueueDelete    = 50
	MethodQueueDeleteOk  = 51
)

// Basic method IDs.
const (
	MethodBasicQos          = 10
	MethodBasicQosOk        = 11
	MethodBasicConsume      = 20
	MethodBasicConsumeOk    = 21
	MethodBasicCancel       = 30
	MethodBasicCancelOk     = 31
	MethodBasicPublish      = 40
	MethodBasicReturn       = 50
	MethodBasicDeliver      = 60
	MethodBasicGet          = 70
	MethodBasicGetOk        = 71
	MethodBasicGetEmpty     = 72
	MethodBasicAck          = 80
	MethodBasicReject       = 90
	MethodBasicRecoverAsync = 100
	MethodBasicRecover      = 110
	MethodBasicRecoverOk    = 111
	MethodBasicNack         = 120 // AMQP publisher confirms / nack extension
)

// Confirm method IDs.
const (
	MethodConfirmSelect   = 10
	MethodConfirmSelectOk = 11
)

// Transaction method IDs.
const (
	MethodTxSelect     = 10
	MethodTxSelectOk   = 11
	MethodTxCommit     = 20
	MethodTxCommitOk   = 21
	MethodTxRollback   = 30
	MethodTxRollbackOk = 31
)

// Reply codes.
const (
	ReplySuccess            = 200
	ReplyContentTooLarge    = 311
	ReplyNoRoute            = 312
	ReplyNoConsumers        = 313
	ReplyAccessRefused      = 403
	ReplyNotFound           = 404
	ReplyResourceLocked     = 405
	ReplyPreconditionFailed = 406
	ReplyChannelError       = 504
	ReplyUnexpectedFrame    = 505
	ReplySyntaxError        = 502
	ReplyCommandInvalid     = 503
	ReplyInternalError      = 541
)

// Frame represents an AMQP frame.
type Frame struct {
	Type      uint8
	ChannelID uint16
	Payload   []byte
}

// MethodFrame represents an AMQP method frame.
type MethodFrame struct {
	ClassID  uint16
	MethodID uint16
	Args     []byte
}

// HeaderFrame represents an AMQP content header frame.
type HeaderFrame struct {
	ClassID       uint16
	BodySize      uint64
	PropertyFlags uint16
	PropertyBuf   []byte
}

// Content carries a full AMQP message (method + header + body).
type Content struct {
	Method *MethodFrame
	Header *HeaderFrame
	Body   []byte
}

// PropertyFlags returns a bitmask of which properties are set.
func (c *Content) PropertyFlags() uint16 {
	if c.Header == nil {
		return 0
	}
	return c.Header.PropertyFlags
}

// Delivery represents a message delivered to a consumer.
type Delivery struct {
	DeliveryTag uint64
	Redeliver   bool
	Exchange    string
	RoutingKey  string
	Body        []byte
	Properties  *Properties
	Headers     map[string]any
}

// Properties holds AMQP message properties.
type Properties struct {
	ContentType     string
	ContentEncoding string
	Headers         map[string]any
	DeliveryMode    uint8
	Priority        uint8
	CorrelationID   string
	ReplyTo         string
	Expiration      string
	MessageID       string
	Timestamp       uint64
	Type            string
	UserID          string
	AppID           string
	ClusterID       string
}
