package broker

import (
	"context"
	"time"
)

type BrokerEvent struct {
	Broker string
	State  BrokerState
	Error  error
	Time   time.Time
}
type PublishEvent struct {
	Message      Message
	Destinations []Destination
	Result       PublishResult
	Error        error
	Time         time.Time
}
type RoutingEvent struct {
	Message      Message
	Route        Route
	Destinations []Destination
	Error        error
	Time         time.Time
}
type MovementEvent struct {
	Request MoveRequest
	Result  MoveResult
	Error   error
	Time    time.Time
}
type ConsumerEvent struct {
	Consumer string
	Group    string
	Queue    string
	State    ConsumerState
	Error    error
	Time     time.Time
}
type MessageEvent struct {
	Message  Message
	Consumer string
	Queue    string
	Error    error
	Time     time.Time
}
type TaskEvent struct {
	Task  TaskInfo
	Error error
	Time  time.Time
}
type StorageEvent struct {
	Message   Message
	Operation string
	Error     error
	Time      time.Time
}
type RetryEvent struct {
	Message Message
	Delay   time.Duration
	Error   error
	Time    time.Time
}
type DLQEvent struct {
	Message Message
	Queue   string
	Reason  string
	Error   error
	Time    time.Time
}

type Hooks struct {
	OnInitializing func(context.Context, BrokerEvent)
	OnRecovering   func(context.Context, BrokerEvent)
	OnStarted      func(context.Context, BrokerEvent)
	OnReady        func(context.Context, BrokerEvent)
	OnDegraded     func(context.Context, BrokerEvent)
	OnPaused       func(context.Context, BrokerEvent)
	OnResumed      func(context.Context, BrokerEvent)
	OnDraining     func(context.Context, BrokerEvent)
	OnStopped      func(context.Context, BrokerEvent)
	OnFailed       func(context.Context, BrokerEvent)

	BeforePublish      func(context.Context, PublishEvent) error
	AfterPublish       func(context.Context, PublishEvent)
	OnPublishRejected  func(context.Context, PublishEvent)
	OnPublishConfirmed func(context.Context, PublishEvent)
	OnPublishFailed    func(context.Context, PublishEvent)

	BeforeRoute       func(context.Context, RoutingEvent) error
	AfterRoute        func(context.Context, RoutingEvent)
	OnRouteMiss       func(context.Context, RoutingEvent)
	OnRouteConflict   func(context.Context, RoutingEvent)
	OnMessageMoved    func(context.Context, MovementEvent)
	OnMessageCopied   func(context.Context, MovementEvent)
	OnMessageRerouted func(context.Context, MovementEvent)

	OnConsumerRegistered func(context.Context, ConsumerEvent)
	OnConsumerAssigned   func(context.Context, ConsumerEvent)
	OnConsumerRevoked    func(context.Context, ConsumerEvent)
	BeforeHandle         func(context.Context, MessageEvent) error
	AfterHandle          func(context.Context, MessageEvent)
	OnHandlerError       func(context.Context, MessageEvent)
	OnPanic              func(context.Context, MessageEvent)
	OnAck                func(context.Context, MessageEvent)
	OnNack               func(context.Context, MessageEvent)
	OnLeaseExtended      func(context.Context, MessageEvent)
	OnConsumerPaused     func(context.Context, ConsumerEvent)
	OnConsumerResumed    func(context.Context, ConsumerEvent)
	OnConsumerDraining   func(context.Context, ConsumerEvent)
	OnConsumerStopped    func(context.Context, ConsumerEvent)

	OnTaskCreated      func(context.Context, TaskEvent)
	OnTaskScheduled    func(context.Context, TaskEvent)
	OnTaskQueued       func(context.Context, TaskEvent)
	OnTaskStarted      func(context.Context, TaskEvent)
	OnTaskProgress     func(context.Context, TaskEvent)
	OnTaskSucceeded    func(context.Context, TaskEvent)
	OnTaskFailed       func(context.Context, TaskEvent)
	OnTaskRetrying     func(context.Context, TaskEvent)
	OnTaskCancelled    func(context.Context, TaskEvent)
	OnTaskTimedOut     func(context.Context, TaskEvent)
	OnTaskCompensating func(context.Context, TaskEvent)
	OnTaskCompensated  func(context.Context, TaskEvent)
	OnTaskArchived     func(context.Context, TaskEvent)

	BeforeAppend    func(context.Context, StorageEvent) error
	AfterAppend     func(context.Context, StorageEvent)
	OnCommit        func(context.Context, StorageEvent)
	OnRollback      func(context.Context, StorageEvent)
	OnFsyncSlow     func(context.Context, StorageEvent)
	OnCorruption    func(context.Context, StorageEvent)
	OnRecoveryStart func(context.Context, StorageEvent)
	OnRecoveryDone  func(context.Context, StorageEvent)

	OnRetryDecision  func(context.Context, RetryEvent)
	OnRetryScheduled func(context.Context, RetryEvent)
	OnRetryExhausted func(context.Context, RetryEvent)
	OnDeadLettered   func(context.Context, DLQEvent)
	OnDLQReplay      func(context.Context, DLQEvent)
	OnDLQPurge       func(context.Context, DLQEvent)
	OnDLQArchive     func(context.Context, DLQEvent)
}
