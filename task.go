package broker

import (
	"context"
	"time"
)

type TaskInfo struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Queue      string            `json:"queue"`
	Status     TaskStatus        `json:"status"`
	TenantID   string            `json:"tenant_id,omitempty"`
	MessageID  string            `json:"message_id,omitempty"`
	Attempts   int               `json:"attempts,omitempty"`
	Progress   Progress          `json:"progress,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	StartedAt  time.Time         `json:"started_at,omitempty"`
	UpdatedAt  time.Time         `json:"updated_at"`
	FinishedAt time.Time         `json:"finished_at,omitempty"`
	LastError  string            `json:"last_error,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Result     []byte            `json:"result,omitempty"`
}

type TaskResult struct {
	Task    TaskInfo `json:"task"`
	Payload []byte   `json:"payload,omitempty"`
	Error   error    `json:"-"`
}

type TaskDelivery struct {
	Delivery
	Task TaskInfo
}

func (d TaskDelivery) Decode(out any) error { return d.Delivery.Decode(out) }

type TaskHandle interface {
	ID() string
	Status(context.Context) (TaskStatus, error)
	Info(context.Context) (TaskInfo, error)
	Progress(context.Context) (Progress, error)
	Result(context.Context, any) error
	Wait(context.Context) (TaskResult, error)
	Cancel(context.Context, string) error
	Retry(context.Context) error
	Replay(context.Context) error
	Logs(context.Context) ([]MessageHistory, error)
}

type taskHandle struct {
	b  *Broker
	id string
}

func (h taskHandle) ID() string { return h.id }
func (h taskHandle) Info(ctx context.Context) (TaskInfo, error) {
	h.b.taskMu.RLock()
	if t := h.b.tasks[h.id]; t != nil {
		out := cloneTask(*t)
		h.b.taskMu.RUnlock()
		if !tenantAllowed(ctx, out.TenantID) {
			return TaskInfo{}, ErrUnauthorized
		}
		return out, nil
	}
	h.b.taskMu.RUnlock()
	if h.b.stateStore != nil {
		if t, ok, err := h.b.stateStore.GetTask(ctx, h.id); err != nil {
			return TaskInfo{}, err
		} else if ok {
			if !tenantAllowed(ctx, t.TenantID) {
				return TaskInfo{}, ErrUnauthorized
			}
			return t, nil
		}
	}
	return TaskInfo{}, ErrMessageNotFound
}
func (h taskHandle) Status(ctx context.Context) (TaskStatus, error) {
	t, err := h.Info(ctx)
	return t.Status, err
}
func (h taskHandle) Progress(ctx context.Context) (Progress, error) {
	t, err := h.Info(ctx)
	return t.Progress, err
}
func (h taskHandle) Result(ctx context.Context, out any) error {
	t, err := h.Info(ctx)
	if err != nil {
		return err
	}
	if out != nil {
		return h.b.codec.Unmarshal(t.Result, out)
	}
	return nil
}

// defaultTaskWait bounds Wait when the caller supplies a context with no deadline.
//
// Without it, a task routed to a queue that has no handler never reaches a terminal
// state and Wait blocks until the process exits: SubmitTask defaults the queue to
// "tasks.<name>" while RegisterTask lets the caller override the queue, so a
// mismatch is silent and the caller hangs rather than failing.
const defaultTaskWait = 30 * time.Second

func (h taskHandle) Wait(ctx context.Context) (TaskResult, error) {
	if _, err := h.Info(ctx); err != nil {
		return TaskResult{}, err
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTaskWait)
		defer cancel()
	}
	ch := make(chan TaskResult, 1)
	h.b.taskMu.Lock()
	if t := h.b.tasks[h.id]; t != nil && (t.Status == TaskSucceeded || t.Status == TaskFailed || t.Status == TaskCancelled || t.Status == TaskDeadLettered) {
		res := TaskResult{Task: cloneTask(*t), Payload: append([]byte(nil), t.Result...)}
		if t.LastError != "" {
			res.Error = &BrokerError{Code: "task_failed", Message: t.LastError}
		}
		h.b.taskMu.Unlock()
		return res, res.Error
	}
	h.b.taskWait[h.id] = append(h.b.taskWait[h.id], ch)
	h.b.taskMu.Unlock()
	select {
	case r := <-ch:
		return r, r.Error
	case <-ctx.Done():
		return TaskResult{}, ctx.Err()
	}
}
func (h taskHandle) Cancel(ctx context.Context, reason string) error {
	if _, err := h.Info(ctx); err != nil {
		return err
	}
	h.b.taskMu.Lock()
	t := h.b.tasks[h.id]
	var ti TaskInfo
	if t != nil {
		t.Status = TaskCancelled
		t.LastError = reason
		t.UpdatedAt = time.Now()
		t.FinishedAt = time.Now()
		ti = cloneTask(*t)
	}
	h.b.taskMu.Unlock()
	if ti.ID != "" {
		h.b.persistTask(ctx, ti)
	}
	// Stop the work, not just the bookkeeping. Without this the handler kept
	// running and later overwrote the cancellation with its own result.
	h.b.cancelTaskRun(h.id)
	if ti.ID != "" {
		if msg, ok, _ := h.b.store.Get(ctx, ti.MessageID); ok {
			msg.Status = MessageCancelled
			msg.LastError = reason
			_ = h.b.store.Update(ctx, msg)
			n := Notification{Type: "task.cancelled", Source: h.b.cfg.Name, Severity: SeverityWarn, MessageID: msg.ID, TaskID: h.id, Queue: msg.Queue, Consumer: msg.ConsumerID, ConsumerGroup: msg.ConsumerGroup, TenantID: msg.TenantID, TraceID: msg.TraceID, Timestamp: time.Now(), Data: map[string]any{"reason": reason}}
			h.b.notify.Publish(n)
			if msg.ConsumerID != "" {
				h.b.emitConsumerNotification(msg.ConsumerID, n)
			}
		}
		if h.b.cfg.Hooks.OnTaskCancelled != nil {
			h.b.cfg.Hooks.OnTaskCancelled(ctx, TaskEvent{Task: ti, Error: errf(ErrCancelled, "%s", reason), Time: time.Now()})
		}
	}
	h.b.finishTaskWaiters(h.id)
	return nil
}
func (h taskHandle) Retry(ctx context.Context) error {
	t, err := h.Info(ctx)
	if err != nil {
		return err
	}
	_, err = h.b.store.Move(ctx, MoveRequest{SourceType: DestinationQueue, Source: t.Queue, DestinationType: DestinationQueue, Destination: t.Queue, MessageIDs: []string{t.MessageID}, Mode: MoveReplay, ResetAttempts: true, ResetError: true, Reason: "task retry"})
	return err
}
func (h taskHandle) Replay(ctx context.Context) error { return h.Retry(ctx) }
func (h taskHandle) Logs(ctx context.Context) ([]MessageHistory, error) {
	t, err := h.Info(ctx)
	if err != nil {
		return nil, err
	}
	return h.b.store.History(ctx, t.MessageID)
}

type TaskFuture interface{ TaskHandle }

// Task returns a handle for a task by id, or nil when no such task is known.
//
// Cancel, Retry, Status and Wait were reachable only through the handle returned by
// SubmitTask, which lives in memory and is lost on restart. Operator tooling that
// holds a task id from a log, a database or a previous process therefore had no way
// to cancel or inspect it: there was no Broker.GetTask, and GetTask existed only on
// the internal StateStore.
//
// The returned handle reads through to the StateStore, so a task submitted before a
// restart is still addressable.
func (b *Broker) Task(id string) TaskHandle {
	if id == "" {
		return nil
	}
	return taskHandle{b: b, id: id}
}

// GetTask returns the current state of a task, and whether it is known.
func (b *Broker) GetTask(ctx context.Context, id string) (TaskInfo, bool) {
	info, err := taskHandle{b: b, id: id}.Info(ctx)
	if err != nil {
		return TaskInfo{}, false
	}
	return info, true
}

// CancelTask cancels a task by id. It reports whether the task was known.
func (b *Broker) CancelTask(ctx context.Context, id, reason string) error {
	h := b.Task(id)
	if h == nil {
		return ErrInvalidMessage
	}
	return h.Cancel(ctx, reason)
}

// RetryTask returns a failed task to its queue by id.
func (b *Broker) RetryTask(ctx context.Context, id string) error {
	h := b.Task(id)
	if h == nil {
		return ErrInvalidMessage
	}
	return h.Retry(ctx)
}

func (p *Publisher) SubmitTask(ctx context.Context, name string, payload any, opts ...TaskOption) (TaskHandle, error) {
	var o TaskOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	resolvedTenant, err := resolveTenant(ctx, o.TenantID)
	if err != nil {
		return nil, err
	}
	o.TenantID = resolvedTenant
	id := o.ID
	if id == "" {
		id = p.b.nextID("task")
	}
	queue := o.Queue
	if queue == "" {
		queue = "tasks." + name
	}
	now := time.Now()
	info := TaskInfo{ID: id, Name: name, Queue: queue, Status: TaskCreated, TenantID: o.TenantID, CreatedAt: now, UpdatedAt: now, Metadata: cloneMap(o.Metadata)}
	p.b.taskMu.Lock()
	// Store a separate allocation rather than &info. Keeping a pointer to the local
	// meant the broker owned the same struct this function kept reading after
	// unlocking, so a consumer that started the task concurrently raced this
	// function's persistTask. The window was narrow before and became routine once
	// queue wakeup removed the poll delay.
	p.b.tasks[id] = &TaskInfo{
		ID: info.ID, Name: info.Name, Queue: info.Queue, Status: info.Status,
		TenantID: info.TenantID, CreatedAt: info.CreatedAt, UpdatedAt: info.UpdatedAt,
		Metadata: cloneMap(info.Metadata),
	}
	p.b.taskMu.Unlock()
	p.b.persistTask(ctx, info)
	if p.b.cfg.Hooks.OnTaskCreated != nil {
		p.b.cfg.Hooks.OnTaskCreated(ctx, TaskEvent{Task: info, Time: now})
	}
	pubOpts := []PublishOption{WithQueue(queue), WithType(name), WithTaskHeaders(id, name), WithPriority(o.Priority), WithDelay(o.Delay), WithRunAt(o.RunAt), WithTenantID(o.TenantID), WithIdempotencyKey(o.IdempotencyKey), WithMetadata(o.Metadata), WithMaxAttempts(o.MaxAttempts)}
	res, err := p.Publish(ctx, name, payload, pubOpts...)
	if err != nil {
		p.b.recordPublisher(ctx, PublisherRecord{ID: id, Operation: "submit_task", Subject: name, Queue: queue, TaskID: id, Status: "failed", Error: err.Error(), CreatedAt: now, UpdatedAt: time.Now()})
		p.b.failTask(ctx, id, err)
		return nil, err
	}
	p.b.taskMu.Lock()
	if t := p.b.tasks[id]; t != nil {
		t.Status = TaskQueued
		if o.Delay > 0 || !o.RunAt.IsZero() {
			t.Status = TaskScheduled
		}
		t.MessageID = res.MessageID
		t.UpdatedAt = time.Now()
		info = cloneTask(*t)
	}
	p.b.taskMu.Unlock()
	p.b.persistTask(ctx, info)
	p.b.recordPublisher(ctx, PublisherRecord{ID: id, Operation: "submit_task", Subject: name, Queue: queue, MessageID: res.MessageID, TaskID: id, Status: "queued", CreatedAt: now, UpdatedAt: time.Now()})
	if p.b.cfg.Hooks.OnTaskQueued != nil {
		ti, _ := taskHandle{b: p.b, id: id}.Info(ctx)
		p.b.cfg.Hooks.OnTaskQueued(ctx, TaskEvent{Task: ti, Time: time.Now()})
	}
	return taskHandle{b: p.b, id: id}, nil
}
func (p *Publisher) SubmitTaskAsync(ctx context.Context, name string, payload any, opts ...TaskOption) TaskFuture {
	h, err := p.SubmitTask(ctx, name, payload, opts...)
	if err != nil {
		id := p.b.nextID("taskerr")
		p.b.failTask(ctx, id, err)
		return taskHandle{b: p.b, id: id}
	}
	return h
}

func WithTaskHeaders(id, name string) PublishOption {
	return func(o *PublishOptions) {
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		o.Headers["x-task-id"] = id
		o.Headers["x-task-name"] = name
		o.Type = name
	}
}

func (b *Broker) RegisterTask(ctx context.Context, name string, handler TaskHandler, cfg ...ConsumerConfig) (*Subscription, error) {
	if handler == nil {
		return nil, ErrNoHandler
	}
	c := ConsumerConfig{Name: "task." + name, Group: "task." + name, Queue: "tasks." + name, Concurrency: 1, AckMode: AckAuto, VisibilityTimeout: b.cfg.DefaultVisibility, RetryPolicy: b.cfg.DefaultRetry}
	if len(cfg) > 0 {
		c = cfg[0]
		if c.Queue == "" {
			c.Queue = "tasks." + name
		}
		if c.Name == "" {
			c.Name = "task." + name
		}
		if c.Group == "" {
			c.Group = c.Name
		}
	}
	return b.Subscribe(ctx, c, func(ctx context.Context, d Delivery) error {
		tid := d.Message.Headers["x-task-id"]
		ti, _ := taskHandle{b: b, id: tid}.Info(ctx)
		// Each invocation gets its own cancellable context, registered against the
		// task id so Cancel can actually stop the work. Previously the handler
		// received the subscription's context, so a cancel could not reach it: the
		// status changed to cancelled while the handler kept running, and on return
		// it overwrote the cancellation with succeeded.
		runCtx, cancel := context.WithCancel(ctx)
		b.registerTaskCancel(tid, cancel)
		defer func() {
			cancel()
			b.clearTaskCancel(tid)
		}()
		td := TaskDelivery{Delivery: d, Task: ti}
		res, err := handler(runCtx, td)
		if b.taskCancelled(tid) {
			// Cancelled: acknowledge the message so it is not retried, but record no
			// result. Returning the handler's context error here made the consumer
			// treat the cancellation as a failure and failTask overwrote the
			// cancelled status.
			return nil
		}
		if err != nil {
			return err
		}
		if res != nil {
			_ = d.Respond(runCtx, res)
		}
		return nil
	})
}

// registerTaskCancel records the cancel function for a running task handler.
func (b *Broker) registerTaskCancel(id string, cancel context.CancelFunc) {
	if id == "" {
		return
	}
	b.taskMu.Lock()
	if b.taskCancels == nil {
		b.taskCancels = map[string]context.CancelFunc{}
	}
	b.taskCancels[id] = cancel
	b.taskMu.Unlock()
}

func (b *Broker) clearTaskCancel(id string) {
	if id == "" {
		return
	}
	b.taskMu.Lock()
	delete(b.taskCancels, id)
	b.taskMu.Unlock()
}

// cancelTaskRun stops a running task handler, if one is running. It reports whether
// a handler was found.
func (b *Broker) cancelTaskRun(id string) bool {
	b.taskMu.Lock()
	cancel, ok := b.taskCancels[id]
	b.taskMu.Unlock()
	if !ok {
		return false
	}
	cancel()
	return true
}

// taskCancelled reports whether a task has been cancelled. Terminal transitions
// consult this so a cancelled task cannot be overwritten by the result of work that
// was already in flight when the cancel landed.
func (b *Broker) taskCancelled(id string) bool {
	b.taskMu.RLock()
	defer b.taskMu.RUnlock()
	t := b.tasks[id]
	return t != nil && t.Status == TaskCancelled
}

func (b *Broker) startTask(ctx context.Context, id string) {
	b.taskMu.Lock()
	t := b.tasks[id]
	if t != nil && t.Status == TaskCancelled {
		// Cancelled before the handler started. Leave it cancelled; resurrecting it
		// here is what made a cancel-before-start report success.
		b.taskMu.Unlock()
		return
	}
	if t != nil {
		t.Status = TaskRunning
		t.StartedAt = time.Now()
		t.UpdatedAt = t.StartedAt
	}
	ti := TaskInfo{}
	if t != nil {
		ti = cloneTask(*t)
	}
	b.taskMu.Unlock()
	if t != nil {
		b.persistTask(ctx, ti)
		b.notify.Publish(Notification{Type: "task.started", Source: b.cfg.Name, Severity: SeverityInfo, MessageID: ti.MessageID, TaskID: ti.ID, Queue: ti.Queue, TenantID: ti.TenantID, Timestamp: time.Now(), Data: map[string]any{"name": ti.Name}})
		if b.cfg.Hooks.OnTaskStarted != nil {
			b.cfg.Hooks.OnTaskStarted(ctx, TaskEvent{Task: ti, Time: time.Now()})
		}
	}
}
func (b *Broker) updateTaskProgress(ctx context.Context, id string, p Progress) {
	b.taskMu.Lock()
	t := b.tasks[id]
	if t != nil {
		t.Status = TaskProgressing
		t.Progress = p
		t.UpdatedAt = time.Now()
	}
	ti := TaskInfo{}
	if t != nil {
		ti = cloneTask(*t)
	}
	b.taskMu.Unlock()
	if t != nil {
		b.persistTask(ctx, ti)
		b.notify.Publish(Notification{Type: "task.progress", Source: b.cfg.Name, Severity: SeverityInfo, MessageID: ti.MessageID, TaskID: ti.ID, Queue: ti.Queue, TenantID: ti.TenantID, Timestamp: time.Now(), Data: map[string]any{"name": ti.Name, "percent": p.Percent, "message": p.Message}})
		if b.cfg.Hooks.OnTaskProgress != nil {
			b.cfg.Hooks.OnTaskProgress(ctx, TaskEvent{Task: ti, Time: time.Now()})
		}
	}
}
func (b *Broker) retryTask(ctx context.Context, id string, err error) {
	b.taskMu.Lock()
	t := b.tasks[id]
	if t != nil {
		t.Status = TaskRetrying
		t.Attempts++
		t.LastError = err.Error()
		t.UpdatedAt = time.Now()
	}
	ti := TaskInfo{}
	if t != nil {
		ti = cloneTask(*t)
	}
	b.taskMu.Unlock()
	if t != nil {
		b.persistTask(ctx, ti)
	}
	if t != nil && b.cfg.Hooks.OnTaskRetrying != nil {
		b.cfg.Hooks.OnTaskRetrying(ctx, TaskEvent{Task: ti, Error: err, Time: time.Now()})
	}
}
func (b *Broker) failTask(ctx context.Context, id string, err error) {
	b.taskMu.Lock()
	t := b.tasks[id]
	if t != nil && t.Status == TaskCancelled {
		// A cancelled task stays cancelled. A failure arriving from work that was
		// already in flight must not rewrite the operator's intent, and must not
		// wake waiters with a terminal result either.
		b.taskMu.Unlock()
		return
	}
	if t == nil {
		t = &TaskInfo{ID: id, Status: TaskFailed, CreatedAt: time.Now()}
		b.tasks[id] = t
	}
	t.Status = TaskFailed
	t.LastError = err.Error()
	t.UpdatedAt = time.Now()
	t.FinishedAt = time.Now()
	ti := cloneTask(*t)
	b.taskMu.Unlock()
	b.persistTask(ctx, ti)
	b.notify.Publish(Notification{Type: "task.failed", Source: b.cfg.Name, Severity: SeverityError, MessageID: ti.MessageID, TaskID: ti.ID, Queue: ti.Queue, TenantID: ti.TenantID, Timestamp: time.Now(), Data: map[string]any{"name": ti.Name, "error": err.Error()}})
	if b.cfg.Hooks.OnTaskFailed != nil {
		b.cfg.Hooks.OnTaskFailed(ctx, TaskEvent{Task: ti, Error: err, Time: time.Now()})
	}
	b.finishTaskWaiters(id)
}
func (b *Broker) completeTask(ctx context.Context, m Message, result []byte, err error) {
	if m.TaskID == "" {
		m.TaskID = m.Headers["x-task-id"]
	}
	if m.TaskID == "" {
		return
	}
	if err != nil {
		b.failTask(ctx, m.TaskID, err)
		return
	}
	b.taskMu.Lock()
	t := b.tasks[m.TaskID]
	if t != nil && t.Status == TaskCancelled {
		// The task was cancelled while the handler was running. Its result must not
		// resurrect it as succeeded.
		b.taskMu.Unlock()
		return
	}
	alreadySucceeded := false
	if t != nil {
		alreadySucceeded = t.Status == TaskSucceeded
		t.Status = TaskSucceeded
		if result != nil || len(t.Result) == 0 {
			t.Result = append([]byte(nil), result...)
		}
		t.UpdatedAt = time.Now()
		if t.FinishedAt.IsZero() {
			t.FinishedAt = time.Now()
		}
	}
	ti := TaskInfo{}
	if t != nil {
		ti = cloneTask(*t)
	}
	b.taskMu.Unlock()
	if t != nil {
		b.persistTask(ctx, ti)
	}
	if t != nil && !alreadySucceeded {
		b.notify.Publish(Notification{Type: "task.succeeded", Source: b.cfg.Name, Severity: SeverityInfo, MessageID: ti.MessageID, TaskID: ti.ID, Queue: ti.Queue, TenantID: ti.TenantID, Timestamp: time.Now(), Data: map[string]any{"name": ti.Name}})
		if b.cfg.Hooks.OnTaskSucceeded != nil {
			b.cfg.Hooks.OnTaskSucceeded(ctx, TaskEvent{Task: ti, Time: time.Now()})
		}
	}
	if !alreadySucceeded {
		b.finishTaskWaiters(m.TaskID)
	}
}
func (b *Broker) finishTaskWaiters(id string) {
	b.taskMu.Lock()
	t := b.tasks[id]
	waiters := b.taskWait[id]
	delete(b.taskWait, id)
	var res TaskResult
	if t != nil {
		res = TaskResult{Task: cloneTask(*t), Payload: append([]byte(nil), t.Result...)}
		if t.LastError != "" {
			res.Error = &BrokerError{Code: "task_failed", Message: t.LastError}
		}
	}
	b.taskMu.Unlock()
	for _, ch := range waiters {
		select {
		case ch <- res:
		default:
		}
	}
}
func cloneTask(t TaskInfo) TaskInfo {
	t.Metadata = cloneMap(t.Metadata)
	t.Result = append([]byte(nil), t.Result...)
	return t
}

func (b *Broker) persistTask(ctx context.Context, t TaskInfo) {
	if b != nil && b.stateStore != nil && t.ID != "" {
		_ = b.stateStore.PutTask(ctx, t)
	}
}
