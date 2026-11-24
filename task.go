package broker

import (
	"context"
	"errors"
	"fmt"
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
	info, err := h.Info(ctx)
	if err != nil {
		return TaskResult{}, err
	}
	if result, terminal := terminalTaskResult(info); terminal {
		return result, result.Error
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTaskWait)
		defer cancel()
	}
	ch := make(chan TaskResult, 1)
	h.b.taskMu.Lock()
	if t := h.b.tasks[h.id]; t != nil {
		if res, terminal := terminalTaskResult(*t); terminal {
			h.b.taskMu.Unlock()
			return res, res.Error
		}
	}
	h.b.taskWait[h.id] = append(h.b.taskWait[h.id], ch)
	h.b.taskMu.Unlock()
	select {
	case r := <-ch:
		return r, r.Error
	case <-ctx.Done():
		h.b.removeTaskWaiter(h.id, ch)
		return TaskResult{}, ctx.Err()
	}
}

func (b *Broker) removeTaskWaiter(id string, waiter chan TaskResult) {
	b.taskMu.Lock()
	waiters := b.taskWait[id]
	for i, ch := range waiters {
		if ch != waiter {
			continue
		}
		copy(waiters[i:], waiters[i+1:])
		waiters[len(waiters)-1] = nil
		waiters = waiters[:len(waiters)-1]
		if len(waiters) == 0 {
			delete(b.taskWait, id)
		} else {
			b.taskWait[id] = waiters
		}
		break
	}
	b.taskMu.Unlock()
}

func terminalTaskResult(task TaskInfo) (TaskResult, bool) {
	if isTerminalTaskStatus(task.Status) {
		result := TaskResult{Task: cloneTask(task), Payload: append([]byte(nil), task.Result...)}
		if task.LastError != "" {
			result.Error = &BrokerError{Code: "task_failed", Message: task.LastError}
		}
		return result, true
	}
	return TaskResult{}, false
}

func isTerminalTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskSucceeded, TaskFailed, TaskCancelled, TaskDeadLettered:
		return true
	default:
		return false
	}
}

func (h taskHandle) Cancel(ctx context.Context, reason string) error {
	info, err := h.Info(ctx)
	if err != nil {
		return err
	}
	h.b.taskMu.Lock()
	current := h.b.tasks[h.id]
	if current == nil {
		current = &info
	}
	switch current.Status {
	case TaskCancelled:
		h.b.taskMu.Unlock()
		return nil
	case TaskSucceeded, TaskFailed, TaskDeadLettered:
		status := current.Status
		h.b.taskMu.Unlock()
		return errf(ErrInvalidMessage, "task %s is already terminal with status %s", h.id, status)
	}
	next := cloneTask(*current)
	next.Status = TaskCancelled
	next.LastError = reason
	next.UpdatedAt = time.Now()
	next.FinishedAt = next.UpdatedAt
	if err := h.b.persistTask(ctx, next); err != nil {
		h.b.taskMu.Unlock()
		return fmt.Errorf("persist task cancellation: %w", err)
	}
	h.b.tasks[h.id] = &next
	h.b.taskMu.Unlock()
	ti := cloneTask(next)
	var cancelErrors []error
	// Stop the work, not just the bookkeeping. Without this the handler kept
	// running and later overwrote the cancellation with its own result.
	h.b.cancelTaskRun(h.id)
	if ti.MessageID != "" {
		msg, ok, getErr := h.b.store.Get(ctx, ti.MessageID)
		if getErr != nil {
			cancelErrors = append(cancelErrors, fmt.Errorf("load task message for cancellation: %w", getErr))
		} else if ok {
			msg.Status = MessageCancelled
			msg.LastError = reason
			if err := h.b.store.Update(ctx, msg); err != nil {
				cancelErrors = append(cancelErrors, fmt.Errorf("cancel task message: %w", err))
			} else {
				n := Notification{Type: "task.cancelled", Source: h.b.cfg.Name, Severity: SeverityWarn, MessageID: msg.ID, TaskID: h.id, Queue: msg.Queue, Consumer: msg.ConsumerID, ConsumerGroup: msg.ConsumerGroup, TenantID: msg.TenantID, TraceID: msg.TraceID, Timestamp: time.Now(), Data: map[string]any{"reason": reason}}
				h.b.notify.Publish(n)
				if msg.ConsumerID != "" {
					h.b.emitConsumerNotification(msg.ConsumerID, n)
				}
			}
		}
	}
	if h.b.cfg.Hooks.OnTaskCancelled != nil {
		h.b.cfg.Hooks.OnTaskCancelled(ctx, TaskEvent{Task: ti, Error: errf(ErrCancelled, "%s", reason), Time: time.Now()})
	}
	h.b.finishTaskWaiters(h.id)
	return errors.Join(cancelErrors...)
}
func (h taskHandle) Retry(ctx context.Context) error {
	t, err := h.Info(ctx)
	if err != nil {
		return err
	}
	h.b.taskMu.Lock()
	current := h.b.tasks[h.id]
	if current == nil {
		restored := cloneTask(t)
		current = &restored
	}
	if current.Status != TaskFailed && current.Status != TaskDeadLettered {
		status := current.Status
		h.b.taskMu.Unlock()
		return errf(ErrInvalidMessage, "task %s is not in a retryable state: %s", h.id, status)
	}
	if current.MessageID == "" {
		h.b.taskMu.Unlock()
		return ErrMessageNotFound
	}
	msg, ok, err := h.b.store.Get(ctx, current.MessageID)
	if err != nil {
		h.b.taskMu.Unlock()
		return err
	}
	if !ok {
		h.b.taskMu.Unlock()
		return ErrMessageNotFound
	}
	sourceType := DestinationQueue
	if msg.Status == MessageDeadLettered {
		sourceType = DestinationDLQ
	}
	mode := MoveReplay
	preserveID := false
	if sourceType == DestinationDLQ || msg.Queue != t.Queue {
		mode = MoveReroute
		preserveID = true
	}
	result, err := h.b.store.Move(ctx, MoveRequest{
		SourceType: sourceType, Source: msg.Queue,
		DestinationType: DestinationQueue, Destination: t.Queue,
		MessageIDs: []string{msg.ID}, Mode: mode, PreserveID: preserveID,
		ResetAttempts: true, ResetError: true, Reason: "task retry",
	})
	if err != nil {
		h.b.taskMu.Unlock()
		return err
	}
	if result.Moved == 0 && result.Copied == 0 && len(result.DestinationMessageIDs) == 0 {
		h.b.taskMu.Unlock()
		return ErrMessageNotFound
	}

	next := cloneTask(*current)
	if len(result.DestinationMessageIDs) > 0 {
		// Replay copies the message, so the task must follow the destination ID
		// for cancellation, logs, and any later retry. Reroutes preserve their ID.
		next.MessageID = result.DestinationMessageIDs[0]
	}
	next.Status = TaskQueued
	if msg.AvailableAt.After(time.Now()) {
		next.Status = TaskScheduled
	}
	next.Attempts = 0
	next.LastError = ""
	next.FinishedAt = time.Time{}
	next.UpdatedAt = time.Now()
	if err := h.b.persistTask(ctx, next); err != nil {
		// The message move has committed. Keep the live task runnable so the
		// consumer can recover it; the caller receives the persistence failure and
		// the next task transition will try to restore durable state.
		h.b.tasks[h.id] = &next
		h.b.taskMu.Unlock()
		return fmt.Errorf("task %s was requeued but status persistence failed: %w", h.id, err)
	}
	h.b.tasks[h.id] = &next
	h.b.taskMu.Unlock()
	return nil
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
	if _, exists := p.b.tasks[id]; exists {
		p.b.taskMu.Unlock()
		return nil, ErrTaskAlreadyExists
	}
	if p.b.stateStore != nil {
		if _, exists, err := p.b.stateStore.GetTask(ctx, id); err != nil {
			p.b.taskMu.Unlock()
			return nil, fmt.Errorf("check task %s before submit: %w", id, err)
		} else if exists {
			p.b.taskMu.Unlock()
			return nil, ErrTaskAlreadyExists
		}
	}
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
	if err := p.b.persistTask(ctx, info); err != nil {
		p.b.taskMu.Lock()
		if current := p.b.tasks[id]; current != nil && current.Status == TaskCreated && current.CreatedAt.Equal(now) {
			delete(p.b.tasks, id)
		}
		p.b.taskMu.Unlock()
		return nil, fmt.Errorf("persist task %s before publish: %w", id, err)
	}
	if p.b.cfg.Hooks.OnTaskCreated != nil {
		p.b.cfg.Hooks.OnTaskCreated(ctx, TaskEvent{Task: info, Time: now})
	}
	pubOpts := []PublishOption{WithQueue(queue), WithType(name), WithTaskHeaders(id, name), WithPriority(o.Priority), WithDelay(o.Delay), WithRunAt(o.RunAt), WithTenantID(o.TenantID), WithIdempotencyKey(o.IdempotencyKey), WithMetadata(o.Metadata), WithMaxAttempts(o.MaxAttempts)}
	res, err := p.Publish(ctx, name, payload, pubOpts...)
	if err != nil {
		p.b.recordPublisher(ctx, PublisherRecord{ID: id, Operation: "submit_task", Subject: name, Queue: queue, TaskID: id, Status: "failed", Error: err.Error(), CreatedAt: now, UpdatedAt: time.Now()})
		if taskErr := p.b.failTask(ctx, id, err); taskErr != nil {
			return taskHandle{b: p.b, id: id}, errors.Join(err, fmt.Errorf("persist failed task state: %w", taskErr))
		}
		return nil, err
	}
	p.b.taskMu.Lock()
	current := p.b.tasks[id]
	if current == nil {
		p.b.taskMu.Unlock()
		return taskHandle{b: p.b, id: id}, fmt.Errorf("task %s was published but its local state disappeared", id)
	}
	next := cloneTask(*current)
	if next.Status == TaskCreated {
		next.Status = TaskQueued
		if o.Delay > 0 || !o.RunAt.IsZero() {
			next.Status = TaskScheduled
		}
	}
	next.MessageID = res.MessageID
	next.UpdatedAt = time.Now()
	if err := p.b.persistTask(ctx, next); err != nil {
		p.b.taskMu.Unlock()
		return taskHandle{b: p.b, id: id}, fmt.Errorf("task %s was published but status persistence failed: %w", id, err)
	}
	p.b.tasks[id] = &next
	info = cloneTask(next)
	p.b.taskMu.Unlock()
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
		if h != nil {
			return h
		}
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
		if !b.registerTaskCancel(tid, cancel) {
			cancel()
			return nil
		}
		defer func() {
			cancel()
			b.clearTaskCancel(tid)
		}()
		if runCtx.Err() != nil || b.taskTerminal(tid) {
			return nil
		}
		td := TaskDelivery{Delivery: d, Task: ti}
		res, err := handler(runCtx, td)
		if b.taskTerminal(tid) {
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
func (b *Broker) registerTaskCancel(id string, cancel context.CancelFunc) bool {
	if id == "" {
		return true
	}
	b.taskMu.Lock()
	if t := b.tasks[id]; t != nil && (t.Status == TaskCancelled || t.Status == TaskSucceeded) {
		b.taskMu.Unlock()
		return false
	}
	if b.taskCancels == nil {
		b.taskCancels = map[string]context.CancelFunc{}
	}
	b.taskCancels[id] = cancel
	b.taskMu.Unlock()
	return true
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

// taskTerminal reports whether a task has reached a terminal state. Handler
// wrappers use it to avoid replacing a terminal task result with late work.
func (b *Broker) taskTerminal(id string) bool {
	b.taskMu.RLock()
	defer b.taskMu.RUnlock()
	t := b.tasks[id]
	return t != nil && isTerminalTaskStatus(t.Status)
}

func (b *Broker) hydrateTask(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	b.taskMu.RLock()
	current := b.tasks[id]
	if current != nil {
		tenant := current.TenantID
		b.taskMu.RUnlock()
		if !tenantAllowed(ctx, tenant) {
			return ErrUnauthorized
		}
		return nil
	}
	b.taskMu.RUnlock()
	if b.stateStore == nil {
		return nil
	}
	restored, ok, err := b.stateStore.GetTask(ctx, id)
	if err != nil || !ok {
		return err
	}
	if !tenantAllowed(ctx, restored.TenantID) {
		return ErrUnauthorized
	}
	b.taskMu.Lock()
	if current = b.tasks[id]; current == nil {
		copy := cloneTask(restored)
		b.tasks[id] = &copy
	} else if !tenantAllowed(ctx, current.TenantID) {
		b.taskMu.Unlock()
		return ErrUnauthorized
	}
	b.taskMu.Unlock()
	return nil
}

func (b *Broker) startTask(ctx context.Context, id string) (bool, error) {
	if err := b.hydrateTask(ctx, id); err != nil {
		return false, err
	}
	b.taskMu.Lock()
	t := b.tasks[id]
	if t != nil && (t.Status == TaskCancelled || t.Status == TaskSucceeded) {
		// Cancellation and success are final. Failed/dead-lettered tasks can be
		// retried, including recovery when the message move committed before the
		// task status write did.
		b.taskMu.Unlock()
		return false, nil
	}
	if t == nil {
		b.taskMu.Unlock()
		return true, nil
	}
	next := cloneTask(*t)
	next.Status = TaskRunning
	next.StartedAt = time.Now()
	next.UpdatedAt = next.StartedAt
	if err := b.persistTask(ctx, next); err != nil {
		b.taskMu.Unlock()
		return false, fmt.Errorf("persist task %s start: %w", id, err)
	}
	*t = next
	b.taskMu.Unlock()
	ti := cloneTask(next)
	b.notify.Publish(Notification{Type: "task.started", Source: b.cfg.Name, Severity: SeverityInfo, MessageID: ti.MessageID, TaskID: ti.ID, Queue: ti.Queue, TenantID: ti.TenantID, Timestamp: time.Now(), Data: map[string]any{"name": ti.Name}})
	if b.cfg.Hooks.OnTaskStarted != nil {
		b.cfg.Hooks.OnTaskStarted(ctx, TaskEvent{Task: ti, Time: time.Now()})
	}
	return true, nil
}
func (b *Broker) updateTaskProgress(ctx context.Context, id string, p Progress) error {
	if err := b.hydrateTask(ctx, id); err != nil {
		return err
	}
	b.taskMu.Lock()
	t := b.tasks[id]
	if t == nil {
		b.taskMu.Unlock()
		return nil
	}
	next := cloneTask(*t)
	next.Status = TaskProgressing
	next.Progress = p
	next.UpdatedAt = time.Now()
	if err := b.persistTask(ctx, next); err != nil {
		b.taskMu.Unlock()
		return err
	}
	*t = next
	b.taskMu.Unlock()
	ti := cloneTask(next)
	b.notify.Publish(Notification{Type: "task.progress", Source: b.cfg.Name, Severity: SeverityInfo, MessageID: ti.MessageID, TaskID: ti.ID, Queue: ti.Queue, TenantID: ti.TenantID, Timestamp: time.Now(), Data: map[string]any{"name": ti.Name, "percent": p.Percent, "message": p.Message}})
	if b.cfg.Hooks.OnTaskProgress != nil {
		b.cfg.Hooks.OnTaskProgress(ctx, TaskEvent{Task: ti, Time: time.Now()})
	}
	return nil
}
func (b *Broker) retryTask(ctx context.Context, id string, err error) error {
	if err := b.hydrateTask(ctx, id); err != nil {
		return fmt.Errorf("load task %s before retry transition: %w", id, err)
	}
	b.taskMu.Lock()
	current := b.tasks[id]
	if current == nil {
		b.taskMu.Unlock()
		return nil
	}
	if isTerminalTaskStatus(current.Status) {
		b.taskMu.Unlock()
		return nil
	}
	next := cloneTask(*current)
	next.Status = TaskRetrying
	next.Attempts++
	next.LastError = err.Error()
	next.UpdatedAt = time.Now()
	if persistErr := b.persistTask(ctx, next); persistErr != nil {
		b.taskMu.Unlock()
		return fmt.Errorf("persist task %s retry transition: %w", id, persistErr)
	}
	*b.tasks[id] = next
	b.taskMu.Unlock()
	if b.cfg.Hooks.OnTaskRetrying != nil {
		b.cfg.Hooks.OnTaskRetrying(ctx, TaskEvent{Task: next, Error: err, Time: time.Now()})
	}
	return nil
}
func (b *Broker) failTask(ctx context.Context, id string, err error) error {
	if loadErr := b.hydrateTask(ctx, id); loadErr != nil {
		return fmt.Errorf("load task %s before failure transition: %w", id, loadErr)
	}
	b.taskMu.Lock()
	current := b.tasks[id]
	if current != nil && isTerminalTaskStatus(current.Status) {
		// A cancelled task stays cancelled. A failure arriving from work that was
		// already in flight must not rewrite the operator's intent, and must not
		// wake waiters with a terminal result either.
		b.taskMu.Unlock()
		return nil
	}
	now := time.Now()
	next := TaskInfo{ID: id, CreatedAt: now}
	if current != nil {
		next = cloneTask(*current)
	}
	next.Status = TaskFailed
	next.LastError = err.Error()
	next.UpdatedAt = now
	next.FinishedAt = now
	if persistErr := b.persistTask(ctx, next); persistErr != nil {
		b.taskMu.Unlock()
		return fmt.Errorf("persist task %s failure transition: %w", id, persistErr)
	}
	b.tasks[id] = &next
	b.taskMu.Unlock()
	ti := cloneTask(next)
	b.notify.Publish(Notification{Type: "task.failed", Source: b.cfg.Name, Severity: SeverityError, MessageID: ti.MessageID, TaskID: ti.ID, Queue: ti.Queue, TenantID: ti.TenantID, Timestamp: time.Now(), Data: map[string]any{"name": ti.Name, "error": err.Error()}})
	if b.cfg.Hooks.OnTaskFailed != nil {
		b.cfg.Hooks.OnTaskFailed(ctx, TaskEvent{Task: ti, Error: err, Time: time.Now()})
	}
	b.finishTaskWaiters(id)
	return nil
}
func (b *Broker) completeTask(ctx context.Context, m Message, result []byte, err error) error {
	if m.TaskID == "" {
		m.TaskID = m.Headers["x-task-id"]
	}
	if m.TaskID == "" {
		return nil
	}
	if err := b.hydrateTask(ctx, m.TaskID); err != nil {
		return fmt.Errorf("load task %s before completion transition: %w", m.TaskID, err)
	}
	if err != nil {
		return b.failTask(ctx, m.TaskID, err)
	}
	b.taskMu.Lock()
	current := b.tasks[m.TaskID]
	if current != nil && isTerminalTaskStatus(current.Status) {
		// A terminal task result must not be replaced by work that was already in
		// flight when the terminal transition landed.
		b.taskMu.Unlock()
		return nil
	}
	if current == nil {
		b.taskMu.Unlock()
		return nil
	}
	alreadySucceeded := current.Status == TaskSucceeded
	if alreadySucceeded {
		b.taskMu.Unlock()
		return nil
	}
	now := time.Now()
	next := cloneTask(*current)
	next.Status = TaskSucceeded
	if result != nil || len(next.Result) == 0 {
		next.Result = append([]byte(nil), result...)
	}
	next.UpdatedAt = now
	if next.FinishedAt.IsZero() {
		next.FinishedAt = now
	}
	if persistErr := b.persistTask(ctx, next); persistErr != nil {
		b.taskMu.Unlock()
		return fmt.Errorf("persist task %s completion transition: %w", m.TaskID, persistErr)
	}
	b.tasks[m.TaskID] = &next
	b.taskMu.Unlock()
	ti := cloneTask(next)
	if !alreadySucceeded {
		b.notify.Publish(Notification{Type: "task.succeeded", Source: b.cfg.Name, Severity: SeverityInfo, MessageID: ti.MessageID, TaskID: ti.ID, Queue: ti.Queue, TenantID: ti.TenantID, Timestamp: time.Now(), Data: map[string]any{"name": ti.Name}})
		if b.cfg.Hooks.OnTaskSucceeded != nil {
			b.cfg.Hooks.OnTaskSucceeded(ctx, TaskEvent{Task: ti, Time: time.Now()})
		}
	}
	b.finishTaskWaiters(m.TaskID)
	return nil
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

func (b *Broker) persistTask(ctx context.Context, t TaskInfo) error {
	if b != nil && b.stateStore != nil && t.ID != "" {
		return b.stateStore.PutTask(ctx, t)
	}
	return nil
}
