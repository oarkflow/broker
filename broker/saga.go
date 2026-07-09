package broker

import (
	"context"
	"sync"
	"time"
)

type SagaStatus string

const (
	SagaCreated      SagaStatus = "created"
	SagaExecuting    SagaStatus = "executing"
	SagaCompleted    SagaStatus = "completed"
	SagaCompensating SagaStatus = "compensating"
	SagaCompensated  SagaStatus = "compensated"
	SagaFailed       SagaStatus = "failed"
)

type SagaStepAction struct {
	TaskName string      `json:"task_name"`
	Queue    string      `json:"queue,omitempty"`
	Payload  any         `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Timeout  time.Duration `json:"timeout,omitempty"`
}

type SagaStep struct {
	Name        string         `json:"name"`
	Action      SagaStepAction `json:"action"`
	Compensate  SagaStepAction `json:"compensate,omitempty"`
	Optional    bool           `json:"optional,omitempty"`
}

type SagaDefinition struct {
	Name        string            `json:"name"`
	ID          string            `json:"id,omitempty"`
	Steps       []SagaStep        `json:"steps"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	TenantID    string            `json:"tenant_id,omitempty"`
	Timeout     time.Duration     `json:"timeout,omitempty"`
}

type SagaStepResult struct {
	StepName    string     `json:"step_name"`
	TaskID      string     `json:"task_id,omitempty"`
	Status      TaskStatus `json:"status"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt time.Time  `json:"completed_at,omitempty"`
	Error       string     `json:"error,omitempty"`
	Result      []byte     `json:"result,omitempty"`
}

type SagaInstance struct {
	SagaDefinition
	ID        string            `json:"id"`
	Status    SagaStatus        `json:"status"`
	Results   []SagaStepResult  `json:"results"`
	CurrentStep int             `json:"current_step"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	FinishedAt time.Time        `json:"finished_at,omitempty"`
	Error     string            `json:"error,omitempty"`
	mu        sync.Mutex        `json:"-"`
}

func (s *SagaInstance) clone() SagaInstance {
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]SagaStepResult, len(s.Results))
	copy(results, s.Results)
	steps := make([]SagaStep, len(s.Steps))
	copy(steps, s.Steps)
	meta := cloneMap(s.Metadata)
	return SagaInstance{
		SagaDefinition: SagaDefinition{
			Name: s.Name, ID: s.ID, Steps: steps,
			Metadata: meta, TenantID: s.TenantID, Timeout: s.Timeout,
		},
		ID: s.ID, Status: s.Status, Results: results,
		CurrentStep: s.CurrentStep, CreatedAt: s.CreatedAt,
		UpdatedAt: s.UpdatedAt, FinishedAt: s.FinishedAt, Error: s.Error,
	}
}

type SagaEngine struct {
	b      *Broker
	mu     sync.RWMutex
	sagas  map[string]*SagaInstance
}

func NewSagaEngine(b *Broker) *SagaEngine {
	return &SagaEngine{b: b, sagas: map[string]*SagaInstance{}}
}

func (e *SagaEngine) CreateSaga(ctx context.Context, def SagaDefinition) *SagaInstance {
	id := def.ID
	if id == "" {
		id = e.b.nextID("saga")
	}
	now := time.Now()
	inst := &SagaInstance{
		SagaDefinition: def,
		ID:             id,
		Status:         SagaCreated,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	e.mu.Lock()
	e.sagas[id] = inst
	e.mu.Unlock()
	e.b.notify.Publish(Notification{
		Type: "saga.created", Source: e.b.cfg.Name, Severity: SeverityInfo,
		TaskID: id, TenantID: def.TenantID, Timestamp: now,
		Data: map[string]any{"name": def.Name, "steps": len(def.Steps)},
	})
	return inst
}

func (e *SagaEngine) GetSaga(id string) (SagaInstance, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s, ok := e.sagas[id]
	if !ok {
		return SagaInstance{}, false
	}
	return s.clone(), true
}

func (e *SagaEngine) ListSagas() []SagaInstance {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]SagaInstance, 0, len(e.sagas))
	for _, s := range e.sagas {
		out = append(out, s.clone())
	}
	return out
}

func (e *SagaEngine) ListSagasByStatus(st SagaStatus) []SagaInstance {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]SagaInstance, 0)
	for _, s := range e.sagas {
		if s.Status == st {
			out = append(out, s.clone())
		}
	}
	return out
}

func (e *SagaEngine) ExecuteSaga(ctx context.Context, sagaID string) error {
	e.mu.RLock()
	s, ok := e.sagas[sagaID]
	e.mu.RUnlock()
	if !ok {
		return ErrMessageNotFound
	}
	s.mu.Lock()
	if s.Status != SagaCreated {
		s.mu.Unlock()
		return errf(ErrInvalidMessage, "saga %s already started", sagaID)
	}
	s.Status = SagaExecuting
	s.UpdatedAt = time.Now()
	s.mu.Unlock()
	go e.executeSteps(s)
	return nil
}

func (e *SagaEngine) executeSteps(s *SagaInstance) {
	ctx := context.Background()
	for i := s.CurrentStep; i < len(s.Steps); i++ {
		step := s.Steps[i]
		s.mu.Lock()
		s.CurrentStep = i
		s.UpdatedAt = time.Now()
		s.mu.Unlock()
		taskID := e.b.nextID("saga-" + s.ID + "-" + step.Name)
		opts := []TaskOption{
			WithTaskQueue(step.Action.Queue),
			WithTaskID(taskID),
			WithTaskTenantID(s.TenantID),
			WithTaskMetadata(step.Action.Metadata),
		}
		if step.Action.Timeout > 0 {
			opts = append(opts, WithTaskTimeout(step.Action.Timeout))
		}
		if s.Timeout > 0 {
			opts = append(opts, WithTaskTimeout(s.Timeout))
		}
		if e.b.cfg.Hooks.OnTaskCompensating != nil {
			e.b.cfg.Hooks.OnTaskCompensating(ctx, TaskEvent{
				Task: TaskInfo{
					ID: taskID, Name: step.Action.TaskName,
					Queue: step.Action.Queue, Status: TaskRunning,
					TenantID: s.TenantID,
				},
				Time: time.Now(),
			})
		}
		e.b.notify.Publish(Notification{
			Type: "saga.step_executing", Source: e.b.cfg.Name,
			Severity: SeverityInfo, TaskID: s.ID, Timestamp: time.Now(),
			Data: map[string]any{
				"saga_name": s.Name, "step": step.Name,
				"step_index": i, "task_id": taskID,
			},
		})
		h, err := e.b.Publisher().SubmitTask(ctx, step.Action.TaskName, step.Action.Payload, opts...)
		if err != nil {
			e.failSaga(s, i, step.Name, err.Error())
			return
		}
		res, err := h.Wait(ctx)
		sr := SagaStepResult{
			StepName:  step.Name,
			TaskID:    taskID,
			Status:    TaskSucceeded,
			StartedAt: time.Now(),
		}
		if err != nil {
			sr.Status = TaskFailed
			sr.Error = err.Error()
			sr.CompletedAt = time.Now()
			e.appendResult(s, sr)
			if step.Optional {
				continue
			}
			e.failSaga(s, i, step.Name, err.Error())
			e.compensate(s, i)
			return
		}
		sr.Result = res.Payload
		sr.CompletedAt = time.Now()
		e.appendResult(s, sr)
	}
	s.mu.Lock()
	s.Status = SagaCompleted
	s.FinishedAt = time.Now()
	s.UpdatedAt = s.FinishedAt
	s.mu.Unlock()
	e.b.notify.Publish(Notification{
		Type: "saga.completed", Source: e.b.cfg.Name,
		Severity: SeverityInfo, TaskID: s.ID, Timestamp: time.Now(),
		Data: map[string]any{"saga_name": s.Name, "steps": len(s.Steps)},
	})
	if e.b.cfg.Hooks.OnTaskSucceeded != nil {
		e.b.cfg.Hooks.OnTaskSucceeded(ctx, TaskEvent{
			Task: TaskInfo{ID: s.ID, Name: s.Name, Status: TaskSucceeded},
			Time: time.Now(),
		})
	}
}

func (e *SagaEngine) compensate(s *SagaInstance, failedAt int) {
	ctx := context.Background()
	s.mu.Lock()
	s.Status = SagaCompensating
	s.UpdatedAt = time.Now()
	s.mu.Unlock()
	e.b.notify.Publish(Notification{
		Type: "saga.compensating", Source: e.b.cfg.Name,
		Severity: SeverityWarn, TaskID: s.ID, Timestamp: time.Now(),
		Data: map[string]any{"saga_name": s.Name, "failed_at_step": failedAt},
	})
	if e.b.cfg.Hooks.OnTaskCompensating != nil {
		e.b.cfg.Hooks.OnTaskCompensating(ctx, TaskEvent{
			Task: TaskInfo{ID: s.ID, Name: s.Name, Status: TaskCompensating},
			Time: time.Now(),
		})
	}
	for i := failedAt - 1; i >= 0; i-- {
		step := s.Steps[i]
		if step.Compensate.TaskName == "" {
			continue
		}
		compTaskID := e.b.nextID("saga-comp-" + s.ID + "-" + step.Name)
		opts := []TaskOption{
			WithTaskQueue(step.Compensate.Queue),
			WithTaskID(compTaskID),
			WithTaskTenantID(s.TenantID),
		}
		e.b.notify.Publish(Notification{
			Type: "saga.compensating_step", Source: e.b.cfg.Name,
			Severity: SeverityWarn, TaskID: s.ID, Timestamp: time.Now(),
			Data: map[string]any{
				"saga_name": s.Name, "step": step.Name,
				"comp_task_id": compTaskID,
			},
		})
		h, err := e.b.Publisher().SubmitTask(ctx, step.Compensate.TaskName, step.Compensate.Payload, opts...)
		if err != nil {
			e.b.notify.Publish(Notification{
				Type: "saga.compensation_failed", Source: e.b.cfg.Name,
				Severity: SeverityError, TaskID: s.ID, Timestamp: time.Now(),
				Data: map[string]any{
					"saga_name": s.Name, "step": step.Name, "error": err.Error(),
				},
			})
			continue
		}
		_, _ = h.Wait(ctx)
	}
	s.mu.Lock()
	s.Status = SagaCompensated
	s.FinishedAt = time.Now()
	s.UpdatedAt = s.FinishedAt
	s.mu.Unlock()
	e.b.notify.Publish(Notification{
		Type: "saga.compensated", Source: e.b.cfg.Name,
		Severity: SeverityWarn, TaskID: s.ID, Timestamp: time.Now(),
		Data: map[string]any{"saga_name": s.Name},
	})
	if e.b.cfg.Hooks.OnTaskCompensated != nil {
		e.b.cfg.Hooks.OnTaskCompensated(ctx, TaskEvent{
			Task: TaskInfo{ID: s.ID, Name: s.Name, Status: TaskCompensated},
			Time: time.Now(),
		})
	}
}

func (e *SagaEngine) failSaga(s *SagaInstance, step int, stepName, errStr string) {
	s.mu.Lock()
	s.Status = SagaFailed
	s.Error = errStr
	s.UpdatedAt = time.Now()
	s.mu.Unlock()
	e.b.notify.Publish(Notification{
		Type: "saga.failed", Source: e.b.cfg.Name,
		Severity: SeverityError, TaskID: s.ID, Timestamp: time.Now(),
		Data: map[string]any{
			"saga_name": s.Name, "step": stepName,
			"step_index": step, "error": errStr,
		},
	})
	if e.b.cfg.Hooks.OnTaskFailed != nil {
		e.b.cfg.Hooks.OnTaskFailed(context.Background(), TaskEvent{
			Task: TaskInfo{ID: s.ID, Name: s.Name, Status: TaskFailed, LastError: errStr},
			Error: errf(ErrPermanent, errStr), Time: time.Now(),
		})
	}
}

func (e *SagaEngine) appendResult(s *SagaInstance, sr SagaStepResult) {
	s.mu.Lock()
	s.Results = append(s.Results, sr)
	s.UpdatedAt = time.Now()
	s.mu.Unlock()
	e.b.notify.Publish(Notification{
		Type: "saga.step_completed", Source: e.b.cfg.Name,
		Severity: SeverityInfo, TaskID: s.ID, Timestamp: time.Now(),
		Data: map[string]any{
			"saga_name": s.Name, "step": sr.StepName,
			"task_id": sr.TaskID, "status": sr.Status,
			"error": sr.Error,
		},
	})
}
