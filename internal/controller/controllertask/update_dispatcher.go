package controllertask

import (
	"context"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// UpdateDispatcher selects the closed Agent/native recovery owners. Native
// bootstrap may be absent only while no native Controller update is claimed.
type UpdateDispatcher struct {
	agent      UpdateExecutor
	controller UpdateExecutor
	mu         sync.Mutex
	taskID     string
	active     UpdateExecutor
}

func NewUpdateDispatcher(agent, controller UpdateExecutor) (*UpdateDispatcher, error) {
	if agent == nil {
		return nil, errs.New(errs.KindInternal, "Agent update recovery is required")
	}
	return &UpdateDispatcher{agent: agent, controller: controller}, nil
}

func (dispatcher *UpdateDispatcher) owner(task etcd.TaskRecord) (UpdateExecutor, error) {
	if task.Executor != taskjournal.TaskExecutorController || !isPlatformUpdate(task) {
		return nil, errs.New(errs.KindValidationFailed, "platform update Task is invalid")
	}
	if task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceAgent {
		return dispatcher.agent, nil
	}
	if dispatcher.controller == nil {
		return nil, errs.New(errs.KindStateConflict, "native update recovery bootstrap is unavailable")
	}
	return dispatcher.controller, nil
}

func (dispatcher *UpdateDispatcher) Restore(ctx context.Context, claim etcd.TaskAssignment) error {
	owner, err := dispatcher.owner(claim.Task.Record)
	if err != nil {
		return err
	}
	if err := owner.Restore(ctx, claim); err != nil {
		return err
	}
	dispatcher.mu.Lock()
	dispatcher.taskID, dispatcher.active = claim.Task.Record.ID, owner
	dispatcher.mu.Unlock()
	return nil
}

func (dispatcher *UpdateDispatcher) Execute(
	ctx context.Context,
	task etcd.TaskRecord,
	deadline time.Time,
) (taskjournal.TaskStatus, error) {
	owner, err := dispatcher.owner(task)
	if err != nil {
		return "", err
	}
	return owner.Execute(ctx, task, deadline)
}

func (dispatcher *UpdateDispatcher) Abort(ctx context.Context, taskID string) error {
	dispatcher.mu.Lock()
	owner, active := dispatcher.active, dispatcher.taskID
	dispatcher.mu.Unlock()
	if owner == nil || active != taskID {
		return errs.New(errs.KindStateConflict, "platform update Task is not restored")
	}
	return owner.Abort(ctx, taskID)
}
