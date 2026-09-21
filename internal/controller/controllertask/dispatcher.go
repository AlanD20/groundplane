package controllertask

import (
	"context"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	corebackup "github.com/AlanD20/groundplane/internal/core/backup"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BackupKeyRotationExecutor interface {
	Execute(context.Context, etcd.TaskRecord) error
}

type Dispatcher struct {
	fallback  Handler
	rotations BackupKeyRotationExecutor
}

func NewDispatcher(fallback Handler, rotations BackupKeyRotationExecutor) (*Dispatcher, error) {
	if fallback == nil || rotations == nil {
		return nil, errs.New(errs.KindInternal, "Controller Task dispatcher dependencies are required")
	}
	return &Dispatcher{fallback: fallback, rotations: rotations}, nil
}

func (dispatcher *Dispatcher) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "Controller Task context is required")
	}
	if task.Type != taskjournal.TaskRotate {
		return dispatcher.fallback.Execute(ctx, task)
	}
	if task.Executor != taskjournal.TaskExecutorController ||
		task.Status != taskjournal.TaskStatusRunning ||
		ids.Validate(ids.KindTask, task.ID) != nil ||
		ids.Validate(ids.KindOperation, task.OperationID) != nil ||
		ids.Validate(ids.KindPlan, task.PlanID) != nil ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil ||
		task.Owner.EnvironmentID != task.Target ||
		task.PlanHash != corebackup.KeyRotationPlanHash() ||
		task.RenderGeneration != 1 ||
		task.TimeoutSeconds != corebackup.KeyRotationTimeoutSeconds ||
		len(task.Params) != 0 ||
		len(task.Steps) != 0 {
		return errs.New(errs.KindValidationFailed, "Controller backup key rotation Task is invalid")
	}
	return dispatcher.rotations.Execute(ctx, task)
}
