package dispatch

import (
	"context"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type controllerTaskExecutor interface {
	Execute(context.Context, etcd.TaskRecord) error
}

type HierarchyExecutor interface {
	Execute(context.Context, string) error
}

type HierarchyDispatcher struct {
	fallback  controllerTaskExecutor
	deletions HierarchyExecutor
}

func NewHierarchyDispatcher(
	fallback controllerTaskExecutor,
	deletions HierarchyExecutor,
) (*HierarchyDispatcher, error) {
	if fallback == nil || deletions == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy deletion Task dispatcher dependencies are required")
	}
	return &HierarchyDispatcher{fallback: fallback, deletions: deletions}, nil
}

func (dispatcher *HierarchyDispatcher) Execute(
	ctx context.Context,
	task etcd.TaskRecord,
) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "hierarchy deletion Task context is required")
	}
	if task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceHierarchyDeletion {
		return dispatcher.fallback.Execute(ctx, task)
	}
	if err := validateHierarchyDeletionTask(task); err != nil {
		return err
	}
	return dispatcher.deletions.Execute(ctx, task.ID)
}

func validateHierarchyDeletionTask(task etcd.TaskRecord) error {
	if task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskRemove ||
		ids.Validate(ids.KindTask, task.ID) != nil || len(task.Params) != 3 ||
		task.Params[taskjournal.TaskHierarchyDeletionOperationParam] == "" {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion Controller Task is invalid")
	}
	targetKind := hierarchydeletion.TargetKind(task.Params[taskjournal.TaskHierarchyDeletionTargetKindParam])
	var idKind ids.Kind
	switch targetKind {
	case hierarchydeletion.TargetTenant:
		idKind = ids.KindTenant
	case hierarchydeletion.TargetProject:
		idKind = ids.KindProject
	case hierarchydeletion.TargetEnvironment:
		idKind = ids.KindEnvironment
	case hierarchydeletion.TargetBackingService:
		idKind = ids.KindProject
	default:
		return errs.New(errs.KindValidationFailed, "hierarchy deletion Controller Task target kind is invalid")
	}
	if ids.Validate(idKind, task.Target) != nil {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion Controller Task target is invalid")
	}
	return nil
}
