package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type controllerTaskExecutor interface {
	Execute(context.Context, etcd.TaskRecord) error
}

type hierarchyDeletionTaskExecutor interface {
	Execute(context.Context, string) error
}

type hierarchyDeletionTaskDispatcher struct {
	fallback  controllerTaskExecutor
	deletions hierarchyDeletionTaskExecutor
}

func newHierarchyDeletionTaskDispatcher(
	fallback controllerTaskExecutor,
	deletions hierarchyDeletionTaskExecutor,
) (*hierarchyDeletionTaskDispatcher, error) {
	if fallback == nil || deletions == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy deletion Task dispatcher dependencies are required")
	}
	return &hierarchyDeletionTaskDispatcher{fallback: fallback, deletions: deletions}, nil
}

func (dispatcher *hierarchyDeletionTaskDispatcher) Execute(
	ctx context.Context,
	task etcd.TaskRecord,
) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "hierarchy deletion Task context is required")
	}
	if task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceHierarchyDeletion {
		return dispatcher.fallback.Execute(ctx, task)
	}
	if err := validateHierarchyDeletionTask(task); err != nil {
		return err
	}
	return dispatcher.deletions.Execute(ctx, task.ID)
}

func validateHierarchyDeletionTask(task etcd.TaskRecord) error {
	if task.Executor != etcd.TaskExecutorController || task.Type != etcd.TaskRemove ||
		ids.Validate(ids.KindTask, task.ID) != nil || len(task.Params) != 3 ||
		task.Params[etcd.TaskHierarchyDeletionOperationParam] == "" {
		return errs.New(errs.KindValidationFailed, "hierarchy deletion Controller Task is invalid")
	}
	targetKind := hierarchydeletion.TargetKind(task.Params[etcd.TaskHierarchyDeletionTargetKindParam])
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
