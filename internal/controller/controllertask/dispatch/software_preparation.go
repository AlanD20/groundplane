package dispatch

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SoftwarePreparationDispatcher adds the one native preparation resource to
// the existing closed Controller Task dispatch chain.
type SoftwarePreparationDispatcher struct {
	fallback    controllerTaskExecutor
	preparation controllerTaskExecutor
}

func NewSoftwarePreparationDispatcher(
	fallback controllerTaskExecutor,
	preparation controllerTaskExecutor,
) (*SoftwarePreparationDispatcher, error) {
	if fallback == nil || preparation == nil {
		return nil, errs.New(errs.KindInternal, "software preparation Task dispatcher dependencies are required")
	}
	return &SoftwarePreparationDispatcher{fallback: fallback, preparation: preparation}, nil
}

func (dispatcher *SoftwarePreparationDispatcher) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceSoftwarePreparation {
		return dispatcher.fallback.Execute(ctx, task)
	}
	if ctx == nil || task.Type != taskjournal.TaskPrepare ||
		task.Executor != taskjournal.TaskExecutorController {
		return errs.New(errs.KindValidationFailed, "software preparation Controller Task is invalid")
	}
	return dispatcher.preparation.Execute(ctx, task)
}
