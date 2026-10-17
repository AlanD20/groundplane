package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *HierarchyDeletionRepository) PrepareHierarchyAttachFailure(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, reason attachexecution.FailureReason, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	return repository.prepareHierarchyAttachFailure(ctx, view, input, reason, at, false)
}

func (repository *HierarchyDeletionRepository) PrepareInterruptedHierarchyAttachFailure(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	return repository.prepareHierarchyAttachFailure(ctx, view, input, attachexecution.ExecutionInterrupted, at, true)
}

// Hierarchy owns the frozen action and dependency guards. Command-boundary
// failure semantics are shared with direct Detach, not a second execution path.
func (repository *HierarchyDeletionRepository) prepareHierarchyAttachFailure(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, reason attachexecution.FailureReason, at time.Time, interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	if view.Work.Item.Item.Operation != workexecution.Hierarchy {
		return workqueue.Change{}, workexecution.Completion{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	capture, err := repository.LoadHierarchyAttachCapture(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer capture.Clear()
	if err := validateHierarchyAttachSource(view.Work, capture, input); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change, result, err := attachremoval.PrepareRemovalFailure(ctx, repository.store, view, input, reason, at, interrupted)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change.Conditions = append(change.Conditions, capture.Conditions...)
	return change, result, nil
}

func (repository *HierarchyDeletionRepository) LoadHierarchyAttachFailure(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	if view.Work.Item.Item.Operation != workexecution.Hierarchy {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return attachremoval.LoadRemovalFailure(ctx, repository.store, view)
}
