package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareHierarchyAttachUnstartedRetry joins activation to the operator's Retry.
// Pin staging alone cannot dispatch work, and the saved envelope is never
// regenerated from current Secrets or an old etcd revision.
func (repository *HierarchyDeletionRepository) prepareHierarchyAttachUnstartedRetry(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	active, err := repository.LoadHierarchyAttachCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer active.Clear()
	if active.Revision != 0 {
		return repository.HierarchyAttachAdmission(ctx, view)
	}
	capture, _, found, err := repository.LoadHierarchyPreparedCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer capture.Clear()
	if !found {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable,
			"removal stopped before its hook inputs were captured; Retry cannot select new inputs")
	}
	var prepared tasksecretpins.Prepared
	if len(capture.Sources) != 0 {
		prepared, err = repository.PrepareHierarchyAttachPins(ctx, view, capture)
		if err != nil {
			return workqueue.Change{}, err
		}
		binding, err := prepared.Binding()
		if err != nil {
			return workqueue.Change{}, err
		}
		capture.Pins = &binding
	}
	return repository.PrepareHierarchyAttachCapture(ctx, view, capture, prepared)
}
