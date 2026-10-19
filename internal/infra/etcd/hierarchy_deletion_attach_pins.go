package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareHierarchyAttachPins stages only the exact Secret identities selected
// from the removal snapshot. Activation is part of capture publication, not here.
func (repository *HierarchyDeletionRepository) PrepareHierarchyAttachPins(ctx context.Context, view workqueue.View,
	capture attachremoval.WorkCapture,
) (tasksecretpins.Prepared, error) {
	if capture.TaskID != view.Task.Task.ID || capture.WorkItemID != view.Item.Item.ID ||
		capture.OperationID != HierarchyAttachOperation(
			view,
		) || capture.Hook == nil || len(capture.Sources) == 0 || capture.Pins != nil ||
		!hierarchyCaptureMayActivate(view) {
		return tasksecretpins.Prepared{}, errs.New(
			errs.KindValidationFailed,
			"Attach Secret preparation has invalid captured ownership",
		)
	}
	source, err := repository.LoadHierarchyAttachInput(ctx, view)
	if err != nil {
		return tasksecretpins.Prepared{}, err
	}
	defer attachremoval.Clear(&source.Input)
	digest, err := attachremoval.Digest(source.Input)
	if err != nil || capture.FrozenSHA256 != digest ||
		capture.BackingProjectID != source.Input.Attach.BackingProjectID {
		return tasksecretpins.Prepared{}, errs.New(errs.KindStateConflict, "Attach Secret preparation source changed")
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, capture.BackingProjectID)
	if err != nil {
		return tasksecretpins.Prepared{}, err
	}
	envelope, err := repository.hierarchyPreparedCaptureGuard(ctx, view, capture)
	if err != nil {
		return tasksecretpins.Prepared{}, err
	}
	return pins.PrepareOwned(
		ctx,
		capture.OperationID,
		capture.TaskID,
		capture.WorkItemID,
		capture.Sources,
		[]tasksecretpins.Condition{
			{Key: envelope.Key, ModRevision: envelope.ModRevision},
			{Key: workqueue.TaskKey(view.Task.Task.ID), ModRevision: view.Task.Revision},
			{Key: workqueue.ItemKey(view.Item.Item.ID), ModRevision: view.Item.Revision},
			{Key: attachremoval.WorkCaptureKey(view.Item.Item.ID)},
		},
	)
}
