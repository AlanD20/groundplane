package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func hierarchyAttachPreparedKey(workID string) string {
	return "/v1/private/hierarchy-attach-prepared/" + workID
}

func (repository *HierarchyDeletionRepository) LoadHierarchyPreparedCapture(ctx context.Context, view workqueue.View,
) (attachremoval.WorkCapture, keyvalue.Condition, bool, error) {
	key := hierarchyAttachPreparedKey(view.Item.Item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return attachremoval.WorkCapture{}, keyvalue.Condition{}, false, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 {
		return attachremoval.WorkCapture{}, keyvalue.Condition{}, false, errs.New(errs.KindStateConflict, "hierarchy prepared capture read is incomplete")
	}
	guard := keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}
	if read.Values[0] == nil {
		return attachremoval.WorkCapture{}, guard, false, nil
	}
	capture, err := attachremoval.DecodePreparedCapture(read.Values[0].Value)
	if err != nil {
		return attachremoval.WorkCapture{}, keyvalue.Condition{}, false, err
	}
	if capture.TaskID != view.Task.Task.ID || capture.WorkItemID != view.Item.Item.ID ||
		capture.OperationID != HierarchyAttachOperation(view) || view.Item.Item.Input == nil || capture.Action != *view.Item.Item.Input {
		capture.Clear()
		return attachremoval.WorkCapture{}, keyvalue.Condition{}, false, errs.New(errs.KindStateConflict, "hierarchy prepared capture belongs to different work")
	}
	return capture, guard, true, nil
}

func (repository *HierarchyDeletionRepository) PrepareHierarchyCaptureEnvelope(ctx context.Context, view workqueue.View,
	capture attachremoval.WorkCapture,
) (workqueue.Change, error) {
	if view.Task.Task.PublicationComplete || view.Task.Task.Stop != nil || view.Task.Task.FinishedAt != nil ||
		view.Item.Item.AttemptNumber != 0 || view.Item.Item.ActiveAttemptID != "" || view.Item.Revision <= 0 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "hierarchy envelope requires closed unexecuted publication")
	}
	current, err := repository.LoadHierarchyAttachCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer current.Clear()
	digest, err := attachremoval.Digest(current.Source.Input)
	if err != nil || current.Revision != 0 || capture.FrozenSHA256 != digest || capture.TaskID != view.Task.Task.ID ||
		capture.WorkItemID != view.Item.Item.ID || capture.OperationID != HierarchyAttachOperation(view) ||
		view.Item.Item.Input == nil || capture.Action != *view.Item.Item.Input ||
		capture.BackingProjectID != current.Source.Input.Attach.BackingProjectID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "hierarchy envelope differs from its frozen source")
	}
	raw, err := attachremoval.EncodePreparedCapture(capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := hierarchyAttachPreparedKey(view.Item.Item.ID)
	return workqueue.Change{Conditions: append(current.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(view.Task.Task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(view.Item.Item.ID), ModRevision: view.Item.Revision},
		keyvalue.Condition{Key: key}), Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}}, nil
}

func (repository *HierarchyDeletionRepository) hierarchyPreparedCaptureGuard(ctx context.Context, view workqueue.View,
	capture attachremoval.WorkCapture,
) (keyvalue.Condition, error) {
	prepared, guard, found, err := repository.LoadHierarchyPreparedCapture(ctx, view)
	if err != nil {
		return keyvalue.Condition{}, err
	}
	defer prepared.Clear()
	capture.Pins = nil
	left, leftErr := attachremoval.EncodePreparedCapture(capture)
	right, rightErr := attachremoval.EncodePreparedCapture(prepared)
	same := found && leftErr == nil && rightErr == nil && bytes.Equal(left, right)
	clear(left)
	clear(right)
	if !same {
		return keyvalue.Condition{}, errs.New(errs.KindStateConflict, "hierarchy activation changed its sealed hook envelope")
	}
	return guard, nil
}

// Capture publication is either initial preparation behind the execution
// barrier, or an explicit Retry of failed work that never acquired an attempt.
// Retry may activate a saved envelope, but cannot select new hook inputs.
func hierarchyCaptureMayActivate(view workqueue.View) bool {
	task, item := view.Task.Task, view.Item.Item
	if task.Stop != nil || task.CollectionStartedAt != nil || item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" || item.ForwardRevoked {
		return false
	}
	if !task.PublicationComplete {
		return task.FinishedAt == nil && item.Phase == workexecution.Ready
	}
	return item.Phase == workexecution.Settled && item.Outcome == workexecution.Failed &&
		item.Cleanup == workexecution.CleanupNone
}
