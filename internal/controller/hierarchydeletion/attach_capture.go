package hierarchydeletion

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

func (owner *WorkPublication) prepareAttachCapture(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	current, err := owner.records.LoadHierarchyAttachCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer current.Clear()
	if current.Revision != 0 {
		return workqueue.Change{Conditions: current.Conditions}, nil
	}
	digest, err := attachremoval.Digest(current.Source.Input)
	if err != nil {
		return workqueue.Change{}, err
	}
	capture, _, found, err := owner.records.LoadHierarchyPreparedCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() { capture.Clear() }()
	if !found {
		capture = attachremoval.WorkCapture{Schema: 1, TaskID: view.Task.Task.ID, WorkItemID: view.Item.Item.ID,
			OperationID: etcd.HierarchyAttachOperation(view), Action: *view.Item.Item.Input, FrozenSHA256: digest,
			BackingProjectID: current.Source.Input.Attach.BackingProjectID}
		if current.Source.Input.Backing.Desired.Adapter == "custom" {
			capture.Hook, err = owner.attachPlanner.CaptureHookInputs(ctx, capture.OperationID, current.Source.Input)
			if err != nil {
				return workqueue.Change{}, err
			}
			if capture.Hook != nil {
				capture.Sources = append([]tasksecretpinrecord.Record(nil), capture.Hook.SecretSources...)
			}
		}
		return owner.records.PrepareHierarchyCaptureEnvelope(ctx, view, capture)
	}
	prepared := tasksecretpins.Prepared{}
	if len(capture.Sources) != 0 {
		prepared, err = owner.records.PrepareHierarchyAttachPins(ctx, view, capture)
		if err != nil {
			return workqueue.Change{}, err
		}
		binding, err := prepared.Binding()
		if err != nil {
			return workqueue.Change{}, err
		}
		capture.Pins = &binding
	}
	change, err := owner.records.PrepareHierarchyAttachCapture(ctx, view, capture, prepared)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
	}
	return change, err
}
