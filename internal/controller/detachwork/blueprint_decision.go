package detachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Owner) prepareDecision(ctx context.Context, view workqueue.View, at time.Time,
) (workqueue.Change, error) {
	change, err := owner.inputs.PrepareDetachDecision(ctx, view, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionApply || task.Stop != nil || item.ForwardRevoked ||
		!at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return change, nil
	}
	frozen, capture, err := owner.inputs.LoadBlueprintInput(ctx, view)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	defer frozen.Clear()
	graph, err := owner.units.LoadCapturedGraph(ctx, task, view.ReadRevision)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	var unit *blueprintunits.Unit
	for index, captured := range graph.Record.Items {
		if captured.ID == item.ID {
			unit = &graph.Record.Units[index]
			break
		}
	}
	if unit == nil || unit.Target.Kind != ids.KindAttach || !unit.Removal || item.Input == nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach decision has no captured Blueprint removal")
	}
	snapshot, err := owner.units.LoadAtRevision(ctx, task.Scope.EnvironmentID, view.ReadRevision)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	receipt, err := blueprintunits.PrepareControllerDetachRemoval(
		snapshot,
		*unit,
		frozen.OperationID,
		capture.ModRevision,
		*item.Input,
	)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(
		change.Conditions,
		capture,
		keyvalue.Condition{
			Key:         blueprintunits.CapturedGraphKey(snapshot.EnvironmentID, task.ID),
			ModRevision: graph.Revision,
		},
	)
	change.Conditions = append(change.Conditions, receipt.Conditions()...)
	change.Mutations = append(change.Mutations, receipt.Mutations()...)
	return change, nil
}

func joinBlueprintReceipt(change workqueue.Change, receipt workqueue.Change, err error) (workqueue.Change, error) {
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, receipt.Conditions...)
	change.Mutations = append(change.Mutations, receipt.Mutations...)
	return change, nil
}
