package blueprintunits

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareEntryWorkSettlement joins the value receipt and native work outcome.
// The Entry owner must add the exact generation write and its captured-input
// comparisons to the same transaction. There is no Agent assignment or separate
// success write that could leave the value applied but its work unresolved.
func PrepareEntryWorkSettlement(snapshot Snapshot, view workqueue.View, unit Unit,
	valueGenerationID string, at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if view.ReadRevision != snapshot.ReadRevision || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionApply ||
		task.ID != snapshot.HeadTaskID || task.Scope.EnvironmentID != snapshot.EnvironmentID ||
		!task.PublicationComplete || task.Stop != nil || task.FinishedAt != nil ||
		task.CollectionStartedAt != nil || !at.Before(task.Deadline) || !at.Before(item.Deadline) ||
		item.TaskID != task.ID || item.Operation != workexecution.EntrySettlement ||
		item.Placement.Executor != workexecution.Controller || item.TargetID != unit.Target.ID ||
		unit.Target.Kind != ids.KindEnvEntry || item.Input == nil ||
		item.ForwardRevoked || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Phase != workexecution.Ready {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Entry value cannot settle outside its current ready Apply work",
		)
	}
	operationID := ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "entry-value/"+item.ID)
	receipt, err := prepareControllerEntryReceipt(snapshot, unit, operationID, valueGenerationID)
	if err != nil {
		return workqueue.Change{}, err
	}
	evidence, result, err := prepareEntryWorkResult(view, valueGenerationID, at)
	if err != nil {
		receipt.Clear()
		return workqueue.Change{}, err
	}
	settled, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: workexecution.Succeeded, Evidence: evidence, EffectsSettled: true,
	}, at)
	if err != nil {
		receipt.Clear()
		keyvalue.ClearMutationValues(result.Mutations)
		return workqueue.Change{}, err
	}
	settled.Conditions = append(settled.Conditions, receipt.Conditions()...)
	settled.Mutations = append(settled.Mutations, receipt.Mutations()...)
	settled.Conditions = append(settled.Conditions, result.Conditions...)
	settled.Mutations = append(settled.Mutations, result.Mutations...)
	return settled, nil
}
