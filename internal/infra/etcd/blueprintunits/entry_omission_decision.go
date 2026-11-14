package blueprintunits

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Preparation can prove that an omitted Entry has no acknowledged files.
// Its owner joins that proof and metadata retirement to this no-attempt receipt.
func PrepareEntryOmissionDecision(snapshot Snapshot, view workqueue.View, unit Unit,
	valueGenerationID string, at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item

	if task.Action != workexecution.ActionApply || snapshot.HeadTaskID != task.ID ||
		view.ReadRevision != snapshot.ReadRevision {
		return workqueue.Change{}, invalidRecord()
	}
	if item.Operation != workexecution.EntryConfiguration || item.TargetID != unit.Target.ID ||
		!unit.Removal || unit.Target.Kind != ids.KindEnvEntry {
		return workqueue.Change{}, invalidRecord()
	}
	if item.Input != nil || item.ForwardRevoked || !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return workqueue.Change{}, invalidRecord()
	}
	operationID := ids.DeriveAt(ids.KindOperation, task.CreatedAt, item.ID, "entry-omission")
	receipt, err := prepareControllerEntryReceipt(snapshot, unit, operationID, valueGenerationID)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareInputDecision(view, task.Intent, workexecution.Succeeded,
		"Removed the Entry definition and values; no acknowledged files used this Entry", at)
	if err != nil {
		receipt.Clear()
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, receipt.Conditions()...)
	change.Mutations = append(change.Mutations, receipt.Mutations()...)
	return change, nil
}
