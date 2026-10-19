package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareControllerBinding must commit with the visible work item's settlement
// and resource receipt. It never executes a provisioning hook or changes intent.
func (repository *InputRepository) PrepareControllerBinding(
	ctx context.Context,
	work workqueue.View,
) (workqueue.Change, error) {
	source, err := repository.LoadCaptured(ctx, work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer source.Clear()
	generation := source.Generation.Record
	if !generation.SettlesInController() {
		return workqueue.Change{}, checkpointConflict("Attach requires Agent provisioning before settlement")
	}
	if work.Task.Task.Action == workexecution.ActionAttach {
		guards, err := repository.PendingBindingConditions(ctx, work)
		if err != nil {
			return workqueue.Change{}, err
		}
		change, err := repository.prepareBindingTransition(ctx, work, core.AttachPending, core.AttachReady)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, guards...)
		return change, nil
	}
	if generation.Binding != nil {
		change := workqueue.Change{Conditions: append([]keyvalue.Condition(nil), source.Conditions...)}
		change.Conditions = append(change.Conditions, generation.Binding.Conditions()...)
		for _, id := range []string{generation.AttachID, generation.CredentialOwnerID} {
			change.Conditions = append(change.Conditions, keyvalue.Condition{Key: deletions.TombstoneKey("attach", id)})
		}
		for _, grant := range generation.Binding.Grants {
			change.Conditions = append(
				change.Conditions,
				keyvalue.Condition{Key: deletions.TombstoneKey("attach", grant.Record.ID)},
			)
		}
		for _, id := range []string{generation.EnvironmentID, generation.BackingEnvironmentID} {
			fence, err := environmentfence.LoadOrdinary(ctx, repository.store, id, work.ReadRevision)
			if err != nil {
				return workqueue.Change{}, err
			}
			change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
		}
		return change, nil
	}
	record, err := generation.ControllerBinding(work.Task.Task.CreatedAt)
	if err != nil {
		return workqueue.Change{}, err
	}
	return repository.publishBinding(ctx, work, generation, record, source.Conditions)
}
