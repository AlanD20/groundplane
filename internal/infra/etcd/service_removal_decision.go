package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareServiceRemovalDecision handles only a never-deployed direct Service.
// The scheduler joins its dependency and resource-claim comparisons before
// committing this definition change with work settlement.
func (repository *ServiceRepository) PrepareServiceRemovalDecision(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionRemove && task.Action != workexecution.ActionApply ||
		item.Operation != workexecution.ServiceRemoval ||
		item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" ||
		item.Phase != workexecution.Ready ||
		item.Input == nil ||
		item.Cleanup != workexecution.CleanupNone ||
		!task.PublicationComplete ||
		task.FinishedAt != nil ||
		task.CollectionStartedAt != nil ||
		at.IsZero() ||
		at.Location() != time.UTC ||
		at.Before(item.UpdatedAt) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Service removal is not a ready definition-only action",
		)
	}
	inputs, err := serviceremovalwork.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		return inputs.PrepareUnstartedStop(ctx, view, at)
	}
	input, capture, err := inputs.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if input.Runtime != nil {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Service removal requires its captured Agent execution",
		)
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	snapshot, err := ledger.LoadAtRevision(ctx, input.Scope.EnvironmentID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	absence, err := blueprintunits.RequireInitialServiceAbsence(snapshot, input.ServiceID)
	if err != nil {
		return workqueue.Change{}, err
	}
	var state workqueue.Change
	if task.Action == workexecution.ActionApply {
		state, err = repository.prepareBlueprintServiceRemovalState(ctx, inputs, input, view)
	} else {
		state, err = repository.prepareDirectServiceRemovalState(ctx, inputs, input, view.ReadRevision)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	var receipt workqueue.Change
	if task.Action == workexecution.ActionApply {
		receipt, err = repository.prepareServiceDeclarationReceipt(ctx, ledger, snapshot, view, input)
		if err != nil {
			keyvalue.ClearMutationValues(state.Mutations)
			return workqueue.Change{}, err
		}
	}
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: workexecution.Succeeded, Evidence: *item.Input, EffectsSettled: true,
	}, at)
	if err != nil {
		keyvalue.ClearMutationValues(state.Mutations)
		keyvalue.ClearMutationValues(receipt.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, capture)
	change.Conditions = append(change.Conditions, absence...)
	change.Conditions = append(change.Conditions, state.Conditions...)
	change.Conditions = append(change.Conditions, receipt.Conditions...)
	change.Mutations = append(change.Mutations, state.Mutations...)
	change.Mutations = append(change.Mutations, receipt.Mutations...)
	return change, nil
}
