package detachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReconcileInterrupted accounts for the replaced process on the same host.
// It cannot impersonate that process or replay its adapter/hook commands.
func (owner *Owner) ReconcileInterrupted(ctx context.Context, view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	identity, attempt := authorization.Identity(), view.Attempt.Attempt
	if identity.HostID != attempt.Executor.HostID || identity == attempt.Executor || view.CurrentExecutor ||
		view.Work.Item.Item.Operation != workexecution.Detach {
		return errs.New(
			errs.KindStateConflict,
			"Detach interruption requires the authenticated replacement on its captured host",
		)
	}
	completion, err := owner.queue.ReadInterruptedDetachAt(ctx, view)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	var proof workqueue.Change
	if attempt.Direction == workexecution.Recovery {
		proof, completion.Result, err = owner.inputs.PrepareInterruptedDetachInspection(ctx, view, at)
	} else {
		input, inputErr := owner.forwardInput(ctx, view)
		if inputErr != nil {
			return inputErr
		}
		defer attachexecution.ClearRemoval(input.Removal)
		if checkpoint := view.Checkpoint.Record; checkpoint != nil && checkpoint.Sequence == uint64(input.CommandCount())*2 {
			proof, err = owner.inputs.PrepareDetachCompletion(ctx, view, input, at)
			outcome := workexecution.Succeeded
			if view.Work.Task.Task.Stop != nil {
				outcome = view.Work.Task.Task.Stop.Outcome
			} else if view.Work.Item.Item.ForwardRevoked {
				outcome = workexecution.Superseded
			}
			completion.Result = workexecution.Completion{Outcome: outcome, Evidence: checkpoint.Evidence, EffectsSettled: true}
		} else {
			proof, completion.Result, err = owner.inputs.PrepareDetachFailure(ctx, view, input, attachexecution.ExecutionInterrupted, at, true)
		}
	}
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(proof.Mutations)
	completion.At = at
	var change workqueue.Change
	if attempt.Direction == workexecution.Forward && !completion.Result.EffectsSettled {
		change, err = workqueue.PrepareCompletionWithRecovery(completion, at.Add(attachexecution.InspectionBudget))
	} else {
		change, err = workqueue.PrepareCompletion(completion)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(
			errs.KindStateConflict,
			"Detach interruption ownership changed before its evidence could settle",
		)
	}
	return nil
}
