package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The caller authenticates the replacement on the same host. This only accounts
// for saved evidence; it neither impersonates the old process nor runs a hook.
func (owner *AgentWork) reconcileInterruptedAttach(
	ctx context.Context,
	view workqueue.ResumeView,
	guards []keyvalue.Condition,
) error {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		return owner.reconcileInterruptedAttachInspection(ctx, view, guards)
	}
	input, err := owner.capturedAttachInput(ctx, view)
	if err != nil {
		return err
	}
	defer attachexecution.ClearRemoval(input.Removal)
	completion, err := owner.queue.ReadInterruptedHierarchyAt(ctx, view)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	var proof workqueue.Change
	if checkpoint := view.Checkpoint.Record; checkpoint != nil &&
		checkpoint.Sequence == uint64(input.CommandCount())*2 {
		proof, err = owner.records.PrepareInterruptedHierarchyAttachCompletion(ctx, view, input, at)
		outcome := workexecution.Succeeded
		if view.Work.Task.Task.Stop != nil {
			outcome = view.Work.Task.Task.Stop.Outcome
		} else if view.Work.Item.Item.ForwardRevoked {
			outcome = workexecution.Superseded
		}
		completion.Result = workexecution.Completion{
			Outcome:        outcome,
			Evidence:       checkpoint.Evidence,
			EffectsSettled: true,
		}
	} else {
		proof, completion.Result, err = owner.records.PrepareInterruptedHierarchyAttachFailure(ctx, view, input, at)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(proof.Mutations)
	completion.At = at
	var change workqueue.Change
	if !completion.Result.EffectsSettled {
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
			"Attach interruption ownership changed before its saved result was accounted for",
		)
	}
	return nil
}

func (owner *AgentWork) reconcileInterruptedAttachInspection(
	ctx context.Context,
	view workqueue.ResumeView,
	guards []keyvalue.Condition,
) error {
	completion, err := owner.queue.ReadInterruptedHierarchyAt(ctx, view)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	proof, result, err := owner.records.PrepareInterruptedAttachInspection(ctx, view, at)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(proof.Mutations)
	completion.At, completion.Result = at, result
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	committed, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Attach inspection interruption ownership changed before settlement")
	}
	return nil
}
