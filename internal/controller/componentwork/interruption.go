package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReconcileInterrupted records only durable evidence from the replaced executor.
// A new Agent on the same host does not impersonate the old process or receive
// permission to rerun an ambiguous effect.
func (owner *AgentWork) ReconcileInterrupted(
	ctx context.Context,
	view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	actor, previous := authorization.Identity(), view.Attempt.Attempt.Executor
	if actor.Executor != workexecution.Agent || actor.HostID != previous.HostID || actor == previous ||
		view.CurrentExecutor || !view.ActiveAttempt || view.Work.Task.Task.CollectionStartedAt != nil {
		return errs.New(
			errs.KindStateConflict,
			"Component interruption requires the replacement Agent on the same host",
		)
	}
	input, _, _, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return err
	}
	completion, err := owner.queue.ReadInterruptedComponentAt(ctx, view)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	finalSequence, err := componentexecution.FinalSequence(input)
	if err != nil {
		return err
	}
	var proof workqueue.Change
	if checkpoint := view.Checkpoint.Record; view.Attempt.Attempt.Direction == workexecution.Forward &&
		checkpoint != nil &&
		checkpoint.Sequence == finalSequence {
		capture, applied, applyErr := owner.inputs.PrepareAppliedState(ctx, view, input, at)
		if applyErr != nil {
			return applyErr
		}
		proof = applied
		receipt, receiptErr := owner.ledger.PrepareComponentCompletionReceipt(ctx, view, capture.Removal, input.Remove)
		if receiptErr != nil {
			keyvalue.ClearMutationValues(proof.Mutations)
			return receiptErr
		}
		proof.Conditions = append(proof.Conditions, receipt.Conditions...)
		proof.Mutations = append(proof.Mutations, receipt.Mutations...)
		completion.Result = workexecution.Completion{
			Outcome:        completedComponentOutcome(view.Work),
			Evidence:       checkpoint.Evidence,
			EffectsSettled: true,
		}
	} else {
		proof, completion.Result, err = owner.inputs.PrepareFailure(ctx, view, componentexecution.ExecutionInterrupted, at, true)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(proof.Mutations)
	completion.At = at
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	result, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Component interruption ownership changed before settlement")
	}
	return nil
}
