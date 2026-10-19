package entrywork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
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
		return errs.New(errs.KindStateConflict, "Entry interruption requires the replacement Agent on the same host")
	}
	input, _, _, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return err
	}
	completion, err := owner.queue.ReadInterruptedEntryAt(ctx, view)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	finalSequence, err := entryexecution.FinalSequence(view.Attempt.Attempt, input)
	if err != nil {
		return err
	}
	var proof workqueue.Change
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		proof, completion.Result, err = owner.inputs.PrepareRecoveryInterruption(ctx, view, at)
	} else if checkpoint := view.Checkpoint.Record; checkpoint != nil && checkpoint.Sequence == finalSequence {
		applied, applyErr := owner.inputs.PrepareAppliedState(ctx, view, input, at)
		if applyErr != nil {
			return applyErr
		}
		proof = applied
		completion.Result = workexecution.Completion{Outcome: completedEntryOutcome(view.Work), Evidence: checkpoint.Evidence, EffectsSettled: true}
	} else {
		proof, completion.Result, err = owner.inputs.PrepareFailure(ctx, view, entryexecution.ExecutionInterrupted, at, true)
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
		return errs.New(errs.KindStateConflict, "Entry interruption ownership changed before settlement")
	}
	return nil
}
