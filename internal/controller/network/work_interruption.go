package network

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReconcileInterrupted records only durable evidence from the replaced executor.
// A new Agent on the same host does not impersonate the old process or receive
// permission to rerun an ambiguous effect.
func (owner *AgentWork) ReconcileInterrupted(ctx context.Context, view workqueue.ResumeView, authorization agentregistration.WorkAuthorization) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	actor, previous := authorization.Identity(), view.Attempt.Attempt.Executor
	if actor.Executor != workexecution.Agent || actor.HostID != previous.HostID || actor == previous ||
		view.CurrentExecutor || !view.ActiveAttempt || view.Work.Task.Task.CollectionStartedAt != nil {
		return errs.New(errs.KindStateConflict, "Network interruption requires the replacement Agent on the same host")
	}
	input, _, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return err
	}
	completion, err := owner.queue.ReadInterruptedNetworkAt(ctx, view)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	const finalSequence uint64 = 2
	var proof workqueue.Change
	if checkpoint := view.Checkpoint.Record; view.Attempt.Attempt.Direction == workexecution.Forward && checkpoint != nil && checkpoint.Sequence == finalSequence {
		proof, err = owner.inputs.PrepareNetworkWorkCompletion(ctx, view, input, at)
		completion.Result = workexecution.Completion{Outcome: completedNetworkOutcome(view.Work), Evidence: checkpoint.Evidence, EffectsSettled: true}
	} else {
		proof, completion.Result, err = owner.inputs.PrepareFailure(ctx, view, networkexecution.ExecutionInterrupted, at, true)
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
		return errs.New(errs.KindStateConflict, "Network interruption ownership changed before settlement")
	}
	return nil
}
