package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReconcileInterrupted can account for durable proof without asking the new
// process to impersonate its predecessor. An in-flight boundary records failure
// with retained claims; it is never interpreted as proof that deletion stopped.
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
		return errs.New(errs.KindStateConflict, "removal interruption requires the replacement Agent on the same host")
	}
	captured, err := owner.records.LoadHierarchyWork(ctx, view.Work)
	if err != nil {
		return err
	}
	if captured.Action.ActionKind == hierarchydeletion.HierarchyDeletionAttachDeprovision {
		return owner.reconcileInterruptedAttach(ctx, view, guards)
	}
	message, err := owner.capturedEnvironmentAssignment(ctx, view)
	if err != nil {
		return err
	}
	input, err := hierarchyexecution.DecodeEnvironmentAssignment(message, previous)
	if err != nil {
		return err
	}
	input.Attempt = view.Attempt.Attempt
	completion, err := owner.queue.ReadInterruptedHierarchyAt(ctx, view)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	var proof workqueue.Change
	if checkpoint := view.Checkpoint.Record; checkpoint != nil && checkpoint.Sequence == 4 {
		proof, err = owner.records.PrepareInterruptedHierarchyEnvironmentCompletion(ctx, view, input, at)
		completion.Result = workexecution.Completion{
			Outcome:        workexecution.Succeeded,
			Evidence:       checkpoint.Evidence,
			EffectsSettled: true,
		}
	} else {
		proof, completion.Result, err = owner.records.PrepareInterruptedHierarchyEnvironmentFailure(ctx, view, input, at)
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
		return errs.New(errs.KindStateConflict, "removal interruption ownership changed before accounting completed")
	}
	return nil
}
