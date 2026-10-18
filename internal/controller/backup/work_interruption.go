package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *AgentWork) ReconcileInterrupted(ctx context.Context, view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	actor, previous := authorization.Identity(), view.Attempt.Attempt.Executor
	if actor.Executor != workexecution.Agent || actor.HostID != previous.HostID || actor == previous ||
		view.CurrentExecutor || !view.ActiveAttempt || view.Work.Task.Task.CollectionStartedAt != nil {
		return errs.New(errs.KindStateConflict, "Backup interruption requires the replacement Agent on the same host")
	}
	completion, err := owner.queue.ReadInterruptedBackupAt(ctx, view)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	var proof workqueue.Change
	var result workexecution.Completion
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		proof, result, err = owner.inputs.PrepareRecoveryFailure(ctx, view, executionplan.BackupExecutionInterrupted, at, true)
	} else {
		proof, result, err = owner.inputs.PrepareInterrupted(ctx, view, at)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(proof.Mutations)
	completion.Result, completion.At = result, at
	var change workqueue.Change
	if result.EffectsSettled || view.Attempt.Attempt.Direction == workexecution.Recovery || view.Work.Item.Item.Operation != workexecution.BackupPrune {
		change, err = workqueue.PrepareCompletion(completion)
	} else {
		// One captured cleanup attempt; a failed cleanup is not automatically
		// requeued. Retain the existing prune operation's maximum time budget.
		deadline := at.Add(time.Duration(executionplan.MaximumBackupPruneStepTimeoutSeconds) * time.Second)
		change, err = workqueue.PrepareCompletionWithRecovery(completion, deadline)
	}
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
		return errs.New(errs.KindStateConflict, "Backup interruption ownership changed before recording the failure")
	}
	return nil
}
