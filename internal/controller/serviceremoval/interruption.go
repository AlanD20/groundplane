package serviceremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *AgentWork) ReconcileInterrupted(ctx context.Context, view workqueue.ResumeView, authorization agentregistration.WorkAuthorization) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	actor, previous := authorization.Identity(), view.Attempt.Attempt.Executor
	if actor.Executor != workexecution.Agent || actor.HostID != previous.HostID || actor == previous ||
		view.CurrentExecutor || !view.ActiveAttempt || view.Work.Task.Task.CollectionStartedAt != nil ||
		view.Work.Item.Item.Operation != workexecution.ServiceRemoval {
		return errs.New(errs.KindStateConflict, "service removal interruption requires the replacement Agent on the same host")
	}
	captured, _, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return err
	}
	input, err := BuildHostInput(captured)
	if err != nil {
		return err
	}
	completion, err := owner.queue.ReadInterruptedServiceRemovalAt(ctx, view)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	proof, outcome, err := owner.interruptedResult(ctx, view, input, at)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(proof.Mutations)
	completion.At, completion.Result = at, outcome
	change, err := workqueue.PrepareCompletion(completion)
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
		return errs.New(errs.KindStateConflict, "service removal interruption ownership changed before settlement")
	}
	return nil
}

func (owner *AgentWork) interruptedResult(ctx context.Context, view workqueue.ResumeView, input *agentpb.ServiceLifecycleWorkInput, at time.Time) (workqueue.Change, workexecution.Completion, error) {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		saved, err := owner.inputs.LoadRecovery(ctx, view.Work, input)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		if view.Checkpoint.Record != nil {
			progress, _, err := owner.inputs.LoadRecoveryProgress(ctx, view, input, saved.Capture)
			if err != nil {
				return workqueue.Change{}, workexecution.Completion{}, err
			}
			if progress.Phase == removalexecution.Complete && !progress.InFlight {
				return owner.records.PrepareRecoveredServiceRemoval(ctx, view, input, at)
			}
		}
		return owner.inputs.PrepareRecoveryFailure(ctx, view, input, lifecycleexecution.ExecutionInterrupted, at, true)
	}
	progress, _, err := owner.inputs.LoadProgress(ctx, view, input)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if progress != nil && progress.BeganAt == nil && len(progress.Completed) == len(input.Members) &&
		view.Work.Task.Task.Stop == nil && !view.Work.Item.Item.ForwardRevoked {
		proof, err := owner.records.PrepareServiceRemovalCompletion(ctx, view, input, at)
		return proof, workexecution.Completion{Outcome: workexecution.Succeeded, Evidence: view.Checkpoint.Record.Evidence, EffectsSettled: true}, err
	}
	return owner.inputs.PrepareFailure(ctx, view, input, lifecycleexecution.ExecutionInterrupted, at, true)
}
