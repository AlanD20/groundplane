package attachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *Reports) LoadWorkAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.AttachProvisionAssignment, error) {
	return owner.inputs.LoadWorkAssignment(ctx, view)
}

// ReconcileInterrupted settles only saved proof. A replacement Agent never
// inherits permission to repeat a provisioning command from the old process.
func (owner *Reports) ReconcileInterrupted(ctx context.Context, view workqueue.ResumeView, authorization agentregistration.WorkAuthorization) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	actor, previous := authorization.Identity(), view.Attempt.Attempt.Executor
	if actor.Executor != workexecution.Agent || actor.HostID != previous.HostID || actor == previous ||
		view.CurrentExecutor || !view.ActiveAttempt || view.Work.Task.Task.CollectionStartedAt != nil {
		return errs.New(errs.KindStateConflict, "Attach interruption requires the replacement Agent on the same host")
	}
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		return owner.interruptInspection(ctx, view, guards)
	}
	input, err := owner.inputs.Forward(ctx, view)
	if err != nil {
		return err
	}
	defer attachexecution.ClearProvision(input.Provision)
	completion, err := owner.queue.ReadInterruptedAttachAt(ctx, view)
	if err != nil {
		return err
	}
	progress, _, err := owner.inputs.repository.LoadProgress(ctx, view, input)
	if err != nil {
		return err
	}
	authority, err := input.Authority()
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	var proof, binding workqueue.Change
	if progress != nil && progress.Complete(input.Attempt, authority) {
		var evidence workexecution.Input
		proof, evidence, err = owner.inputs.repository.PrepareCompletion(ctx, view, input, at)
		if err != nil {
			return err
		}
		defer keyvalue.ZeroMutationBytes(proof.Mutations)
		binding, err = owner.units.PrepareAttachCompletionReceipt(ctx, view, evidence)
		completion.Result = workexecution.Completion{Outcome: workexecution.Succeeded, Evidence: evidence, EffectsSettled: true}
		if view.Work.Task.Task.Stop != nil {
			completion.Result.Outcome = view.Work.Task.Task.Stop.Outcome
		} else if view.Work.Item.Item.ForwardRevoked {
			completion.Result.Outcome = workexecution.Superseded
		}
	} else {
		proof, completion.Result, err = owner.inputs.repository.PrepareFailure(ctx, view, input, attachexecution.ExecutionInterrupted, at, true)
		if err != nil {
			return err
		}
		defer keyvalue.ZeroMutationBytes(proof.Mutations)
		binding, err = owner.inputs.repository.PrepareFailedBinding(ctx, view.Work)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(binding.Mutations)
	completion.At = at
	var terminal workqueue.Change
	if completion.Result.EffectsSettled {
		terminal, err = workqueue.PrepareCompletion(completion)
	} else {
		terminal, err = workqueue.PrepareCompletionWithRecovery(completion, at.Add(attachexecution.InspectionBudget))
	}
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(terminal.Mutations)
	result, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, binding, terminal)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Attach interruption ownership changed before settlement")
	}
	owner.reportForwardResult(ctx, view.Attempt.Attempt, progress != nil && progress.Complete(input.Attempt, authority))
	return nil
}
