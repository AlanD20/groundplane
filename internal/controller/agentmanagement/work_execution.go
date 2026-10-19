package agentmanagement

import (
	"context"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Work) run(ctx context.Context, delivery workqueue.Delivery, forward bool) error {
	input, view, err := owner.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt, capture := view.Attempt.Attempt, input.Record
	if attempt.StartedAt == nil {
		return errs.New(errs.KindStateConflict, "Agent lifecycle requires its durable execution start")
	}
	saved, found, err := owner.inputs.ReadResult(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if found && saved.Record.Result.Settled() {
		return nil
	}
	operationContext, cancel := context.WithCancel(ctx)
	active := &activeLifecycle{attemptID: attempt.ID, cancel: cancel}
	owner.mu.Lock()
	if owner.active[capture.TaskID] != nil {
		owner.mu.Unlock()
		cancel()
		return errs.New(errs.KindResourceInUse, "Agent lifecycle already has an executing attempt")
	}
	owner.active[capture.TaskID] = active
	owner.mu.Unlock()
	defer func() {
		cancel()
		owner.mu.Lock()
		delete(owner.active, capture.TaskID)
		owner.mu.Unlock()
	}()
	// Stop can arrive between the first snapshot and installing the local
	// cancellation hook. Observe the durable request again before any effect.
	_, view, err = owner.loadDelivery(operationContext, delivery)
	if err != nil {
		return err
	}
	stopping := view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked
	forward = forward && view.CurrentExecutor && attempt.Direction == workexecution.Forward && !stopping
	if capture.Action == workexecution.ActionUpdate {
		resume, err := owner.sessions.PauseAssignments(operationContext, capture.AgentID, math.MaxUint64)
		if err != nil {
			return err
		}
		defer resume()
	}
	// Removal and enrollment cleanup must keep Agent recovery dispatch open
	// while AbortActive drains captured effects. Their durable forward fence,
	// not a blanket channel pause, closes new work before credential revocation.
	var failure *taskprogress.Failure
	if found {
		failure = taskprogress.CloneFailure(saved.Record.Result.Failure)
	}
	var result agentoperations.Result
	var effectErr error
	if forward {
		owner.report(
			operationContext,
			delivery,
			"agent_lifecycle",
			"Apply captured Agent change",
			"Use only the selected image, credentials and generation",
			true,
			nil,
		)
		result, effectErr = owner.executeFresh(operationContext, capture)
		owner.report(
			operationContext,
			delivery,
			"agent_lifecycle",
			"Apply captured Agent change",
			"Use only the selected image, credentials and generation",
			false,
			effectErr,
		)
		owner.mu.Lock()
		stopping = stopping || active.stopped
		owner.mu.Unlock()
		if effectErr == nil && !stopping && operationContext.Err() == nil {
			return owner.save(ctx, attempt, result)
		}
		if ctx.Err() == context.Canceled {
			return ctx.Err()
		}
		if effectErr == nil {
			effectErr = operationContext.Err()
		}
		if effectErr == nil {
			effectErr = context.Canceled
		}
		failure = taskprogress.CaptureControllerFailure(effectErr)
	}
	if failure == nil && stopping {
		failure = taskprogress.CaptureControllerFailure(context.Canceled)
	}
	recovery, finish := context.WithTimeout(context.WithoutCancel(ctx), localagent.RemovalRecoveryTimeout)
	defer finish()
	owner.report(
		recovery,
		delivery,
		"agent_reconcile",
		"Reconcile Agent effects",
		"Verify authenticated readiness or retire only this operation's captured runtime",
		true,
		nil,
	)
	result, recoveryErr := owner.recoverLifecycle(recovery, capture, failure, stopping)
	owner.report(
		recovery,
		delivery,
		"agent_reconcile",
		"Reconcile Agent effects",
		"Verify authenticated readiness or retire only this operation's captured runtime",
		false,
		recoveryErr,
	)
	if recoveryErr != nil {
		if failure == nil {
			failure = taskprogress.CaptureControllerFailure(recoveryErr)
		}
		result = agentoperations.Result{
			Outcome:  workexecution.Failed,
			Observed: agentoperations.Unresolved,
			Failure:  failure,
		}
	}
	if stopping {
		result.Outcome = workexecution.Cancelled
	}
	if result.Outcome != workexecution.Succeeded && result.Failure == nil {
		result.Failure = failure
	}
	if err := owner.save(recovery, attempt, result); err != nil {
		return err
	}
	if recoveryErr != nil {
		return recoveryErr
	}
	return effectErr
}

func (owner *Work) save(ctx context.Context, attempt workexecution.Attempt, result agentoperations.Result) error {
	saving, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	return owner.inputs.SaveResult(saving, owner.queue, attempt, result, time.Now().UTC())
}

func (owner *Work) executeFresh(ctx context.Context, capture agentoperations.Capture) (agentoperations.Result, error) {
	switch capture.Action {
	case workexecution.ActionCreate:
		agent, err := owner.agents.Enroll(ctx, enrollment(capture))
		if err != nil {
			return agentoperations.Result{}, err
		}
		return agentoperations.Result{
			Outcome:    workexecution.Succeeded,
			Observed:   agentoperations.ReadyDesired,
			Generation: agent.Generation,
		}, nil
	case workexecution.ActionRemove:
		if err := owner.agents.Remove(ctx, capture.AgentID, capture.Image, capture.StartingGeneration); err != nil {
			return agentoperations.Result{}, err
		}
		return agentoperations.Result{Outcome: workexecution.Succeeded, Observed: agentoperations.Absent}, nil
	case workexecution.ActionUpdate:
		if err := owner.pauseReplacement(ctx, capture); err != nil {
			return agentoperations.Result{}, err
		}
		result, err := owner.agents.RecoverUpdate(ctx, replacement(capture), localagent.UpdateFinish, nil)
		if err != nil {
			return agentoperations.Result{}, err
		}
		return owner.finishReplacement(ctx, capture, result, nil)
	default:
		return agentoperations.Result{}, errs.New(errs.KindInternal, "Agent lifecycle action has no executor")
	}
}

func (owner *Work) recoverLifecycle(
	ctx context.Context,
	capture agentoperations.Capture,
	failure *taskprogress.Failure,
	stopping bool,
) (agentoperations.Result, error) {
	result := agentoperations.Result{Outcome: workexecution.Failed, Failure: failure}
	switch capture.Action {
	case workexecution.ActionCreate:
		ready, err := owner.agents.RecoverEnrollment(ctx, enrollment(capture), failure == nil && !stopping)
		if err != nil {
			return result, err
		}
		result.Observed = agentoperations.Absent
		if ready {
			result.Outcome, result.Observed, result.Generation = workexecution.Succeeded, agentoperations.ReadyDesired, 1
		}
		return result, nil
	case workexecution.ActionRemove:
		removed, err := owner.agents.RecoverRemoval(ctx, capture.AgentID, capture.Image, capture.StartingGeneration)
		if err != nil {
			return result, err
		}
		result.Observed = agentoperations.ReadyPrevious
		result.Generation = capture.StartingGeneration
		if removed {
			result.Observed, result.Generation = agentoperations.Absent, 0
			if failure == nil && !stopping {
				result.Outcome = workexecution.Succeeded
			}
		}
		return result, nil
	case workexecution.ActionUpdate:
		if err := owner.pauseReplacement(ctx, capture); err != nil {
			return result, err
		}
		health, err := owner.agents.Health(ctx, capture.AgentID)
		if err != nil {
			return result, err
		}
		goal := localagent.UpdateRestore
		if failure == nil && !stopping && health.Agent.Phase == localagent.PhaseReady &&
			health.Agent.Image == capture.Image &&
			health.Agent.Generation == capture.StartingGeneration+1 {
			// This is qualification of an already committed Ready generation,
			// not permission to start or finish a new candidate replacement.
			goal = localagent.UpdateFinish
		}
		observed, err := owner.agents.RecoverUpdate(ctx, replacement(capture), goal, failure)
		if err != nil {
			return result, err
		}
		return owner.finishReplacement(ctx, capture, observed, failure)
	default:
		return result, errs.New(errs.KindInternal, "Agent lifecycle action has no recovery owner")
	}
}

func (owner *Work) pauseReplacement(ctx context.Context, capture agentoperations.Capture) error {
	health, err := owner.agents.Health(ctx, capture.AgentID)
	if err != nil {
		return err
	}
	return owner.registrations.PauseReplacement(
		ctx,
		capture.AgentID,
		capture.StartingGeneration,
		health.Agent.Generation,
	)
}

func (owner *Work) finishReplacement(
	ctx context.Context,
	capture agentoperations.Capture,
	observed localagent.UpdateResult,
	failure *taskprogress.Failure,
) (agentoperations.Result, error) {
	health, err := owner.agents.Health(ctx, capture.AgentID)
	if err != nil {
		return agentoperations.Result{}, err
	}
	if health.Agent.Phase != localagent.PhaseReady || !health.Online || !health.Healthy {
		return agentoperations.Result{}, errs.New(
			errs.KindStateConflict,
			"Agent has not proved authenticated readiness",
		)
	}
	result := agentoperations.Result{
		Outcome:    workexecution.Failed,
		Observed:   agentoperations.ReadyPrevious,
		Generation: health.Agent.Generation,
		Failure:    failure,
	}
	if observed == localagent.UpdateReadyDesired {
		if health.Agent.Generation != capture.StartingGeneration+1 || health.Agent.Image != capture.Image {
			return result, errs.New(errs.KindStateConflict, "Agent desired-generation readiness changed")
		}
		result.Outcome, result.Observed = workexecution.Succeeded, agentoperations.ReadyDesired
	} else if observed != localagent.UpdateReadyPrevious || health.Agent.Image != capture.PreviousImage ||
		health.Agent.Generation != capture.StartingGeneration && health.Agent.Generation != capture.StartingGeneration+2 {
		return result, errs.New(errs.KindStateConflict, "Agent predecessor readiness changed")
	} else {
		retained, err := owner.agents.ReplacementFailure(ctx, replacement(capture))
		if err != nil {
			return result, err
		}
		if retained != nil {
			result.Failure = retained
		}
	}
	return result, nil
}
