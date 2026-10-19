package backingruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backinghookruntime"
	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) hook(
	ctx context.Context,
	input backingprovision.HelperInput,
	exchange Exchange,
	inspectOnly bool,
) (backingprovision.Observation, error) {
	assignment := input.Assignment
	saved, err := executor.journal.Backing(ctx, assignment.Attempt, assignment.Procedure)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	if saved == nil || saved.Progress == nil || saved.Progress.Last == nil || saved.Progress.Last.ContainerID == "" {
		return backingprovision.Observation{}, invalidInput()
	}
	if hook := saved.Hook; hook != nil {
		if hook.Result != nil {
			return *hook.Result, nil
		}
		if hook.ReadyAt != nil && hook.ExitedAt == nil {
			observed, err := executor.commands.Inspect(ctx, hook.ContainerID, hook.ExecID)
			if err != nil {
				return backingprovision.Observation{}, err
			}
			if observed.Running {
				return backingprovision.Observation{}, errs.New(
					errs.KindStateConflict,
					"backing creation after-start hook is still running",
				)
			}
			if err := executor.journal.ObserveBackingHookExit(ctx, assignment.Attempt, assignment.Procedure,
				hook.ExecID, observed.ExitCode, time.Now().UTC()); err != nil {
				return backingprovision.Observation{}, err
			}
		}
		// Exit alone cannot prove the private no-facts result was validated.
		// Never rerun a command whose saved result is missing.
		return backingprovision.Observation{}, errs.New(
			errs.KindStateConflict,
			"backing creation after-start hook was interrupted before its validated result was saved; cleanup inspection is required",
		)
	}
	if inspectOnly {
		return backingprovision.Observation{}, errs.New(errs.KindStateConflict,
			"backing creation after-start hook has no saved process identity; automatic replay is disabled")
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	stage, err := backingprovision.StageID(assignment.Attempt.WorkItemID, backingprovision.AfterStart)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	delivery, err := exchange.BackingProvisionHook(
		ctx,
		&agentpb.BackingProvisionHookRequest{Attempt: identity, StageId: stage},
	)
	defer func() {
		if delivery != nil && delivery.Hook != nil {
			for _, value := range delivery.Hook.Inputs {
				clear(value.Value)
			}
		}
	}()
	if err != nil {
		return backingprovision.Observation{}, err
	}
	if err := backingprovision.ValidateHookDelivery(input, delivery); err != nil {
		return backingprovision.Observation{}, err
	}
	definition, values, _, err := executionplan.DecodeBackingHookProcedure(delivery.Hook)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	if err := executor.ownership.Check(); err != nil {
		return backingprovision.Observation{}, err
	}
	if err := backinghookruntime.ExecuteLifecycle(ctx, executor.commands, hookRecorder{executor: executor, assignment: assignment},
		saved.Progress.Last.ContainerID, definition, values); err != nil {
		return backingprovision.Observation{}, err
	}
	return executor.journal.SaveBackingHookResult(ctx, assignment.Attempt, assignment.Procedure, time.Now().UTC())
}

type hookRecorder struct {
	executor   *Executor
	assignment backingprovision.Assignment
}

func (recorder hookRecorder) Created(ctx context.Context, containerID, execID string, at time.Time) error {
	if err := recorder.executor.ownership.Check(); err != nil {
		return err
	}
	return recorder.executor.journal.PrepareBackingHookExec(
		ctx,
		recorder.assignment.Attempt,
		recorder.assignment.Procedure,
		containerID,
		execID,
		at,
	)
}

func (recorder hookRecorder) Ready(ctx context.Context, execID string, at time.Time) error {
	if err := recorder.executor.ownership.Check(); err != nil {
		return err
	}
	return recorder.executor.journal.AuthorizeBackingHookExec(
		ctx,
		recorder.assignment.Attempt,
		recorder.assignment.Procedure,
		execID,
		at,
	)
}

func (recorder hookRecorder) Exited(ctx context.Context, execID string, exitCode int, at time.Time) error {
	return recorder.executor.journal.ObserveBackingHookExit(
		ctx,
		recorder.assignment.Attempt,
		recorder.assignment.Procedure,
		execID,
		exitCode,
		at,
	)
}
