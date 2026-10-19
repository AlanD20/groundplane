package lifecycleruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backinghookruntime"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (host *RuntimeHost) executeHook(
	ctx context.Context,
	input Input,
	stage lifecycleexecution.Stage,
	saved *agentworkjournal.LifecycleRecord,
	inspectOnly bool,
	exchange Exchange,
) (*lifecycleexecution.Receipt, error) {
	if hook := saved.Hook; hook != nil {
		if hook.Result != nil {
			result := *hook.Result
			return &result, nil
		}
		if hook.ReadyAt != nil && hook.ExitedAt == nil {
			observed, err := host.commands.Inspect(ctx, hook.ContainerID, hook.ExecID)
			if err != nil {
				return nil, err
			}
			if observed.Running {
				return nil, errs.New(errs.KindStateConflict, "Previous lifecycle hook is still running")
			}
			if err := host.journal.ObserveLifecycleHookExit(ctx, input.attempt, input.procedure, hook.ExecID, observed.ExitCode, time.Now().UTC()); err != nil {
				return nil, err
			}
		}
		return nil, errs.New(
			errs.KindStateConflict,
			"Lifecycle hook was interrupted without a saved result; automatic replay is disabled",
		)
	}
	if inspectOnly {
		return nil, nil
	}
	if exchange == nil {
		return nil, invalidInput()
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return nil, err
	}
	delivery, err := exchange.LifecycleHook(
		ctx,
		&agentpb.ServiceLifecycleHookRequest{Attempt: identity, StageId: stage.ID},
	)
	if err != nil {
		return nil, err
	}
	defer clearHookDelivery(delivery)
	if err := executionplan.ValidateLifecycleHookDelivery(input.attempt, input.procedure, delivery); err != nil {
		return nil, err
	}
	containerID, err := composehelper.ResolveLifecycleHookContainer(ctx, host.runner, input.procedure)
	if err != nil {
		return nil, err
	}
	definition, values, _, err := executionplan.DecodeBackingHookProcedure(delivery.Hook)
	if err != nil {
		return nil, err
	}
	recorder := hookRecorder{host: host, input: input}
	if err := backinghookruntime.ExecuteLifecycle(ctx, host.commands, recorder, containerID, definition, values); err != nil {
		return nil, err
	}
	updated, err := host.journal.Lifecycle(ctx, input.attempt, input.procedure)
	if err != nil {
		return nil, err
	}
	if updated == nil || updated.Hook == nil || updated.Hook.ExitedAt == nil || updated.Hook.ExitCode == nil ||
		*updated.Hook.ExitCode != 0 {
		return nil, invalidInput()
	}
	at := time.Now().UTC()
	raw, err := json.Marshal(struct {
		Attempt   string    `json:"attempt"`
		Stage     string    `json:"stage"`
		Container string    `json:"container"`
		Exec      string    `json:"exec"`
		Finished  time.Time `json:"finished"`
	}{input.attempt.ID, stage.ID, containerID, updated.Hook.ExecID, at})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(raw)
	result := lifecycleexecution.Receipt{StageID: stage.ID, State: lifecycleexecution.HookSucceeded, ObservedAt: at,
		Evidence: workexecution.Input{ID: input.attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}}
	if err := host.journal.SaveLifecycleHookResult(ctx, input.attempt, input.procedure, result); err != nil {
		return nil, err
	}
	return &result, nil
}

type hookRecorder struct {
	host  *RuntimeHost
	input Input
}

func (recorder hookRecorder) Created(ctx context.Context, containerID, execID string, at time.Time) error {
	if err := recorder.host.ownership.Check(); err != nil {
		return err
	}
	return recorder.host.journal.PrepareLifecycleHookExec(
		ctx,
		recorder.input.attempt,
		recorder.input.procedure,
		containerID,
		execID,
		at,
	)
}

func (recorder hookRecorder) Ready(ctx context.Context, execID string, at time.Time) error {
	if err := recorder.host.ownership.Check(); err != nil {
		return err
	}
	return recorder.host.journal.AuthorizeLifecycleHookExec(
		ctx,
		recorder.input.attempt,
		recorder.input.procedure,
		execID,
		at,
	)
}

func (recorder hookRecorder) Exited(ctx context.Context, execID string, exitCode int, at time.Time) error {
	return recorder.host.journal.ObserveLifecycleHookExit(
		ctx,
		recorder.input.attempt,
		recorder.input.procedure,
		execID,
		exitCode,
		at,
	)
}
