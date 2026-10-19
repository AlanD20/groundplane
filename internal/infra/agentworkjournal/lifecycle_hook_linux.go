package agentworkjournal

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func lifecycleHookWritable(
	state *attemptRecord,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
) bool {
	return matchesLifecycle(state, attempt, input) && state.Lifecycle.Completion == nil &&
		state.Lifecycle.Acknowledged &&
		state.Lifecycle.Progress.BeganAt != nil &&
		state.Lifecycle.Stages[len(state.Lifecycle.Progress.Completed)].Expected == lifecycleexecution.HookSucceeded
}

// PrepareLifecycleHookExec persists identity before Docker attaches the stdin
// gate. Even an identical second call cannot authorize another exec.
func (journal *Journal) PrepareLifecycleHookExec(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	containerID, execID string,
	at time.Time,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !lifecycleHookWritable(state, attempt, input) || state.Lifecycle.Failure != nil || state.Lifecycle.Hook != nil {
		return conflict()
	}
	state.Lifecycle.Hook = &LifecycleHookRecord{
		Boundary:    state.Lifecycle.Progress,
		ContainerID: containerID,
		ExecID:      execID,
		CreatedAt:   at.UTC(),
	}
	return journal.writeRecord(ctx, *state)
}

// AuthorizeLifecycleHookExec is called after the wrapper's readiness message
// and before writing its single permission line. It rejects duplicate grants.
func (journal *Journal) AuthorizeLifecycleHookExec(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	execID string,
	at time.Time,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !lifecycleHookWritable(state, attempt, input) || state.Lifecycle.Failure != nil || state.Lifecycle.Hook == nil {
		return conflict()
	}
	hook := state.Lifecycle.Hook
	if hook.ExecID != execID || hook.ReadyAt != nil {
		return conflict()
	}
	ready := at.UTC()
	hook.ReadyAt = &ready
	return journal.writeRecord(ctx, *state)
}

// ObserveLifecycleHookExit records only process exit. Successful output parsing
// and a separately saved Result are required before reporting hook success.
func (journal *Journal) ObserveLifecycleHookExit(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	execID string,
	exitCode int,
	at time.Time,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !lifecycleHookWritable(state, attempt, input) || state.Lifecycle.Hook == nil {
		return conflict()
	}
	hook := state.Lifecycle.Hook
	if hook.ExecID != execID || hook.ReadyAt == nil {
		return conflict()
	}
	if hook.ExitedAt != nil {
		if *hook.ExitCode != exitCode {
			return conflict()
		}
		return nil
	}
	exited := at.UTC()
	hook.ExitedAt, hook.ExitCode = &exited, &exitCode
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) SaveLifecycleHookResult(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	result lifecycleexecution.Receipt,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !lifecycleHookWritable(state, attempt, input) || state.Lifecycle.Failure != nil || state.Lifecycle.Hook == nil {
		return conflict()
	}
	hook := state.Lifecycle.Hook
	if hook.Result != nil {
		if *hook.Result != result {
			return conflict()
		}
		return nil
	}
	hook.Result = &result
	return journal.writeRecord(ctx, *state)
}
