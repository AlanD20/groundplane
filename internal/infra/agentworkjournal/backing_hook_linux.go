package agentworkjournal

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func backingHookWritable(state *attemptRecord, attempt workexecution.Attempt, layout backingprovision.Layout) bool {
	return matchesBacking(state, attempt, layout) && state.Backing.Completion == nil && state.Backing.Failure == nil &&
		state.Backing.Acknowledged && state.Backing.Progress != nil && state.Backing.Progress.InFlight &&
		state.Backing.Progress.Stage == backingprovision.AfterStart
}

func (journal *Journal) PrepareBackingHookExec(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput, containerID, execID string, at time.Time,
) error {
	layout, err := backingprovision.CaptureLayout(attempt, input)
	if err != nil {
		return err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !backingHookWritable(state, attempt, layout) || state.Backing.Hook != nil {
		return conflict()
	}
	state.Backing.Hook = &BackingHookRecord{
		Boundary:    *state.Backing.Progress,
		ContainerID: containerID,
		ExecID:      execID,
		CreatedAt:   at.UTC(),
	}
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) AuthorizeBackingHookExec(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput, execID string, at time.Time,
) error {
	layout, err := backingprovision.CaptureLayout(attempt, input)
	if err != nil {
		return err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !backingHookWritable(state, attempt, layout) || state.Backing.Hook == nil ||
		state.Backing.Hook.ExecID != execID || state.Backing.Hook.ReadyAt != nil {
		return conflict()
	}
	ready := at.UTC()
	state.Backing.Hook.ReadyAt = &ready
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) ObserveBackingHookExit(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput, execID string, exitCode int, at time.Time,
) error {
	layout, err := backingprovision.CaptureLayout(attempt, input)
	if err != nil {
		return err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !matchesBacking(state, attempt, layout) || state.Backing.Hook == nil ||
		state.Backing.Hook.ExecID != execID || state.Backing.Hook.ReadyAt == nil {
		return conflict()
	}
	hook := state.Backing.Hook
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

func (journal *Journal) SaveBackingHookResult(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput, at time.Time,
) (backingprovision.Observation, error) {
	layout, err := backingprovision.CaptureLayout(attempt, input)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	if !backingHookWritable(state, attempt, layout) || state.Backing.Hook == nil {
		return backingprovision.Observation{}, conflict()
	}
	hook := state.Backing.Hook
	if hook.Result != nil {
		return *hook.Result, nil
	}
	if hook.ExitedAt == nil || hook.ExitCode == nil || *hook.ExitCode != 0 {
		return backingprovision.Observation{}, conflict()
	}
	evidence, err := backingprovision.HookSuccessEvidence(attempt, layout, hook.ContainerID)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	result := backingprovision.Observation{
		ServiceID:   layout.ServiceID,
		Input:       layout.Input,
		Stage:       backingprovision.AfterStart,
		ContainerID: hook.ContainerID,
		HookResult:  evidence,
		At:          at.UTC(),
	}
	hook.Result = &result
	return result, journal.writeRecord(ctx, *state)
}
