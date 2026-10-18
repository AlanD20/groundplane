package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) LoadEnvironmentProvision(ctx context.Context, attempt workexecution.Attempt) (*workexecution.Attempt, *EnvironmentProvisionRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return nil, nil, err
	}
	if state.EnvironmentProvision == nil || !workexecution.SameAssignment(state.Attempt, attempt) {
		return nil, nil, conflict()
	}
	return &state.Attempt, state.EnvironmentProvision, nil
}

func (journal *Journal) EnvironmentProvision(ctx context.Context, attempt workexecution.Attempt) (*EnvironmentProvisionRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if state.EnvironmentProvision == nil || !workexecution.SameAssignment(state.Attempt, attempt) ||
		state.Attempt.StartedAt == nil || attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*attempt.StartedAt) {
		return nil, conflict()
	}
	return state.EnvironmentProvision, nil
}

// SaveEnvironmentProvision preserves the first failure before independent
// inspection. Once submitted, the exact result is replayed, never refined under
// a potentially accepted acknowledgement.
func (journal *Journal) SaveEnvironmentProvision(ctx context.Context, attempt workexecution.Attempt, result environmentexecution.Result) error {
	if attempt.StartedAt == nil || result.Validate(attempt) != nil {
		return invalid()
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
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return err
		}
		if _, exists := attempts[attempt.ID]; !exists && len(attempts) >= MaximumAttempts {
			return conflict()
		}
		state = &attemptRecord{Attempt: attempt}
	} else if !workexecution.SameAssignment(state.Attempt, attempt) || state.EnvironmentProvision == nil ||
		state.Attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*attempt.StartedAt) {
		return conflict()
	}
	if saved := state.EnvironmentProvision; saved != nil {
		if saved.Result == result {
			return nil
		}
		if saved.Submitted || saved.Receipt != nil || saved.Result.HelpersAbsent ||
			saved.Result.ObservedAt.After(result.ObservedAt) || saved.Result.Failure != "" && saved.Result.Failure != result.Failure {
			return conflict()
		}
	}
	state.EnvironmentProvision = &EnvironmentProvisionRecord{Result: result}
	state.UpdatedAt = result.ObservedAt
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) SubmitEnvironmentProvision(ctx context.Context, attempt workexecution.Attempt) (*EnvironmentProvisionRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	if state == nil || state.EnvironmentProvision == nil || !workexecution.SameAssignment(state.Attempt, attempt) {
		return nil, conflict()
	}
	state.EnvironmentProvision.Submitted = true
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.EnvironmentProvision, nil
}

func (journal *Journal) AcknowledgeEnvironmentProvision(ctx context.Context, attempt workexecution.Attempt, ack *agentpb.EnvironmentProvisionResultAck) (workexecution.Attempt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if state == nil || state.EnvironmentProvision == nil || !state.EnvironmentProvision.Submitted || !workexecution.SameAssignment(state.Attempt, attempt) {
		return workexecution.Attempt{}, conflict()
	}
	terminal, err := environmentexecution.DecodeResultAck(attempt, state.EnvironmentProvision.Result, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if previous := state.EnvironmentProvision.Receipt; previous != nil {
		if previous.Outcome != terminal.Outcome || !previous.FinishedAt.Equal(*terminal.FinishedAt) || *previous.Result != *terminal.Result {
			return workexecution.Attempt{}, conflict()
		}
		return *previous, nil
	}
	state.EnvironmentProvision.Receipt = &terminal
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
