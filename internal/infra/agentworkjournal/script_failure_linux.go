package agentworkjournal

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareScriptFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
) (ScriptFailureDelivery, error) {
	if attempt.Validate() != nil || attempt.FinishedAt != nil {
		return ScriptFailureDelivery{}, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return ScriptFailureDelivery{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return ScriptFailureDelivery{}, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return ScriptFailureDelivery{}, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return ScriptFailureDelivery{}, conflict()
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: attempt.AssignedAt}
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) {
		return ScriptFailureDelivery{}, conflict()
	}
	if state.ScriptFailure != nil {
		return *state.ScriptFailure, nil
	}
	state.ScriptFailure = &ScriptFailureDelivery{}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return ScriptFailureDelivery{}, err
	}
	return *state.ScriptFailure, nil
}

func (journal *Journal) LoadScriptFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
) (*ScriptFailureDelivery, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) {
		return nil, conflict()
	}
	return state.ScriptFailure, nil
}

func (journal *Journal) AcknowledgeScriptFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	ack *agentpb.ScriptWorkFailureAck,
) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil || ack.FinishedAt == nil ||
		ack.FinishedAt.CheckValid() != nil {
		return workexecution.Attempt{}, invalid()
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, attempt) {
		return workexecution.Attempt{}, conflict()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if state == nil || state.ScriptFailure == nil || !workexecution.SameAssignment(state.Attempt, attempt) {
		return workexecution.Attempt{}, conflict()
	}
	terminal := state.Attempt
	finished := ack.FinishedAt.AsTime().UTC()
	terminal.FinishedAt, terminal.Outcome = &finished, workexecution.Outcome(ack.Outcome)
	terminal.Result = &workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	if terminal.Validate() != nil || finished.Before(state.UpdatedAt) {
		return workexecution.Attempt{}, invalid()
	}
	if prior := state.ScriptFailure.Receipt; prior != nil {
		if prior.Outcome != terminal.Outcome || !prior.FinishedAt.Equal(finished) || *prior.Result != *terminal.Result {
			return workexecution.Attempt{}, conflict()
		}
		return *prior, nil
	}
	state.ScriptFailure.Receipt = &terminal
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
