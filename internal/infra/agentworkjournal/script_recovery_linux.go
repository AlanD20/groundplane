package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (journal *Journal) BeginScriptRecovery(ctx context.Context, input scriptexecution.RecoveryInput) (*ScriptRecoveryRecord, error) {
	if input.Validate() != nil || input.Attempt.StartedAt == nil {
		return nil, invalid()
	}
	message, err := scriptexecution.EncodeRecoveryAssignment(input)
	if err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return nil, err
	}
	if state != nil {
		if state.ScriptFailure != nil {
			return nil, conflict()
		}
		if !workexecution.SameAssignment(state.Attempt, input.Attempt) || state.Attempt.StartedAt == nil ||
			!state.Attempt.StartedAt.Equal(*input.Attempt.StartedAt) || state.ScriptRecovery == nil || !bytes.Equal(state.ScriptRecovery.Assignment, raw) {
			return nil, conflict()
		}
		return state.ScriptRecovery, nil
	}
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return nil, err
	}
	if _, reserved := attempts[input.Attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
		return nil, conflict()
	}
	state = &attemptRecord{Attempt: input.Attempt, UpdatedAt: *input.Attempt.StartedAt, ScriptRecovery: &ScriptRecoveryRecord{Assignment: raw}}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.ScriptRecovery, nil
}

// LoadScriptRecovery returns the captured input, including its acknowledged
// Start time, so completed cleanup can be replayed without another Start grant.
func (journal *Journal) LoadScriptRecovery(ctx context.Context, attempt workexecution.Attempt) (scriptexecution.RecoveryInput, *ScriptRecoveryRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return scriptexecution.RecoveryInput{}, nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return scriptexecution.RecoveryInput{}, nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) || state.ScriptRecovery == nil {
		return scriptexecution.RecoveryInput{}, nil, conflict()
	}
	input, err := state.ScriptRecovery.input(state.Attempt)
	if err != nil {
		return scriptexecution.RecoveryInput{}, nil, err
	}
	return input, state.ScriptRecovery, nil
}

func (journal *Journal) SaveScriptRecoveryProof(ctx context.Context, attempt workexecution.Attempt, proof scriptexecution.RecoveryProof) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.ScriptFailure != nil || !workexecution.SameAssignment(state.Attempt, attempt) || state.ScriptRecovery == nil {
		return conflict()
	}
	input, err := state.ScriptRecovery.input(state.Attempt)
	if err != nil {
		return err
	}
	raw, _, err := scriptexecution.EncodeRecoveryProof(input, proof)
	if err != nil {
		return err
	}
	if saved := state.ScriptRecovery.Proof; saved != nil {
		prior, _, err := scriptexecution.EncodeRecoveryProof(input, *saved)
		if err != nil {
			return err
		}
		if !bytes.Equal(prior, raw) {
			return conflict()
		}
		return nil
	}
	state.ScriptRecovery.Proof, state.UpdatedAt = &proof, proof.ObservedAt
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) AcknowledgeScriptRecovery(ctx context.Context, attempt workexecution.Attempt, ack *agentpb.ScriptRecoveryCompletionAck) (workexecution.Attempt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if state == nil || state.ScriptFailure != nil || !workexecution.SameAssignment(state.Attempt, attempt) || state.ScriptRecovery == nil || state.ScriptRecovery.Proof == nil {
		return workexecution.Attempt{}, conflict()
	}
	input, err := state.ScriptRecovery.input(state.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := scriptexecution.DecodeRecoveryCompletion(input, *state.ScriptRecovery.Proof, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved := state.ScriptRecovery.Receipt; saved != nil {
		if !saved.FinishedAt.Equal(*terminal.FinishedAt) || *saved.Result != *terminal.Result || saved.Outcome != terminal.Outcome {
			return workexecution.Attempt{}, conflict()
		}
		return *saved, nil
	}
	state.ScriptRecovery.Receipt = &terminal
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
