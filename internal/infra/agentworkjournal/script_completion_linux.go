package agentworkjournal

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ScriptCompletion struct {
	Record  ScriptRecord
	Receipt *workexecution.Attempt
}

func (record attemptRecord) validateScriptCompletion() error {
	terminal := record.ScriptCompletion
	script, err := record.script()
	if err != nil {
		return err
	}
	if terminal == nil || terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil ||
		!workexecution.SameAssignment(record.Attempt, *terminal) || record.Attempt.Direction != workexecution.Forward ||
		record.Release != nil || record.Recovery != nil || record.Failure != nil || record.Effect != nil || record.Completion != nil ||
		script == nil || script.Ack == nil || script.Report.State != agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN ||
		script.Report.Sequence != record.Sequence || terminal.FinishedAt.Before(record.UpdatedAt) ||
		terminal.Result.ID != script.Ack.EvidenceId || terminal.Result.SHA256 != hex.EncodeToString(script.Ack.EvidenceSha256) {
		return conflict()
	}
	return nil
}

// LoadScriptCompletion bypasses Start when command cleanup is already durable.
// A missing receipt means only settlement must be replayed, never the command.
func (journal *Journal) LoadScriptCompletion(ctx context.Context, attempt workexecution.Attempt) (*ScriptCompletion, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) || state.Release != nil || state.Recovery != nil {
		return nil, conflict()
	}
	script, err := state.script()
	if err != nil {
		return nil, err
	}
	if script == nil || script.Ack == nil || script.Report.State != agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
		return nil, nil
	}
	return &ScriptCompletion{Record: *script, Receipt: state.ScriptCompletion}, nil
}

func (journal *Journal) AcknowledgeScriptCompletion(ctx context.Context, attempt workexecution.Attempt, ack *agentpb.ScriptWorkCompletionAck) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.CheckpointEvidence == nil || ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil {
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
	if state == nil || !workexecution.SameAssignment(state.Attempt, attempt) || state.Sequence != ack.CheckpointSequence {
		return workexecution.Attempt{}, conflict()
	}
	terminal := state.Attempt
	finished := ack.FinishedAt.AsTime().UTC()
	terminal.FinishedAt = &finished
	terminal.Outcome = workexecution.Outcome(ack.Outcome)
	terminal.Result = &workexecution.Input{ID: ack.CheckpointEvidence.InputId, SHA256: hex.EncodeToString(ack.CheckpointEvidence.Sha256)}
	if saved := state.ScriptCompletion; saved != nil {
		if saved.Outcome != terminal.Outcome || !saved.FinishedAt.Equal(finished) || *saved.Result != *terminal.Result {
			return workexecution.Attempt{}, conflict()
		}
		return *saved, nil
	}
	state.ScriptCompletion = &terminal
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
