package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type RecoveryRecord struct {
	Forward    workexecution.Attempt                       `json:"forward"`
	Failure    releaseexecution.Failure                    `json:"failure"`
	Group      *releaseexecution.GroupRestorationAuthority `json:"group,omitempty"`
	Report     releaseexecution.RecoveryReport             `json:"report"`
	Effect     *RecoveryEffectRecord                       `json:"effect,omitempty"`
	Completion *releaseexecution.CompletionReceipt         `json:"completion,omitempty"`
}

func (record RecoveryRecord) input(attempt workexecution.Attempt) releaseexecution.RecoveryInput {
	return releaseexecution.RecoveryInput{Input: releaseexecution.Input{Attempt: attempt}, Forward: record.Forward, Failure: record.Failure, Group: releaseexecution.CloneGroupRestoration(record.Group)}
}

func (journal *Journal) PrepareRecovery(ctx context.Context, input releaseexecution.RecoveryInput, next releaseexecution.RecoveryProgress) (*releaseexecution.RecoveryReport, error) {
	if input.Validate() != nil || input.Input.Attempt.StartedAt == nil {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Input.Attempt.ID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return nil, err
		}
		if _, reserved := attempts[input.Input.Attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return nil, conflict()
		}
		state = &attemptRecord{Attempt: input.Input.Attempt}
	}
	if !workexecution.SameAssignment(state.Attempt, input.Input.Attempt) || state.Attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*input.Input.Attempt.StartedAt) {
		return nil, conflict()
	}
	var prior *releaseexecution.RecoveryProgress
	if state.Recovery != nil {
		if state.Recovery.Forward.ID != input.Forward.ID || *state.Recovery.Forward.Result != *input.Forward.Result ||
			!releaseexecution.EqualGroupRestoration(state.Recovery.Group, input.Group) {
			return nil, conflict()
		}
		prior = &state.Recovery.Report.Progress
	}
	if err := releaseexecution.ValidateRecoveryTransition(input, prior, next); err != nil {
		return nil, err
	}
	if prior != nil && releaseexecution.EqualRecoveryProgress(*prior, next) {
		return &state.Recovery.Report, nil
	}
	pending, err := state.pending()
	if err != nil {
		return nil, err
	}
	if pending || next.Sequence != state.Sequence+1 || next.UpdatedAt.Before(state.UpdatedAt) {
		return nil, conflict()
	}
	if !next.InFlight {
		if state.Recovery == nil || state.Recovery.Effect == nil || !state.Recovery.Effect.Acknowledged || next.Evidence == nil ||
			state.Recovery.Effect.Boundary.Progress.Sequence != state.Sequence || state.Recovery.Effect.Reference != *next.Evidence {
			return nil, conflict()
		}
	}
	stage := next
	if prior != nil && !next.InFlight {
		stage = *prior
	}
	stageID, err := stage.StageID(input)
	if err != nil {
		return nil, err
	}
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	var effect *RecoveryEffectRecord
	if state.Recovery != nil {
		effect = state.Recovery.Effect
	}
	state.Recovery = &RecoveryRecord{Forward: input.Forward, Failure: input.Failure, Group: releaseexecution.CloneGroupRestoration(input.Group), Report: releaseexecution.RecoveryReport{StageID: stageID, Progress: next}, Effect: effect}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return &state.Recovery.Report, nil
}

func (journal *Journal) AcknowledgeRecovery(ctx context.Context, ack workexecution.Checkpoint) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, ack.AttemptID)
	if err != nil {
		return err
	}
	if state == nil || state.Recovery == nil {
		return conflict()
	}
	expected, err := state.Recovery.Report.Checkpoint(state.Recovery.input(state.Attempt))
	if err != nil || !releaseexecution.EqualCheckpoint(expected, ack) {
		return conflict()
	}
	state.Recovery.Report.Ack = &ack
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) Recovery(ctx context.Context, input releaseexecution.RecoveryInput) (*releaseexecution.RecoveryReport, error) {
	_, report, err := journal.LoadRecovery(ctx, input)
	return report, err
}

// LoadRecovery restores the persisted start identity as well as its cursor.
// A redelivered assignment need not carry the start receipt already on disk.
func (journal *Journal) LoadRecovery(ctx context.Context, input releaseexecution.RecoveryInput) (releaseexecution.RecoveryInput, *releaseexecution.RecoveryReport, error) {
	if input.Validate() != nil {
		return input, nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return input, nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Input.Attempt.ID)
	if err != nil || state == nil {
		return input, nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, input.Input.Attempt) || state.Recovery == nil ||
		state.Recovery.Forward.ID != input.Forward.ID || *state.Recovery.Forward.Result != *input.Forward.Result ||
		!releaseexecution.EqualGroupRestoration(state.Recovery.Group, input.Group) {
		return input, nil, conflict()
	}
	input.Input.Attempt = state.Attempt
	if err := state.Recovery.Report.Validate(input); err != nil {
		return input, nil, err
	}
	return input, &state.Recovery.Report, nil
}
