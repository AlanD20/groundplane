package agentworkjournal

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type RecoveryEffectRecord struct {
	Boundary     releaseexecution.RecoveryReport `json:"boundary"`
	Reference    workexecution.Input             `json:"reference"`
	Raw          []byte                          `json:"raw"`
	Acknowledged bool                            `json:"acknowledged"`
}

func (journal *Journal) PrepareRecoveryEffect(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	boundary releaseexecution.RecoveryReport,
	result releaseexecution.RecoveryEffectResult,
	at time.Time,
) (*RecoveryEffectRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Input.Attempt.ID)
	if err != nil {
		return nil, err
	}
	if state == nil || state.Recovery == nil || state.Recovery.Report.Ack == nil ||
		!workexecution.SameAssignment(state.Attempt, input.Input.Attempt) || state.Sequence < boundary.Sequence ||
		state.Recovery.Report.StageID != boundary.StageID || !releaseexecution.EqualRecoveryProgress(state.Recovery.Report.Progress, boundary.Progress) {
		return nil, conflict()
	}
	if existing := state.Recovery.Effect; existing != nil {
		if existing.Boundary.Progress.Sequence == boundary.Progress.Sequence {
			if _, err := releaseexecution.DecodeRecoveryEffect(input, boundary, existing.Raw, existing.Reference); err != nil {
				return nil, err
			}
			return existing, nil
		}
		if !existing.Acknowledged {
			return nil, conflict()
		}
	}
	raw, reference, err := releaseexecution.EncodeRecoveryEffect(input, boundary, result, at)
	if err != nil {
		return nil, err
	}
	state.Recovery.Effect = &RecoveryEffectRecord{Boundary: boundary, Reference: reference, Raw: raw}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Recovery.Effect, nil
}

func (journal *Journal) RecoveryEffect(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	boundary releaseexecution.RecoveryReport,
) (*RecoveryEffectRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Input.Attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, input.Input.Attempt) || state.Recovery == nil {
		return nil, conflict()
	}
	record := state.Recovery.Effect
	if record == nil || record.Boundary.Progress.Sequence != boundary.Progress.Sequence {
		return nil, nil
	}
	if _, err := releaseexecution.DecodeRecoveryEffect(input, boundary, record.Raw, record.Reference); err != nil {
		return nil, err
	}
	return record, nil
}

func (journal *Journal) AcknowledgeRecoveryEffect(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	boundary releaseexecution.RecoveryReport,
	reference workexecution.Input,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Input.Attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.Recovery == nil || state.Recovery.Effect == nil ||
		!workexecution.SameAssignment(state.Attempt, input.Input.Attempt) {
		return conflict()
	}
	record := state.Recovery.Effect
	if record.Reference != reference {
		return conflict()
	}
	if _, err := releaseexecution.DecodeRecoveryEffect(input, boundary, record.Raw, reference); err != nil {
		return err
	}
	record.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}
