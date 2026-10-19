package agentworkjournal

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (journal *Journal) PrepareEffect(
	ctx context.Context,
	input releaseexecution.Input,
	boundary releaseexecution.CheckpointReport,
	result releaseexecution.EffectResult,
	at time.Time,
) (*EffectRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return nil, err
	}
	if state == nil || state.Release == nil || state.Release.Ack == nil || state.Sequence != boundary.Sequence ||
		!workexecution.SameAssignment(
			state.Attempt,
			input.Attempt,
		) || !releaseexecution.EqualProgress(state.Release.Progress, boundary.Progress) ||
		state.Release.StageID != boundary.StageID {
		return nil, conflict()
	}
	if state.Effect != nil && state.Effect.Boundary.Sequence == boundary.Sequence {
		if _, err := releaseexecution.DecodeEffectEvidence(input, boundary, state.Effect.Raw, state.Effect.Reference); err != nil {
			return nil, err
		}
		return state.Effect, nil
	}
	if state.Effect != nil && !state.Effect.Acknowledged {
		return nil, conflict()
	}
	raw, reference, err := releaseexecution.EncodeEffectEvidence(input, boundary, result, at)
	if err != nil {
		return nil, err
	}
	state.Effect = &EffectRecord{Boundary: boundary, Reference: reference, Raw: raw}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Effect, nil
}

func (journal *Journal) Effect(
	ctx context.Context,
	input releaseexecution.Input,
	boundary releaseexecution.CheckpointReport,
) (*EffectRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, input.Attempt) {
		return nil, conflict()
	}
	if state.Effect == nil || state.Effect.Boundary.Sequence != boundary.Sequence {
		return nil, nil
	}
	if _, err := releaseexecution.DecodeEffectEvidence(input, boundary, state.Effect.Raw, state.Effect.Reference); err != nil {
		return nil, err
	}
	return state.Effect, nil
}

func (journal *Journal) AcknowledgeEffect(
	ctx context.Context,
	input releaseexecution.Input,
	boundary releaseexecution.CheckpointReport,
	reference workexecution.Input,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.Effect == nil || !workexecution.SameAssignment(state.Attempt, input.Attempt) ||
		state.Effect.Reference != reference {
		return conflict()
	}
	if _, err := releaseexecution.DecodeEffectEvidence(input, boundary, state.Effect.Raw, reference); err != nil {
		return err
	}
	state.Effect.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}
