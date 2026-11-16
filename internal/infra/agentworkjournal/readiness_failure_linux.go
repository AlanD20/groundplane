package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// ReadinessFailure returns the immutable failed observation under the exact
// locally acknowledged cursor. Callers may deliver it, never replay its effects.
func (journal *Journal) ReadinessFailure(
	ctx context.Context,
	input releaseexecution.Input,
) (releaseexecution.Input, *EffectRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return input, nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil || state == nil || state.Release == nil || state.Effect == nil {
		return input, nil, err
	}
	if !workexecution.SameAssignment(input.Attempt, state.Attempt) {
		return input, nil, conflict()
	}
	input.Attempt = state.Attempt
	if state.Release.Sequence != state.Effect.Boundary.Sequence {
		return input, nil, nil
	}
	if state.Release.Ack == nil {
		return input, nil, conflict()
	}
	evidence, err := releaseexecution.DecodeEffectEvidence(
		input,
		state.Effect.Boundary,
		state.Effect.Raw,
		state.Effect.Reference,
	)
	if err != nil {
		return input, nil, err
	}
	if evidence.Result.ReadinessFailure == nil {
		return input, nil, nil
	}
	return input, state.Effect, nil
}
