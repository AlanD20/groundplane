package agentworkjournal

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// resolverState is called under the journal lock. Executable input is supplied
// by the authenticated assignment and never persisted into compact history.
func (journal *Journal) resolverState(ctx context.Context, attempt workexecution.Attempt,
	input resolverexecution.Input, create bool, at time.Time,
) (*attemptRecord, error) {
	layout, err := resolverexecution.CaptureLayout(attempt, input)
	if err != nil {
		return nil, err
	}
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		if !create {
			return nil, nil
		}
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return nil, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return nil, conflict()
		}
		return &attemptRecord{Attempt: attempt, UpdatedAt: at.UTC(), Resolver: &ResolverRecord{Layout: layout}}, nil
	}
	if state.Resolver == nil || state.Resolver.Layout != layout || !workexecution.SameAssignment(state.Attempt, attempt) ||
		(state.Attempt.StartedAt == nil) != (attempt.StartedAt == nil) ||
		attempt.StartedAt != nil && !state.Attempt.StartedAt.Equal(*attempt.StartedAt) {
		return nil, conflict()
	}
	entry := state.Resolver
	if entry.Proof != nil && (input.Candidate == nil || resolverexecution.ValidateServingProof(*input.Candidate, entry.Proof) != nil) {
		return nil, conflict()
	}
	if entry.Source != nil && entry.Recovery != nil {
		if _, _, err := resolverexecution.EncodeRecoveryReport(attempt, input, *entry.Source, *entry.Recovery); err != nil {
			return nil, err
		}
	}
	if entry.Receipt != nil && entry.Source == nil && entry.FailureReason == "" {
		if entry.Progress == nil {
			return nil, conflict()
		}
		evidence, err := resolverexecution.CompletionEvidence(*entry.Receipt, input, *entry.Progress, entry.Verification, entry.Proof)
		if err != nil || evidence != *entry.Receipt.Result {
			return nil, conflict()
		}
	}
	return state, nil
}

func (journal *Journal) Resolver(ctx context.Context, attempt workexecution.Attempt, input resolverexecution.Input) (*ResolverRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.resolverState(ctx, attempt, input, false, time.Time{})
	if err != nil || state == nil {
		return nil, err
	}
	return state.Resolver, nil
}

func (journal *Journal) PrepareResolver(ctx context.Context, attempt workexecution.Attempt,
	input resolverexecution.Input, next resolverexecution.Progress,
) (*ResolverRecord, error) {
	layout, err := resolverexecution.CaptureLayout(attempt, input)
	if err != nil || next.Validate(attempt, layout) != nil || next.Sequence == 0 {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.resolverState(ctx, attempt, input, true, next.UpdatedAt)
	if err != nil {
		return nil, err
	}
	entry := state.Resolver
	if entry.Source != nil || entry.Receipt != nil || entry.FailureReason != "" {
		return nil, conflict()
	}
	previous := entry.Progress
	if previous != nil && resolverexecution.EqualProgress(*previous, next) {
		return entry, nil
	}
	if previous != nil && !entry.Acknowledged {
		return nil, conflict()
	}
	if err := resolverexecution.ValidateTransition(attempt, layout, previous, next); err != nil {
		return nil, err
	}
	if previous != nil && previous.Stage == resolverexecution.VerifyDNS && !next.InFlight {
		observed, err := resolverexecution.ServingObservation(attempt, input, entry.Proof)
		if err != nil || next.Last == nil || *next.Last != observed {
			return nil, conflict()
		}
		verification := next
		entry.Verification = &verification
	}
	if next.Stage == resolverexecution.Complete && input.Enabled && (entry.Proof == nil || entry.Verification == nil) {
		return nil, conflict()
	}
	entry.Progress, entry.Acknowledged = &next, false
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return entry, nil
}

func (journal *Journal) AcknowledgeResolver(ctx context.Context, attempt workexecution.Attempt,
	input resolverexecution.Input, ack *agentpb.ResolverWorkCheckpointAck,
) error {
	layout, err := resolverexecution.CaptureLayout(attempt, input)
	if err != nil || ack == nil {
		return invalid()
	}
	progress, err := resolverexecution.DecodeProgress(attempt, layout, ack.Progress)
	if err != nil {
		return err
	}
	if err := resolverexecution.CheckpointAck(attempt, layout, progress, ack); err != nil {
		return err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.resolverState(ctx, attempt, input, false, time.Time{})
	if err != nil {
		return err
	}
	if state == nil || state.Resolver.Progress == nil || state.Resolver.Receipt != nil || state.Resolver.FailureReason != "" ||
		!resolverexecution.EqualProgress(*state.Resolver.Progress, progress) {
		return conflict()
	}
	if state.Resolver.Acknowledged {
		return nil
	}
	state.Resolver.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) SaveResolverProof(ctx context.Context, attempt workexecution.Attempt,
	input resolverexecution.Input, proof *agentpb.DNSResolverObservationEvidence,
) error {
	observed, err := resolverexecution.ServingObservation(attempt, input, proof)
	if err != nil {
		return err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.resolverState(ctx, attempt, input, false, time.Time{})
	if err != nil {
		return err
	}
	if state == nil || state.Resolver.Progress == nil || !state.Resolver.Acknowledged || !state.Resolver.Progress.InFlight ||
		state.Resolver.Progress.Stage != resolverexecution.VerifyDNS || state.Resolver.Receipt != nil || state.Resolver.FailureReason != "" ||
		observed.At.Before(*state.Resolver.Progress.BeganAt) {
		return conflict()
	}
	if previous := state.Resolver.Proof; previous != nil {
		if !proto.Equal(previous, proof) {
			return conflict()
		}
		return nil
	}
	state.Resolver.Proof = proto.CloneOf(proof)
	return journal.writeRecord(ctx, *state)
}
