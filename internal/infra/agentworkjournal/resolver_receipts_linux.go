package agentworkjournal

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) CompleteResolver(ctx context.Context, attempt workexecution.Attempt,
	input resolverexecution.Input, ack *agentpb.ResolverWorkResultAck,
) (workexecution.Attempt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.resolverState(ctx, attempt, input, false, time.Time{})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if state == nil || state.Resolver.Progress == nil || !state.Resolver.Acknowledged ||
		state.Resolver.FailureReason != "" ||
		state.Resolver.Source != nil ||
		ack == nil ||
		len(ack.Failure) != 0 {
		return workexecution.Attempt{}, conflict()
	}
	entry := state.Resolver
	evidence, err := resolverexecution.CompletionEvidence(
		attempt,
		input,
		*entry.Progress,
		entry.Verification,
		entry.Proof,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := resolverexecution.DecodeTerminal(attempt, "completion", evidence, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if terminal.FinishedAt.Before(entry.Progress.UpdatedAt) ||
		terminal.Outcome != workexecution.Succeeded && terminal.Outcome != workexecution.Cancelled &&
			terminal.Outcome != workexecution.Superseded {
		return workexecution.Attempt{}, conflict()
	}
	if saved := entry.Receipt; saved != nil {
		if !sameResolverReceipt(saved, terminal) {
			return workexecution.Attempt{}, conflict()
		}
		return *saved, nil
	}
	entry.Receipt = &terminal
	return terminal, journal.writeRecord(ctx, *state)
}

func (journal *Journal) PrepareResolverFailure(ctx context.Context, attempt workexecution.Attempt,
	input resolverexecution.Input, reason resolverexecution.FailureReason, at time.Time,
) (*ResolverRecord, error) {
	if attempt.Direction != workexecution.Forward ||
		reason != resolverexecution.ExecutionFailed && reason != resolverexecution.Interrupted &&
			reason != resolverexecution.DeadlineExpired ||
		at.Before(attempt.AssignedAt) ||
		attempt.StartedAt != nil && at.Before(*attempt.StartedAt) ||
		reason == resolverexecution.DeadlineExpired && at.Before(attempt.Deadline) {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.resolverState(ctx, attempt, input, true, at)
	if err != nil {
		return nil, err
	}
	entry := state.Resolver
	if entry.Source != nil || entry.Receipt != nil ||
		entry.Progress != nil && entry.Progress.Stage == resolverexecution.Complete ||
		at.Before(state.UpdatedAt) {
		return nil, conflict()
	}
	if entry.FailureReason != "" {
		return entry, nil
	}
	entry.FailureReason = reason
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return entry, nil
}

func (journal *Journal) AcknowledgeResolverFailure(ctx context.Context, attempt workexecution.Attempt,
	input resolverexecution.Input, ack *agentpb.ResolverWorkResultAck,
) (workexecution.Attempt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.resolverState(ctx, attempt, input, false, time.Time{})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if state == nil || state.Resolver.FailureReason == "" || ack == nil {
		return workexecution.Attempt{}, conflict()
	}
	entry := state.Resolver
	evidence, err := resolverexecution.DecodeReference(ack.Evidence)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := resolverexecution.DecodeTerminal(attempt, "failure", evidence, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	failure, err := resolverexecution.DecodeFailure(terminal, entry.Layout, ack.Failure, evidence)
	if err != nil || failure.Reason != entry.FailureReason || terminal.FinishedAt.Before(state.UpdatedAt) {
		return workexecution.Attempt{}, conflict()
	}
	if cursor := failure.Checkpoint; cursor != nil {
		if entry.Progress == nil || cursor.Sequence > entry.Progress.Sequence {
			return workexecution.Attempt{}, conflict()
		}
		if cursor.Sequence == entry.Progress.Sequence {
			local, err := entry.Progress.Checkpoint(attempt, entry.Layout)
			if err != nil || local != *cursor {
				return workexecution.Attempt{}, conflict()
			}
		} else if entry.Acknowledged || cursor.Sequence+1 != entry.Progress.Sequence {
			return workexecution.Attempt{}, conflict()
		}
	} else if entry.Progress != nil && (entry.Acknowledged || entry.Progress.Sequence != 1) {
		return workexecution.Attempt{}, conflict()
	}
	if saved := entry.Receipt; saved != nil {
		if !sameResolverReceipt(saved, terminal) {
			return workexecution.Attempt{}, conflict()
		}
		return *saved, nil
	}
	entry.Failure, entry.Receipt = append([]byte(nil), ack.Failure...), &terminal
	return terminal, journal.writeRecord(ctx, *state)
}

func sameResolverReceipt(saved *workexecution.Attempt, terminal workexecution.Attempt) bool {
	return saved != nil && sameResolverTerminal(*saved, terminal) && saved.Outcome == terminal.Outcome &&
		saved.Result != nil && terminal.Result != nil && *saved.Result == *terminal.Result && saved.FinishedAt != nil &&
		saved.FinishedAt.Equal(*terminal.FinishedAt)
}
