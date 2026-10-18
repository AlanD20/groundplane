package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (journal *Journal) RetireResolver(ctx context.Context, terminal workexecution.Attempt) error {
	if terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil {
		return invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, terminal.ID)
	if err != nil || state == nil {
		return err
	}
	if state.Resolver == nil || !sameResolverReceipt(state.Resolver.Receipt, terminal) {
		return conflict()
	}
	entry := state.Resolver
	if entry.FailureReason != "" && terminal.StartedAt != nil {
		return nil
	}
	if entry.Source != nil {
		if entry.Recovery == nil || entry.Recovery.Status != resolverexecution.RecoveryComplete {
			return nil
		}
		if err := journal.retireRecoveredResolver(ctx, *entry); err != nil {
			return err
		}
	}
	return journal.removeRecord(ctx, terminal.ID)
}

func (journal *Journal) retireRecoveredResolver(ctx context.Context, recovered ResolverRecord) error {
	if recovered.Receipt == nil || recovered.Source == nil || recovered.Recovery == nil ||
		recovered.Recovery.Status != resolverexecution.RecoveryComplete {
		return invalid()
	}
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return err
	}
	forward := recovered.Source.Forward
	for id, committed := range attempts {
		if !committed || id == recovered.Receipt.ID {
			continue
		}
		state, err := journal.readRecord(ctx, id)
		if err != nil {
			return err
		}
		if state == nil || state.Resolver == nil || state.Attempt.WorkItemID != recovered.Receipt.WorkItemID {
			continue
		}
		if id == forward.ID {
			if !workexecution.SameAssignment(state.Attempt, forward) || state.Resolver.Layout != recovered.Layout ||
				state.Resolver.Source != nil || state.Resolver.Receipt != nil && state.Resolver.FailureReason == "" ||
				state.Resolver.Receipt != nil && !sameResolverReceipt(state.Resolver.Receipt, forward) {
				return conflict()
			}
		} else {
			source := state.Resolver.Source
			if source == nil || source.Forward.ID != forward.ID {
				continue
			}
			if state.Attempt.TaskID != recovered.Receipt.TaskID || state.Attempt.Executor.HostID != recovered.Receipt.Executor.HostID ||
				state.Attempt.Input != recovered.Receipt.Input || state.Attempt.Number >= recovered.Receipt.Number ||
				state.Resolver.Layout != recovered.Layout || source.Evidence != recovered.Source.Evidence ||
				state.Resolver.Receipt != nil && state.Resolver.Receipt.Outcome == workexecution.Succeeded {
				return conflict()
			}
		}
		// The accepted restoration joined the stable exact work/input helper,
		// accounting for earlier cleanup orphans as well as the failed forward.
		if err := journal.removeRecord(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
