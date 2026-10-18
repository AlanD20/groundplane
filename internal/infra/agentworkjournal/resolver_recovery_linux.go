package agentworkjournal

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareResolverRecovery(ctx context.Context, attempt workexecution.Attempt,
	input resolverexecution.Input, source resolverexecution.Recovery,
) (*ResolverRecord, error) {
	layout, err := resolverexecution.CaptureLayout(attempt, input)
	if err != nil || attempt.StartedAt == nil || source.ValidateFor(attempt, layout) != nil {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.resolverState(ctx, attempt, input, true, *attempt.StartedAt)
	if err != nil {
		return nil, err
	}
	if state.Resolver.Source != nil {
		expected, err := resolverexecution.EncodeRecovery(source)
		if err != nil {
			return nil, err
		}
		actual, err := resolverexecution.EncodeRecovery(*state.Resolver.Source)
		if err != nil || !bytes.Equal(expected, actual) {
			return nil, conflict()
		}
		return state.Resolver, nil
	}
	if state.Resolver.Progress != nil || state.Resolver.Receipt != nil || state.Resolver.FailureReason != "" {
		return nil, conflict()
	}
	state.Resolver.Source = &source
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Resolver, nil
}

func (journal *Journal) SaveResolverRecovery(ctx context.Context, attempt workexecution.Attempt,
	input resolverexecution.Input, report resolverexecution.RecoveryReport,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.resolverState(ctx, attempt, input, false, time.Time{})
	if err != nil {
		return err
	}
	if state == nil || state.Resolver.Source == nil || state.Resolver.Receipt != nil {
		return conflict()
	}
	entry := state.Resolver
	raw, _, err := resolverexecution.EncodeRecoveryReport(attempt, input, *entry.Source, report)
	if err != nil {
		return err
	}
	if previous := entry.Recovery; previous != nil {
		prior, _, err := resolverexecution.EncodeRecoveryReport(attempt, input, *entry.Source, *previous)
		if err != nil || !bytes.Equal(prior, raw) {
			return conflict()
		}
		return nil
	}
	if report.ObservedAt.Before(state.UpdatedAt) {
		return conflict()
	}
	entry.Recovery = &report
	state.UpdatedAt = report.ObservedAt
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) AcknowledgeResolverRecovery(ctx context.Context, attempt workexecution.Attempt,
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
	if state == nil || state.Resolver.Source == nil || state.Resolver.Recovery == nil || ack == nil || len(ack.Failure) != 0 {
		return workexecution.Attempt{}, conflict()
	}
	entry := state.Resolver
	_, evidence, err := resolverexecution.EncodeRecoveryReport(attempt, input, *entry.Source, *entry.Recovery)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := resolverexecution.DecodeTerminal(attempt, "recovery", evidence, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	outcome := workexecution.Failed
	if entry.Recovery.Status == resolverexecution.RecoveryComplete {
		outcome = workexecution.Succeeded
	}
	if terminal.Outcome != outcome || terminal.FinishedAt.Before(state.UpdatedAt) {
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
