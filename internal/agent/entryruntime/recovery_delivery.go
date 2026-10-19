package entryruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) deliverRecovery(
	ctx context.Context,
	input Input,
	exchange Exchange,
	report entryexecution.RecoveryReport,
) (workexecution.Attempt, error) {
	if input.recovery == nil || input.procedure == nil {
		return workexecution.Attempt{}, invalidInput()
	}
	raw, evidence, err := entryexecution.EncodeRecoveryReport(
		input.attempt,
		*input.recovery,
		input.procedure,
		input.boundary,
		report,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.RecoverEntryConfiguration(
		ctx,
		&agentpb.EntryConfigurationWorkRecoveryReportRequest{Attempt: identity, Report: raw},
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil || ack.StartedAt == nil ||
		ack.FinishedAt == nil ||
		ack.StartedAt.CheckValid() != nil ||
		ack.FinishedAt.CheckValid() != nil ||
		input.attempt.StartedAt == nil ||
		!input.attempt.StartedAt.Equal(ack.StartedAt.AsTime()) {
		return workexecution.Attempt{}, invalidInput()
	}
	attempt, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.attempt) {
		return workexecution.Attempt{}, invalidInput()
	}
	ackEvidence := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	outcome := workexecution.Failed
	settled := report.Settled(input.procedure)
	if settled {
		outcome = workexecution.Succeeded
	}
	finished := ack.FinishedAt.AsTime().UTC()
	if ackEvidence != evidence || ack.PredecessorRestored != settled || ack.Outcome != string(outcome) ||
		finished.Before(report.ObservedAt) {
		return workexecution.Attempt{}, invalidInput()
	}
	terminal := input.attempt
	terminal.Result, terminal.Outcome, terminal.FinishedAt = &evidence, outcome, &finished
	if err := terminal.Validate(); err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.AcknowledgeEntryRecovery(ctx, input.attempt, input.procedure, terminal); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
