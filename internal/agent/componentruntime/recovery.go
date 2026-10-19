package componentruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) runRecovery(
	ctx context.Context,
	input Input,
	exchange Exchange,
) (workexecution.Attempt, error) {
	saved, err := executor.journal.PrepareComponentRecovery(
		ctx,
		input.attempt,
		input.procedure,
		*input.recovery,
		input.boundary,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved.Receipt != nil {
		return *saved.Receipt, nil
	}
	if saved.Failure != nil {
		return executor.deliverFailure(ctx, input, exchange, saved.Failure)
	}
	if saved.Report == nil {
		// Restore must join any earlier helper and inspect the captured
		// predecessor before deciding which restoration effects remain.
		// It must never execute the failed candidate again.
		recoveryCtx, cancel := context.WithDeadline(ctx, input.attempt.Deadline)
		report, err := executor.host.Restore(recoveryCtx, input)
		cancel()
		if err != nil {
			return executor.fail(ctx, input, exchange, err)
		}
		if err := executor.journal.SaveComponentRecoveryReport(ctx, input.attempt, input.procedure, report); err != nil {
			return workexecution.Attempt{}, err
		}
		saved.Report = &report
	}
	raw, evidence, err := componentexecution.EncodeRecoveryReport(
		input.attempt,
		*input.recovery,
		input.procedure,
		input.boundary,
		*saved.Report,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.RecoverComponent(
		ctx,
		&agentpb.ComponentWorkRecoveryReportRequest{Attempt: identity, Report: raw},
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil ||
		ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil || !ack.PredecessorRestored ||
		ack.FinishedAt.AsTime().Before(saved.Report.ObservedAt) {
		return workexecution.Attempt{}, invalidInput()
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.attempt) ||
		ack.Evidence.InputId != evidence.ID || hex.EncodeToString(ack.Evidence.Sha256) != evidence.SHA256 {
		return workexecution.Attempt{}, invalidInput()
	}
	terminal := input.attempt
	finished := ack.FinishedAt.AsTime()
	terminal.FinishedAt, terminal.Result, terminal.Outcome = &finished, &evidence, workexecution.Succeeded
	if err := terminal.Validate(); err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.AcknowledgeComponentRecovery(ctx, input.attempt, input.procedure, terminal); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
