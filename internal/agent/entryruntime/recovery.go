package entryruntime

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (executor *Executor) recover(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	ack, err := exchange.Start(ctx, input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	input, reconcile, err := input.WithStart(ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	saved, err := executor.journal.PrepareEntryRecovery(ctx, input.attempt, input.procedure, *input.recovery, input.boundary)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved.Receipt != nil {
		return *saved.Receipt, nil
	}
	if saved.Report != nil {
		return executor.deliverRecovery(ctx, input, exchange, *saved.Report)
	}
	bounded, cancel := context.WithDeadline(ctx, input.attempt.Deadline)
	if reconcile {
		cancel()
		bounded, cancel = context.WithTimeout(ctx, 30*time.Second)
	}
	defer cancel()
	report, err := executor.host.Restore(bounded, input, reconcile)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.SaveEntryRecoveryReport(ctx, input.attempt, input.procedure, report); err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.deliverRecovery(ctx, input, exchange, report)
}

func (host *RuntimeHost) Restore(ctx context.Context, input Input, inspectOnly bool) (entryexecution.RecoveryReport, error) {
	if input.recovery == nil || input.attempt.StartedAt == nil {
		return entryexecution.RecoveryReport{}, invalidInput()
	}
	if err := host.ownership.CheckRecovery(input.recovery.Forward, input.attempt); err != nil {
		return entryexecution.RecoveryReport{}, err
	}
	report := entryexecution.RecoveryReport{AttemptID: input.attempt.ID, ForwardID: input.recovery.Forward.ID, Failure: input.recovery.Evidence, EnvironmentID: input.procedure.EnvironmentId, Input: input.attempt.Input}
	captured := entryexecution.FileInput{Attempt: input.attempt, Procedure: input.procedure, Recovery: input.recovery, Progress: input.boundary}
	// A failed stop cannot authorize file restoration. Return a partial result
	// so the Controller retains unresolved effects instead of replaying forward.
	if err := host.consumers.Stop(ctx, captured); err == nil {
		report.EffectsStopped = true
		ready, err := host.configuration.Run(ctx, captured, inspectOnly)
		if err == nil && ready {
			report.FilesRestored = true
			if input.procedure.PriorConfiguration != nil {
				report.PriorConfigurationSHA256 = hex.EncodeToString(input.procedure.PriorConfiguration.Sha256)
			}
			if len(input.procedure.RunningConsumerIds) != 0 {
				ready, err := host.consumers.Restore(ctx, captured, inspectOnly)
				if err == nil && ready {
					report.ConsumersRestored = true
					report.BaselineSHA256 = hex.EncodeToString(input.procedure.Baseline.YamlSha256)
				}
			}
		}
	}
	if err := host.ownership.Check(); err != nil {
		return entryexecution.RecoveryReport{}, err
	}
	report.ObservedAt = time.Now().UTC()
	return report, report.Validate(input.attempt, *input.recovery, input.procedure, input.boundary)
}
