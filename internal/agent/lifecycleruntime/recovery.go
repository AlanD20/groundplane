package lifecycleruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) runRecovery(
	ctx context.Context,
	input Input,
	exchange Exchange,
) (workexecution.Attempt, error) {
	if input.recovery == nil || input.attempt.StartedAt == nil {
		return workexecution.Attempt{}, invalidInput()
	}
	stages, err := lifecycleexecution.Stages(input.procedure)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	saved, err := executor.journal.PrepareLifecycleRecovery(
		ctx,
		input.attempt,
		agentworkjournal.LifecycleRecoveryRecord{Stages: stages, Source: *input.recovery},
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved.Receipt != nil {
		return *saved.Receipt, nil
	}
	if saved.Report == nil {
		report, err := executor.host.InspectRecovery(ctx, input)
		if err != nil {
			reason := lifecycleexecution.ExecutionFailed
			if !time.Now().Before(input.attempt.Deadline) {
				reason = lifecycleexecution.DeadlineExpired
			} else if ctx.Err() != nil {
				reason = lifecycleexecution.ExecutionInterrupted
			}
			report = lifecycleexecution.RecoveryReport{
				AttemptID:     input.attempt.ID,
				ForwardID:     input.recovery.Forward.ID,
				Failure:       input.recovery.Evidence,
				FailureReason: reason,
				ObservedAt:    time.Now().UTC(),
			}
		}
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		err = executor.journal.SaveLifecycleRecoveryReport(saveCtx, input.attempt, report)
		cancel()
		if err != nil {
			return workexecution.Attempt{}, err
		}
		saved.Report = &report
	}
	raw, _, err := lifecycleexecution.EncodeRecoveryReport(
		input.attempt,
		input.procedure,
		*input.recovery,
		*saved.Report,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	deliveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	ack, err := exchange.RecoverLifecycle(
		deliveryCtx,
		&agentpb.ServiceLifecycleRecoveryReportRequest{Attempt: identity, Report: raw},
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := lifecycleexecution.DecodeRecoveryAck(
		input.attempt,
		input.procedure,
		*input.recovery,
		*saved.Report,
		ack,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.AcknowledgeLifecycleRecovery(deliveryCtx, input.attempt, terminal); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
