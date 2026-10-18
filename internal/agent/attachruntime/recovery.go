package attachruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) RetireRecovery(ctx context.Context, terminal workexecution.Attempt) error {
	return executor.journal.RetireProvisionInspection(ctx, terminal)
}

func (executor *Executor) Recover(ctx context.Context, input attachexecution.ProvisionRecoveryInput, exchange Exchange) (*workexecution.Attempt, error) {
	if ctx == nil || exchange == nil || input.Validate() != nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach inspection requires its captured recovery assignment")
	}
	savedAttempt, saved, err := executor.journal.LoadProvisionInspection(ctx, input)
	if err != nil {
		return nil, err
	}
	var report attachexecution.ProvisionInspection
	if saved != nil {
		input.Attempt = *savedAttempt
		if saved.Receipt != nil {
			return saved.Receipt, nil
		}
		report = saved.Report
	} else {
		input, report, err = executor.inspectRecovery(ctx, input, exchange)
		if err != nil {
			return nil, err
		}
		storage, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err = executor.journal.SaveProvisionInspection(storage, input, report)
		cancel()
		if err != nil {
			return nil, err
		}
	}
	request, err := attachexecution.EncodeProvisionInspectionRequest(input, report)
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ack, err := exchange.ReportProvisionRecovery(bounded, request)
	if err != nil {
		return nil, err
	}
	terminal, err := attachexecution.DecodeProvisionInspectionAck(input, report, ack)
	if err != nil {
		return nil, err
	}
	if err := executor.journal.AcknowledgeProvisionInspection(bounded, input, terminal); err != nil {
		return nil, err
	}
	return &terminal, nil
}

func (executor *Executor) inspectRecovery(ctx context.Context, input attachexecution.ProvisionRecoveryInput, exchange Exchange) (attachexecution.ProvisionRecoveryInput, attachexecution.ProvisionInspection, error) {
	interrupted := func() attachexecution.ProvisionInspection {
		return attachexecution.ProvisionInspection{AttemptID: input.Attempt.ID, ForwardID: input.Source.Forward.ID,
			Failure: input.Source.Evidence, CommandOrdinal: uint32((input.Source.Failure.Checkpoint.Sequence - 1) / 2),
			State: attachexecution.InspectionInterrupted, ObservedAt: time.Now().UTC()}
	}
	if err := ctx.Err(); err != nil {
		return input, attachexecution.ProvisionInspection{}, err
	}
	if !time.Now().UTC().Before(input.Attempt.Deadline) {
		return input, interrupted(), nil
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return input, attachexecution.ProvisionInspection{}, err
	}
	ack, err := exchange.Start(ctx, &agentpb.WorkStartRequest{Attempt: identity})
	if err != nil {
		if ctx.Err() == nil && !time.Now().UTC().Before(input.Attempt.Deadline) {
			return input, interrupted(), nil
		}
		return input, attachexecution.ProvisionInspection{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(ack, input.Attempt)
	if err != nil {
		return input, attachexecution.ProvisionInspection{}, err
	}
	input.Attempt.StartedAt = &started
	inspection, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	report, err := executor.host.InspectProvision(inspection, input)
	cancel()
	if err != nil && ctx.Err() == nil {
		return input, interrupted(), nil
	}
	return input, report, err
}
