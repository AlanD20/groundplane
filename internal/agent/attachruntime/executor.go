package attachruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Exchange interface {
	FactStore
	ReportProvisionRecovery(
		context.Context,
		*agentpb.AttachProvisionRecoveryReportRequest,
	) (*agentpb.AttachProvisionRecoveryReportAck, error)
	FailAttachProvision(
		context.Context,
		*agentpb.AttachProvisionFailureRequest,
	) (*agentpb.AttachProvisionFailureAck, error)
	Start(context.Context, *agentpb.WorkStartRequest) (*agentpb.WorkStartAck, error)
	AttachProvisionCheckpoint(
		context.Context,
		*agentpb.AttachProvisionCheckpointRequest,
	) (*agentpb.AttachProvisionCheckpointAck, error)
	CompleteAttachProvision(
		context.Context,
		*agentpb.AttachProvisionCompletionRequest,
	) (*agentpb.AttachProvisionCompletionAck, error)
}

type Executor struct {
	journal *agentworkjournal.Journal
	host    Host
}

func NewExecutor(journal *agentworkjournal.Journal, host Host) (*Executor, error) {
	if journal == nil || host == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Attach provisioning executor requires its durable journal and host runtime",
		)
	}
	return &Executor{journal: journal, host: host}, nil
}

func (executor *Executor) Execute(ctx context.Context, input attachexecution.ProvisionInput,
	exchange Exchange,
) (*attachexecution.ProvisionProgress, error) {
	if ctx == nil || exchange == nil || input.Validate() != nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach provisioning requires a valid captured assignment")
	}
	savedAttempt, saved, err := executor.journal.LoadAttachProvision(ctx, input)
	if err != nil {
		return nil, err
	}
	if saved != nil && saved.Acknowledged && !saved.Report.Progress.InFlight &&
		saved.Report.Progress.Completed == saved.Authority.Commands {
		input.Attempt = *savedAttempt
		if saved.Completion == nil {
			if err := executor.complete(ctx, input, saved.Report.Progress, exchange); err != nil {
				return &saved.Report.Progress, err
			}
		}
		return &saved.Report.Progress, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	ack, err := exchange.Start(ctx, &agentpb.WorkStartRequest{Attempt: identity})
	if err != nil {
		return nil, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(ack, input.Attempt)
	if err != nil {
		return nil, err
	}
	input.Attempt.StartedAt = &started
	sequence, err := executor.journal.Sequence(ctx, input.Attempt)
	if err != nil {
		return nil, err
	}
	if sequence < ack.CheckpointSequence {
		return nil, errs.New(
			errs.KindStateConflict,
			"Agent Attach journal is missing acknowledged progress; command execution requires reconciliation",
		)
	}
	checkpoints, err := checkpointdelivery.NewAttachProvision(
		input,
		executor.journal,
		exchange.AttachProvisionCheckpoint,
	)
	if err != nil {
		return nil, err
	}
	progress, err := Run(ctx, input, checkpoints, executor.host, exchange)
	if err != nil {
		return progress, err
	}
	if err := executor.complete(ctx, input, *progress, exchange); err != nil {
		return progress, err
	}
	return progress, nil
}

func (executor *Executor) complete(ctx context.Context, input attachexecution.ProvisionInput,
	progress attachexecution.ProvisionProgress, exchange Exchange,
) error {
	request, err := attachexecution.EncodeProvisionCompletion(input, progress)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ack, err := exchange.CompleteAttachProvision(bounded, request)
	if err != nil {
		return err
	}
	_, err = executor.journal.CompleteAttachProvision(bounded, input, ack)
	return err
}

func (executor *Executor) Retire(ctx context.Context, input attachexecution.ProvisionInput) error {
	failure, err := executor.journal.LoadAttachProvisionFailure(ctx, input.Attempt)
	if err != nil {
		return err
	}
	if failure != nil {
		if failure.Receipt == nil {
			return errs.New(errs.KindStateConflict, "Attach provisioning failure has no saved acknowledgement")
		}
		return executor.journal.Retire(ctx, *failure.Receipt)
	}
	_, saved, err := executor.journal.LoadAttachProvision(ctx, input)
	if err != nil {
		return err
	}
	if saved == nil || saved.Completion == nil {
		return errs.New(errs.KindStateConflict, "Attach provisioning has no saved completion acknowledgement")
	}
	return executor.journal.Retire(ctx, *saved.Completion)
}
