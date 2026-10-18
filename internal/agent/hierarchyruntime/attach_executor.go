package hierarchyruntime

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

type AttachExchange interface {
	CompleteAttachRecovery(context.Context, *agentpb.AttachRecoveryCompletionRequest) (*agentpb.AttachRecoveryCompletionAck, error)
	FailAttachRemoval(context.Context, *agentpb.AttachRemovalFailureRequest) (*agentpb.AttachRemovalFailureAck, error)
	Start(context.Context, *agentpb.WorkStartRequest) (*agentpb.WorkStartAck, error)
	AttachRemovalCheckpoint(context.Context, *agentpb.AttachRemovalCheckpointRequest) (*agentpb.AttachRemovalCheckpointAck, error)
	CompleteAttachRemoval(context.Context, *agentpb.AttachRemovalCompletionRequest) (*agentpb.AttachRemovalCompletionAck, error)
}

type AttachExecutor struct {
	journal *agentworkjournal.Journal
	host    AttachHost
}

func NewAttachExecutor(journal *agentworkjournal.Journal, host AttachHost) (*AttachExecutor, error) {
	if journal == nil || host == nil {
		return nil, errs.New(errs.KindInternal, "Attach removal executor requires its durable journal and host runtime")
	}
	return &AttachExecutor{journal: journal, host: host}, nil
}

func (executor *AttachExecutor) Execute(ctx context.Context, input attachexecution.RemovalInput,
	exchange AttachExchange,
) (*attachexecution.RemovalProgress, error) {
	if ctx == nil || exchange == nil || input.Validate() != nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach removal requires a valid captured assignment")
	}
	savedAttempt, saved, err := executor.journal.LoadAttachRemoval(ctx, input)
	if err != nil {
		return nil, err
	}
	if saved != nil && saved.Acknowledged && !saved.Report.Progress.InFlight && saved.Report.Progress.Completed == saved.Authority.Commands {
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
		return nil, errs.New(errs.KindStateConflict, "Agent Attach journal is missing acknowledged progress; command execution requires reconciliation")
	}
	checkpoints, err := checkpointdelivery.NewAttachRemoval(input, executor.journal, exchange.AttachRemovalCheckpoint)
	if err != nil {
		return nil, err
	}
	progress, err := RunAttachRemoval(ctx, input, checkpoints, executor.host)
	if err != nil {
		return progress, err
	}
	if err := executor.complete(ctx, input, *progress, exchange); err != nil {
		return progress, err
	}
	return progress, nil
}

func (executor *AttachExecutor) complete(ctx context.Context, input attachexecution.RemovalInput,
	progress attachexecution.RemovalProgress, exchange AttachExchange,
) error {
	request, err := attachexecution.EncodeRemovalCompletion(input, progress)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ack, err := exchange.CompleteAttachRemoval(bounded, request)
	if err != nil {
		return err
	}
	_, err = executor.journal.CompleteAttachRemoval(bounded, input, ack)
	return err
}

func (executor *AttachExecutor) Retire(ctx context.Context, input attachexecution.RemovalInput) error {
	failure, err := executor.journal.LoadAttachFailure(ctx, input.Attempt)
	if err != nil {
		return err
	}
	if failure != nil {
		if failure.Receipt == nil {
			return errs.New(errs.KindStateConflict, "Attach removal failure has no saved acknowledgement")
		}
		return executor.journal.Retire(ctx, *failure.Receipt)
	}
	_, saved, err := executor.journal.LoadAttachRemoval(ctx, input)
	if err != nil {
		return err
	}
	if saved == nil || saved.Completion == nil {
		return errs.New(errs.KindStateConflict, "Attach removal has no saved completion acknowledgement")
	}
	return executor.journal.RetireCompletedAttach(ctx, *saved.Completion)
}
