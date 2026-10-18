package hierarchyruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Exchange interface {
	FailEnvironmentRemoval(context.Context, *agentpb.EnvironmentRemovalFailureRequest) (*agentpb.EnvironmentRemovalFailureAck, error)
	Start(context.Context, *agentpb.WorkStartRequest) (*agentpb.WorkStartAck, error)
	EnvironmentRemovalCheckpoint(context.Context, *agentpb.EnvironmentRemovalCheckpointRequest) (*agentpb.EnvironmentRemovalCheckpointAck, error)
	CompleteEnvironmentRemoval(context.Context, *agentpb.EnvironmentRemovalCompletionRequest) (*agentpb.EnvironmentRemovalCompletionAck, error)
}

type Executor struct {
	volumeRoot string
	journal    *agentworkjournal.Journal
	host       EnvironmentHost
}

func NewExecutor(volumeRoot string, journal *agentworkjournal.Journal, host EnvironmentHost) (*Executor, error) {
	if environmentpath.ValidateRoot(volumeRoot) != nil || journal == nil || host == nil {
		return nil, errs.New(errs.KindInternal, "Environment removal executor requires its configured volume root, journal and host runtime")
	}
	return &Executor{volumeRoot: volumeRoot, journal: journal, host: host}, nil
}

// ExecuteEnvironment obtains saved Start authority and accounts for a retained
// in-flight effect. Its caller settles the result; returning an error does not
// assert absence or release the Controller's resource claims.
func (executor *Executor) ExecuteEnvironment(ctx context.Context, input hierarchyexecution.EnvironmentInput,
	exchange Exchange,
) (*hierarchyexecution.EnvironmentProgress, error) {
	if ctx == nil || exchange == nil || input.Validate() != nil {
		return nil, errs.New(errs.KindValidationFailed, "Environment removal requires a valid captured assignment")
	}
	scope, err := environmentpath.Parse(executor.volumeRoot, input.Removal.ExpectedVolumeDir)
	if err != nil {
		return nil, err
	}
	if scope.EnvironmentID != input.Removal.EnvironmentId {
		return nil, errs.New(errs.KindStateConflict, "Environment removal directory belongs to a different Environment")
	}
	savedAttempt, saved, err := executor.journal.LoadEnvironmentRemoval(ctx, input)
	if err != nil {
		return nil, err
	}
	if saved != nil && saved.Acknowledged && saved.Report.Progress.Phase == hierarchyexecution.EnvironmentRemoved {
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
	input, _, err = input.WithStart(ack)
	if err != nil {
		return nil, err
	}
	sequence, err := executor.journal.Sequence(ctx, input.Attempt)
	if err != nil {
		return nil, err
	}
	if sequence < ack.CheckpointSequence {
		return nil, errs.New(errs.KindStateConflict, "Agent removal journal is missing acknowledged progress; host effects require reconciliation")
	}
	checkpoints, err := checkpointdelivery.NewEnvironmentRemoval(input, executor.journal, exchange.EnvironmentRemovalCheckpoint)
	if err != nil {
		return nil, err
	}
	progress, err := RunEnvironmentRemoval(ctx, input, checkpoints, executor.host)
	if err != nil {
		return progress, err
	}
	if err := executor.complete(ctx, input, *progress, exchange); err != nil {
		return progress, err
	}
	return progress, nil
}

func (executor *Executor) complete(ctx context.Context, input hierarchyexecution.EnvironmentInput, progress hierarchyexecution.EnvironmentProgress, exchange Exchange) error {
	request, err := hierarchyexecution.EncodeEnvironmentCompletion(input, progress)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ack, err := exchange.CompleteEnvironmentRemoval(bounded, request)
	if err != nil {
		return err
	}
	_, err = executor.journal.CompleteEnvironmentRemoval(bounded, input, ack)
	return err
}

func (executor *Executor) RetireEnvironment(ctx context.Context, input hierarchyexecution.EnvironmentInput) error {
	failure, err := executor.journal.LoadEnvironmentFailure(ctx, input.Attempt)
	if err != nil {
		return err
	}
	if failure != nil {
		if failure.Receipt == nil {
			return errs.New(errs.KindStateConflict, "Environment removal failure has no saved acknowledgement")
		}
		return executor.journal.Retire(ctx, *failure.Receipt)
	}
	_, saved, err := executor.journal.LoadEnvironmentRemoval(ctx, input)
	if err != nil {
		return err
	}
	if saved == nil || saved.Completion == nil {
		return errs.New(errs.KindStateConflict, "Environment removal has no saved completion acknowledgement")
	}
	return executor.journal.Retire(ctx, *saved.Completion)
}
