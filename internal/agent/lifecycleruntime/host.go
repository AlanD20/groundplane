package lifecycleruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/backingcommand"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Helper interface {
	ExecuteLifecycleWork(
		context.Context,
		string,
		*agentpb.ServiceLifecycleHelperRequest,
	) (*agentpb.ServiceLifecycleHelperResponse, error)
}

type RuntimeHost struct {
	root      string
	journal   *agentworkjournal.Journal
	helper    Helper
	ownership *agentownership.Lease
	commands  *backingcommand.Executor
	runner    runner.Runner
}

func NewHost(
	root string,
	journal *agentworkjournal.Journal,
	helper Helper,
	ownership *agentownership.Lease,
	commands *backingcommand.Executor,
	taskRunner runner.Runner,
) (*RuntimeHost, error) {
	if environmentpath.ValidateRoot(root) != nil || journal == nil || helper == nil || ownership == nil ||
		commands == nil ||
		taskRunner == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Service lifecycle host requires its root, journal, helper and ownership",
		)
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &RuntimeHost{
		root:      root,
		journal:   journal,
		helper:    helper,
		ownership: ownership,
		commands:  commands,
		runner:    taskRunner,
	}, nil
}

func (host *RuntimeHost) Apply(
	ctx context.Context,
	input Input,
	stage lifecycleexecution.Stage,
	exchange Exchange,
) error {
	_, err := host.execute(ctx, input, stage, false, exchange)
	return err
}

func (host *RuntimeHost) Inspect(
	ctx context.Context,
	input Input,
	stage lifecycleexecution.Stage,
) (*lifecycleexecution.Receipt, error) {
	return host.execute(ctx, input, stage, true, nil)
}

func (host *RuntimeHost) execute(
	ctx context.Context,
	input Input,
	stage lifecycleexecution.Stage,
	inspectOnly bool,
	exchange Exchange,
) (*lifecycleexecution.Receipt, error) {
	if input.procedure == nil || input.attempt.StartedAt == nil {
		return nil, invalidInput()
	}
	if err := host.ownership.Check(); err != nil {
		return nil, err
	}
	saved, err := host.journal.Lifecycle(ctx, input.attempt, input.procedure)
	if err != nil {
		return nil, err
	}
	if saved == nil || !saved.Acknowledged || saved.Completion != nil || saved.Failure != nil ||
		saved.Progress.BeganAt == nil ||
		len(saved.Progress.Completed) >= len(saved.Stages) ||
		saved.Stages[len(saved.Progress.Completed)] != stage {
		return nil, errs.New(
			errs.KindStateConflict,
			"Service lifecycle host operation has no matching acknowledged checkpoint",
		)
	}
	if stage.Expected == lifecycleexecution.HookSucceeded {
		return host.executeHook(ctx, input, stage, saved, inspectOnly, exchange)
	}
	report, err := input.checkpointRequest(saved.Progress)
	if err != nil {
		return nil, err
	}
	checkpoint, err := saved.Progress.Checkpoint(input.attempt, input.procedure)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return nil, err
	}
	request := &agentpb.ServiceLifecycleHelperRequest{
		Assignment: &agentpb.ServiceLifecycleAssignment{Attempt: attempt, Input: proto.CloneOf(input.procedure)},
		Start:      &agentpb.WorkStartAck{Attempt: attempt, StartedAt: timestamppb.New(*input.attempt.StartedAt)},
		Report:     report, Acknowledgement: &agentpb.ServiceLifecycleCheckpointAck{Attempt: attempt,
			Sequence: checkpoint.Sequence, StageId: checkpoint.StageID, RecordedAt: timestamppb.New(checkpoint.RecordedAt),
			Evidence: &agentpb.WorkInputReference{
				InputId: checkpoint.Evidence.ID,
				Sha256:  digest,
			}}, InspectOnly: inspectOnly,
	}
	if input.continuation != nil {
		request.Assignment.Continuation, err = lifecycleexecution.EncodeContinuation(
			input.procedure,
			*input.continuation,
		)
		if err != nil {
			return nil, err
		}
	}
	captured, err := executionplan.OpenLifecycleHelperRequest(request)
	if err != nil {
		return nil, err
	}
	response, err := host.helper.ExecuteLifecycleWork(ctx, host.root, request)
	if err != nil {
		return nil, err
	}
	return executionplan.ReadLifecycleHelperReceipt(captured, response)
}

func clearHookDelivery(delivery *agentpb.ServiceLifecycleHookDelivery) {
	if delivery == nil || delivery.Hook == nil {
		return
	}
	for _, value := range delivery.Hook.Inputs {
		if value != nil {
			clear(value.Value)
			value.Value = nil
		}
	}
	for _, value := range delivery.Hook.Facts {
		if value != nil {
			clear(value.Value)
			value.Value = nil
		}
	}
}
