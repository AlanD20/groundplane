package removalruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Helper interface {
	ExecuteRemovalRecovery(
		context.Context,
		string,
		*agentpb.ServiceRemovalRecoveryHelperRequest,
	) (*agentpb.ServiceRemovalRecoveryHelperResponse, error)
	QuiesceRemovalRecovery(
		context.Context,
		*agentpb.ServiceRemovalRecoveryHelperRequest,
	) (*agentpb.ServiceRemovalRecoveryHelperResponse, error)
}

// Configuration restores or verifies only the immutable configuration selected
// by the captured runtime. The delivery owner must not resolve latest Entries.
type Configuration interface {
	RestoreRemovalConfiguration(
		context.Context,
		*agentpb.ServiceRemovalRecoveryHelperRequest,
	) (*agentpb.ServiceRemovalRecoveryHelperResponse, error)
}

type RuntimeHost struct {
	root          string
	journal       *agentworkjournal.Journal
	helper        Helper
	configuration Configuration
	ownership     *agentownership.Lease
}

func NewHost(
	root string,
	journal *agentworkjournal.Journal,
	helper Helper,
	configuration Configuration,
	ownership *agentownership.Lease,
) (*RuntimeHost, error) {
	if environmentpath.ValidateRoot(root) != nil || journal == nil || helper == nil || configuration == nil ||
		ownership == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Service removal recovery requires its host root, journal, helpers, configuration and ownership",
		)
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &RuntimeHost{
		root:          root,
		journal:       journal,
		helper:        helper,
		configuration: configuration,
		ownership:     ownership,
	}, nil
}

func (host *RuntimeHost) Inspect(
	ctx context.Context,
	input Input,
	progress removalexecution.Progress,
) (*workexecution.Input, error) {
	return host.execute(ctx, input, progress, true, nil)
}

func (host *RuntimeHost) Apply(
	ctx context.Context,
	input Input,
	progress removalexecution.Progress,
	grant workexecution.Checkpoint,
) error {
	_, err := host.execute(ctx, input, progress, false, &grant)
	return err
}

func (host *RuntimeHost) execute(
	ctx context.Context,
	input Input,
	progress removalexecution.Progress,
	inspect bool,
	grant *workexecution.Checkpoint,
) (*workexecution.Input, error) {
	if err := host.ownership.CheckRecovery(input.capture.Source.Forward, input.attempt); err != nil {
		return nil, err
	}
	saved, err := host.journal.RemovalRecovery(ctx, input.attempt, input.procedure, input.capture)
	if err != nil {
		return nil, err
	}
	if saved == nil || !saved.Acknowledged || saved.Completion != nil || saved.Failure != nil ||
		!progress.InFlight || !removalexecution.EqualProgress(saved.Progress, progress) {
		return nil, errs.New(errs.KindStateConflict, "Service removal recovery has no matching acknowledged host step")
	}
	checkpoint, err := progress.Checkpoint(input.attempt, input.procedure, input.capture)
	if err != nil {
		return nil, err
	}
	if !inspect && (grant == nil || *grant != checkpoint) {
		return nil, invalidInput()
	}
	for _, member := range input.procedure.Members {
		scope, err := environmentpath.Parse(host.root, member.Artifact.AuthorizedVolumeDir)
		if err != nil || scope.EnvironmentID != input.procedure.EnvironmentId {
			return nil, invalidInput()
		}
	}
	request, err := input.helperRequest(progress, checkpoint, inspect)
	if err != nil {
		return nil, err
	}
	captured, err := executionplan.OpenRemovalRecoveryHelperRequest(request)
	if err != nil {
		return nil, err
	}
	var response *agentpb.ServiceRemovalRecoveryHelperResponse
	switch progress.Phase {
	case removalexecution.QuiesceExecutors:
		response, err = host.helper.QuiesceRemovalRecovery(ctx, request)
	case removalexecution.RestoreConfiguration:
		response, err = host.configuration.RestoreRemovalConfiguration(ctx, request)
	case removalexecution.InspectRuntime, removalexecution.RestoreRuntime, removalexecution.VerifyRestoration:
		response, err = host.helper.ExecuteRemovalRecovery(ctx, host.root, request)
	default:
		return nil, invalidInput()
	}
	if err != nil {
		return nil, err
	}
	if err := host.ownership.CheckRecovery(input.capture.Source.Forward, input.attempt); err != nil {
		return nil, err
	}
	return executionplan.ReadRemovalRecoveryHelperReceipt(captured, response)
}

func (input Input) helperRequest(
	progress removalexecution.Progress,
	checkpoint workexecution.Checkpoint,
	inspect bool,
) (*agentpb.ServiceRemovalRecoveryHelperRequest, error) {
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return nil, err
	}
	raw, err := removalexecution.EncodeCapture(input.procedure, input.capture)
	if err != nil {
		return nil, err
	}
	report, err := input.CheckpointRequest(progress)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.ServiceRemovalRecoveryHelperRequest{
		Assignment: &agentpb.ServiceRemovalRecoveryAssignment{
			Attempt:         identity,
			Removal:         proto.CloneOf(input.procedure),
			CapturedFailure: raw,
			Configuration:   proto.CloneOf(input.configuration),
		},
		Start:  &agentpb.WorkStartAck{Attempt: identity, StartedAt: timestamppb.New(*input.attempt.StartedAt)},
		Report: report, Acknowledgement: &agentpb.ServiceRemovalRecoveryCheckpointAck{Attempt: identity,
			Sequence: checkpoint.Sequence, StageId: checkpoint.StageID, RecordedAt: timestamppb.New(checkpoint.RecordedAt),
			Evidence: &agentpb.WorkInputReference{
				InputId: checkpoint.Evidence.ID,
				Sha256:  digest,
			}}, InspectOnly: inspect,
	}, nil
}
