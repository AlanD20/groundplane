package volumeruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Helper interface {
	ExecuteVolumeWork(
		context.Context,
		string,
		*agentpb.VolumeWorkHelperRequest,
	) (*agentpb.VolumeWorkHelperResponse, error)
}

type RuntimeHost struct {
	root      string
	journal   *agentworkjournal.Journal
	helper    Helper
	ownership *agentownership.Lease
}

func NewHost(
	root string,
	journal *agentworkjournal.Journal,
	helper Helper,
	ownership *agentownership.Lease,
) (*RuntimeHost, error) {
	if environmentpath.ValidateRoot(root) != nil || journal == nil || helper == nil {
		return nil, errs.New(errs.KindInternal, "Volume host requires its root, journal and helper")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &RuntimeHost{root: root, journal: journal, helper: helper, ownership: ownership}, nil
}

func (host *RuntimeHost) Apply(ctx context.Context, input Input, stage volumeexecution.Stage) error {
	_, err := host.execute(ctx, input, stage, false)
	return err
}

func (host *RuntimeHost) Inspect(
	ctx context.Context,
	input Input,
	stage volumeexecution.Stage,
) (volumeexecution.Observation, error) {
	return host.execute(ctx, input, stage, true)
}

func (host *RuntimeHost) execute(
	ctx context.Context,
	input Input,
	stage volumeexecution.Stage,
	inspectOnly bool,
) (volumeexecution.Observation, error) {
	if input.procedure == nil || input.attempt.StartedAt == nil {
		return volumeexecution.Observation{}, invalidInput()
	}
	if err := host.ownership.Check(); err != nil {
		return volumeexecution.Observation{}, err
	}
	saved, err := host.journal.Volume(ctx, input.attempt, input.procedure.Action, input.procedure.VolumeId)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	if saved == nil || !saved.Acknowledged || saved.Completion != nil || saved.Failure != nil ||
		!saved.Progress.InFlight ||
		saved.Progress.Stage != stage {
		return volumeexecution.Observation{}, errs.New(
			errs.KindStateConflict,
			"Volume host operation has no matching acknowledged checkpoint",
		)
	}
	report, err := input.checkpointRequest(saved.Progress)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	checkpoint, err := saved.Progress.Checkpoint(input.attempt, input.procedure.Action, input.procedure.VolumeId)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	request := &agentpb.VolumeWorkHelperRequest{
		Assignment: &agentpb.VolumeWorkAssignment{Attempt: attempt, Input: proto.CloneOf(input.procedure)},
		Start:      &agentpb.WorkStartAck{Attempt: attempt, StartedAt: timestamppb.New(*input.attempt.StartedAt)},
		Report:     report, Acknowledgement: &agentpb.VolumeWorkCheckpointAck{
			Attempt: attempt, Sequence: checkpoint.Sequence, StageId: checkpoint.StageID,
			Evidence:   &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest},
			RecordedAt: timestamppb.New(checkpoint.RecordedAt),
		}, InspectOnly: inspectOnly,
	}
	if stage == volumeexecution.RemoveDirectory && !inspectOnly {
		return host.removeDirectory(ctx, input, request)
	}
	captured, err := executionplan.OpenVolumeHelperRequest(request)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	response, err := host.helper.ExecuteVolumeWork(ctx, host.root, request)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	if err := host.ownership.Check(); err != nil {
		return volumeexecution.Observation{}, err
	}
	return executionplan.ReadVolumeHelperObservation(captured, response)
}
