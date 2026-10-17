package networkruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Helper interface {
	ExecuteNetworkWork(context.Context, *agentpb.NetworkWorkHelperRequest) (*agentpb.NetworkWorkHelperResponse, error)
}

type RuntimeHost struct {
	journal   *agentworkjournal.Journal
	helper    Helper
	ownership *agentownership.Lease
}

func NewHost(journal *agentworkjournal.Journal, helper Helper, ownership *agentownership.Lease) (*RuntimeHost, error) {
	if journal == nil || helper == nil {
		return nil, errs.New(errs.KindInternal, "Network host requires its journal and helper")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &RuntimeHost{journal: journal, helper: helper, ownership: ownership}, nil
}

func (host *RuntimeHost) Apply(ctx context.Context, input Input, stage networkexecution.Stage) error {
	_, err := host.execute(ctx, input, stage, false)
	return err
}

func (host *RuntimeHost) Inspect(ctx context.Context, input Input, stage networkexecution.Stage) (networkexecution.Observation, error) {
	return host.execute(ctx, input, stage, true)
}

func (host *RuntimeHost) execute(ctx context.Context, input Input, stage networkexecution.Stage, inspectOnly bool) (networkexecution.Observation, error) {
	if input.procedure == nil || input.attempt.StartedAt == nil {
		return networkexecution.Observation{}, invalidInput()
	}
	if err := host.ownership.Check(); err != nil {
		return networkexecution.Observation{}, err
	}
	saved, err := host.journal.Network(ctx, input.attempt, input.procedure.Action, input.procedure.NetworkId)
	if err != nil {
		return networkexecution.Observation{}, err
	}
	if saved == nil || !saved.Acknowledged || saved.Completion != nil || saved.Failure != nil || !saved.Progress.InFlight || saved.Progress.Stage != stage {
		return networkexecution.Observation{}, errs.New(errs.KindStateConflict, "Network host operation has no matching acknowledged checkpoint")
	}
	report, err := input.checkpointRequest(saved.Progress)
	if err != nil {
		return networkexecution.Observation{}, err
	}
	checkpoint, err := saved.Progress.Checkpoint(input.attempt, input.procedure.Action, input.procedure.NetworkId)
	if err != nil {
		return networkexecution.Observation{}, err
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return networkexecution.Observation{}, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return networkexecution.Observation{}, err
	}
	request := &agentpb.NetworkWorkHelperRequest{
		Assignment: &agentpb.NetworkWorkAssignment{Attempt: attempt, Input: proto.CloneOf(input.procedure)},
		Start:      &agentpb.WorkStartAck{Attempt: attempt, StartedAt: timestamppb.New(*input.attempt.StartedAt)},
		Report:     report, Acknowledgement: &agentpb.NetworkWorkCheckpointAck{
			Attempt: attempt, Sequence: checkpoint.Sequence, StageId: checkpoint.StageID,
			Evidence:   &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest},
			RecordedAt: timestamppb.New(checkpoint.RecordedAt),
		}, InspectOnly: inspectOnly,
	}
	captured, err := executionplan.OpenNetworkHelperRequest(request)
	if err != nil {
		return networkexecution.Observation{}, err
	}
	response, err := host.helper.ExecuteNetworkWork(ctx, request)
	if err != nil {
		return networkexecution.Observation{}, err
	}
	if err := host.ownership.Check(); err != nil {
		return networkexecution.Observation{}, err
	}
	return executionplan.ReadNetworkHelperObservation(captured, response)
}
