package componentruntime

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Helper interface {
	ExecuteComponentWork(context.Context, *agentpb.ComponentWorkHelperRequest) (*agentpb.ComponentWorkHelperResponse, error)
	StopComponentWork(context.Context, *agentpb.ComponentWorkHelperRequest) error
}

type RuntimeHost struct {
	journal       *agentworkjournal.Journal
	helper        Helper
	ownership     *agentownership.Lease
	configuration *Configuration
}

func NewHost(journal *agentworkjournal.Journal, helper Helper, ownership *agentownership.Lease, configuration *Configuration) (*RuntimeHost, error) {
	if journal == nil || helper == nil || configuration == nil {
		return nil, errs.New(errs.KindInternal, "Component host requires its journal and helper")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &RuntimeHost{journal: journal, helper: helper, ownership: ownership, configuration: configuration}, nil
}

func (host *RuntimeHost) Apply(ctx context.Context, input Input, stage componentexecution.Stage) error {
	_, err := host.execute(ctx, input, stage, false)
	return err
}

func (host *RuntimeHost) Inspect(ctx context.Context, input Input, stage componentexecution.Stage) (componentexecution.Observation, error) {
	return host.execute(ctx, input, stage, true)
}

func (host *RuntimeHost) execute(ctx context.Context, input Input, stage componentexecution.Stage, inspectOnly bool) (componentexecution.Observation, error) {
	if input.procedure == nil || input.attempt.StartedAt == nil {
		return componentexecution.Observation{}, invalidInput()
	}
	if err := host.ownership.Check(); err != nil {
		return componentexecution.Observation{}, err
	}
	saved, err := host.journal.Component(ctx, input.attempt, input.procedure)
	if err != nil {
		return componentexecution.Observation{}, err
	}
	if saved == nil || !saved.Acknowledged || saved.Completion != nil || saved.Failure != nil || !saved.Progress.InFlight || saved.Progress.Stage != stage {
		return componentexecution.Observation{}, errs.New(errs.KindStateConflict, "Component host operation has no matching acknowledged checkpoint")
	}
	report, err := input.checkpointRequest(saved.Progress)
	if err != nil {
		return componentexecution.Observation{}, err
	}
	checkpoint, err := saved.Progress.Checkpoint(input.attempt, input.procedure)
	if err != nil {
		return componentexecution.Observation{}, err
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return componentexecution.Observation{}, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return componentexecution.Observation{}, err
	}
	request := &agentpb.ComponentWorkHelperRequest{
		Assignment: &agentpb.ComponentWorkAssignment{Attempt: attempt, Input: proto.CloneOf(input.procedure)},
		Start:      &agentpb.WorkStartAck{Attempt: attempt, StartedAt: timestamppb.New(*input.attempt.StartedAt)},
		Report:     report, Acknowledgement: &agentpb.ComponentWorkCheckpointAck{
			Attempt: attempt, Sequence: checkpoint.Sequence, StageId: checkpoint.StageID,
			Evidence:   &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest},
			RecordedAt: timestamppb.New(checkpoint.RecordedAt),
		}, InspectOnly: inspectOnly,
	}
	captured, err := componentexecution.OpenHelperRequest(request)
	if err != nil {
		return componentexecution.Observation{}, err
	}
	if stage == componentexecution.WriteConfiguration {
		ready, err := host.configuration.Run(ctx, captured, inspectOnly)
		if err != nil {
			return componentexecution.Observation{}, err
		}
		if err := host.ownership.Check(); err != nil {
			return componentexecution.Observation{}, err
		}
		observation := componentexecution.Observation{ComponentID: input.procedure.ComponentId,
			Input: input.attempt.Input, Stage: stage, ConfigurationWritten: ready, At: time.Now().UTC()}
		response, err := componentexecution.HelperObservation(captured, observation)
		if err != nil {
			return componentexecution.Observation{}, err
		}
		return componentexecution.ReadHelperObservation(captured, response)
	}
	response, err := host.helper.ExecuteComponentWork(ctx, request)
	if err != nil {
		return componentexecution.Observation{}, err
	}
	if err := host.ownership.Check(); err != nil {
		return componentexecution.Observation{}, err
	}
	return componentexecution.ReadHelperObservation(captured, response)
}
