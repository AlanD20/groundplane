package entryruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Helper interface {
	StopEntryConfigurationWork(context.Context, *agentpb.EntryConfigurationWorkHelperRequest) error
	ExecuteEntryConfigurationWork(context.Context, *agentpb.EntryConfigurationWorkHelperRequest) (*agentpb.EntryConfigurationWorkHelperResponse, error)
}

type HelperConsumers struct{ helper Helper }

func NewHelperConsumers(helper Helper) (*HelperConsumers, error) {
	if helper == nil {
		return nil, errs.New(errs.KindInternal, "Entry workload helper is not configured")
	}
	return &HelperConsumers{helper: helper}, nil
}

func (consumers *HelperConsumers) Reconcile(ctx context.Context, input entryexecution.FileInput) error {
	_, err := consumers.execute(ctx, input, false)
	return err
}

func (consumers *HelperConsumers) Inspect(ctx context.Context, input entryexecution.FileInput) (bool, error) {
	return consumers.execute(ctx, input, true)
}

func (consumers *HelperConsumers) execute(ctx context.Context, input entryexecution.FileInput, inspectOnly bool) (bool, error) {
	if ctx == nil || input.Progress.Validate(input.Attempt, input.Procedure) != nil || !input.Progress.InFlight {
		return false, invalidInput()
	}
	checkpoint, err := input.Progress.Checkpoint(input.Attempt, input.Procedure)
	if err != nil {
		return false, err
	}
	progress, err := entryexecution.EncodeProgress(input.Attempt, input.Procedure, input.Progress)
	if err != nil {
		return false, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return false, err
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return false, err
	}
	request := &agentpb.EntryConfigurationWorkHelperRequest{
		Assignment: &agentpb.EntryConfigurationWorkAssignment{Attempt: identity, Input: proto.CloneOf(input.Procedure)},
		Start:      &agentpb.WorkStartAck{Attempt: identity, StartedAt: timestamppb.New(*input.Attempt.StartedAt)},
		Report:     &agentpb.EntryConfigurationWorkCheckpointRequest{Attempt: identity, Progress: progress},
		Acknowledgement: &agentpb.EntryConfigurationWorkCheckpointAck{Attempt: identity,
			Sequence: checkpoint.Sequence, StageId: checkpoint.StageID, RecordedAt: timestamppb.New(checkpoint.RecordedAt),
			Evidence: &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest}},
		InspectOnly: inspectOnly,
	}
	captured, err := entryexecution.OpenHelperRequest(request)
	if err != nil {
		return false, err
	}
	response, err := consumers.helper.ExecuteEntryConfigurationWork(ctx, request)
	if err != nil {
		return false, err
	}
	observed, err := entryexecution.ReadHelperObservation(captured, response)
	if err != nil {
		return false, err
	}
	return observed.Satisfies(input.Progress.Stage), nil
}
