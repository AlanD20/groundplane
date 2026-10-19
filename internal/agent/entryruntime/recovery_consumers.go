package entryruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func recoveryRequest(
	input entryexecution.FileInput,
	inspectOnly bool,
) (*agentpb.EntryConfigurationWorkHelperRequest, error) {
	if input.Recovery == nil || input.Attempt.StartedAt == nil {
		return nil, invalidInput()
	}
	source, err := entryexecution.EncodeRecovery(*input.Recovery, input.Procedure)
	if err != nil {
		return nil, err
	}
	boundary, err := entryexecution.EncodeProgress(input.Recovery.Forward, input.Procedure, input.Progress)
	if err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	request := &agentpb.EntryConfigurationWorkHelperRequest{
		Assignment: &agentpb.EntryConfigurationWorkAssignment{
			Attempt:          identity,
			Input:            input.Procedure,
			RecoverySource:   source,
			RecoveryBoundary: boundary,
		},
		Start: &agentpb.WorkStartAck{
			Attempt:   identity,
			StartedAt: timestamppb.New(*input.Attempt.StartedAt),
		}, InspectOnly: inspectOnly,
	}
	_, err = entryexecution.OpenHelperRequest(request)
	return request, err
}

func (consumers *HelperConsumers) Stop(ctx context.Context, input entryexecution.FileInput) error {
	request, err := recoveryRequest(input, false)
	if err != nil {
		return err
	}
	return consumers.helper.StopEntryConfigurationWork(ctx, request)
}

func (consumers *HelperConsumers) Restore(
	ctx context.Context,
	input entryexecution.FileInput,
	inspectOnly bool,
) (bool, error) {
	request, err := recoveryRequest(input, inspectOnly)
	if err != nil {
		return false, err
	}
	captured, err := entryexecution.OpenHelperRequest(request)
	if err != nil {
		return false, err
	}
	response, err := consumers.helper.ExecuteEntryConfigurationWork(ctx, request)
	if err != nil {
		return false, err
	}
	observed, err := entryexecution.ReadHelperRestoration(captured, response)
	return observed.Restored, err
}
