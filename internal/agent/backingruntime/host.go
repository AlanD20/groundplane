package backingruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (executor *Executor) helperRequest(ctx context.Context, assignment backingprovision.Assignment,
	stage backingprovision.Stage, inspectOnly bool,
) (*agentpb.BackingProvisionHelperRequest, backingprovision.HelperInput, error) {
	if err := executor.ownership.Check(); err != nil {
		return nil, backingprovision.HelperInput{}, err
	}
	saved, err := executor.journal.Backing(ctx, assignment.Attempt, assignment.Procedure)
	if err != nil {
		return nil, backingprovision.HelperInput{}, err
	}
	if saved == nil || !saved.Acknowledged || saved.Progress == nil || !saved.Progress.InFlight || saved.Progress.Stage != stage ||
		saved.Completion != nil || saved.Failure != nil {
		return nil, backingprovision.HelperInput{}, invalidInput()
	}
	report, err := checkpointRequest(assignment, *saved.Progress)
	if err != nil {
		return nil, backingprovision.HelperInput{}, err
	}
	checkpoint, err := saved.Progress.Checkpoint(assignment.Attempt, saved.Layout)
	if err != nil {
		return nil, backingprovision.HelperInput{}, err
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, backingprovision.HelperInput{}, err
	}
	message, err := backingprovision.EncodeAssignment(assignment)
	if err != nil {
		return nil, backingprovision.HelperInput{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return nil, backingprovision.HelperInput{}, err
	}
	request := &agentpb.BackingProvisionHelperRequest{Assignment: message,
		Start:  &agentpb.WorkStartAck{Attempt: identity, StartedAt: timestamppb.New(*assignment.Attempt.StartedAt)},
		Report: report, Acknowledgement: &agentpb.BackingProvisionCheckpointAck{Attempt: identity, Progress: report.Progress,
			Evidence: &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest}}, InspectOnly: inspectOnly}
	input, err := backingprovision.OpenHelperRequest(request)
	return request, input, err
}

func (executor *Executor) execute(ctx context.Context, assignment backingprovision.Assignment, exchange Exchange,
	stage backingprovision.Stage, inspectOnly bool,
) (backingprovision.Observation, error) {
	request, captured, err := executor.helperRequest(ctx, assignment, stage, inspectOnly)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	switch stage {
	case backingprovision.WriteConfiguration:
		return executor.configure(ctx, captured, exchange, inspectOnly)
	case backingprovision.AfterStart:
		return executor.hook(ctx, captured, exchange, inspectOnly)
	case backingprovision.VerifyRuntime:
		saved, err := executor.journal.Backing(ctx, assignment.Attempt, assignment.Procedure)
		if err != nil {
			return backingprovision.Observation{}, err
		}
		if saved.Runtime != nil {
			// Replay the actual saved final inspection, not a new observation
			// with different timing that would change terminal evidence.
			return backingprovision.Observation{ServiceID: captured.Layout.ServiceID, Input: assignment.Attempt.Input,
				Stage: stage, RuntimeVerified: true, RuntimeReady: true, Healthy: captured.Layout.Healthcheck,
				ContainerID: saved.Runtime.ContainerID, At: saved.Runtime.ObservedAt}, nil
		}
	case backingprovision.WaitReady:
		// This stage only polls; resuming its wait cannot repeat an effect.
		request.InspectOnly = false
		captured.InspectOnly = false
	}
	response, err := executor.helper.ExecuteBackingProvision(ctx, executor.volumeRoot, request)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	if err := executor.ownership.Check(); err != nil {
		return backingprovision.Observation{}, err
	}
	observed, runtime, err := backingprovision.ReadHelperObservation(captured, response)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	if runtime != nil {
		if err := executor.journal.SaveBackingRuntime(ctx, assignment.Attempt, assignment.Procedure, *runtime); err != nil {
			return backingprovision.Observation{}, err
		}
	}
	return observed, nil
}
