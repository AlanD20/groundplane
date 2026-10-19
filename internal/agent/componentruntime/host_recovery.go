package componentruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (host *RuntimeHost) Restore(ctx context.Context, input Input) (componentexecution.RecoveryReport, error) {
	if input.recovery == nil || input.procedure == nil || input.attempt.StartedAt == nil {
		return componentexecution.RecoveryReport{}, invalidInput()
	}
	if err := host.ownership.Check(); err != nil {
		return componentexecution.RecoveryReport{}, err
	}
	saved, err := host.journal.PrepareComponentRecovery(
		ctx,
		input.attempt,
		input.procedure,
		*input.recovery,
		input.boundary,
	)
	if err != nil {
		return componentexecution.RecoveryReport{}, err
	}
	if saved.Failure != nil || saved.Receipt != nil || saved.Report != nil {
		return componentexecution.RecoveryReport{}, invalidInput()
	}
	source, err := componentexecution.EncodeRecovery(*input.recovery, input.procedure)
	if err != nil {
		return componentexecution.RecoveryReport{}, err
	}
	boundary, err := componentexecution.EncodeProgress(input.recovery.Forward, input.procedure, input.boundary)
	if err != nil {
		return componentexecution.RecoveryReport{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return componentexecution.RecoveryReport{}, err
	}
	request := &agentpb.ComponentWorkHelperRequest{
		Assignment: &agentpb.ComponentWorkAssignment{
			Attempt:          identity,
			Input:            input.procedure,
			RecoverySource:   source,
			RecoveryBoundary: boundary,
		},
		Start: &agentpb.WorkStartAck{Attempt: identity, StartedAt: timestamppb.New(*input.attempt.StartedAt)},
	}
	captured, err := componentexecution.OpenHelperRequest(request)
	if err != nil {
		return componentexecution.RecoveryReport{}, err
	}
	if err := host.ownership.CheckRecovery(input.recovery.Forward, input.attempt); err != nil {
		return componentexecution.RecoveryReport{}, err
	}
	if err := host.helper.StopComponentWork(ctx, request); err != nil {
		return componentexecution.RecoveryReport{}, err
	}
	if len(input.procedure.Files) != 0 {
		if _, err := host.configuration.Run(ctx, captured, false); err != nil {
			return componentexecution.RecoveryReport{}, err
		}
	}
	response, err := host.helper.ExecuteComponentWork(ctx, request)
	if err != nil {
		return componentexecution.RecoveryReport{}, err
	}
	if len(input.procedure.Files) != 0 {
		ready, err := host.configuration.Run(ctx, captured, true)
		if err != nil {
			return componentexecution.RecoveryReport{}, err
		}
		if !ready {
			return componentexecution.RecoveryReport{}, invalidInput()
		}
	}
	if err := host.ownership.Check(); err != nil {
		return componentexecution.RecoveryReport{}, err
	}
	return componentexecution.ReadHelperRecovery(captured, response)
}
