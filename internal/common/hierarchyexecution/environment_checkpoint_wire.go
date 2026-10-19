package hierarchyexecution

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeEnvironmentCheckpointRequest(
	input EnvironmentInput,
	report EnvironmentReport,
) (*agentpb.EnvironmentRemovalCheckpointRequest, error) {
	raw, err := EncodeEnvironmentReport(input, report)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.EnvironmentRemovalCheckpointRequest{Attempt: attempt, Report: raw}, nil
}

func DecodeEnvironmentCheckpointRequest(
	input EnvironmentInput,
	request *agentpb.EnvironmentRemovalCheckpointRequest,
) (EnvironmentReport, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return EnvironmentReport{}, invalidEnvironmentProgress()
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Attempt) {
		return EnvironmentReport{}, invalidEnvironmentProgress()
	}
	return DecodeEnvironmentReport(input, request.Report)
}

func EncodeEnvironmentCheckpointAck(
	input EnvironmentInput,
	report EnvironmentReport,
) (*agentpb.EnvironmentRemovalCheckpointAck, error) {
	request, err := EncodeEnvironmentCheckpointRequest(input, report)
	if err != nil {
		return nil, err
	}
	return &agentpb.EnvironmentRemovalCheckpointAck{Attempt: request.Attempt, Report: request.Report}, nil
}

func ValidateEnvironmentCheckpointAck(
	input EnvironmentInput,
	report EnvironmentReport,
	ack *agentpb.EnvironmentRemovalCheckpointAck,
) error {
	if ack == nil || executionplan.RejectUnknown(ack) != nil {
		return invalidEnvironmentProgress()
	}
	attempt, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Attempt) {
		return invalidEnvironmentProgress()
	}
	expected, err := EncodeEnvironmentReport(input, report)
	if err != nil {
		return err
	}
	if !bytes.Equal(expected, ack.Report) {
		return invalidEnvironmentProgress()
	}
	return nil
}
