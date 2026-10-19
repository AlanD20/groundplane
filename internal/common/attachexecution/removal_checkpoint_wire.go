package attachexecution

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeRemovalCheckpointRequest(
	input RemovalInput,
	report RemovalReport,
) (*agentpb.AttachRemovalCheckpointRequest, error) {
	raw, err := EncodeRemovalReport(input, report)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachRemovalCheckpointRequest{Attempt: attempt, Report: raw}, nil
}

func DecodeRemovalCheckpointRequest(
	input RemovalInput,
	request *agentpb.AttachRemovalCheckpointRequest,
) (RemovalReport, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return RemovalReport{}, invalid("Attach removal checkpoint request is malformed")
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Attempt) {
		return RemovalReport{}, invalid("Attach removal checkpoint belongs to another execution")
	}
	return DecodeRemovalReport(input, request.Report)
}

func EncodeRemovalCheckpointAck(input RemovalInput, report RemovalReport) (*agentpb.AttachRemovalCheckpointAck, error) {
	request, err := EncodeRemovalCheckpointRequest(input, report)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachRemovalCheckpointAck{Attempt: request.Attempt, Report: request.Report}, nil
}

func ValidateRemovalCheckpointAck(
	input RemovalInput,
	report RemovalReport,
	ack *agentpb.AttachRemovalCheckpointAck,
) error {
	if ack == nil || executionplan.RejectUnknown(ack) != nil {
		return invalid("Attach removal acknowledgement is malformed")
	}
	attempt, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Attempt) {
		return invalid("Attach removal acknowledgement belongs to another execution")
	}
	expected, err := EncodeRemovalReport(input, report)
	if err != nil {
		return err
	}
	if !bytes.Equal(expected, ack.Report) {
		return invalid("Attach removal acknowledgement differs from the saved report")
	}
	return nil
}
