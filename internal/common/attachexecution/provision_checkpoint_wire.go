package attachexecution

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeProvisionCheckpointRequest(input ProvisionInput, report ProvisionReport) (*agentpb.AttachProvisionCheckpointRequest, error) {
	raw, err := EncodeProvisionReport(input, report)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachProvisionCheckpointRequest{Attempt: attempt, Report: raw}, nil
}

func DecodeProvisionCheckpointRequest(input ProvisionInput, request *agentpb.AttachProvisionCheckpointRequest) (ProvisionReport, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return ProvisionReport{}, invalid("Attach provisioning checkpoint request is malformed")
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Attempt) {
		return ProvisionReport{}, invalid("Attach provisioning checkpoint belongs to another execution")
	}
	return DecodeProvisionReport(input, request.Report)
}

func EncodeProvisionCheckpointAck(input ProvisionInput, report ProvisionReport) (*agentpb.AttachProvisionCheckpointAck, error) {
	request, err := EncodeProvisionCheckpointRequest(input, report)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachProvisionCheckpointAck{Attempt: request.Attempt, Report: request.Report}, nil
}

func ValidateProvisionCheckpointAck(input ProvisionInput, report ProvisionReport, ack *agentpb.AttachProvisionCheckpointAck) error {
	if ack == nil || executionplan.RejectUnknown(ack) != nil {
		return invalid("Attach provisioning acknowledgement is malformed")
	}
	attempt, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Attempt) {
		return invalid("Attach provisioning acknowledgement belongs to another execution")
	}
	expected, err := EncodeProvisionReport(input, report)
	if err != nil {
		return err
	}
	if !bytes.Equal(expected, ack.Report) {
		return invalid("Attach provisioning acknowledgement differs from the saved report")
	}
	return nil
}
