package releaseexecution

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeRecoveryRequest(input RecoveryInput, report RecoveryReport) (*agentpb.ReleaseRecoveryCheckpointRequest, error) {
	if report.Ack != nil || report.Validate(input) != nil {
		return nil, invalidProgress()
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Input.Attempt)
	if err != nil {
		return nil, err
	}
	raw, err := canonicalCheckpoint(report)
	if err != nil {
		return nil, err
	}
	return &agentpb.ReleaseRecoveryCheckpointRequest{Attempt: attempt, Report: raw}, nil
}

func DecodeRecoveryRequest(input RecoveryInput, request *agentpb.ReleaseRecoveryCheckpointRequest) (RecoveryReport, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || len(request.Report) == 0 || len(request.Report) > MaximumCheckpointBytes {
		return RecoveryReport{}, invalidProgress()
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, input.Input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Input.Attempt) {
		return RecoveryReport{}, invalidProgress()
	}
	report, err := jcs.Decode[RecoveryReport](request.Report)
	if err != nil || report.Ack != nil || report.Validate(input) != nil {
		return RecoveryReport{}, invalidProgress()
	}
	canonical, err := canonicalCheckpoint(report)
	if err != nil || !bytes.Equal(canonical, request.Report) {
		return RecoveryReport{}, invalidProgress()
	}
	return report, nil
}

func EncodeRecoveryAck(input RecoveryInput, report RecoveryReport) (*agentpb.ReleaseRecoveryCheckpointAck, error) {
	if report.Validate(input) != nil {
		return nil, invalidProgress()
	}
	checkpoint, err := report.Checkpoint(input)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Input.Attempt)
	if err != nil {
		return nil, err
	}
	raw, err := canonicalCheckpoint(checkpoint)
	if err != nil {
		return nil, err
	}
	return &agentpb.ReleaseRecoveryCheckpointAck{Attempt: attempt, Checkpoint: raw}, nil
}

func DecodeRecoveryAck(input RecoveryInput, report RecoveryReport, ack *agentpb.ReleaseRecoveryCheckpointAck) (workexecution.Checkpoint, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || len(ack.Checkpoint) == 0 || len(ack.Checkpoint) > MaximumCheckpointBytes || report.Validate(input) != nil {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	attempt, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Input.Attempt) {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	checkpoint, err := jcs.Decode[workexecution.Checkpoint](ack.Checkpoint)
	if err != nil {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	expected, err := report.Checkpoint(input)
	if err != nil || !EqualCheckpoint(expected, checkpoint) {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	canonical, err := canonicalCheckpoint(checkpoint)
	if err != nil || !bytes.Equal(canonical, ack.Checkpoint) {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	return checkpoint, nil
}
