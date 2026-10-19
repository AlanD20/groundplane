package releaseexecution

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const MaximumCheckpointBytes = 16 * 1024

func EncodeCheckpointRequest(input Input, report CheckpointReport) (*agentpb.ReleaseWorkCheckpointRequest, error) {
	if report.Ack != nil || report.ValidateEnvelope(input.Attempt) != nil || report.Progress.Validate(input) != nil {
		return nil, invalidProgress()
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	raw, err := canonicalCheckpoint(report)
	if err != nil {
		return nil, err
	}
	return &agentpb.ReleaseWorkCheckpointRequest{Attempt: attempt, Report: raw}, nil
}

// DecodeCheckpointRequest uses the stored started attempt and captured input.
// It validates the envelope only; the owner must also validate the transition
// and atomically compare permission/evidence before publishing its receipt.
func DecodeCheckpointRequest(input Input, request *agentpb.ReleaseWorkCheckpointRequest) (CheckpointReport, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || len(request.Report) == 0 ||
		len(request.Report) > MaximumCheckpointBytes {
		return CheckpointReport{}, invalidProgress()
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Attempt) {
		return CheckpointReport{}, invalidProgress()
	}
	report, err := jcs.Decode[CheckpointReport](request.Report)
	if err != nil || report.Ack != nil || report.ValidateEnvelope(input.Attempt) != nil ||
		report.Progress.Validate(input) != nil {
		return CheckpointReport{}, invalidProgress()
	}
	canonical, err := canonicalCheckpoint(report)
	if err != nil || !bytes.Equal(canonical, request.Report) {
		return CheckpointReport{}, invalidProgress()
	}
	return report, nil
}

func EncodeCheckpointAck(input Input, report CheckpointReport) (*agentpb.ReleaseWorkCheckpointAck, error) {
	if report.ValidateEnvelope(input.Attempt) != nil || report.Progress.Validate(input) != nil {
		return nil, invalidProgress()
	}
	checkpoint, err := report.Checkpoint(input.Attempt)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	raw, err := canonicalCheckpoint(checkpoint)
	if err != nil {
		return nil, err
	}
	return &agentpb.ReleaseWorkCheckpointAck{Attempt: attempt, Checkpoint: raw}, nil
}

func DecodeCheckpointAck(
	input Input,
	report CheckpointReport,
	ack *agentpb.ReleaseWorkCheckpointAck,
) (workexecution.Checkpoint, error) {
	if ack == nil || len(ack.ProtoReflect().GetUnknown()) != 0 || len(ack.Checkpoint) == 0 ||
		len(ack.Checkpoint) > MaximumCheckpointBytes {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	attempt, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, input.Attempt) {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	checkpoint, err := jcs.Decode[workexecution.Checkpoint](ack.Checkpoint)
	if err != nil {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	expected, err := report.Checkpoint(input.Attempt)
	if err != nil || !EqualCheckpoint(expected, checkpoint) {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	canonical, err := canonicalCheckpoint(checkpoint)
	if err != nil || !bytes.Equal(canonical, ack.Checkpoint) {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	return checkpoint, nil
}

func canonicalCheckpoint(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > MaximumCheckpointBytes {
		return nil, invalidProgress()
	}
	return jcs.Canonicalize(raw)
}
