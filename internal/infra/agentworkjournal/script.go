// Package agentworkjournal owns durable delivery state for native Agent work.
// Reports are persisted before transport; an acknowledgement advances the cursor
// without changing the attempt or authorizing another external effect.
package agentworkjournal

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const MaximumRecordBytes = 64 * 1024

// ScriptRecord retains the last report even after acknowledgement so a caller
// interrupted before observing the ack cannot allocate another sequence for it.
type ScriptRecord struct {
	Attempt workexecution.Attempt
	Report  *agentpb.ScriptCheckpointRequest
	Ack     *agentpb.ScriptCheckpointAck
}

type scriptDiskRecord struct {
	Schema  uint32                `json:"schema"`
	Attempt workexecution.Attempt `json:"attempt"`
	Report  []byte                `json:"report"`
	Ack     []byte                `json:"ack,omitempty"`
}

func (record ScriptRecord) Validate() error {
	if record.Attempt.Validate() != nil || record.Attempt.Executor.Executor != workexecution.Agent ||
		record.Attempt.FinishedAt != nil {
		return invalid()
	}
	report, err := executionplan.ValidateScriptCheckpointRequest(record.Report)
	if err != nil {
		return err
	}
	if report.TaskId != record.Attempt.TaskID || report.WorkItemId != record.Attempt.WorkItemID ||
		report.AttemptId != record.Attempt.ID || report.RecordedAt.AsTime().Before(record.Attempt.AssignedAt) {
		return invalid()
	}
	if record.Ack != nil {
		_, err = executionplan.ValidateScriptCheckpointAck(record.Ack, report)
	}
	return err
}

// PrepareScript is called under the journal lock. An existing pending report
// must be acknowledged before a different report can acquire the next sequence.
// Replay keeps the original timestamp and sequence, including after reconnect.
func PrepareScript(attempt workexecution.Attempt, previous *ScriptRecord, sequence uint64,
	request *agentpb.ScriptCheckpointRequest, at time.Time,
) (ScriptRecord, error) {
	if request == nil || request.Sequence != 0 || sequence == 0 || attempt.Validate() != nil ||
		at.IsZero() || at.Before(attempt.AssignedAt) {
		return ScriptRecord{}, invalid()
	}
	next := ScriptRecord{Attempt: attempt, Report: proto.Clone(request).(*agentpb.ScriptCheckpointRequest)}
	next.Report.Sequence, next.Report.RecordedAt = sequence, timestamppb.New(at.UTC())
	if previous != nil {
		if err := previous.Validate(); err != nil {
			return ScriptRecord{}, err
		}
		if !workexecution.SameAssignment(previous.Attempt, attempt) {
			return ScriptRecord{}, conflict()
		}
		candidate := proto.Clone(next.Report).(*agentpb.ScriptCheckpointRequest)
		candidate.Sequence = previous.Report.Sequence
		candidate.RecordedAt = proto.Clone(previous.Report.RecordedAt).(*timestamppb.Timestamp)
		if proto.Equal(candidate, previous.Report) {
			return cloneScriptRecord(*previous), nil
		}
		if previous.Ack == nil || previous.Report.Sequence >= sequence ||
			at.Before(previous.Report.RecordedAt.AsTime()) {
			return ScriptRecord{}, conflict()
		}
	}
	if err := next.Validate(); err != nil {
		return ScriptRecord{}, err
	}
	return next, nil
}

func AcknowledgeScript(record ScriptRecord, ack *agentpb.ScriptCheckpointAck) (ScriptRecord, error) {
	if err := record.Validate(); err != nil {
		return ScriptRecord{}, err
	}
	validated, err := executionplan.ValidateScriptCheckpointAck(ack, record.Report)
	if err != nil {
		return ScriptRecord{}, err
	}
	if record.Ack != nil && !proto.Equal(record.Ack, validated) {
		return ScriptRecord{}, conflict()
	}
	next := cloneScriptRecord(record)
	next.Ack = validated
	return next, nil
}

func cloneScriptRecord(record ScriptRecord) ScriptRecord {
	next := record
	next.Report = proto.Clone(record.Report).(*agentpb.ScriptCheckpointRequest)
	if record.Ack != nil {
		next.Ack = proto.Clone(record.Ack).(*agentpb.ScriptCheckpointAck)
	}
	return next
}

func encodeScript(record ScriptRecord) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	data := scriptDiskRecord{Schema: 1, Attempt: record.Attempt}
	var err error
	data.Report, err = (proto.MarshalOptions{Deterministic: true}).Marshal(record.Report)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if record.Ack != nil {
		data.Ack, err = (proto.MarshalOptions{Deterministic: true}).Marshal(record.Ack)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(raw) > MaximumRecordBytes {
		return nil, invalid()
	}
	return jcs.Canonicalize(raw)
}

func decodeScript(raw []byte) (ScriptRecord, error) {
	if len(raw) == 0 || len(raw) > MaximumRecordBytes {
		return ScriptRecord{}, invalid()
	}
	data, err := jcs.Decode[scriptDiskRecord](raw)
	if err != nil || data.Schema != 1 {
		return ScriptRecord{}, invalid()
	}
	record := ScriptRecord{Attempt: data.Attempt, Report: &agentpb.ScriptCheckpointRequest{}}
	if proto.Unmarshal(data.Report, record.Report) != nil {
		return ScriptRecord{}, invalid()
	}
	if len(data.Ack) != 0 {
		record.Ack = &agentpb.ScriptCheckpointAck{}
		if proto.Unmarshal(data.Ack, record.Ack) != nil {
			return ScriptRecord{}, invalid()
		}
	}
	encoded, err := encodeScript(record)
	if err != nil {
		return ScriptRecord{}, err
	}
	if !bytes.Equal(raw, encoded) {
		return ScriptRecord{}, invalid()
	}
	return record, nil
}

func invalid() error {
	return errs.New(errs.KindValidationFailed, "Agent work journal record is invalid")
}
func conflict() error {
	return errs.New(errs.KindStateConflict, "Agent work report conflicts with its durable delivery state")
}
