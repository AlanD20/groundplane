// Package agentterminaljournal owns the crash-durable Agent Backup terminal
// delivery journal. It does not choose process rebinding or staging dispositions.
package agentterminaljournal

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	MaximumRecords     = 1024
	MaximumRecordBytes = 32 * 1024
)

type Phase string

const (
	PhasePending Phase = "pending"
	PhaseApplied Phase = "applied"
	PhaseRetired Phase = "retired"
)

type Record struct {
	Ack     *agentpb.TaskAck
	Receipt *agentpb.TaskTerminalReceiptAck
	Phase   Phase
}

type durableRecord struct {
	Schema  uint32 `json:"schema"`
	Phase   Phase  `json:"phase"`
	Ack     []byte `json:"ack"`
	Receipt []byte `json:"receipt,omitempty"`
}

func validateRecord(record Record) error {
	if record.Ack.GetBackupResult() == nil || record.Ack.GetBackupResult().RecoveryRequired == nil {
		return invalidRecord()
	}
	if failed := record.Ack.GetBackupResult().FailedStepId; failed != "" && ids.Validate(ids.KindStep, failed) != nil {
		return invalidRecord()
	}
	if _, err := executionplan.TaskAcknowledgementSHA256(record.Ack); err != nil {
		return err
	}
	switch record.Phase {
	case PhasePending:
		if record.Receipt != nil {
			return invalidRecord()
		}
	case PhaseApplied, PhaseRetired:
		if err := receiptMatchesAck(record.Receipt, record.Ack); err != nil {
			return err
		}
	default:
		return invalidRecord()
	}
	return nil
}

func receiptMatchesAck(receipt *agentpb.TaskTerminalReceiptAck, ack *agentpb.TaskAck) error {
	if err := executionplan.ValidateTaskTerminalReceiptAck(receipt); err != nil {
		return err
	}
	digest, err := executionplan.TaskAcknowledgementSHA256(ack)
	if err != nil {
		return err
	}
	if receipt.TaskId != ack.TaskId || receipt.AssignmentId != ack.AssignmentId ||
		receipt.AssignmentGeneration != ack.AssignmentGeneration ||
		!bytes.Equal(receipt.PlanHash, ack.PlanHash) ||
		!bytes.Equal(receipt.TaskAckSha256, digest) {
		return conflict()
	}
	return nil
}

func encodeRecord(record Record) ([]byte, error) {
	if err := validateRecord(record); err != nil {
		return nil, err
	}
	data := durableRecord{Schema: 1, Phase: record.Phase}
	var err error
	data.Ack, err = (proto.MarshalOptions{Deterministic: true}).Marshal(record.Ack)
	if err != nil {
		return nil, fileError(err)
	}
	if record.Receipt != nil {
		data.Receipt, err = (proto.MarshalOptions{Deterministic: true}).Marshal(record.Receipt)
		if err != nil {
			return nil, fileError(err)
		}
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fileError(err)
	}
	if len(raw) > MaximumRecordBytes {
		return nil, invalidRecord()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaximumRecordBytes {
		return nil, invalidRecord()
	}
	return canonical, nil
}

func decodeRecord(raw []byte) (Record, error) {
	if len(raw) == 0 || len(raw) > MaximumRecordBytes {
		return Record{}, invalidRecord()
	}
	data, err := jcs.Decode[durableRecord](raw)
	if err != nil {
		return Record{}, err
	}
	if data.Schema != 1 {
		return Record{}, invalidRecord()
	}
	record := Record{Phase: data.Phase, Ack: &agentpb.TaskAck{}}
	if proto.Unmarshal(data.Ack, record.Ack) != nil {
		return Record{}, invalidRecord()
	}
	if len(data.Receipt) != 0 {
		record.Receipt = &agentpb.TaskTerminalReceiptAck{}
		if proto.Unmarshal(data.Receipt, record.Receipt) != nil {
			return Record{}, invalidRecord()
		}
	}
	encoded, err := encodeRecord(record)
	if err != nil {
		return Record{}, err
	}
	if !bytes.Equal(raw, encoded) {
		return Record{}, invalidRecord()
	}
	return record, nil
}

func invalidRecord() error {
	return errs.New(errs.KindValidationFailed, "Agent terminal journal record is invalid")
}
func conflict() error {
	return errs.New(errs.KindStateConflict, "Agent terminal journal receipt or phase conflicts")
}
func fileError(err error) error {
	if err == nil {
		return nil
	}
	return errs.Wrap(errs.KindStorageUnavailable, err)
}
