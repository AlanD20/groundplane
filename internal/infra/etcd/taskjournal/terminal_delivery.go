package taskjournal

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type TaskTerminalDeliveryPhase string

const (
	TaskTerminalDeliveryCommitted TaskTerminalDeliveryPhase = "committed"
	TaskTerminalDeliveryApplied   TaskTerminalDeliveryPhase = "applied"
	TaskTerminalDeliveryRetired   TaskTerminalDeliveryPhase = "retired"
)

type TaskTerminalDeliveryRecord struct {
	Receipt *agentpb.TaskTerminalReceiptAck
	Report  *agentpb.TaskAck
	Phase   TaskTerminalDeliveryPhase
}

type terminalDeliveryData struct {
	Schema  uint32                    `json:"schema"`
	Receipt []byte                    `json:"receipt"`
	Report  []byte                    `json:"report"`
	Phase   TaskTerminalDeliveryPhase `json:"phase"`
}

func TaskTerminalDeliveryKey(taskID string) string {
	return "/v1/runtime/task-terminal-deliveries/" + taskID
}

func EncodeTaskTerminalDelivery(record TaskTerminalDeliveryRecord) ([]byte, error) {
	if err := executionplan.ValidateTaskTerminalReceiptAck(record.Receipt); err != nil {
		return nil, err
	}
	digest, err := executionplan.TaskAcknowledgementSHA256(record.Report)
	if err != nil || !bytes.Equal(digest, record.Receipt.TaskAckSha256) ||
		record.Report.TaskId != record.Receipt.TaskId ||
		record.Report.AssignmentId != record.Receipt.AssignmentId ||
		record.Report.AssignmentGeneration != record.Receipt.AssignmentGeneration ||
		!bytes.Equal(record.Report.PlanHash, record.Receipt.PlanHash) {
		return nil, corruptTaskTerminalDelivery()
	}
	switch record.Phase {
	case TaskTerminalDeliveryCommitted, TaskTerminalDeliveryApplied, TaskTerminalDeliveryRetired:
	default:
		return nil, corruptTaskTerminalDelivery()
	}
	receipt, err := (proto.MarshalOptions{Deterministic: true}).Marshal(record.Receipt)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	report, err := (proto.MarshalOptions{Deterministic: true}).Marshal(record.Report)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return json.Marshal(terminalDeliveryData{Schema: 1, Receipt: receipt, Report: report, Phase: record.Phase})
}

func DecodeTaskTerminalDelivery(value []byte) (TaskTerminalDeliveryRecord, error) {
	if len(value) == 0 || len(value) > 8192 || recordcodec.RejectDuplicateFields(value) != nil {
		return TaskTerminalDeliveryRecord{}, corruptTaskTerminalDelivery()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data terminalDeliveryData
	if err := decoder.Decode(&data); err != nil || recordcodec.RequireEOF(decoder) != nil || data.Schema != 1 {
		return TaskTerminalDeliveryRecord{}, corruptTaskTerminalDelivery()
	}
	receipt := &agentpb.TaskTerminalReceiptAck{}
	if err := proto.Unmarshal(data.Receipt, receipt); err != nil {
		return TaskTerminalDeliveryRecord{}, corruptTaskTerminalDelivery()
	}
	report := &agentpb.TaskAck{}
	if err := proto.Unmarshal(data.Report, report); err != nil {
		return TaskTerminalDeliveryRecord{}, corruptTaskTerminalDelivery()
	}
	record := TaskTerminalDeliveryRecord{Receipt: receipt, Report: report, Phase: data.Phase}
	encoded, err := EncodeTaskTerminalDelivery(record)
	if err != nil || !bytes.Equal(value, encoded) {
		return TaskTerminalDeliveryRecord{}, corruptTaskTerminalDelivery()
	}
	return record, nil
}

func corruptTaskTerminalDelivery() error {
	return errs.New(errs.KindInternal, "Task terminal delivery record is corrupt")
}
