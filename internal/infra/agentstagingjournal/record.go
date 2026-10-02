// Package agentstagingjournal persists the exact Agent startup disposition
// authority. It never interprets missing state as permission to discard stages.
package agentstagingjournal

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const MaximumRecordBytes = 128 * 1024

type Phase string

const (
	PhasePlanRecorded    Phase = "plan-recorded"
	PhaseApplied         Phase = "applied"
	PhaseReceiptAccepted Phase = "receipt-accepted"
)

type Record struct {
	ProcessGeneration [16]byte
	Inventory         *agentpb.BackupStagingInventory
	Plan              *agentpb.BackupStagingRecoveryPlan
	Ack               *agentpb.BackupStagingRecoveryAck
	Receipt           *agentpb.BackupStagingRecoveryAckReceipt
	Phase             Phase
}

type durableRecord struct {
	Schema            uint32 `json:"schema"`
	ProcessGeneration []byte `json:"process_generation"`
	Inventory         []byte `json:"inventory"`
	Plan              []byte `json:"plan"`
	Ack               []byte `json:"ack,omitempty"`
	Receipt           []byte `json:"receipt,omitempty"`
	Phase             Phase  `json:"phase"`
}

func validateRecord(record Record) error {
	if record.ProcessGeneration == ([16]byte{}) {
		return invalidRecord()
	}
	if err := executionplan.ValidateBackupStagingInventory(record.Inventory); err != nil {
		return err
	}
	if err := executionplan.ValidateBackupStagingRecoveryPlan(record.Inventory, record.Plan); err != nil {
		return err
	}
	switch record.Phase {
	case PhasePlanRecorded:
		if record.Ack != nil || record.Receipt != nil {
			return invalidRecord()
		}
	case PhaseApplied:
		if record.Receipt != nil {
			return invalidRecord()
		}
		return ackMatchesPlan(record.Ack, record)
	case PhaseReceiptAccepted:
		if err := ackMatchesPlan(record.Ack, record); err != nil {
			return err
		}
		return receiptMatchesRecord(record.Receipt, record)
	default:
		return invalidRecord()
	}
	return nil
}

func ackMatchesPlan(ack *agentpb.BackupStagingRecoveryAck, record Record) error {
	if ack == nil || executionplan.RejectUnknown(ack) != nil {
		return invalidRecord()
	}
	inventorySHA, err := executionplan.BackupStagingInventorySHA256(record.Inventory)
	if err != nil {
		return err
	}
	planSHA, err := executionplan.BackupStagingRecoveryPlanSHA256(record.Plan)
	if err != nil {
		return err
	}
	if !bytes.Equal(ack.InventorySha256, inventorySHA) || !bytes.Equal(ack.AppliedPlanSha256, planSHA) ||
		ack.AppliedDispositionCount != uint32(len(record.Plan.Dispositions)+len(record.Plan.VolumeDispositions)) {
		return conflict()
	}
	_, err = executionplan.BackupStagingRecoveryAckSHA256(ack)
	return err
}

func validateReceipt(receipt *agentpb.BackupStagingRecoveryAckReceipt) error {
	if receipt == nil || executionplan.RejectUnknown(receipt) != nil || len(receipt.ProcessGeneration) != 16 ||
		len(
			receipt.InventorySha256,
		) != sha256.Size || len(receipt.AppliedPlanSha256) != sha256.Size || len(receipt.RecoveryAckSha256) != sha256.Size {
		return invalidRecord()
	}
	return nil
}

func receiptMatchesRecord(receipt *agentpb.BackupStagingRecoveryAckReceipt, record Record) error {
	if err := validateReceipt(receipt); err != nil {
		return err
	}
	if err := ackMatchesPlan(record.Ack, record); err != nil {
		return err
	}
	ackSHA, err := executionplan.BackupStagingRecoveryAckSHA256(record.Ack)
	if err != nil {
		return err
	}
	if !bytes.Equal(receipt.ProcessGeneration, record.ProcessGeneration[:]) ||
		!bytes.Equal(
			receipt.InventorySha256,
			record.Ack.InventorySha256,
		) || !bytes.Equal(receipt.AppliedPlanSha256, record.Ack.AppliedPlanSha256) ||
		!bytes.Equal(receipt.RecoveryAckSha256, ackSHA) {
		return conflict()
	}
	return nil
}

func encodeRecord(record Record) ([]byte, error) {
	if err := validateRecord(record); err != nil {
		return nil, err
	}
	data := durableRecord{
		Schema:            1,
		Phase:             record.Phase,
		ProcessGeneration: append([]byte(nil), record.ProcessGeneration[:]...),
	}
	var err error
	data.Inventory, err = (proto.MarshalOptions{Deterministic: true}).Marshal(record.Inventory)
	if err != nil {
		return nil, fileError(err)
	}
	data.Plan, err = (proto.MarshalOptions{Deterministic: true}).Marshal(record.Plan)
	if err != nil {
		return nil, fileError(err)
	}
	if record.Ack != nil {
		data.Ack, err = (proto.MarshalOptions{Deterministic: true}).Marshal(record.Ack)
		if err != nil {
			return nil, fileError(err)
		}
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
	if data.Schema != 1 || len(data.ProcessGeneration) != 16 {
		return Record{}, invalidRecord()
	}
	record := Record{
		Phase:     data.Phase,
		Inventory: &agentpb.BackupStagingInventory{},
		Plan:      &agentpb.BackupStagingRecoveryPlan{},
	}
	copy(record.ProcessGeneration[:], data.ProcessGeneration)
	if proto.Unmarshal(data.Inventory, record.Inventory) != nil || proto.Unmarshal(data.Plan, record.Plan) != nil {
		return Record{}, invalidRecord()
	}
	if len(data.Ack) != 0 {
		record.Ack = &agentpb.BackupStagingRecoveryAck{}
		if proto.Unmarshal(data.Ack, record.Ack) != nil {
			return Record{}, invalidRecord()
		}
	}
	if len(data.Receipt) != 0 {
		record.Receipt = &agentpb.BackupStagingRecoveryAckReceipt{}
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
	return errs.New(errs.KindValidationFailed, "Agent staging journal record or filesystem authority is invalid")
}
func conflict() error {
	return errs.New(errs.KindStateConflict, "Agent staging journal process, plan or receipt conflicts")
}
func fileError(err error) error {
	if err == nil {
		return nil
	}
	return errs.Wrap(errs.KindStorageUnavailable, err)
}
