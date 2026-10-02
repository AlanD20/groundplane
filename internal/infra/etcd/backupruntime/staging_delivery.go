package backupruntime

import (
	"bytes"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackupStagingTaskReference struct {
	TaskID                string `json:"task_id"`
	Revision              int64  `json:"revision"`
	RequiresTaskAuthority bool   `json:"requires_task_authority"`
}

type BackupStagingDeliveryRecord struct {
	AgentID           string
	AgentGeneration   uint64
	ProcessGeneration [16]byte
	Inventory         *agentpb.BackupStagingInventory
	Plan              *agentpb.BackupStagingRecoveryPlan
	Ack               *agentpb.BackupStagingRecoveryAck
	Tasks             []BackupStagingTaskReference
}

// AllowsGenerationReplacement carries no file or Task authority across an
// Agent replacement: both inventories must be empty and the old handshake settled.
func (record BackupStagingDeliveryRecord) AllowsGenerationReplacement(next BackupStagingDeliveryRecord) bool {
	return record.AgentID == next.AgentID && next.AgentGeneration > record.AgentGeneration &&
		record.Ack != nil && next.Ack == nil && len(record.Tasks) == 0 && len(next.Tasks) == 0 &&
		record.Inventory != nil && next.Inventory != nil &&
		len(record.Inventory.Entries) == 0 && len(record.Inventory.VolumeRestores) == 0 &&
		len(next.Inventory.Entries) == 0 && len(next.Inventory.VolumeRestores) == 0
}

type backupStagingDeliveryData struct {
	AgentID           string                       `json:"agent_id"`
	AgentGeneration   uint64                       `json:"agent_generation"`
	ProcessGeneration string                       `json:"process_generation"`
	Inventory         []byte                       `json:"inventory"`
	Plan              []byte                       `json:"plan"`
	Ack               []byte                       `json:"ack,omitempty"`
	Tasks             []BackupStagingTaskReference `json:"tasks"`
}

func BackupStagingDeliveryKey(agentID string) string {
	return "/v1/runtime/backup-staging-delivery/" + agentID
}

func EncodeBackupStagingDelivery(record BackupStagingDeliveryRecord) ([]byte, error) {
	if ids.Validate(ids.KindAgent, record.AgentID) != nil || record.AgentGeneration == 0 ||
		record.ProcessGeneration == ([16]byte{}) ||
		executionplan.ValidateBackupStagingRecoveryPlan(record.Inventory, record.Plan) != nil ||
		len(record.Tasks) > executionplan.MaximumBackupRecoveredStages {
		return nil, CorruptBackupRuntimeRecord()
	}
	seen := make(map[string]struct{}, len(record.Tasks))
	for _, reference := range record.Tasks {
		if ids.Validate(ids.KindTask, reference.TaskID) != nil || reference.Revision <= 0 {
			return nil, CorruptBackupRuntimeRecord()
		}
		if _, duplicate := seen[reference.TaskID]; duplicate {
			return nil, CorruptBackupRuntimeRecord()
		}
		seen[reference.TaskID] = struct{}{}
	}
	if (len(record.Tasks) == 0) != (len(record.Inventory.Entries)+len(record.Inventory.VolumeRestores) == 0) {
		return nil, CorruptBackupRuntimeRecord()
	}
	data := backupStagingDeliveryData{AgentID: record.AgentID, AgentGeneration: record.AgentGeneration,
		ProcessGeneration: hex.EncodeToString(record.ProcessGeneration[:]), Tasks: record.Tasks}
	marshal := proto.MarshalOptions{Deterministic: true}
	var err error
	if data.Inventory, err = marshal.Marshal(record.Inventory); err != nil {
		return nil, CorruptBackupRuntimeRecord()
	}
	if data.Plan, err = marshal.Marshal(record.Plan); err != nil {
		return nil, CorruptBackupRuntimeRecord()
	}
	if record.Ack != nil {
		if _, err := executionplan.BackupStagingRecoveryAckSHA256(record.Ack); err != nil {
			return nil, err
		}
		planSHA, err := executionplan.BackupStagingRecoveryPlanSHA256(record.Plan)
		if err != nil || !bytes.Equal(record.Ack.InventorySha256, record.Plan.InventorySha256) ||
			!bytes.Equal(
				record.Ack.AppliedPlanSha256,
				planSHA,
			) || int(record.Ack.AppliedDispositionCount) !=
			len(record.Plan.Dispositions)+len(record.Plan.VolumeDispositions) {
			return nil, CorruptBackupRuntimeRecord()
		}
		if data.Ack, err = marshal.Marshal(record.Ack); err != nil {
			return nil, CorruptBackupRuntimeRecord()
		}
	}
	return recordcodec.Encode("backup-staging-delivery", data)
}

func DecodeBackupStagingDelivery(value []byte) (BackupStagingDeliveryRecord, error) {
	if len(value) == 0 || len(value) > 128*1024 {
		return BackupStagingDeliveryRecord{}, CorruptBackupRuntimeRecord()
	}
	data, err := recordcodec.Decode[backupStagingDeliveryData](value, "backup-staging-delivery")
	if err != nil {
		return BackupStagingDeliveryRecord{}, err
	}
	process, err := hex.DecodeString(data.ProcessGeneration)
	if err != nil || len(process) != 16 {
		return BackupStagingDeliveryRecord{}, CorruptBackupRuntimeRecord()
	}
	record := BackupStagingDeliveryRecord{
		AgentID:         data.AgentID,
		AgentGeneration: data.AgentGeneration,
		Tasks:           data.Tasks,
		Inventory:       &agentpb.BackupStagingInventory{},
		Plan:            &agentpb.BackupStagingRecoveryPlan{},
	}
	copy(record.ProcessGeneration[:], process)
	if proto.Unmarshal(data.Inventory, record.Inventory) != nil || proto.Unmarshal(data.Plan, record.Plan) != nil {
		return BackupStagingDeliveryRecord{}, CorruptBackupRuntimeRecord()
	}
	if data.Ack != nil {
		record.Ack = &agentpb.BackupStagingRecoveryAck{}
		if proto.Unmarshal(data.Ack, record.Ack) != nil {
			return BackupStagingDeliveryRecord{}, CorruptBackupRuntimeRecord()
		}
	}
	encoded, err := EncodeBackupStagingDelivery(record)
	if err != nil || !bytes.Equal(encoded, value) {
		return BackupStagingDeliveryRecord{}, CorruptBackupRuntimeRecord()
	}
	return record, nil
}
