package backupruntime

import (
	"bytes"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackupStagingDeliveryRecord struct {
	HostID            string
	AgentID           string
	AgentGeneration   uint64
	ProcessGeneration [16]byte
	Inventory         *agentpb.BackupStagingInventory
	Plan              *agentpb.BackupStagingRecoveryPlan
	Ack               *agentpb.BackupStagingRecoveryAck
	Guards            []keyvalue.Condition
}

// AllowsGenerationReplacement replaces a settled handshake or carries the exact
// unfinished plan forward. Publication independently checks every stage against
// native Task authority; a later generation may reconcile only terminal Tasks.
func (record BackupStagingDeliveryRecord) AllowsGenerationReplacement(next BackupStagingDeliveryRecord) bool {
	if record.HostID != next.HostID || record.AgentID != next.AgentID || next.AgentGeneration <= record.AgentGeneration ||
		next.Ack != nil || record.Inventory == nil || next.Inventory == nil {
		return false
	}
	return record.Ack != nil || record.Plan != nil &&
		proto.Equal(record.Inventory, next.Inventory) && proto.Equal(record.Plan, next.Plan)
}

type backupStagingDeliveryData struct {
	HostID            string               `json:"host_id"`
	AgentID           string               `json:"agent_id"`
	AgentGeneration   uint64               `json:"agent_generation"`
	ProcessGeneration string               `json:"process_generation"`
	Inventory         []byte               `json:"inventory"`
	Plan              []byte               `json:"plan"`
	Ack               []byte               `json:"ack,omitempty"`
	Guards            []keyvalue.Condition `json:"guards"`
}

func BackupStagingDeliveryKey(agentID string) string {
	return "/v1/runtime/backup-staging-delivery/" + agentID
}

func EncodeBackupStagingDelivery(record BackupStagingDeliveryRecord) ([]byte, error) {
	if ids.Validate(ids.KindHost, record.HostID) != nil || ids.Validate(ids.KindAgent, record.AgentID) != nil || record.AgentGeneration == 0 ||
		record.ProcessGeneration == ([16]byte{}) ||
		executionplan.ValidateBackupStagingRecoveryPlan(record.Inventory, record.Plan) != nil {
		return nil, CorruptBackupRuntimeRecord()
	}
	if (len(record.Guards) == 0) != (len(record.Inventory.Entries)+len(record.Inventory.VolumeRestores) == 0) || len(record.Guards) > 256 {
		return nil, CorruptBackupRuntimeRecord()
	}
	previous := ""
	for _, guard := range record.Guards {
		if guard.Key <= previous || guard.ModRevision < 0 || guard.Prefix {
			return nil, CorruptBackupRuntimeRecord()
		}
		previous = guard.Key
	}
	data := backupStagingDeliveryData{HostID: record.HostID, AgentID: record.AgentID, AgentGeneration: record.AgentGeneration,
		ProcessGeneration: hex.EncodeToString(record.ProcessGeneration[:]), Guards: record.Guards}
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
		HostID:          data.HostID,
		AgentID:         data.AgentID,
		AgentGeneration: data.AgentGeneration,
		Guards:          data.Guards,
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
