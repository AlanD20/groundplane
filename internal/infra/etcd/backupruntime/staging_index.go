package backupruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// This immutable claim-time index lets a bounded opaque startup inventory find
// its original Task authority without scanning Task history or current labels.
type BackupStagingIndexRecord struct {
	Schema          uint32 `json:"schema"`
	TaskID          string `json:"task_id"`
	StepID          string `json:"step_id"`
	PointID         string `json:"point_id"`
	AgentID         string `json:"agent_id"`
	AgentGeneration uint64 `json:"agent_generation"`
	PlanSHA256      string `json:"plan_sha256"`
}

func BackupStagingIndexKey(recoverySHA []byte) string {
	return "/v1/runtime/backup-staging-index/" + hex.EncodeToString(recoverySHA)
}

func (record BackupStagingIndexRecord) RecoveryKey() ([]byte, error) {
	return executionplan.BackupStagingRecoveryKey(record.TaskID, record.StepID, record.PointID)
}

func EncodeBackupStagingIndex(record BackupStagingIndexRecord) ([]byte, error) {
	if _, err := record.RecoveryKey(); err != nil || record.Schema != 1 ||
		ids.Validate(ids.KindAgent, record.AgentID) != nil || record.AgentGeneration == 0 ||
		!recordcodec.ValidSHA256(record.PlanSHA256) {
		return nil, CorruptBackupRuntimeRecord()
	}
	return recordcodec.Encode("backup-staging-index", record)
}

func DecodeBackupStagingIndex(value []byte) (BackupStagingIndexRecord, error) {
	record, err := recordcodec.Decode[BackupStagingIndexRecord](value, "backup-staging-index")
	if err != nil {
		return BackupStagingIndexRecord{}, err
	}
	if _, err := EncodeBackupStagingIndex(record); err != nil {
		return BackupStagingIndexRecord{}, err
	}
	return record, nil
}
