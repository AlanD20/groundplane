package backupconfiguration

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ConfigRestoreTransferOwner identifies the sealed, not-yet-live generation.
// Its records are recovery input, not Entry resources or materialized files.
type ConfigRestoreTransferOwner struct {
	Transfer             ConfigTransferOwner `json:"transfer"`
	EnvironmentID        string              `json:"environment_id"`
	GenerationID         string              `json:"generation_id"`
	RenderGeneration     uint64              `json:"render_generation"`
	BaselineRevisionID   string              `json:"baseline_revision_id"`
	BaselineHeadRevision int64               `json:"baseline_head_revision"`
	ContentSHA256        string              `json:"content_sha256"`
	SourceSizeBytes      uint64              `json:"source_size_bytes"`
	SourceSHA256         string              `json:"source_sha256"`
}

// Every frame is protected, including metadata and Entry-end digests. This
// avoids persisting a second plaintext representation of selected Secrets.
type ConfigRestoreTransferRecord struct {
	Owner     ConfigRestoreTransferOwner        `json:"owner"`
	Sequence  uint64                            `json:"sequence"`
	Protected BackupConfigProtectedChunkPayload `json:"protected"`
}

type ConfigRestoreTransferBatch struct {
	Owner   ConfigRestoreTransferOwner
	Records []ConfigRestoreTransferRecord
}

func ValidateConfigRestoreTransferOwner(owner ConfigRestoreTransferOwner) error {
	if ValidateConfigTransferOwner(owner.Transfer) != nil ||
		ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil ||
		owner.Transfer.Binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE ||
		ids.Validate(ids.KindConfig, owner.GenerationID) != nil || owner.RenderGeneration == 0 ||
		ids.Validate(ids.KindTask, owner.BaselineRevisionID) != nil || owner.BaselineHeadRevision <= 0 ||
		!recordcodec.ValidSHA256(owner.ContentSHA256) || !recordcodec.ValidSHA256(owner.SourceSHA256) ||
		owner.SourceSizeBytes == 0 || owner.SourceSizeBytes > backupconfig.MaxSourceBytes {
		return captureSnapshotConflict()
	}
	return nil
}

func ConfigRestoreTransferRecordPrefix(binding backupconfigtransfer.Binding) string {
	return configTransferPrefix(binding) + "restore-record/"
}

func ConfigRestoreTransferRecordKey(binding backupconfigtransfer.Binding, sequence uint64) string {
	return ConfigRestoreTransferRecordPrefix(binding) + fmt.Sprintf("%020d", sequence)
}

func EncodeConfigRestoreTransferRecord(record ConfigRestoreTransferRecord) ([]byte, error) {
	if ValidateConfigRestoreTransferOwner(record.Owner) != nil || record.Sequence == 0 ||
		validateBackupConfigProtectedChunk(&record.Protected) != nil {
		return nil, captureSnapshotConflict()
	}
	return recordcodec.Encode("backup-config-restore-transfer-record", record)
}

func DecodeConfigRestoreTransferRecord(raw []byte) (ConfigRestoreTransferRecord, error) {
	if len(raw) == 0 || len(raw) > 96<<10 {
		return ConfigRestoreTransferRecord{}, captureSnapshotConflict()
	}
	record, err := recordcodec.Decode[ConfigRestoreTransferRecord](raw, "backup-config-restore-transfer-record")
	if err != nil {
		return ConfigRestoreTransferRecord{}, err
	}
	canonical, err := EncodeConfigRestoreTransferRecord(record)
	if err != nil || !bytes.Equal(raw, canonical) {
		clear(record.Protected.Ciphertext)
		return ConfigRestoreTransferRecord{}, captureSnapshotConflict()
	}
	return record, nil
}

func ClearConfigRestoreTransferRecords(records []ConfigRestoreTransferRecord) {
	for index := range records {
		clear(records[index].Protected.Ciphertext)
		records[index].Protected.Ciphertext = nil
	}
}

func validateConfigRestoreTransferPruneEntry(taskID, key string, raw []byte) error {
	if !strings.Contains(key, "/restore-record/") {
		return captureSnapshotConflict()
	}
	record, err := DecodeConfigRestoreTransferRecord(raw)
	defer clear(record.Protected.Ciphertext)
	if err != nil || record.Owner.Transfer.Binding.TaskID != taskID ||
		ConfigRestoreTransferRecordKey(record.Owner.Transfer.Binding, record.Sequence) != key {
		return captureSnapshotConflict()
	}
	return nil
}
