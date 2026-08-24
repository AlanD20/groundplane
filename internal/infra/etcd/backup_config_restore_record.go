package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	backupConfigRestoreEntryRootPrefix  = "/v1/runtime/config-restore-entries/"
	backupConfigRestoreDescriptorPrefix = "/v1/runtime/config-restore-descriptors/"
	backupConfigRestoreValuePrefix      = "/v1/runtime/config-restore-values/"
	backupConfigRestoreIdentityPrefix   = "/v1/runtime/config-restore-identities/"
)

// BackupConfigRestoreEntryRecord is one fully bounded staged Entry. Restore
// creates fresh immutable value generations, so no old revision is durable.
type BackupConfigRestoreEntryRecord struct {
	TaskID            string `json:"task_id"`
	EntryOrdinal      uint64 `json:"entry_ordinal"`
	EntryID           string `json:"entry_id"`
	ValueGenerationID string `json:"value_generation_id"`
	Secret            bool   `json:"secret"`
	DescriptorLength  uint64 `json:"descriptor_length"`
	DescriptorSHA256  string `json:"descriptor_sha256"`
	DescriptorChunks  uint32 `json:"descriptor_chunks"`
	PlainValueLength  uint64 `json:"plain_value_length"`
	PlainValueSHA256  string `json:"plain_value_sha256,omitempty"`
	ValueChunks       uint32 `json:"value_chunks"`
}

type BackupConfigRestoreDescriptorChunkRecord struct {
	TaskID            string `json:"task_id"`
	EntryOrdinal      uint64 `json:"entry_ordinal"`
	EntryID           string `json:"entry_id"`
	ValueGenerationID string `json:"value_generation_id"`
	Secret            bool   `json:"secret"`
	ChunkOrdinal      uint32 `json:"chunk_ordinal"`
	Offset            uint64 `json:"offset"`
	Length            uint32 `json:"length"`
	SHA256            string `json:"sha256"`
	Content           []byte `json:"content"`
}

type BackupConfigRestoreValueChunkRecord struct {
	TaskID            string                             `json:"task_id"`
	EntryOrdinal      uint64                             `json:"entry_ordinal"`
	EntryID           string                             `json:"entry_id"`
	ValueGenerationID string                             `json:"value_generation_id"`
	Secret            bool                               `json:"secret"`
	ChunkOrdinal      uint32                             `json:"chunk_ordinal"`
	Storage           BackupConfigChunkStorage           `json:"storage"`
	Plain             *BackupConfigPlainChunkPayload     `json:"plain,omitempty"`
	Protected         *BackupConfigProtectedChunkPayload `json:"protected,omitempty"`
}

func backupConfigRestoreEntryPrefix(taskID string) string {
	return backupConfigRestoreEntryRootPrefix + taskID + "/"
}

func backupConfigRestoreEntryKey(taskID string, entryOrdinal uint64) string {
	return backupConfigRestoreEntryPrefix(taskID) + backupConfigEntryOrdinal(entryOrdinal)
}

func backupConfigRestoreDescriptorChunkPrefix(taskID string, entryOrdinal uint64) string {
	return backupConfigRestoreDescriptorPrefix + taskID + "/" + backupConfigEntryOrdinal(entryOrdinal) + "/"
}

func backupConfigRestoreDescriptorChunkKey(taskID string, entryOrdinal uint64, chunkOrdinal uint32) string {
	return backupConfigRestoreDescriptorChunkPrefix(taskID, entryOrdinal) + backupConfigChunkOrdinal(chunkOrdinal)
}

func backupConfigRestoreValueChunkPrefix(taskID string, entryOrdinal uint64) string {
	return backupConfigRestoreValuePrefix + taskID + "/" + backupConfigEntryOrdinal(entryOrdinal) + "/"
}

func backupConfigRestoreValueChunkKey(taskID string, entryOrdinal uint64, chunkOrdinal uint32) string {
	return backupConfigRestoreValueChunkPrefix(taskID, entryOrdinal) + backupConfigChunkOrdinal(chunkOrdinal)
}

func backupConfigRestoreEntryIdentityPrefix(taskID string) string {
	return backupConfigRestoreIdentityPrefix + taskID + "/id/"
}

func backupConfigRestoreEntryIdentityKey(taskID string, entryID string) string {
	return backupConfigRestoreEntryIdentityPrefix(taskID) + entryID
}

func backupConfigRestoreDestinationPrefix(taskID string) string {
	return backupConfigRestoreIdentityPrefix + taskID + "/destination/"
}

func backupConfigRestoreDestinationKey(taskID string, destination string) string {
	return backupConfigRestoreDestinationPrefix(taskID) + encodeDynamicSegment(destination)
}

func encodeBackupConfigRestoreEntryRecord(record BackupConfigRestoreEntryRecord) ([]byte, error) {
	if err := validateBackupConfigRestoreEntryRecord(record); err != nil {
		return nil, err
	}
	return encodeBoundedBackupConfigEnvelope("backup-config-restore-entry", record)
}

func decodeBackupConfigRestoreEntryRecord(value []byte) (BackupConfigRestoreEntryRecord, error) {
	record, err := decodeBoundedBackupConfigEnvelope[BackupConfigRestoreEntryRecord](
		value,
		"backup-config-restore-entry",
	)
	if err != nil {
		return BackupConfigRestoreEntryRecord{}, err
	}
	if err := validateBackupConfigRestoreEntryRecord(record); err != nil {
		return BackupConfigRestoreEntryRecord{}, corruptBackupConfigRecord()
	}
	return record, nil
}

func encodeBackupConfigRestoreDescriptorChunkRecord(
	record BackupConfigRestoreDescriptorChunkRecord,
) ([]byte, error) {
	if err := validateBackupConfigRestoreDescriptorChunkRecord(record); err != nil {
		return nil, err
	}
	return encodeBoundedBackupConfigEnvelope("backup-config-restore-descriptor-chunk", record)
}

func decodeBackupConfigRestoreDescriptorChunkRecord(
	value []byte,
) (BackupConfigRestoreDescriptorChunkRecord, error) {
	record, err := decodeBoundedBackupConfigEnvelope[BackupConfigRestoreDescriptorChunkRecord](
		value,
		"backup-config-restore-descriptor-chunk",
	)
	if err != nil {
		return BackupConfigRestoreDescriptorChunkRecord{}, err
	}
	if err := validateBackupConfigRestoreDescriptorChunkRecord(record); err != nil {
		return BackupConfigRestoreDescriptorChunkRecord{}, corruptBackupConfigRecord()
	}
	return record, nil
}

func encodeBackupConfigRestoreValueChunkRecord(record BackupConfigRestoreValueChunkRecord) ([]byte, error) {
	if err := validateBackupConfigRestoreValueChunkRecord(record); err != nil {
		return nil, err
	}
	return encodeBoundedBackupConfigEnvelope("backup-config-restore-value-chunk", record)
}

func decodeBackupConfigRestoreValueChunkRecord(value []byte) (BackupConfigRestoreValueChunkRecord, error) {
	record, err := decodeBoundedBackupConfigEnvelope[BackupConfigRestoreValueChunkRecord](
		value,
		"backup-config-restore-value-chunk",
	)
	if err != nil {
		return BackupConfigRestoreValueChunkRecord{}, err
	}
	if err := validateBackupConfigRestoreValueChunkRecord(record); err != nil {
		clearBackupConfigProtectedChunk(record.Protected)
		return BackupConfigRestoreValueChunkRecord{}, corruptBackupConfigRecord()
	}
	return record, nil
}

func validateBackupConfigRestoreEntryRecord(record BackupConfigRestoreEntryRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindEnvEntry, record.EntryID) != nil ||
		validateStableID(ids.KindConfig, record.ValueGenerationID) != nil {
		return errs.New(errs.KindValidationFailed, "backup config restore Entry identity is invalid")
	}
	return validateBackupConfigEntryChunkSummary(
		record.Secret,
		record.DescriptorLength,
		record.DescriptorSHA256,
		record.DescriptorChunks,
		record.PlainValueLength,
		record.PlainValueSHA256,
		record.ValueChunks,
	)
}

func validateBackupConfigRestoreDescriptorChunkRecord(record BackupConfigRestoreDescriptorChunkRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindEnvEntry, record.EntryID) != nil ||
		validateStableID(ids.KindConfig, record.ValueGenerationID) != nil {
		return errs.New(errs.KindValidationFailed, "backup config restore descriptor identity is invalid")
	}
	return validateBackupConfigDescriptorChunk(
		record.ChunkOrdinal,
		record.Offset,
		record.Length,
		record.SHA256,
		record.Content,
	)
}

func validateBackupConfigRestoreValueChunkRecord(record BackupConfigRestoreValueChunkRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindEnvEntry, record.EntryID) != nil ||
		validateStableID(ids.KindConfig, record.ValueGenerationID) != nil {
		return errs.New(errs.KindValidationFailed, "backup config restore value identity is invalid")
	}
	return validateBackupConfigValueChunkShape(
		record.Secret,
		record.ChunkOrdinal,
		record.Storage,
		record.Plain,
		record.Protected,
	)
}
