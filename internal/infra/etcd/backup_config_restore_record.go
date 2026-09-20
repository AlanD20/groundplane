package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
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
// Attempt ownership lives on the bounded restore authority; staged children
// retain only the stable restore generation and never require retry rewrites.
type BackupConfigRestoreEntryRecord struct {
	RestoreGenerationID string `json:"restore_generation_id"`
	EntryOrdinal        uint64 `json:"entry_ordinal"`
	EntryID             string `json:"entry_id"`
	ValueGenerationID   string `json:"value_generation_id"`
	Secret              bool   `json:"secret"`
	DescriptorLength    uint64 `json:"descriptor_length"`
	DescriptorSHA256    string `json:"descriptor_sha256"`
	DescriptorChunks    uint32 `json:"descriptor_chunks"`
	PlainValueLength    uint64 `json:"plain_value_length"`
	PlainValueSHA256    string `json:"plain_value_sha256,omitempty"`
	ValueChunks         uint32 `json:"value_chunks"`
}

type BackupConfigRestoreDescriptorChunkRecord struct {
	RestoreGenerationID string `json:"restore_generation_id"`
	EntryOrdinal        uint64 `json:"entry_ordinal"`
	EntryID             string `json:"entry_id"`
	ValueGenerationID   string `json:"value_generation_id"`
	Secret              bool   `json:"secret"`
	ChunkOrdinal        uint32 `json:"chunk_ordinal"`
	Offset              uint64 `json:"offset"`
	Length              uint32 `json:"length"`
	SHA256              string `json:"sha256"`
	Content             []byte `json:"content"`
}

type BackupConfigRestoreValueChunkRecord struct {
	RestoreGenerationID string                             `json:"restore_generation_id"`
	EntryOrdinal        uint64                             `json:"entry_ordinal"`
	EntryID             string                             `json:"entry_id"`
	ValueGenerationID   string                             `json:"value_generation_id"`
	Secret              bool                               `json:"secret"`
	ChunkOrdinal        uint32                             `json:"chunk_ordinal"`
	Storage             BackupConfigChunkStorage           `json:"storage"`
	Plain               *BackupConfigPlainChunkPayload     `json:"plain,omitempty"`
	Protected           *BackupConfigProtectedChunkPayload `json:"protected,omitempty"`
}

func backupConfigRestoreEntryPrefix(restoreGenerationID string) (string, error) {
	if err := validateBackupConfigRestoreGenerationID(restoreGenerationID); err != nil {
		return "", err
	}
	return backupConfigRestoreEntryRootPrefix + restoreGenerationID + "/", nil
}

func backupConfigRestoreEntryKey(restoreGenerationID string, entryOrdinal uint64) (string, error) {
	prefix, err := backupConfigRestoreEntryPrefix(restoreGenerationID)
	if err != nil {
		return "", err
	}
	return prefix + backupConfigEntryOrdinal(entryOrdinal), nil
}

func backupConfigRestoreDescriptorChunkPrefix(
	restoreGenerationID string,
	entryOrdinal uint64,
) (string, error) {
	if err := validateBackupConfigRestoreGenerationID(restoreGenerationID); err != nil {
		return "", err
	}
	return backupConfigRestoreDescriptorPrefix + restoreGenerationID + "/" +
		backupConfigEntryOrdinal(entryOrdinal) + "/", nil
}

func backupConfigRestoreDescriptorChunkKey(
	restoreGenerationID string,
	entryOrdinal uint64,
	chunkOrdinal uint32,
) (string, error) {
	prefix, err := backupConfigRestoreDescriptorChunkPrefix(restoreGenerationID, entryOrdinal)
	if err != nil {
		return "", err
	}
	return prefix + backupConfigChunkOrdinal(chunkOrdinal), nil
}

func backupConfigRestoreValueChunkPrefix(
	restoreGenerationID string,
	entryOrdinal uint64,
) (string, error) {
	if err := validateBackupConfigRestoreGenerationID(restoreGenerationID); err != nil {
		return "", err
	}
	return backupConfigRestoreValuePrefix + restoreGenerationID + "/" +
		backupConfigEntryOrdinal(entryOrdinal) + "/", nil
}

func backupConfigRestoreValueChunkKey(
	restoreGenerationID string,
	entryOrdinal uint64,
	chunkOrdinal uint32,
) (string, error) {
	prefix, err := backupConfigRestoreValueChunkPrefix(restoreGenerationID, entryOrdinal)
	if err != nil {
		return "", err
	}
	return prefix + backupConfigChunkOrdinal(chunkOrdinal), nil
}

func backupConfigRestoreEntryIdentityPrefix(restoreGenerationID string) (string, error) {
	if err := validateBackupConfigRestoreGenerationID(restoreGenerationID); err != nil {
		return "", err
	}
	return backupConfigRestoreIdentityPrefix + restoreGenerationID + "/id/", nil
}

func backupConfigRestoreEntryIdentityKey(restoreGenerationID string, entryID string) (string, error) {
	prefix, err := backupConfigRestoreEntryIdentityPrefix(restoreGenerationID)
	if err != nil {
		return "", err
	}
	if recordcodec.ValidateID(ids.KindEnvEntry, entryID) != nil {
		return "", errs.New(errs.KindValidationFailed, "backup config restore Entry id is invalid")
	}
	return prefix + entryID, nil
}

func backupConfigRestoreDestinationPrefix(restoreGenerationID string) (string, error) {
	if err := validateBackupConfigRestoreGenerationID(restoreGenerationID); err != nil {
		return "", err
	}
	return backupConfigRestoreIdentityPrefix + restoreGenerationID + "/destination/", nil
}

func backupConfigRestoreDestinationKey(restoreGenerationID string, destination string) (string, error) {
	prefix, err := backupConfigRestoreDestinationPrefix(restoreGenerationID)
	if err != nil {
		return "", err
	}
	return prefix + recordcodec.EncodeKeySegment(destination), nil
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
	if validateBackupConfigRestoreEntryIdentity(
		record.RestoreGenerationID,
		record.EntryID,
		record.ValueGenerationID,
	) != nil {
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
	if validateBackupConfigRestoreEntryIdentity(
		record.RestoreGenerationID,
		record.EntryID,
		record.ValueGenerationID,
	) != nil {
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
	if validateBackupConfigRestoreEntryIdentity(
		record.RestoreGenerationID,
		record.EntryID,
		record.ValueGenerationID,
	) != nil {
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

func validateBackupConfigRestoreGenerationID(restoreGenerationID string) error {
	if recordcodec.ValidateID(ids.KindConfig, restoreGenerationID) != nil {
		return errs.New(errs.KindValidationFailed, "backup config restore generation id is invalid")
	}
	return nil
}

func validateBackupConfigRestoreBatchIdentities(
	restoreGenerationID string,
	records []BackupConfigRestoreEntryRecord,
	destinations []string,
) error {
	if validateBackupConfigRestoreGenerationID(restoreGenerationID) != nil || len(records) != len(destinations) {
		return errs.New(errs.KindValidationFailed, "backup config restore batch identity is invalid")
	}
	ordinals := make(map[uint64]struct{}, len(records))
	entryKeys := make(map[string]struct{}, len(records))
	destinationKeys := make(map[string]struct{}, len(records))
	valueGenerationIDs := make(map[string]struct{}, len(records))
	for index, record := range records {
		if record.RestoreGenerationID != restoreGenerationID ||
			validateBackupConfigRestoreEntryIdentity(
				record.RestoreGenerationID,
				record.EntryID,
				record.ValueGenerationID,
			) != nil {
			return errs.New(errs.KindValidationFailed, "backup config restore batch identity is invalid")
		}
		entryKey, err := backupConfigRestoreEntryIdentityKey(restoreGenerationID, record.EntryID)
		if err != nil {
			return errs.New(errs.KindValidationFailed, "backup config restore batch identity is invalid")
		}
		destinationKey, err := backupConfigRestoreDestinationKey(restoreGenerationID, destinations[index])
		if err != nil {
			return errs.New(errs.KindValidationFailed, "backup config restore batch identity is invalid")
		}
		if _, exists := ordinals[record.EntryOrdinal]; exists {
			return errs.New(errs.KindValidationFailed, "backup config restore Entry ordinal is duplicated")
		}
		if _, exists := entryKeys[entryKey]; exists {
			return errs.New(errs.KindValidationFailed, "backup config restore Entry id is duplicated")
		}
		if _, exists := destinationKeys[destinationKey]; exists {
			return errs.New(errs.KindValidationFailed, "backup config restore destination is duplicated")
		}
		if _, exists := valueGenerationIDs[record.ValueGenerationID]; exists {
			return errs.New(errs.KindValidationFailed, "backup config restore value generation is duplicated")
		}
		ordinals[record.EntryOrdinal] = struct{}{}
		entryKeys[entryKey] = struct{}{}
		destinationKeys[destinationKey] = struct{}{}
		valueGenerationIDs[record.ValueGenerationID] = struct{}{}
	}
	return nil
}

func validateBackupConfigRestoreEntryIdentity(
	restoreGenerationID string,
	entryID string,
	valueGenerationID string,
) error {
	if validateBackupConfigRestoreGenerationID(restoreGenerationID) != nil ||
		recordcodec.ValidateID(ids.KindEnvEntry, entryID) != nil ||
		recordcodec.ValidateID(ids.KindConfig, valueGenerationID) != nil ||
		restoreGenerationID == valueGenerationID {
		return errs.New(errs.KindValidationFailed, "backup config restore Entry identity is invalid")
	}
	return nil
}
