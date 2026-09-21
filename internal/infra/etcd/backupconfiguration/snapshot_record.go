package backupconfiguration

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumBackupConfigChunkPayloadBytes             = 48 << 10
	MaximumBackupConfigProtectedChunkCiphertextBytes = 64 << 10
	MaximumBackupConfigChunksPerTransaction          = 12
	MaximumBackupConfigBatchMutationBytes            = 768 << 10

	maximumBackupConfigDurableRecordBytes = 256 << 10
	backupConfigEntryOrdinalWidth         = 20
	backupConfigChunkOrdinalWidth         = 10

	backupConfigSnapshotPrefix           = "/v1/runtime/backup-config-snapshots/"
	backupConfigSnapshotEntryRootPrefix  = "/v1/runtime/backup-config-snapshot-entries/"
	backupConfigSnapshotDescriptorPrefix = "/v1/runtime/backup-config-snapshot-descriptors/"
	backupConfigSnapshotValuePrefix      = "/v1/runtime/backup-config-snapshot-values/"
	backupConfigSnapshotByTaskPrefix     = "/v1/indexes/backup-config-snapshots/by-task/"
	backupConfigSnapshotBySnapshotPrefix = "/v1/indexes/backup-config-snapshots/by-snapshot/"
)

type BackupConfigSnapshotState string

const (
	BackupConfigSnapshotUninitialized BackupConfigSnapshotState = "uninitialized"
	BackupConfigSnapshotBuilding      BackupConfigSnapshotState = "building"
	BackupConfigSnapshotSealed        BackupConfigSnapshotState = "sealed"
)

type BackupConfigChunkStorage string

const (
	BackupConfigChunkStoragePlain               BackupConfigChunkStorage = "plain"
	BackupConfigChunkStorageControllerProtected BackupConfigChunkStorage = "controller_protected"
)

// BackupConfigSnapshotRecord is the bounded, restart-safe cursor for one
// fixed-revision Environment config capture. SnapshotID is the original
// owning Task id; retries reuse the same sealed snapshot.
type BackupConfigSnapshotRecord struct {
	SnapshotID             string                    `json:"snapshot_id"`
	EnvironmentID          string                    `json:"environment_id"`
	SourceID               string                    `json:"source_id"`
	State                  BackupConfigSnapshotState `json:"state"`
	ReadRevision           int64                     `json:"read_revision"`
	NextEntryOrdinal       uint64                    `json:"next_entry_ordinal"`
	EntryCount             uint64                    `json:"entry_count"`
	DescriptorChunkCount   uint64                    `json:"descriptor_chunk_count"`
	ValueChunkCount        uint64                    `json:"value_chunk_count"`
	PlainValueBytes        uint64                    `json:"plain_value_bytes"`
	DescriptorChainSHA256  string                    `json:"descriptor_chain_sha256,omitempty"`
	StoredValueChainSHA256 string                    `json:"stored_value_chain_sha256,omitempty"`
	StoredManifestSHA256   string                    `json:"stored_manifest_sha256,omitempty"`
	CreatedAt              time.Time                 `json:"created_at"`
	UpdatedAt              time.Time                 `json:"updated_at"`
}

// BackupConfigSnapshotEntryRecord binds one finalized Entry to the exact
// metadata and immutable value generation read at Snapshot.ReadRevision.
type BackupConfigSnapshotEntryRecord struct {
	SnapshotID              string `json:"snapshot_id"`
	EntryOrdinal            uint64 `json:"entry_ordinal"`
	EntryID                 string `json:"entry_id"`
	EntryRevision           int64  `json:"entry_revision"`
	ValueGenerationID       string `json:"value_generation_id"`
	ValueGenerationRevision int64  `json:"value_generation_revision"`
	Secret                  bool   `json:"secret"`
	DescriptorLength        uint64 `json:"descriptor_length"`
	DescriptorSHA256        string `json:"descriptor_sha256"`
	DescriptorChunks        uint32 `json:"descriptor_chunks"`
	PlainValueLength        uint64 `json:"plain_value_length"`
	PlainValueSHA256        string `json:"plain_value_sha256,omitempty"`
	ValueChunks             uint32 `json:"value_chunks"`
}

type BackupConfigSnapshotDescriptorChunkRecord struct {
	SnapshotID        string `json:"snapshot_id"`
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

type BackupConfigPlainChunkPayload struct {
	Offset        uint64 `json:"offset"`
	ContentLength uint32 `json:"content_length"`
	ContentSHA256 string `json:"content_sha256"`
	Content       []byte `json:"content"`
}

// BackupConfigProtectedChunkPayload mirrors the existing Controller-key
// envelope. Ciphertext is the only persisted payload for a protected chunk.
type BackupConfigProtectedChunkPayload struct {
	EnvelopeVersion  uint8  `json:"envelope_version"`
	Cipher           string `json:"cipher"`
	DigestAlgorithm  string `json:"digest_algorithm"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
	Ciphertext       []byte `json:"ciphertext"`
}

type BackupConfigSnapshotValueChunkRecord struct {
	SnapshotID        string                             `json:"snapshot_id"`
	EntryOrdinal      uint64                             `json:"entry_ordinal"`
	EntryID           string                             `json:"entry_id"`
	ValueGenerationID string                             `json:"value_generation_id"`
	Secret            bool                               `json:"secret"`
	ChunkOrdinal      uint32                             `json:"chunk_ordinal"`
	Storage           BackupConfigChunkStorage           `json:"storage"`
	Plain             *BackupConfigPlainChunkPayload     `json:"plain,omitempty"`
	Protected         *BackupConfigProtectedChunkPayload `json:"protected,omitempty"`
}

type backupConfigSnapshotData struct {
	SnapshotID             string                    `json:"snapshot_id"`
	EnvironmentID          string                    `json:"environment_id"`
	SourceID               string                    `json:"source_id"`
	State                  BackupConfigSnapshotState `json:"state"`
	ReadRevision           int64                     `json:"read_revision"`
	NextEntryOrdinal       uint64                    `json:"next_entry_ordinal"`
	EntryCount             uint64                    `json:"entry_count"`
	DescriptorChunkCount   uint64                    `json:"descriptor_chunk_count"`
	ValueChunkCount        uint64                    `json:"value_chunk_count"`
	PlainValueBytes        uint64                    `json:"plain_value_bytes"`
	DescriptorChainSHA256  string                    `json:"descriptor_chain_sha256,omitempty"`
	StoredValueChainSHA256 string                    `json:"stored_value_chain_sha256,omitempty"`
	StoredManifestSHA256   string                    `json:"stored_manifest_sha256,omitempty"`
	CreatedAt              string                    `json:"created_at"`
	UpdatedAt              string                    `json:"updated_at"`
}

func BackupConfigSnapshotKey(snapshotID string) string {
	return backupConfigSnapshotPrefix + snapshotID
}

func backupConfigSnapshotEntryPrefix(snapshotID string) string {
	return backupConfigSnapshotEntryRootPrefix + snapshotID + "/"
}

func backupConfigSnapshotEntryKey(snapshotID string, entryOrdinal uint64) string {
	return backupConfigSnapshotEntryPrefix(snapshotID) + backupConfigEntryOrdinal(entryOrdinal)
}

func backupConfigSnapshotDescriptorChunkPrefix(snapshotID string, entryOrdinal uint64) string {
	return backupConfigSnapshotDescriptorPrefix + snapshotID + "/" + backupConfigEntryOrdinal(entryOrdinal) + "/"
}

func backupConfigSnapshotDescriptorChunkKey(snapshotID string, entryOrdinal uint64, chunkOrdinal uint32) string {
	return backupConfigSnapshotDescriptorChunkPrefix(snapshotID, entryOrdinal) +
		backupConfigChunkOrdinal(chunkOrdinal)
}

func backupConfigSnapshotValueChunkPrefix(snapshotID string, entryOrdinal uint64) string {
	return backupConfigSnapshotValuePrefix + snapshotID + "/" + backupConfigEntryOrdinal(entryOrdinal) + "/"
}

func backupConfigSnapshotValueChunkKey(snapshotID string, entryOrdinal uint64, chunkOrdinal uint32) string {
	return backupConfigSnapshotValueChunkPrefix(snapshotID, entryOrdinal) + backupConfigChunkOrdinal(chunkOrdinal)
}

func backupConfigSnapshotTaskReferencePrefix(taskID string) string {
	return backupConfigSnapshotByTaskPrefix + taskID + "/"
}

func BackupConfigSnapshotTaskReferenceKey(taskID string, snapshotID string) string {
	return backupConfigSnapshotTaskReferencePrefix(taskID) + snapshotID
}

func backupConfigSnapshotReferenceTaskPrefix(snapshotID string) string {
	return backupConfigSnapshotBySnapshotPrefix + snapshotID + "/"
}

func BackupConfigSnapshotReferenceTaskKey(snapshotID string, taskID string) string {
	return backupConfigSnapshotReferenceTaskPrefix(snapshotID) + taskID
}

func backupConfigEntryOrdinal(ordinal uint64) string {
	return fmt.Sprintf("%0*d", backupConfigEntryOrdinalWidth, ordinal)
}

func backupConfigChunkOrdinal(ordinal uint32) string {
	return fmt.Sprintf("%0*d", backupConfigChunkOrdinalWidth, ordinal)
}

func EncodeBackupConfigSnapshotRecord(record BackupConfigSnapshotRecord) ([]byte, error) {
	if err := validateBackupConfigSnapshotRecord(record); err != nil {
		return nil, err
	}
	data := backupConfigSnapshotData{
		SnapshotID: record.SnapshotID, EnvironmentID: record.EnvironmentID, SourceID: record.SourceID,
		State: record.State, ReadRevision: record.ReadRevision, NextEntryOrdinal: record.NextEntryOrdinal,
		EntryCount: record.EntryCount, DescriptorChunkCount: record.DescriptorChunkCount,
		ValueChunkCount: record.ValueChunkCount, PlainValueBytes: record.PlainValueBytes,
		DescriptorChainSHA256:  record.DescriptorChainSHA256,
		StoredValueChainSHA256: record.StoredValueChainSHA256,
		StoredManifestSHA256:   record.StoredManifestSHA256,
		CreatedAt:              record.CreatedAt.Format(time.RFC3339Nano),
		UpdatedAt:              record.UpdatedAt.Format(time.RFC3339Nano),
	}
	return encodeBoundedBackupConfigEnvelope("backup-config-snapshot", data)
}

func DecodeBackupConfigSnapshotRecord(value []byte) (BackupConfigSnapshotRecord, error) {
	data, err := decodeBoundedBackupConfigEnvelope[backupConfigSnapshotData](value, "backup-config-snapshot")
	if err != nil {
		return BackupConfigSnapshotRecord{}, err
	}
	createdAt, err := parseBackupConfigTimestamp(data.CreatedAt)
	if err != nil {
		return BackupConfigSnapshotRecord{}, corruptBackupConfigRecord()
	}
	updatedAt, err := parseBackupConfigTimestamp(data.UpdatedAt)
	if err != nil {
		return BackupConfigSnapshotRecord{}, corruptBackupConfigRecord()
	}
	record := BackupConfigSnapshotRecord{
		SnapshotID: data.SnapshotID, EnvironmentID: data.EnvironmentID, SourceID: data.SourceID,
		State: data.State, ReadRevision: data.ReadRevision, NextEntryOrdinal: data.NextEntryOrdinal,
		EntryCount: data.EntryCount, DescriptorChunkCount: data.DescriptorChunkCount,
		ValueChunkCount: data.ValueChunkCount, PlainValueBytes: data.PlainValueBytes,
		DescriptorChainSHA256:  data.DescriptorChainSHA256,
		StoredValueChainSHA256: data.StoredValueChainSHA256,
		StoredManifestSHA256:   data.StoredManifestSHA256, CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	if err := validateBackupConfigSnapshotRecord(record); err != nil {
		return BackupConfigSnapshotRecord{}, corruptBackupConfigRecord()
	}
	return record, nil
}

func encodeBackupConfigSnapshotEntryRecord(record BackupConfigSnapshotEntryRecord) ([]byte, error) {
	if err := validateBackupConfigSnapshotEntryRecord(record); err != nil {
		return nil, err
	}
	return encodeBoundedBackupConfigEnvelope("backup-config-snapshot-entry", record)
}

func decodeBackupConfigSnapshotEntryRecord(value []byte) (BackupConfigSnapshotEntryRecord, error) {
	record, err := decodeBoundedBackupConfigEnvelope[BackupConfigSnapshotEntryRecord](
		value,
		"backup-config-snapshot-entry",
	)
	if err != nil {
		return BackupConfigSnapshotEntryRecord{}, err
	}
	if err := validateBackupConfigSnapshotEntryRecord(record); err != nil {
		return BackupConfigSnapshotEntryRecord{}, corruptBackupConfigRecord()
	}
	return record, nil
}

func encodeBackupConfigSnapshotDescriptorChunkRecord(
	record BackupConfigSnapshotDescriptorChunkRecord,
) ([]byte, error) {
	if err := validateBackupConfigSnapshotDescriptorChunkRecord(record); err != nil {
		return nil, err
	}
	return encodeBoundedBackupConfigEnvelope("backup-config-snapshot-descriptor-chunk", record)
}

func decodeBackupConfigSnapshotDescriptorChunkRecord(
	value []byte,
) (BackupConfigSnapshotDescriptorChunkRecord, error) {
	record, err := decodeBoundedBackupConfigEnvelope[BackupConfigSnapshotDescriptorChunkRecord](
		value,
		"backup-config-snapshot-descriptor-chunk",
	)
	if err != nil {
		return BackupConfigSnapshotDescriptorChunkRecord{}, err
	}
	if err := validateBackupConfigSnapshotDescriptorChunkRecord(record); err != nil {
		return BackupConfigSnapshotDescriptorChunkRecord{}, corruptBackupConfigRecord()
	}
	return record, nil
}

func encodeBackupConfigSnapshotValueChunkRecord(record BackupConfigSnapshotValueChunkRecord) ([]byte, error) {
	if err := validateBackupConfigSnapshotValueChunkRecord(record); err != nil {
		return nil, err
	}
	return encodeBoundedBackupConfigEnvelope("backup-config-snapshot-value-chunk", record)
}

func decodeBackupConfigSnapshotValueChunkRecord(value []byte) (BackupConfigSnapshotValueChunkRecord, error) {
	record, err := decodeBoundedBackupConfigEnvelope[BackupConfigSnapshotValueChunkRecord](
		value,
		"backup-config-snapshot-value-chunk",
	)
	if err != nil {
		return BackupConfigSnapshotValueChunkRecord{}, err
	}
	if err := validateBackupConfigSnapshotValueChunkRecord(record); err != nil {
		clearBackupConfigProtectedChunk(record.Protected)
		return BackupConfigSnapshotValueChunkRecord{}, corruptBackupConfigRecord()
	}
	return record, nil
}

func validateBackupConfigSnapshotRecord(record BackupConfigSnapshotRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.SnapshotID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindBackupSource, record.SourceID) != nil {
		return errs.New(errs.KindValidationFailed, "backup config snapshot identity is invalid")
	}
	if !validBackupConfigTimestamp(record.CreatedAt) || !validBackupConfigTimestamp(record.UpdatedAt) ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "backup config snapshot timestamps are invalid")
	}
	switch record.State {
	case BackupConfigSnapshotUninitialized:
		if record.ReadRevision != 0 || record.NextEntryOrdinal != 0 || record.EntryCount != 0 ||
			record.DescriptorChunkCount != 0 || record.ValueChunkCount != 0 || record.PlainValueBytes != 0 ||
			record.DescriptorChainSHA256 != "" || record.StoredValueChainSHA256 != "" ||
			record.StoredManifestSHA256 != "" {
			return errs.New(errs.KindValidationFailed, "uninitialized backup config snapshot contains progress")
		}
		return nil
	case BackupConfigSnapshotBuilding, BackupConfigSnapshotSealed:
	default:
		return errs.New(errs.KindValidationFailed, "backup config snapshot state is invalid")
	}
	if record.ReadRevision <= 0 || record.NextEntryOrdinal != record.EntryCount ||
		(record.EntryCount > 0 && record.DescriptorChunkCount < record.EntryCount) ||
		(record.PlainValueBytes > 0 && record.ValueChunkCount == 0) ||
		!validOptionalBackupConfigChain(record.DescriptorChunkCount, record.DescriptorChainSHA256) ||
		!validOptionalBackupConfigChain(record.ValueChunkCount, record.StoredValueChainSHA256) {
		return errs.New(errs.KindValidationFailed, "backup config snapshot progress is invalid")
	}
	if record.EntryCount == 0 && (record.DescriptorChunkCount != 0 || record.ValueChunkCount != 0 ||
		record.PlainValueBytes != 0) {
		return errs.New(errs.KindValidationFailed, "empty backup config snapshot contains child progress")
	}
	if record.State == BackupConfigSnapshotBuilding && record.StoredManifestSHA256 != "" {
		return errs.New(errs.KindValidationFailed, "building backup config snapshot has a final manifest digest")
	}
	if record.State == BackupConfigSnapshotSealed && !validBackupConfigSHA256(record.StoredManifestSHA256) {
		return errs.New(errs.KindValidationFailed, "sealed backup config snapshot manifest digest is invalid")
	}
	return nil
}

func validateBackupConfigSnapshotEntryRecord(record BackupConfigSnapshotEntryRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.SnapshotID) != nil ||
		recordcodec.ValidateID(ids.KindEnvEntry, record.EntryID) != nil ||
		recordcodec.ValidateID(ids.KindConfig, record.ValueGenerationID) != nil || record.EntryRevision <= 0 ||
		record.ValueGenerationRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "backup config snapshot Entry identity is invalid")
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

func validateBackupConfigSnapshotDescriptorChunkRecord(
	record BackupConfigSnapshotDescriptorChunkRecord,
) error {
	if recordcodec.ValidateID(ids.KindTask, record.SnapshotID) != nil ||
		recordcodec.ValidateID(ids.KindEnvEntry, record.EntryID) != nil ||
		recordcodec.ValidateID(ids.KindConfig, record.ValueGenerationID) != nil {
		return errs.New(errs.KindValidationFailed, "backup config snapshot descriptor identity is invalid")
	}
	return validateBackupConfigDescriptorChunk(
		record.ChunkOrdinal,
		record.Offset,
		record.Length,
		record.SHA256,
		record.Content,
	)
}

func validateBackupConfigSnapshotValueChunkRecord(record BackupConfigSnapshotValueChunkRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.SnapshotID) != nil ||
		recordcodec.ValidateID(ids.KindEnvEntry, record.EntryID) != nil ||
		recordcodec.ValidateID(ids.KindConfig, record.ValueGenerationID) != nil {
		return errs.New(errs.KindValidationFailed, "backup config snapshot value identity is invalid")
	}
	return validateBackupConfigValueChunkShape(
		record.Secret,
		record.ChunkOrdinal,
		record.Storage,
		record.Plain,
		record.Protected,
	)
}

func validateBackupConfigEntryChunkSummary(
	secret bool,
	descriptorLength uint64,
	descriptorSHA256 string,
	descriptorChunks uint32,
	plainValueLength uint64,
	plainValueSHA256 string,
	valueChunks uint32,
) error {
	invalid := !validBackupConfigChunkSummary(descriptorLength, descriptorChunks, false) ||
		!validBackupConfigSHA256(descriptorSHA256)
	if secret {
		invalid = invalid || plainValueLength != 0 || plainValueSHA256 != "" || valueChunks == 0
	} else {
		invalid = invalid || !validBackupConfigChunkSummary(plainValueLength, valueChunks, true) ||
			!validBackupConfigSHA256(plainValueSHA256)
	}
	if invalid {
		return errs.New(errs.KindValidationFailed, "backup config Entry chunk summary is invalid")
	}
	return nil
}

func validBackupConfigChunkSummary(length uint64, chunks uint32, emptyAllowed bool) bool {
	if length == 0 {
		return emptyAllowed && chunks == 0
	}
	expected := ((length - 1) / MaximumBackupConfigChunkPayloadBytes) + 1
	return expected <= uint64(^uint32(0)) && uint64(chunks) == expected
}

func validateBackupConfigDescriptorChunk(
	chunkOrdinal uint32,
	offset uint64,
	length uint32,
	digest string,
	content []byte,
) error {
	if offset != uint64(chunkOrdinal)*MaximumBackupConfigChunkPayloadBytes || length == 0 ||
		length > MaximumBackupConfigChunkPayloadBytes || int(length) != len(content) ||
		!validBackupConfigSHA256(digest) || !backupConfigDigestMatches(content, digest) {
		return errs.New(errs.KindValidationFailed, "backup config descriptor chunk is invalid")
	}
	return nil
}

func validateBackupConfigValueChunkShape(
	secret bool,
	chunkOrdinal uint32,
	storage BackupConfigChunkStorage,
	plain *BackupConfigPlainChunkPayload,
	protected *BackupConfigProtectedChunkPayload,
) error {
	switch storage {
	case BackupConfigChunkStoragePlain:
		if secret || plain == nil || protected != nil || validateBackupConfigPlainChunk(chunkOrdinal, plain) != nil {
			return errs.New(errs.KindValidationFailed, "plain backup config value chunk shape is invalid")
		}
		return nil
	case BackupConfigChunkStorageControllerProtected:
		if !secret || plain != nil || protected == nil || validateBackupConfigProtectedChunk(protected) != nil {
			return errs.New(errs.KindValidationFailed, "protected backup config value chunk shape is invalid")
		}
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "backup config value chunk storage is invalid")
	}
}

func validateBackupConfigPlainChunk(chunkOrdinal uint32, plain *BackupConfigPlainChunkPayload) error {
	if plain.Offset != uint64(chunkOrdinal)*MaximumBackupConfigChunkPayloadBytes || plain.ContentLength == 0 ||
		plain.ContentLength > MaximumBackupConfigChunkPayloadBytes ||
		int(plain.ContentLength) != len(plain.Content) || !validBackupConfigSHA256(plain.ContentSHA256) ||
		!backupConfigDigestMatches(plain.Content, plain.ContentSHA256) {
		return errs.New(errs.KindValidationFailed, "backup config plain chunk is invalid")
	}
	return nil
}

func validateBackupConfigProtectedChunk(protected *BackupConfigProtectedChunkPayload) error {
	if protected.EnvelopeVersion != 1 || protected.Cipher != "age-x25519" ||
		protected.DigestAlgorithm != "sha256" || len(protected.Ciphertext) == 0 ||
		len(protected.Ciphertext) > MaximumBackupConfigProtectedChunkCiphertextBytes ||
		!validBackupConfigSHA256(protected.CiphertextSHA256) ||
		!backupConfigDigestMatches(protected.Ciphertext, protected.CiphertextSHA256) {
		return errs.New(errs.KindValidationFailed, "backup config protected chunk envelope is invalid")
	}
	return nil
}

func validOptionalBackupConfigChain(count uint64, digest string) bool {
	if count == 0 {
		return digest == ""
	}
	return validBackupConfigSHA256(digest)
}

func validBackupConfigSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func backupConfigDigestMatches(content []byte, encodedDigest string) bool {
	want, err := hex.DecodeString(encodedDigest)
	if err != nil || len(want) != sha256.Size {
		return false
	}
	digest := sha256.Sum256(content)
	return subtle.ConstantTimeCompare(digest[:], want) == 1
}

func validBackupConfigTimestamp(value time.Time) bool {
	_, offset := value.Zone()
	return !value.IsZero() && offset == 0
}

func parseBackupConfigTimestamp(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || value != parsed.UTC().Format(time.RFC3339Nano) {
		return time.Time{}, errs.New(errs.KindInternal, "backup config timestamp is invalid")
	}
	return parsed, nil
}

func encodeBoundedBackupConfigEnvelope[T any](kind string, record T) ([]byte, error) {
	encoded, err := recordcodec.Encode(kind, record)
	if err != nil {
		return nil, err
	}
	if len(encoded) > maximumBackupConfigDurableRecordBytes {
		clear(encoded)
		return nil, errs.New(errs.KindValidationFailed, "backup config durable record exceeds its size bound")
	}
	return encoded, nil
}

func decodeBoundedBackupConfigEnvelope[T any](value []byte, kind string) (T, error) {
	var zero T
	if len(value) == 0 || len(value) > maximumBackupConfigDurableRecordBytes {
		return zero, corruptBackupConfigRecord()
	}
	return recordcodec.Decode[T](value, kind)
}

func clearBackupConfigProtectedChunk(protected *BackupConfigProtectedChunkPayload) {
	if protected != nil {
		clear(protected.Ciphertext)
	}
}

func corruptBackupConfigRecord() error {
	return errs.New(errs.KindInternal, "backup config durable record is corrupt")
}
