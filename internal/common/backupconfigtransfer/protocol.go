// Package backupconfigtransfer owns the schema-one Config transfer machine
// primitives shared by the Controller and Agent. It does not persist credits,
// accept records, or grant filesystem/object authority.
package backupconfigtransfer

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

const (
	MetadataCreditRecords uint32 = 46
	MetadataCreditBytes   uint64 = 393216
	ValueCreditRecords    uint32 = 8
	ValueCreditBytes      uint64 = 262144

	creditDigestDomain = "groundplane.backup.config-credit.schema-one.v1\x00"
	chainSeedDomain    = "groundplane.backup.config-transfer-chain-seed.schema-one.v1\x00"
	frameChainDomain   = "groundplane.backup.config-transfer-frame.schema-one.v1\x00"
)

// Binding is the complete immutable identity of one directional transfer.
type Binding struct {
	TaskID       string                        `json:"task_id"`
	AssignmentID string                        `json:"assignment_id"`
	StepID       string                        `json:"step_id"`
	ExecutionID  string                        `json:"execution_id"`
	TransferID   string                        `json:"transfer_id"`
	Direction    agentpb.BackupConfigDirection `json:"direction"`
}

// ContentSHA256 binds staging to the complete sealed Config content authority.
func ContentSHA256(content *agentpb.BackupConfigContentAuthority) ([]byte, error) {
	if !validContent(content) || rejectUnknown(content) != nil {
		return nil, invalid("Config content authority is invalid")
	}
	return deterministicDigest("groundplane.backup.config-content.schema-one.v1\x00", content)
}

func (binding Binding) Validate() error {
	if ids.Validate(ids.KindTask, binding.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, binding.AssignmentID) != nil ||
		ids.Validate(ids.KindStep, binding.StepID) != nil ||
		!validRawULID(binding.ExecutionID) || !validRawULID(binding.TransferID) ||
		!validDirection(binding.Direction) {
		return invalid("transfer binding is invalid")
	}
	return nil
}

// InitialChainSHA256 returns the transfer-bound chain seed used before record
// sequence one. It is a digest, not proof that any record was accepted.
func InitialChainSHA256(binding Binding) ([]byte, error) {
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	seed := &agentpb.BackupConfigCredit{
		TaskId: binding.TaskID, AssignmentId: binding.AssignmentID,
		StepId: binding.StepID, ExecutionId: binding.ExecutionID,
		TransferId: binding.TransferID, Direction: binding.Direction,
	}
	return deterministicDigest(chainSeedDomain, seed)
}

// ValidateFrame returns an owned, unknown-field-free frame bound to binding.
func ValidateFrame(binding Binding, frame *agentpb.BackupConfigTransfer) (*agentpb.BackupConfigTransfer, error) {
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	if frame == nil {
		return nil, invalid("transfer frame is required")
	}
	owned := proto.Clone(frame).(*agentpb.BackupConfigTransfer)
	if err := rejectUnknown(owned); err != nil || !frameMatches(binding, owned) || owned.RecordSequence == 0 {
		ClearFrame(owned)
		return nil, invalid("transfer frame identity or shape is invalid")
	}
	if err := validateFrameRecord(binding.Direction, owned); err != nil {
		ClearFrame(owned)
		return nil, err
	}
	return owned, nil
}

// FrameChainSHA256 advances a transfer chain with the deterministic protobuf
// representation of exactly one validated frame.
func FrameChainSHA256(binding Binding, preceding []byte, frame *agentpb.BackupConfigTransfer) ([]byte, error) {
	if len(preceding) != sha256.Size {
		return nil, invalid("preceding transfer chain digest is invalid")
	}
	owned, err := ValidateFrame(binding, frame)
	if err != nil {
		return nil, err
	}
	defer ClearFrame(owned)
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(owned)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	hash := sha256.New()
	_, _ = hash.Write([]byte(frameChainDomain))
	_, _ = hash.Write(preceding)
	_, _ = hash.Write(encoded)
	return hash.Sum(nil), nil
}

// SealCredit validates an unhashed credit and returns an owned sealed copy.
// The acknowledgement digest authenticates every field except ack_sha256.
func SealCredit(binding Binding, credit *agentpb.BackupConfigCredit) (*agentpb.BackupConfigCredit, error) {
	if credit == nil || len(credit.AckSha256) != 0 {
		return nil, invalid("credit must be unhashed before sealing")
	}
	owned := proto.Clone(credit).(*agentpb.BackupConfigCredit)
	if err := validateCreditShape(binding, owned); err != nil {
		return nil, err
	}
	digest, err := creditDigest(owned)
	if err != nil {
		return nil, err
	}
	owned.AckSha256 = digest
	return owned, nil
}

// ValidateCredit verifies a sealed credit and returns an owned copy.
func ValidateCredit(binding Binding, credit *agentpb.BackupConfigCredit) (*agentpb.BackupConfigCredit, error) {
	if credit == nil || len(credit.AckSha256) != sha256.Size {
		return nil, invalid("sealed credit acknowledgement digest is invalid")
	}
	owned := proto.Clone(credit).(*agentpb.BackupConfigCredit)
	if err := validateCreditShape(binding, owned); err != nil {
		return nil, err
	}
	want, err := creditDigest(owned)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare(owned.AckSha256, want) != 1 {
		return nil, invalid("credit acknowledgement digest does not match its contents")
	}
	return owned, nil
}

func validateCreditShape(binding Binding, credit *agentpb.BackupConfigCredit) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if err := rejectUnknown(credit); err != nil || !creditMatches(binding, credit) ||
		credit.CreditSequence == 0 || credit.NextOrdinal == 0 ||
		credit.NextOrdinal > backupconfig.MaxEntries+1 || len(credit.CumulativeChainSha256) != sha256.Size {
		return invalid("credit identity or committed cursor is invalid")
	}
	if credit.CommittedRecordSequence == 0 {
		seed, err := InitialChainSHA256(binding)
		if err != nil || credit.CreditSequence != 1 || credit.NextOrdinal != 1 ||
			!sameDigest(credit.CumulativeChainSha256, seed) {
			return invalid("initial credit cursor is invalid")
		}
	}
	switch grant := credit.Credit.(type) {
	case *agentpb.BackupConfigCredit_MetadataCredit:
		if grant == nil || grant.MetadataCredit == nil ||
			credit.NextOrdinal != 1 ||
			grant.MetadataCredit.RecordCredit != MetadataCreditRecords ||
			grant.MetadataCredit.ByteCredit != MetadataCreditBytes {
			return invalid("metadata credit grant is invalid")
		}
	case *agentpb.BackupConfigCredit_MetadataAccepted:
		if grant == nil || grant.MetadataAccepted == nil ||
			credit.CommittedRecordSequence == 0 || credit.NextOrdinal != 1 ||
			len(grant.MetadataAccepted.MetadataTranscriptSha256) != sha256.Size ||
			grant.MetadataAccepted.InitialValueCreditBytes != ValueCreditBytes ||
			grant.MetadataAccepted.InitialValueRecordCredit != ValueCreditRecords {
			return invalid("metadata acceptance credit is invalid")
		}
	case *agentpb.BackupConfigCredit_ValueCredit:
		if grant == nil || grant.ValueCredit == nil || credit.CommittedRecordSequence == 0 ||
			grant.ValueCredit.RecordCredit == 0 || grant.ValueCredit.RecordCredit > ValueCreditRecords ||
			grant.ValueCredit.ValueCreditBytes > ValueCreditBytes {
			return invalid("value credit grant is invalid")
		}
	default:
		return invalid("credit grant kind is invalid")
	}
	return nil
}

func creditDigest(credit *agentpb.BackupConfigCredit) ([]byte, error) {
	hashInput := proto.Clone(credit).(*agentpb.BackupConfigCredit)
	hashInput.AckSha256 = nil
	return deterministicDigest(creditDigestDomain, hashInput)
}

func deterministicDigest(domain string, message proto.Message) ([]byte, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	_, _ = hash.Write(encoded)
	return hash.Sum(nil), nil
}

func validateFrameRecord(direction agentpb.BackupConfigDirection, frame *agentpb.BackupConfigTransfer) error {
	switch record := frame.Record.(type) {
	case *agentpb.BackupConfigTransfer_Start:
		if record == nil || record.Start == nil || record.Start.Direction != direction ||
			!validContent(record.Start.Content) {
			return invalid("config transfer start is invalid")
		}
	case *agentpb.BackupConfigTransfer_EntryHeader:
		if record == nil || record.EntryHeader == nil {
			return invalid("config Entry header is invalid")
		}
		entry, err := entryFromHeader(direction, record.EntryHeader)
		if err != nil {
			return err
		}
		chunks := entry.Value.SizeBytes / backupconfig.TransferChunkBytes
		if entry.Value.SizeBytes%backupconfig.TransferChunkBytes != 0 {
			chunks++
		}
		if uint64(record.EntryHeader.ChunkCount) != chunks || proto.Size(frame) > backupconfig.MaxEntryHeaderEnvelopeBytes {
			return invalid("config Entry header chunk count or encoded size is invalid")
		}
	case *agentpb.BackupConfigTransfer_ValueChunk:
		if record == nil || record.ValueChunk == nil || record.ValueChunk.Ordinal == 0 ||
			record.ValueChunk.Ordinal > backupconfig.MaxEntries || len(record.ValueChunk.Content) == 0 ||
			len(record.ValueChunk.Content) > backupconfig.TransferChunkBytes ||
			record.ValueChunk.Offset > backupconfig.MaxSelectedValueBytes-uint64(len(record.ValueChunk.Content)) {
			return invalid("config value chunk is invalid")
		}
	case *agentpb.BackupConfigTransfer_EntryEnd:
		if record == nil || record.EntryEnd == nil || record.EntryEnd.Ordinal == 0 ||
			record.EntryEnd.Ordinal > backupconfig.MaxEntries ||
			record.EntryEnd.ValueSizeBytes > backupconfig.MaxSelectedValueBytes ||
			len(record.EntryEnd.ValueSha256) != sha256.Size {
			return invalid("config Entry end is invalid")
		}
	case *agentpb.BackupConfigTransfer_End:
		if record == nil || record.End == nil || record.End.Direction != direction ||
			!validContent(record.End.Content) || len(record.End.TranscriptSha256) != sha256.Size {
			return invalid("config transfer end is invalid")
		}
	case *agentpb.BackupConfigTransfer_Resume:
		if record == nil || record.Resume == nil || record.Resume.NextOrdinal == 0 ||
			record.Resume.NextOrdinal > backupconfig.MaxEntries+1 ||
			len(record.Resume.ValueChainSha256) != sha256.Size {
			return invalid("config transfer resume is invalid")
		}
	default:
		return invalid("config transfer record kind is invalid")
	}
	return nil
}

func validContent(value *agentpb.BackupConfigContentAuthority) bool {
	return value != nil && len(value.ManifestSha256) == sha256.Size &&
		len(value.MetadataSnapshotSha256) == sha256.Size && value.EntryCount <= backupconfig.MaxEntries &&
		value.TotalSelectedValueBytes <= backupconfig.MaxTotalSelectedValueBytes && value.ManifestSizeBytes > 0 &&
		value.ManifestSizeBytes <= backupconfig.MaxManifestBytes && value.SourceSizeBytes > 0 &&
		value.SourceSizeBytes <= backupconfig.MaxSourceBytes && value.SourceSizeBytes%backupconfig.TarBlockBytes == 0
}

func frameMatches(binding Binding, frame *agentpb.BackupConfigTransfer) bool {
	return frame.TaskId == binding.TaskID && frame.AssignmentId == binding.AssignmentID &&
		frame.StepId == binding.StepID && frame.ExecutionId == binding.ExecutionID && frame.TransferId == binding.TransferID
}

func creditMatches(binding Binding, credit *agentpb.BackupConfigCredit) bool {
	return credit.TaskId == binding.TaskID && credit.AssignmentId == binding.AssignmentID &&
		credit.StepId == binding.StepID && credit.ExecutionId == binding.ExecutionID &&
		credit.TransferId == binding.TransferID && credit.Direction == binding.Direction
}

func validDirection(value agentpb.BackupConfigDirection) bool {
	return value == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE ||
		value == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE
}

func validRawULID(value string) bool {
	if len(value) != 26 {
		return false
	}
	parsed, err := ulid.ParseStrict(value)
	return err == nil && parsed.String() == value
}

func rejectUnknown(message proto.Message) error {
	return executionplan.RejectUnknown(message)
}

// ClearFrame releases selected-value plaintext in a caller-owned validated
// frame. Other frame variants contain authority and metadata only.
func ClearFrame(frame *agentpb.BackupConfigTransfer) {
	if frame == nil || frame.GetValueChunk() == nil {
		return
	}
	clear(frame.GetValueChunk().Content)
	frame.GetValueChunk().Content = nil
}

func sameDigest(left, right []byte) bool {
	return len(left) == sha256.Size && len(right) == sha256.Size && bytes.Equal(left, right)
}

func invalid(message string) error { return errs.New(errs.KindValidationFailed, message) }
