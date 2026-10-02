package agentconfigjournal

import (
	"bytes"
	"context"
	"errors"
	"io/fs"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const cleanupReceiptName = "cleanup.bin"
const cleanupReceiptNextName = "cleanup.next"

// The caller supplies the native cleanup/materialization projection only after its
// exact checkpoint Ack, or from a validated resumed assignment. Its receipt
// survives partial journal deletion; it does not authorize any source mutation.
func (journal *Journal) recordCleanupReceipt(ctx context.Context, completion *agentpb.BackupStepResume) error {
	if err := journal.validateCleanupReceipt(completion); err != nil {
		return err
	}
	if !journal.retired {
		sequence := completion.GetCapture().GetCursor().GetConfigRecordSequence()
		if completion.GetRestore() != nil {
			sequence = completion.GetRestore().GetCursor().GetConfigRecordSequence()
		}
		if !journal.transfer.complete || sequence != journal.lastCreditCommitted || sequence != journal.recordCount {
			return conflict("Config cleanup receipt differs from its durable transfer")
		}
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(completion)
	if err != nil || len(raw) > maximumAuthorityLen {
		return invalidRecord()
	}
	stored, found, err := journal.readCleanupReceipt(ctx)
	if err != nil {
		return err
	}
	if found && !proto.Equal(stored, completion) {
		return conflict("Config cleanup receipt differs from its native projection")
	}
	if err := journal.writeImmutable(ctx, cleanupReceiptName, cleanupReceiptNextName, raw, maximumAuthorityLen); err != nil {
		return err
	}
	journal.retired = true
	return nil
}

func (journal *Journal) readCleanupReceipt(ctx context.Context) (*agentpb.BackupStepResume, bool, error) {
	raw, err := journal.readRawLimit(ctx, cleanupReceiptName, maximumAuthorityLen)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	completion := &agentpb.BackupStepResume{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(raw, completion); err != nil {
		return nil, false, invalidRecord()
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(completion)
	if err != nil || !bytes.Equal(raw, canonical) || journal.validateCleanupReceipt(completion) != nil {
		return nil, false, invalidRecord()
	}
	return completion, true, nil
}

func (journal *Journal) validateCleanupReceipt(completion *agentpb.BackupStepResume) error {
	if completion == nil || executionplan.RejectUnknown(completion) != nil ||
		completion.StepId != journal.binding.StepID ||
		completion.ExecutionId != journal.binding.ExecutionID {
		return invalidRecord()
	}
	switch journal.binding.Direction {
	case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE:
		return journal.validateCaptureCleanup(completion.GetCapture())
	case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE:
		return journal.validateRestoreCleanup(completion.GetRestore())
	default:
		return invalidRecord()
	}
}

func (journal *Journal) validateCaptureCleanup(completion *agentpb.BackupCaptureResume) error {
	if completion == nil || completion.CheckpointSequence == 0 ||
		completion.Phase != agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_SERVICE_RECOVERY ||
		completion.PrecedingCheckpoint.GetDedupeKeyModRevision() <= 0 || !bytes.Equal(completion.PrecedingCheckpoint.GetAuthorityDigest(), journal.step.StepDigest) ||
		completion.Cursor == nil || completion.Cursor.ObjectAttempt != 1 || completion.Cursor.ConfigRecordSequence == 0 ||
		completion.Cursor.ConfigValueOrdinal != journal.expected.EntryCount+1 || len(completion.Cursor.CumulativeChainSha256) != 32 {
		return invalidRecord()
	}
	cleanup := completion.GetSourceCleanupCompleted()
	if cleanup == nil || ids.Validate(ids.KindRecoveryPoint, cleanup.PointId) != nil ||
		cleanup.PointId != journal.step.GetCapture().PointId ||
		cleanup.Evidence == nil ||
		cleanup.Evidence.SourceSizeBytes != journal.expected.SourceSizeBytes ||
		len(cleanup.Evidence.SourceSha256) != 32 ||
		len(cleanup.Evidence.StoredSha256) != 32 {
		return invalidRecord()
	}
	expected, err := backupconfig.AgeStoredSize(cleanup.Evidence.SourceSizeBytes)
	if err != nil || cleanup.Evidence.StoredSizeBytes != expected {
		return invalidRecord()
	}
	return nil
}

// Restore retires the local transcript only after the exact generation's native
// materialization proof. The caller must remove its physical source stage first;
// this receipt cannot authorize live mutation or another download.
func (journal *Journal) validateRestoreCleanup(completion *agentpb.BackupRestoreResume) error {
	config := journal.step.GetRestore().GetConfig()
	if config == nil || completion == nil || completion.CheckpointSequence == 0 ||
		completion.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY ||
		completion.PrecedingCheckpoint.GetDedupeKeyModRevision() <= 0 ||
		!bytes.Equal(completion.PrecedingCheckpoint.GetAuthorityDigest(), journal.step.StepDigest) {
		return invalidRecord()
	}
	cursor, progress := completion.Cursor, completion.GetConfigProgress()
	if cursor == nil || cursor.ObjectAttempt != 1 || cursor.VolumeCursor != 0 || cursor.ServiceCursor != 0 ||
		cursor.ConfigRecordSequence < uint64(
			journal.expected.EntryCount,
		)+2 || cursor.ConfigValueOrdinal != journal.expected.EntryCount+1 ||
		progress == nil || progress.MetadataAccepted == nil || !progress.GetMetadataAccepted() ||
		len(progress.MetadataTranscriptSha256) != 32 || len(cursor.CumulativeChainSha256) != 32 ||
		progress.NextValueOrdinal != cursor.ConfigValueOrdinal || !bytes.Equal(progress.ValueChainSha256, cursor.CumulativeChainSha256) {
		return invalidRecord()
	}
	maximum, err := backupconfigtransfer.MaximumTransferRecords(journal.expected)
	if err != nil || cursor.ConfigRecordSequence > maximum {
		return invalidRecord()
	}
	proof := progress.GetMaterialization()
	cleanup := progress.SourceCleanupCompleted
	if proof == nil || proof.RestoreGenerationId != config.RestoreGenerationId ||
		proof.MaterializedEntryCount != journal.expected.EntryCount || len(proof.MaterializationSha256) != 32 ||
		cleanup == nil || cleanup.PointId != journal.step.GetRestore().PointId ||
		!proto.Equal(cleanup.Evidence, journal.step.GetRestore().ExpectedEvidence) {
		return invalidRecord()
	}
	return nil
}
