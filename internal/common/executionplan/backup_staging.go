package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

const MaximumBackupRecoveredStages = 32
const BackupSourceStagingFinal = "source"
const BackupStoredStagingFinal = "stored"

func BackupStagingAssignmentResumeSHA256(
	authority *agentpb.BackupTaskAuthority,
	resume *agentpb.BackupTaskResume,
) ([]byte, error) {
	validated, err := ValidateBackupTaskResume(resume, authority)
	if err != nil {
		return nil, err
	}
	return backupStagingDigest("groundplane.backup.staging-assignment-resume.schema-one.v1\x00", validated)
}

// BackupStagingRecoveryKey binds the local descriptor namespace to the sealed
// Task step and Recovery Point. No mutable label or path appears on the wire.
func BackupStagingRecoveryKey(taskID, stepID, pointID string) ([]byte, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || ids.Validate(ids.KindStep, stepID) != nil ||
		ids.Validate(ids.KindRecoveryPoint, pointID) != nil {
		return nil, invalidBackupStaging()
	}
	digest := sha256.Sum256(
		[]byte("groundplane.backup.staging-key.schema-one.v1\x00" + taskID + "/" + stepID + "/" + pointID),
	)
	return digest[:], nil
}

func ValidateBackupStagingInventory(inventory *agentpb.BackupStagingInventory) error {
	if inventory == nil || RejectUnknown(inventory) != nil ||
		len(inventory.Entries)+len(inventory.VolumeRestores) > MaximumBackupRecoveredStages {
		return invalidBackupStaging()
	}
	var preceding []byte
	for _, entry := range inventory.Entries {
		if entry == nil || len(entry.RecoveryKeySha256) != sha256.Size ||
			(preceding != nil && bytes.Compare(preceding, entry.RecoveryKeySha256) >= 0) ||
			!validBackupRecoveredFiles(entry.Files) && !(entry.PostgresExecution && len(entry.Files) == 0) {
			return invalidBackupStaging()
		}
		preceding = entry.RecoveryKeySha256
	}
	preceding = nil
	for _, volume := range inventory.VolumeRestores {
		if !validBackupRecoveredVolumeRestore(volume) ||
			(preceding != nil && bytes.Compare(preceding, volume.RecoveryKeySha256) >= 0) {
			return invalidBackupStaging()
		}
		preceding = volume.RecoveryKeySha256
	}
	return nil
}

func validBackupRecoveredFiles(files []*agentpb.BackupRecoveredFile) bool {
	if len(files) < 1 || len(files) > 2 {
		return false
	}
	preceding := agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_UNSPECIFIED
	for _, file := range files {
		if file == nil || len(file.Sha256) != sha256.Size || file.Role <= preceding ||
			(file.Role != agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT &&
				file.Role != agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT) {
			return false
		}
		preceding = file.Role
	}
	return true
}

func validBackupRecoveredVolumeRestore(value *agentpb.BackupRecoveredVolumeRestore) bool {
	if value == nil || RejectUnknown(value) != nil || len(value.RecoveryKeySha256) != sha256.Size ||
		ids.Validate(ids.KindAssignment, value.AssignmentId) != nil ||
		len(value.RestoreGenerationId) != 26 || len(value.StepSha256) != sha256.Size ||
		!value.JournalPresent && !value.ReceiverPresent {
		return false
	}
	if _, err := ulid.ParseStrict(value.RestoreGenerationId); err != nil {
		return false
	}
	if value.JournalPresent {
		if len(value.OldManifestSha256) != sha256.Size || len(value.OldFullTreeSha256) != sha256.Size ||
			len(value.NewManifestSha256) != sha256.Size || len(value.NewFullTreeSha256) != sha256.Size ||
			len(value.JournalChainSha256) != sha256.Size || value.OldEntryCount == 0 ||
			value.OldEntryCount > backupvolume.MaxEntries || value.NewEntryCount == 0 ||
			value.NewEntryCount > backupvolume.MaxEntries ||
			value.JournalRootDeleted && value.JournalRecordCount == 0 && !value.JournalRetiring {
			return false
		}
	} else if len(value.OldManifestSha256) != 0 || len(value.OldFullTreeSha256) != 0 ||
		len(value.NewManifestSha256) != 0 || len(value.NewFullTreeSha256) != 0 ||
		len(value.JournalChainSha256) != 0 || value.OldEntryCount != 0 || value.NewEntryCount != 0 ||
		value.JournalRecordCount != 0 || value.JournalPending || value.JournalRootDeleted ||
		value.JournalPartial || value.JournalRetiring || value.JournalDiscarding {
		return false
	}
	if !value.ReceiverPresent && (value.ReceiverCommittedRecordSequence != 0 || value.ReceiverComplete ||
		value.ReceiverPartial || value.ReceiverRetiring || value.ReceiverDiscarding ||
		len(value.ReceiverContentManifestSha256) != 0 ||
		len(value.ReceiverFullTreeSha256) != 0 || len(value.ReceiverSourceSha256) != 0 ||
		value.ReceiverEntryCount != 0) {
		return false
	}
	if value.ReceiverPresent && (len(value.ReceiverContentManifestSha256) != sha256.Size ||
		len(value.ReceiverFullTreeSha256) != sha256.Size || len(value.ReceiverSourceSha256) != sha256.Size ||
		value.ReceiverEntryCount == 0 || value.ReceiverEntryCount > backupvolume.MaxEntries) {
		return false
	}
	if value.JournalDiscarding && !value.JournalRetiring ||
		value.ReceiverDiscarding && !value.ReceiverRetiring {
		return false
	}
	return !value.ReceiverComplete || value.ReceiverCommittedRecordSequence != 0 || value.ReceiverRetiring
}

func BackupStagingInventorySHA256(inventory *agentpb.BackupStagingInventory) ([]byte, error) {
	if err := ValidateBackupStagingInventory(inventory); err != nil {
		return nil, err
	}
	return backupStagingDigest("groundplane.backup.staging-inventory.schema-one.v1\x00", inventory)
}

// ValidateBackupStagingRecoveryPlan requires every inventoried stage and
// Volume generation exactly once, preserving order and exact evidence.
// Silence grants no cleanup.
func ValidateBackupStagingRecoveryPlan(
	inventory *agentpb.BackupStagingInventory,
	plan *agentpb.BackupStagingRecoveryPlan,
) error {
	digest, err := BackupStagingInventorySHA256(inventory)
	if err != nil {
		return err
	}
	if err := validateBackupStagingPlan(plan); err != nil {
		return err
	}
	if !bytes.Equal(digest, plan.InventorySha256) || len(plan.Dispositions) != len(inventory.Entries) ||
		len(plan.VolumeDispositions) != len(inventory.VolumeRestores) {
		return invalidBackupStaging()
	}
	for index, disposition := range plan.Dispositions {
		entry := inventory.Entries[index]
		if !bytes.Equal(entry.RecoveryKeySha256, disposition.RecoveryKeySha256) {
			return invalidBackupStaging()
		}
		if entry.PostgresExecution && disposition.PostgresGuard == nil {
			return invalidBackupStaging()
		}
		if resume := disposition.GetResumePrepared(); resume != nil {
			if len(resume.ExpectedFiles) != len(entry.Files) {
				return invalidBackupStaging()
			}
			for fileIndex, file := range entry.Files {
				if !proto.Equal(file, resume.ExpectedFiles[fileIndex]) {
					return invalidBackupStaging()
				}
			}
		}
		if required := disposition.GetRecoveryRequired(); required != nil {
			if len(required.ExpectedFiles) != len(entry.Files) {
				return invalidBackupStaging()
			}
			for fileIndex, file := range entry.Files {
				if !proto.Equal(file, required.ExpectedFiles[fileIndex]) {
					return invalidBackupStaging()
				}
			}
		}
	}
	for index, disposition := range plan.VolumeDispositions {
		if !bytes.Equal(inventory.VolumeRestores[index].RecoveryKeySha256, disposition.RecoveryKeySha256) {
			return invalidBackupStaging()
		}
		if (inventory.VolumeRestores[index].JournalPartial || inventory.VolumeRestores[index].JournalRetiring ||
			inventory.VolumeRestores[index].ReceiverRetiring) && disposition.GetResume() != nil {
			return invalidBackupStaging()
		}
	}
	return nil
}

func validateBackupStagingPlan(plan *agentpb.BackupStagingRecoveryPlan) error {
	if plan == nil || RejectUnknown(plan) != nil || len(plan.InventorySha256) != sha256.Size ||
		(len(plan.SupersedesPlanSha256) != 0 && len(plan.SupersedesPlanSha256) != sha256.Size) ||
		len(plan.Dispositions)+len(plan.VolumeDispositions) > MaximumBackupRecoveredStages {
		return invalidBackupStaging()
	}
	var preceding []byte
	for _, disposition := range plan.Dispositions {
		if disposition == nil || len(disposition.RecoveryKeySha256) != sha256.Size ||
			(preceding != nil && bytes.Compare(preceding, disposition.RecoveryKeySha256) >= 0) {
			return invalidBackupStaging()
		}
		preceding = disposition.RecoveryKeySha256
		if err := validatePostgresStagingGuard(disposition); err != nil {
			return err
		}
		switch value := disposition.Disposition.(type) {
		case *agentpb.BackupStagingDisposition_RecoveryRequired:
			required := value.RecoveryRequired
			if required == nil || required.NativeRestoreModRevision <= 0 ||
				len(required.NativeRestoreSha256) != sha256.Size ||
				!validBackupRecoveredFiles(required.ExpectedFiles) && !(disposition.PostgresGuard != nil &&
					disposition.PostgresGuard.RecoveryApply == nil && len(required.ExpectedFiles) == 0) {
				return invalidBackupStaging()
			}
		case *agentpb.BackupStagingDisposition_DiscardRecovered:
			if value.DiscardRecovered == nil {
				return invalidBackupStaging()
			}
		case *agentpb.BackupStagingDisposition_ResumePrepared:
			resume := value.ResumePrepared
			if resume == nil || len(resume.AssignmentResumeSha256) != sha256.Size || !validBackupRecoveredFiles(resume.ExpectedFiles) {
				return invalidBackupStaging()
			}
			if resume.RestartConfigEncryption != nil && resume.RestartPostgresEncryption != nil {
				return invalidBackupStaging()
			}
			if (resume.RestartConfigEncryption != nil || resume.RestartPostgresEncryption != nil) && (len(resume.ExpectedFiles) != 2 ||
				resume.RemainingGrowth != agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_BOUNDED) {
				return invalidBackupStaging()
			}
			switch resume.RemainingGrowth {
			case agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_NO_GROWTH,
				agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_EXCLUSIVE_UNKNOWN:
				if resume.RequiredGrowthBytes != 0 {
					return invalidBackupStaging()
				}
			case agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_BOUNDED:
				if resume.RequiredGrowthBytes == 0 || resume.RequiredGrowthBytes > uint64(1<<63-1) {
					return invalidBackupStaging()
				}
			default:
				return invalidBackupStaging()
			}
		default:
			return invalidBackupStaging()
		}
	}
	preceding = nil
	for _, disposition := range plan.VolumeDispositions {
		if disposition == nil || len(disposition.RecoveryKeySha256) != sha256.Size ||
			(preceding != nil && bytes.Compare(preceding, disposition.RecoveryKeySha256) >= 0) {
			return invalidBackupStaging()
		}
		preceding = disposition.RecoveryKeySha256
		var revision int64
		var digest []byte
		switch value := disposition.Disposition.(type) {
		case *agentpb.BackupVolumeRestoreRecoveryDisposition_Resume:
			if value.Resume == nil || len(value.Resume.AssignmentResumeSha256) != sha256.Size {
				return invalidBackupStaging()
			}
			revision, digest = value.Resume.NativeRestoreModRevision, value.Resume.NativeRestoreSha256
		case *agentpb.BackupVolumeRestoreRecoveryDisposition_Hold:
			if value.Hold == nil {
				return invalidBackupStaging()
			}
			revision, digest = value.Hold.NativeRestoreModRevision, value.Hold.NativeRestoreSha256
		case *agentpb.BackupVolumeRestoreRecoveryDisposition_Cleanup:
			if value.Cleanup == nil {
				return invalidBackupStaging()
			}
			revision, digest = value.Cleanup.NativeRestoreModRevision, value.Cleanup.NativeRestoreSha256
		default:
			return invalidBackupStaging()
		}
		if revision <= 0 || len(digest) != sha256.Size {
			return invalidBackupStaging()
		}
	}
	return nil
}

func BackupStagingRecoveryPlanSHA256(plan *agentpb.BackupStagingRecoveryPlan) ([]byte, error) {
	if err := validateBackupStagingPlan(plan); err != nil {
		return nil, err
	}
	return backupStagingDigest("groundplane.backup.staging-plan.schema-one.v1\x00", plan)
}

func BackupStagingRecoveryAckSHA256(ack *agentpb.BackupStagingRecoveryAck) ([]byte, error) {
	if ack == nil || RejectUnknown(ack) != nil || len(ack.InventorySha256) != sha256.Size ||
		len(ack.AppliedPlanSha256) != sha256.Size || ack.AppliedDispositionCount > MaximumBackupRecoveredStages {
		return nil, invalidBackupStaging()
	}
	return backupStagingDigest("groundplane.backup.staging-recovery-ack.schema-one.v1\x00", ack)
}

func backupStagingDigest(domain string, message proto.Message) ([]byte, error) {
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

func invalidBackupStaging() error {
	return errs.New(errs.KindValidationFailed, "Backup staging recovery evidence is invalid")
}
