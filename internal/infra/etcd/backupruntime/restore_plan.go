package backupruntime

import (
	"bytes"
	"crypto/sha256"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigmaterialization"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateRestoreExecutionPlan selects the exact source-specific authority.
func ValidateRestoreExecutionPlan(record BackupRestoreRecord, plan *agentpb.ExecutionPlan) error {
	switch record.Point.SourceKind {
	case BackupRuntimeSourceConfig:
		return ValidateConfigRestoreExecutionPlan(record, plan)
	case BackupRuntimeSourceVolume:
		return ValidateVolumeRestoreExecutionPlan(record, plan)
	case BackupRuntimeSourceAttach:
		return ValidateDatabaseRestoreExecutionPlan(record, plan)
	default:
		return CorruptBackupRuntimeRecord()
	}
}

// ValidateConfigRestoreExecutionPlan binds the immutable procedure to the
// selected Point and current surviving target, including its exact predecessor.
func ValidateConfigRestoreExecutionPlan(record BackupRestoreRecord, plan *agentpb.ExecutionPlan) error {
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return err
	}
	if ValidateBackupRestoreRecord(record) != nil || record.Point.SourceKind != BackupRuntimeSourceConfig ||
		validated.Operation != agentpb.PlanOperation_PLAN_OPERATION_RESTORE || validated.TargetId != record.EnvironmentID ||
		len(validated.Steps) != 1 || validated.BackupScope == nil {
		return invalidBackupRuntimeRecord("config restore execution plan is invalid")
	}
	target, scope := record.CurrentTarget.Config, validated.BackupScope
	step := validated.Steps[0].GetBackupStep()
	restore := step.GetRestore()
	config := restore.GetConfig()
	if scope.EnvironmentId != record.EnvironmentID ||
		scope.Environment.GetModRevision() != target.EnvironmentRevision ||
		len(step.GetConsumerServiceIds()) != 0 ||
		restore == nil ||
		config == nil ||
		restore.PointId != record.Point.ID ||
		restore.Destination.GetKind() != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT ||
		restore.Destination.GetResourceId() != record.EnvironmentID ||
		restore.Destination.GetResource().GetModRevision() != target.EnvironmentRevision ||
		!backupObjectMatchesWire(record.Point.Object, restore.SourceObject) ||
		restore.SourceObject.GetConnector().GetConnector().GetModRevision() != record.ConnectorRevision ||
		!BackupArtifactEvidenceMatchesWire(record.Point.Evidence, restore.ExpectedEvidence) ||
		config.DestinationEnvironmentId != record.EnvironmentID ||
		config.RestoreGenerationId != record.RestoreGenerationID ||
		config.RenderGeneration != target.RenderGeneration ||
		config.BaselineRevisionId != target.BaselineRevisionID ||
		config.BaselineHeadRevision != target.BaselineHeadRevision ||
		step.StepDeadlineUnixNano != uint64(record.CreatedAt.Add(6*time.Hour).UnixNano()) {
		return invalidBackupRuntimeRecord("config restore selected authority changed")
	}
	archive, err := record.Point.ConfigArchive.Wire()
	if err != nil || !proto.Equal(archive, config.ExpectedArchive) {
		return invalidBackupRuntimeRecord("config restore archive authority changed")
	}
	filesSHA256, err := backupconfigmaterialization.ContextSHA256(record.EnvironmentID, config.Files)
	if err != nil || filesSHA256 != target.FileContextSHA256 {
		return invalidBackupRuntimeRecord("config restore predecessor file authority changed")
	}
	recipient := sha256.Sum256([]byte(record.Point.Recipient))
	encryption := restore.GetEncryption()
	if encryption.GetKind() != agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE ||
		encryption.GetKeyEra() != uint64(
			record.Point.KeyEra,
		) || !bytes.Equal(encryption.RecipientSha256, recipient[:]) {
		return invalidBackupRuntimeRecord("config restore encryption authority changed")
	}
	if record.UsesOldIdentity {
		if encryption.SecretSlotId != backupsecret.OperatorOldAgeIdentitySlotID {
			return invalidBackupRuntimeRecord("config restore old identity slot changed")
		}
	} else if encryption.SecretSlotId != backupsecret.CurrentAgeIdentitySlotID ||
		encryption.SecretSlot.GetModRevision() != record.ExpectedKeyValueRevision {
		return invalidBackupRuntimeRecord("config restore current identity slot changed")
	}
	return nil
}
