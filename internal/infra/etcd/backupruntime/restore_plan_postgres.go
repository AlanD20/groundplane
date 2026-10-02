package backupruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func ValidatePostgresRestoreExecutionPlan(record BackupRestoreRecord, plan *agentpb.ExecutionPlan) error {
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return err
	}
	if ValidateBackupRestoreRecord(record) != nil || record.Point.SourceKind != BackupRuntimeSourceAttach ||
		validated.Operation != agentpb.PlanOperation_PLAN_OPERATION_RESTORE ||
		validated.TargetId != record.EnvironmentID || len(validated.Steps) != 1 ||
		validated.BackupScope == nil || record.CurrentTarget.Postgres == nil {
		return invalidBackupRuntimeRecord("PostgreSQL Restore execution plan is invalid")
	}
	target, scope := record.CurrentTarget.Postgres, validated.BackupScope
	step := validated.Steps[0].GetBackupStep()
	if step == nil || step.GetRestore() == nil || step.GetRestore().GetPostgres() == nil {
		return invalidBackupRuntimeRecord("PostgreSQL Restore Step is missing")
	}
	restore, postgres := step.GetRestore(), step.GetRestore().GetPostgres()
	if scope.EnvironmentId != record.EnvironmentID ||
		scope.Environment.GetModRevision() != target.ConsumerEnvironmentRevision ||
		restore.PointId != record.Point.ID ||
		restore.Destination.GetKind() != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH ||
		restore.Destination.GetResourceId() != record.Point.TargetID ||
		restore.Destination.GetResource().GetModRevision() != target.Source.AttachRevision ||
		postgres.AdapterContractVersion != postgres16protocol.AdapterContractVersion ||
		postgres.DatabaseServiceId != target.Source.BackingServiceID ||
		postgres.DatabaseName != record.Point.Postgres.Database ||
		postgres.RoleName != record.Point.Postgres.Role ||
		!bytes.Equal(postgres.ManagedReleaseIndex, []byte(target.Source.ManagedReleaseIndex)) ||
		step.ExecutionId != record.RestoreGenerationID ||
		!backupObjectMatchesWire(record.Point.Object, restore.SourceObject) ||
		restore.SourceObject.GetConnector().GetConnector().GetModRevision() != record.ConnectorRevision ||
		!BackupArtifactEvidenceMatchesWire(record.Point.Evidence, restore.ExpectedEvidence) ||
		step.StepDeadlineUnixNano != uint64(record.CreatedAt.Add(6*time.Hour).UnixNano()) ||
		len(step.ConsumerServiceIds) != len(target.Consumers) {
		return invalidBackupRuntimeRecord("PostgreSQL Restore selected authority changed")
	}
	attachSHA, err := hex.DecodeString(target.AttachSHA256)
	if err != nil || !bytes.Equal(attachSHA, restore.Destination.Resource.Sha256) {
		return invalidBackupRuntimeRecord("PostgreSQL Restore Attach digest changed")
	}
	for index, consumer := range target.Consumers {
		if step.ConsumerServiceIds[index] != consumer.ServiceID {
			return invalidBackupRuntimeRecord("PostgreSQL Restore consumer order changed")
		}
	}
	services := append([]BackupRestorePostgresServiceSnapshot{target.DatabaseService}, target.Consumers...)
	if len(scope.Services) != len(services) {
		return invalidBackupRuntimeRecord("PostgreSQL Restore Service fact set changed")
	}
	artifactIDs := make(map[string]string, len(services))
	for _, service := range services {
		fact := postgresRestoreScopeService(scope, service.ServiceID)
		if fact == nil || fact.Service.GetModRevision() != service.ServiceRevision ||
			hex.EncodeToString(fact.Service.GetSha256()) != service.ServiceSHA256 ||
			fact.Compose.GetModRevision() != service.ComposeRevision ||
			hex.EncodeToString(fact.Compose.GetSha256()) != service.ComposeSHA256 ||
			fact.PriorRuntimeIntent == nil {
			return invalidBackupRuntimeRecord("PostgreSQL Restore Service fact changed")
		}
		if service.PriorIntent == BackupServiceIntentAbsent {
			if fact.PriorRuntimeIntent.Kind != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT ||
				fact.PriorRuntimeIntent.Intent != nil {
				return invalidBackupRuntimeRecord("PostgreSQL Restore absent Service intent changed")
			}
		} else if fact.PriorRuntimeIntent.Intent.GetModRevision() != service.IntentRevision ||
			hex.EncodeToString(fact.PriorRuntimeIntent.Intent.GetSha256()) != service.IntentSHA256 ||
			service.PriorIntent == BackupServiceIntentRunning &&
				fact.PriorRuntimeIntent.Kind != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING ||
			service.PriorIntent == BackupServiceIntentStopped &&
				fact.PriorRuntimeIntent.Kind != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_STOPPED {
			return invalidBackupRuntimeRecord("PostgreSQL Restore prior Service intent changed")
		}
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(fact)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(encoded)
		clear(encoded)
		if hex.EncodeToString(digest[:]) != service.FactSHA256 {
			return invalidBackupRuntimeRecord("PostgreSQL Restore Service fact hash changed")
		}
		if previous, found := artifactIDs[service.ArtifactID]; found && previous != service.ArtifactSHA256 {
			return invalidBackupRuntimeRecord("PostgreSQL Restore artifact identity is ambiguous")
		}
		artifactIDs[service.ArtifactID] = service.ArtifactSHA256
	}
	if len(validated.Artifacts) != len(artifactIDs) {
		return invalidBackupRuntimeRecord("PostgreSQL Restore artifact set changed")
	}
	for _, artifact := range validated.Artifacts {
		expected, found := artifactIDs[artifact.ArtifactId]
		if !found {
			return invalidBackupRuntimeRecord("PostgreSQL Restore artifact is not selected")
		}
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(encoded)
		clear(encoded)
		if hex.EncodeToString(digest[:]) != expected {
			return invalidBackupRuntimeRecord("PostgreSQL Restore artifact bytes changed")
		}
	}
	switch record.Point.Encryption {
	case BackupRuntimeEncryptionNone:
		if restore.Encryption.GetKind() != agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE ||
			restore.Encryption.SecretSlotId != "" || restore.Encryption.KeyEra != nil {
			return invalidBackupRuntimeRecord("PostgreSQL Restore unencrypted authority changed")
		}
	case BackupRuntimeEncryptionAge:
		recipient := sha256.Sum256([]byte(record.Point.Recipient))
		if restore.Encryption.GetKind() != agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE ||
			restore.Encryption.GetKeyEra() != uint64(record.Point.KeyEra) ||
			!bytes.Equal(restore.Encryption.RecipientSha256, recipient[:]) {
			return invalidBackupRuntimeRecord("PostgreSQL Restore age authority changed")
		}
		if record.UsesOldIdentity {
			if restore.Encryption.SecretSlotId != backupsecret.OperatorOldAgeIdentitySlotID {
				return invalidBackupRuntimeRecord("PostgreSQL Restore old identity slot changed")
			}
		} else if restore.Encryption.SecretSlotId != backupsecret.CurrentAgeIdentitySlotID ||
			restore.Encryption.SecretSlot.GetModRevision() != record.ExpectedKeyValueRevision {
			return invalidBackupRuntimeRecord("PostgreSQL Restore current identity slot changed")
		}
	default:
		return invalidBackupRuntimeRecord("PostgreSQL Restore encryption strategy changed")
	}
	return nil
}

func postgresRestoreScopeService(scope *agentpb.BackupPlanScope, serviceID string) *agentpb.BackupServiceFact {
	for _, fact := range scope.Services {
		if fact.ServiceId == serviceID {
			return fact
		}
	}
	return nil
}
