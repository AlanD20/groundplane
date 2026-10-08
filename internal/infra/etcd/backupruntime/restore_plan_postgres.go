package backupruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func ValidateDatabaseRestoreExecutionPlan(record BackupRestoreRecord, plan *agentpb.ExecutionPlan) error {
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return err
	}
	if ValidateBackupRestoreRecord(record) != nil || record.Point.SourceKind != BackupRuntimeSourceAttach ||
		validated.Operation != agentpb.PlanOperation_PLAN_OPERATION_RESTORE ||
		validated.TargetId != record.EnvironmentID || len(validated.Steps) != 1 || validated.BackupScope == nil {
		return invalidBackupRuntimeRecord("database Restore execution plan is invalid")
	}
	if err := ValidateRestoreVersionReview(record); err != nil {
		return err
	}
	scope := validated.BackupScope
	step := validated.Steps[0].GetBackupStep()
	if step == nil || step.GetRestore() == nil {
		return invalidBackupRuntimeRecord("database Restore Step is missing")
	}
	restore := step.GetRestore()
	var consumerEnvironmentRevision, attachRevision int64
	var attachSHA256 string
	var databaseService BackupRestoreDatabaseServiceSnapshot
	var consumers []BackupRestoreDatabaseServiceSnapshot
	var databaseServiceID string
	switch record.Point.SourceFormat {
	case BackupRuntimeFormatPostgres:
		target, postgres := record.CurrentTarget.Postgres, restore.GetPostgres()
		expectedArchive, archiveErr := record.Point.PostgresArchive.Wire()
		if target == nil || postgres == nil || restore.GetMysql() != nil ||
			archiveErr != nil || !proto.Equal(postgres.ExpectedArchive, expectedArchive) || !proto.Equal(postgres.ExpectedTargetVersions, record.TargetVersions.Wire()) ||
			postgres.AdapterContractVersion != postgres16protocol.AdapterContractVersion ||
			postgres.DatabaseServiceId != target.Source.BackingServiceID ||
			postgres.DatabaseName != record.Point.Postgres.Database ||
			postgres.RoleName != record.Point.Postgres.Role ||
			!bytes.Equal(postgres.ManagedReleaseIndex, []byte(target.Source.ManagedReleaseIndex)) {
			return invalidBackupRuntimeRecord("PostgreSQL Restore selected authority changed")
		}
		consumerEnvironmentRevision, attachRevision, attachSHA256 = target.ConsumerEnvironmentRevision,
			target.Source.AttachRevision, target.AttachSHA256
		databaseService, consumers, databaseServiceID = target.DatabaseService, target.Consumers,
			target.Source.BackingServiceID
	case BackupRuntimeFormatMySQL:
		target, mysql := record.CurrentTarget.MySQL, restore.GetMysql()
		expectedArchive, archiveErr := record.Point.MySQLArchive.Wire()
		if target == nil || mysql == nil || restore.GetPostgres() != nil || archiveErr != nil ||
			!proto.Equal(mysql.ExpectedTargetVersions, record.TargetVersions.Wire()) ||
			mysql.AdapterContractVersion != mysql84protocol.AdapterContractVersion ||
			mysql.RequiredServerMajor != mysql84protocol.ServerMajor ||
			mysql.RequiredServerMinor != mysql84protocol.ServerMinor ||
			mysql.DatabaseServiceId != target.Source.BackingServiceID ||
			mysql.DatabaseName != record.Point.MySQL.Database ||
			mysql.RoleName != record.Point.MySQL.Role ||
			!proto.Equal(
				mysql.ExpectedArchive,
				expectedArchive,
			) ||
			len(mysql.DatabaseImageReferenceSha256) != sha256.Size {
			return invalidBackupRuntimeRecord("MySQL Restore selected authority changed")
		}
		consumerEnvironmentRevision, attachRevision, attachSHA256 = target.ConsumerEnvironmentRevision,
			target.Source.AttachRevision, target.AttachSHA256
		databaseService, consumers, databaseServiceID = target.DatabaseService, target.Consumers,
			target.Source.BackingServiceID
	default:
		return invalidBackupRuntimeRecord("database Restore source format is invalid")
	}
	if scope.EnvironmentId != record.EnvironmentID ||
		scope.Environment.GetModRevision() != consumerEnvironmentRevision ||
		restore.PointId != record.Point.ID ||
		restore.Destination.GetKind() != agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH ||
		restore.Destination.GetResourceId() != record.Point.TargetID ||
		restore.Destination.GetResource().GetModRevision() != attachRevision ||
		step.ExecutionId != record.RestoreGenerationID ||
		!backupObjectMatchesWire(record.Point.Object, restore.SourceObject) ||
		restore.SourceObject.GetConnector().GetConnector().GetModRevision() != record.ConnectorRevision ||
		!BackupArtifactEvidenceMatchesWire(record.Point.Evidence, restore.ExpectedEvidence) ||
		step.StepDeadlineUnixNano != uint64(record.CreatedAt.Add(6*time.Hour).UnixNano()) ||
		len(step.ConsumerServiceIds) != len(consumers) {
		return invalidBackupRuntimeRecord("database Restore selected authority changed")
	}
	attachSHA, err := hex.DecodeString(attachSHA256)
	if err != nil || !bytes.Equal(attachSHA, restore.Destination.Resource.Sha256) {
		return invalidBackupRuntimeRecord("database Restore Attach digest changed")
	}
	for index, consumer := range consumers {
		if step.ConsumerServiceIds[index] != consumer.ServiceID {
			return invalidBackupRuntimeRecord("database Restore consumer order changed")
		}
	}
	services := append([]BackupRestoreDatabaseServiceSnapshot{databaseService}, consumers...)
	if len(scope.Services) != len(services) {
		return invalidBackupRuntimeRecord("PostgreSQL Restore Service fact set changed")
	}
	artifactIDs := make(map[string]string, len(services))
	for _, service := range services {
		fact := databaseRestoreScopeService(scope, service.ServiceID)
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
	if mysql := restore.GetMysql(); mysql != nil {
		fact := databaseRestoreScopeService(scope, databaseServiceID)
		matches := 0
		for _, artifact := range validated.Artifacts {
			for _, service := range artifact.Services {
				if fact != nil && service.ServiceId == databaseServiceID && service.ComposeName == fact.CurrentName {
					imageReference := sha256.Sum256([]byte(service.ImageReference))
					if !bytes.Equal(imageReference[:], mysql.DatabaseImageReferenceSha256) ||
						service.PostgresToolsImage != "" {
						return invalidBackupRuntimeRecord("MySQL Restore workload image changed")
					}
					matches++
				}
			}
		}
		if matches != 1 {
			return invalidBackupRuntimeRecord("MySQL Restore workload is not unique")
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

func databaseRestoreScopeService(scope *agentpb.BackupPlanScope, serviceID string) *agentpb.BackupServiceFact {
	for _, fact := range scope.Services {
		if fact.ServiceId == serviceID {
			return fact
		}
	}
	return nil
}
