package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const MaxDatabaseRestoreConsumers = 12

// The target is selected once, from the surviving Attach and acknowledged
// Compose workloads at one revision. The Point supplies the captured database
// identity; this snapshot supplies only current execution authority.
type BackupRestorePostgresTarget struct {
	Source                      BackupPostgresSourceSnapshot           `json:"source"`
	ConsumerEnvironmentRevision int64                                  `json:"consumer_environment_revision"`
	AttachSHA256                string                                 `json:"attach_sha256"`
	DatabaseService             BackupRestoreDatabaseServiceSnapshot   `json:"database_service"`
	Consumers                   []BackupRestoreDatabaseServiceSnapshot `json:"consumers"`
	DependentIndexes            []BackupRestoreDatabaseDependentIndex  `json:"dependent_indexes"`
}

type BackupRestoreMySQLTarget struct {
	Source                      BackupMySQLSourceSnapshot              `json:"source"`
	ConsumerEnvironmentRevision int64                                  `json:"consumer_environment_revision"`
	AttachSHA256                string                                 `json:"attach_sha256"`
	DatabaseService             BackupRestoreDatabaseServiceSnapshot   `json:"database_service"`
	Consumers                   []BackupRestoreDatabaseServiceSnapshot `json:"consumers"`
	DependentIndexes            []BackupRestoreDatabaseDependentIndex  `json:"dependent_indexes"`
}

type BackupRestoreDatabaseDependentIndex struct {
	Key            string `json:"key"`
	Revision       int64  `json:"revision"`
	AttachID       string `json:"attach_id"`
	AttachRevision int64  `json:"attach_revision"`
	AttachSHA256   string `json:"attach_sha256"`
}

type BackupRestoreDatabaseServiceSnapshot struct {
	ServiceID       string                     `json:"service_id"`
	EnvironmentID   string                     `json:"environment_id"`
	ServiceRevision int64                      `json:"service_revision"`
	ServiceSHA256   string                     `json:"service_sha256"`
	IntentRevision  int64                      `json:"intent_revision"`
	IntentSHA256    string                     `json:"intent_sha256"`
	ComposeRevision int64                      `json:"compose_revision"`
	ComposeSHA256   string                     `json:"compose_sha256"`
	PriorIntent     BackupServiceRuntimeIntent `json:"prior_intent"`
	FactSHA256      string                     `json:"fact_sha256"`
	ArtifactID      string                     `json:"artifact_id"`
	ArtifactSHA256  string                     `json:"artifact_sha256"`
}

type BackupRestoreDatabaseProgress struct {
	ContainerID               string                     `json:"container_id,omitempty"`
	ObservationSHA256         string                     `json:"observation_sha256,omitempty"`
	ObservedRepositoryDigest  string                     `json:"observed_repository_digest,omitempty"`
	ObservedLabelsSHA256      string                     `json:"observed_labels_sha256,omitempty"`
	ServiceMutationStarted    bool                       `json:"service_mutation_started"`
	StopCursor                uint32                     `json:"stop_cursor"`
	StopPhase                 agentpb.BackupServicePhase `json:"stop_phase"`
	StopServiceID             string                     `json:"stop_service_id,omitempty"`
	ApplyStarted              bool                       `json:"apply_started"`
	ApplyExecutionNonce       string                     `json:"apply_execution_nonce,omitempty"`
	ApplyExecID               string                     `json:"apply_exec_id,omitempty"`
	ApplyRepositoryDigest     string                     `json:"apply_repository_digest,omitempty"`
	ApplyLabelsSHA256         string                     `json:"apply_labels_sha256,omitempty"`
	RestoreVerificationSHA256 string                     `json:"restore_verification_sha256,omitempty"`
	RecoveryCursor            uint32                     `json:"recovery_cursor"`
	RecoveryPhase             agentpb.BackupServicePhase `json:"recovery_phase"`
	RecoveryServiceID         string                     `json:"recovery_service_id,omitempty"`
	SourceCleanupCompleted    bool                       `json:"source_cleanup_completed"`
}

func validateBackupRestorePostgresTarget(target BackupRestorePostgresTarget,
	point BackupRecoveryPointSnapshot,
) error {
	if point.Postgres == (BackupPostgresPointIdentity{}) || validateBackupPostgresSnapshot(target.Source) != nil ||
		target.Source.ConsumerEnvironmentID != point.EnvironmentID ||
		target.Source.AttachID != point.TargetID ||
		target.Source.Database != point.Postgres.Database || target.Source.Role != point.Postgres.Role ||
		target.Source.BackingEnvironmentID != point.Postgres.BackingEnvironmentID ||
		target.Source.BackingServiceID != point.Postgres.BackingServiceID ||
		target.Source.ConsumerServiceID != point.Postgres.ConsumerServiceID {
		return invalidBackupRuntimeRecord("postgres Restore target differs from the captured Attach")
	}
	return validateBackupRestoreDatabaseTarget(target.ConsumerEnvironmentRevision, target.AttachSHA256,
		target.Source.ConsumerEnvironmentID, target.Source.BackingEnvironmentID,
		target.Source.BackingServiceID, target.Source.BackingServiceRevision,
		target.Source.ConsumerServiceID, point.EnvironmentID, target.DatabaseService,
		target.Consumers, target.DependentIndexes)
}

func validateBackupRestoreMySQLTarget(target BackupRestoreMySQLTarget, point BackupRecoveryPointSnapshot) error {
	if point.MySQL == (BackupMySQLPointIdentity{}) || validateBackupMySQLSnapshot(target.Source) != nil ||
		target.Source.ConsumerEnvironmentID != point.EnvironmentID || target.Source.AttachID != point.TargetID ||
		target.Source.Database != point.MySQL.Database || target.Source.Role != point.MySQL.Role ||
		target.Source.BackingEnvironmentID != point.MySQL.BackingEnvironmentID ||
		target.Source.BackingServiceID != point.MySQL.BackingServiceID ||
		target.Source.ConsumerServiceID != point.MySQL.ConsumerServiceID {
		return invalidBackupRuntimeRecord("MySQL Restore target differs from the captured Attach")
	}
	return validateBackupRestoreDatabaseTarget(target.ConsumerEnvironmentRevision, target.AttachSHA256,
		target.Source.ConsumerEnvironmentID, target.Source.BackingEnvironmentID,
		target.Source.BackingServiceID, target.Source.BackingServiceRevision,
		target.Source.ConsumerServiceID, point.EnvironmentID, target.DatabaseService,
		target.Consumers, target.DependentIndexes)
}

func validateBackupRestoreDatabaseTarget(consumerEnvironmentRevision int64, attachSHA,
	consumerEnvironmentID, backingEnvironmentID, backingServiceID string, backingServiceRevision int64,
	consumerServiceID, pointEnvironmentID string, databaseService BackupRestoreDatabaseServiceSnapshot,
	consumers []BackupRestoreDatabaseServiceSnapshot, dependentIndexes []BackupRestoreDatabaseDependentIndex,
) error {
	if consumerEnvironmentRevision <= 0 || !recordcodec.ValidSHA256(attachSHA) ||
		validateBackupRestoreDatabaseService(databaseService) != nil ||
		databaseService.ServiceID != backingServiceID || databaseService.EnvironmentID != backingEnvironmentID ||
		databaseService.ServiceRevision != backingServiceRevision ||
		databaseService.PriorIntent != BackupServiceIntentRunning || len(consumers) == 0 ||
		len(consumers) > MaxDatabaseRestoreConsumers {
		return invalidBackupRuntimeRecord("database Restore target evidence is invalid")
	}
	ownerFound := false
	previous := ""
	for _, consumer := range consumers {
		if validateBackupRestoreDatabaseService(consumer) != nil ||
			previous != "" && consumer.ServiceID <= previous ||
			consumer.EnvironmentID != consumerEnvironmentID || consumer.ServiceID == databaseService.ServiceID {
			return invalidBackupRuntimeRecord("database Restore consumers are invalid")
		}
		if consumer.ServiceID == consumerServiceID {
			ownerFound = consumer.EnvironmentID == pointEnvironmentID
		}
		previous = consumer.ServiceID
	}
	if !ownerFound {
		return invalidBackupRuntimeRecord("database Restore lost its owning consumer")
	}
	if len(dependentIndexes) > 2*MaxDatabaseRestoreConsumers {
		return invalidBackupRuntimeRecord("database Restore dependent index set exceeds its bound")
	}
	previous = ""
	for _, index := range dependentIndexes {
		if index.Key == "" || index.Revision <= 0 ||
			recordcodec.ValidateID(ids.KindAttach, index.AttachID) != nil ||
			index.AttachRevision <= 0 || !recordcodec.ValidSHA256(index.AttachSHA256) ||
			previous != "" && index.Key <= previous {
			return invalidBackupRuntimeRecord("database Restore dependent index is invalid")
		}
		previous = index.Key
	}
	return nil
}

func validateBackupRestoreDatabaseService(service BackupRestoreDatabaseServiceSnapshot) error {
	if recordcodec.ValidateID(ids.KindService, service.ServiceID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, service.EnvironmentID) != nil ||
		service.ServiceRevision <= 0 || !recordcodec.ValidSHA256(service.ServiceSHA256) ||
		service.IntentRevision <= 0 || !recordcodec.ValidSHA256(service.IntentSHA256) ||
		service.ComposeRevision <= 0 || !recordcodec.ValidSHA256(service.ComposeSHA256) ||
		!validBackupServiceRuntimeIntent(service.PriorIntent) ||
		!recordcodec.ValidSHA256(service.FactSHA256) ||
		recordcodec.ValidateID(ids.KindConfig, service.ArtifactID) != nil ||
		!recordcodec.ValidSHA256(service.ArtifactSHA256) {
		return invalidBackupRuntimeRecord("database Restore Service fact is invalid")
	}
	return nil
}

func validateBackupRestoreDatabaseProgress(record BackupRestoreRecord, consumerCount int) error {
	if record.DatabaseProgress == nil || record.ServiceCount != uint32(consumerCount) || consumerCount <= 0 {
		return invalidBackupRuntimeRecord("database Restore progress is incomplete")
	}
	progress := record.DatabaseProgress
	if progress.StopCursor > record.ServiceCount || progress.RecoveryCursor > record.ServiceCount ||
		(progress.ContainerID == "") != (progress.ObservationSHA256 == "") ||
		progress.ContainerID != "" && (!validBackupHexID(progress.ContainerID) ||
			!recordcodec.ValidSHA256(progress.ObservationSHA256) ||
			!recordcodec.ValidSHA256(progress.ObservedRepositoryDigest) ||
			!recordcodec.ValidSHA256(progress.ObservedLabelsSHA256)) ||
		progress.ContainerID == "" &&
			(progress.ObservedRepositoryDigest != "" || progress.ObservedLabelsSHA256 != "") ||
		record.MutationStarted != (progress.ServiceMutationStarted || progress.ApplyStarted) {
		return invalidBackupRuntimeRecord("database Restore observation or cursor is invalid")
	}
	if (progress.StopPhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_UNSPECIFIED) !=
		(progress.StopServiceID == "") ||
		(progress.RecoveryPhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_UNSPECIFIED) !=
			(progress.RecoveryServiceID == "") {
		return invalidBackupRuntimeRecord("database Restore Service progress is incomplete")
	}
	if progress.ApplyStarted {
		if progress.ContainerID == "" || progress.StopCursor != record.ServiceCount ||
			!recordcodec.ValidSHA256(progress.ApplyExecutionNonce) ||
			!validBackupHexID(progress.ApplyExecID) ||
			!recordcodec.ValidSHA256(progress.ApplyRepositoryDigest) ||
			!recordcodec.ValidSHA256(progress.ApplyLabelsSHA256) {
			return invalidBackupRuntimeRecord("database Restore apply start is incomplete")
		}
	} else if progress.ApplyExecutionNonce != "" || progress.ApplyExecID != "" ||
		progress.ApplyRepositoryDigest != "" || progress.ApplyLabelsSHA256 != "" ||
		progress.RestoreVerificationSHA256 != "" || progress.RecoveryCursor != 0 ||
		progress.RecoveryPhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_UNSPECIFIED ||
		progress.SourceCleanupCompleted {
		return invalidBackupRuntimeRecord("database Restore advanced before apply start")
	}
	if progress.RestoreVerificationSHA256 != "" &&
		!recordcodec.ValidSHA256(progress.RestoreVerificationSHA256) ||
		progress.RecoveryCursor != 0 && progress.RestoreVerificationSHA256 == "" ||
		progress.SourceCleanupCompleted && (progress.RestoreVerificationSHA256 == "" ||
			progress.RecoveryCursor != record.ServiceCount) {
		return invalidBackupRuntimeRecord("database Restore verification or cleanup is out of order")
	}
	switch record.State {
	case BackupRestoreQueued, BackupRestoreDownloading:
		if *progress != (BackupRestoreDatabaseProgress{}) {
			return invalidBackupRuntimeRecord("database Restore progressed before artifact validation")
		}
	case BackupRestoreArtifactVerified:
		if record.MutationStarted || progress.StopCursor != 0 || progress.StopPhase != 0 || progress.ApplyStarted {
			return invalidBackupRuntimeRecord("database Restore artifact state has mutation progress")
		}
	case BackupRestoreConsumersStopped:
		if progress.ApplyStarted {
			return invalidBackupRuntimeRecord("database Restore stop progress is invalid")
		}
	case BackupRestoreRestoring:
		if !record.MutationStarted || !progress.ApplyStarted || progress.SourceCleanupCompleted {
			return invalidBackupRuntimeRecord("database Restore apply state is invalid")
		}
	case BackupRestoreConsumersRestored:
		if progress.RestoreVerificationSHA256 == "" || progress.RecoveryCursor != record.ServiceCount ||
			progress.SourceCleanupCompleted {
			return invalidBackupRuntimeRecord("database Restore consumers have not recovered")
		}
	case BackupRestoreVerified, BackupRestoreCompleted:
		if !progress.SourceCleanupCompleted || progress.RestoreVerificationSHA256 == "" ||
			progress.RecoveryCursor != record.ServiceCount {
			return invalidBackupRuntimeRecord("database Restore lacks verification and physical source cleanup")
		}
	case BackupRestoreFailedSafe:
		if record.MutationStarted || progress.ApplyStarted || progress.StopCursor != 0 {
			return invalidBackupRuntimeRecord("failed-safe database Restore has mutation progress")
		}
	case BackupRestoreRecoveryRequired:
		if !record.MutationStarted {
			return invalidBackupRuntimeRecord("database Restore recovery requires a mutation intent")
		}
	default:
		return invalidBackupRuntimeRecord("database Restore state is invalid")
	}
	return nil
}

func validBackupHexID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
