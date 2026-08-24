package etcd

import (
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumBackupRuntimeRecordBytes  = 256 * 1024
	maximumBackupObjectKeyBytes      = 1024
	maximumBackupPruneDispatchPoints = 12
)

type BackupRuntimeSourceKind string

const (
	BackupRuntimeSourceAttach BackupRuntimeSourceKind = "attach"
	BackupRuntimeSourceConfig BackupRuntimeSourceKind = "config"
	BackupRuntimeSourceVolume BackupRuntimeSourceKind = "volume"
)

type BackupRuntimeFormat string

const (
	BackupRuntimeFormatPostgres BackupRuntimeFormat = "postgres-custom-v1"
	BackupRuntimeFormatConfig   BackupRuntimeFormat = "environment-config-v1"
	BackupRuntimeFormatVolume   BackupRuntimeFormat = "volume-tar-v1"
)

type BackupRuntimeEncryption string

const (
	BackupRuntimeEncryptionAge  BackupRuntimeEncryption = "age"
	BackupRuntimeEncryptionNone BackupRuntimeEncryption = "none"
)

type BackupDueOutcome string

const (
	BackupDueDispatched     BackupDueOutcome = "dispatched"
	BackupDueSkippedOverlap BackupDueOutcome = "skipped_overlap"
)

type BackupOperationKind string

const (
	BackupOperationBackup   BackupOperationKind = "backup"
	BackupOperationRestore  BackupOperationKind = "restore"
	BackupOperationRotation BackupOperationKind = "rotation"
	BackupOperationDeletion BackupOperationKind = "environment_deletion"
)

type BackupSourceTargetKind string

const (
	BackupSourceTargetAttach             BackupSourceTargetKind = "attach"
	BackupSourceTargetVolume             BackupSourceTargetKind = "volume"
	BackupSourceTargetBackingProject     BackupSourceTargetKind = "backing_project"
	BackupSourceTargetBackingEnvironment BackupSourceTargetKind = "backing_environment"
	BackupSourceTargetBackingService     BackupSourceTargetKind = "backing_service"
)

type BackupRunInitiator string

const (
	BackupRunInitiatorOperator BackupRunInitiator = "operator"
	BackupRunInitiatorSchedule BackupRunInitiator = "schedule"
)

type BackupRunState string

const (
	BackupRunQueued    BackupRunState = "queued"
	BackupRunRunning   BackupRunState = "running"
	BackupRunFailed    BackupRunState = "failed"
	BackupRunCompleted BackupRunState = "completed"
	BackupRunAborted   BackupRunState = "aborted"
	BackupRunTimedOut  BackupRunState = "timed_out"
)

type BackupSourceAttemptState string

const (
	BackupSourceAttemptPending        BackupSourceAttemptState = "pending"
	BackupSourceAttemptCapturing      BackupSourceAttemptState = "capturing"
	BackupSourceAttemptReady          BackupSourceAttemptState = "ready"
	BackupSourceAttemptStaged         BackupSourceAttemptState = "staged"
	BackupSourceAttemptPointCommitted BackupSourceAttemptState = "point_committed"
	BackupSourceAttemptCleanupPending BackupSourceAttemptState = "cleanup_pending"
	BackupSourceAttemptSucceeded      BackupSourceAttemptState = "succeeded"
	BackupSourceAttemptFailed         BackupSourceAttemptState = "failed"
	BackupSourceAttemptUnstarted      BackupSourceAttemptState = "unstarted"
	BackupSourceAttemptOrphaned       BackupSourceAttemptState = "orphaned"
)

type BackupFailureCode string

const (
	BackupFailureCapture          BackupFailureCode = "capture"
	BackupFailureStaging          BackupFailureCode = "staging"
	BackupFailureUpload           BackupFailureCode = "upload"
	BackupFailureHeadVerification BackupFailureCode = "head_verification"
	BackupFailurePointCommit      BackupFailureCode = "point_commit"
	BackupFailureCleanup          BackupFailureCode = "cleanup"
	BackupFailureRetention        BackupFailureCode = "retention"
	BackupFailureAborted          BackupFailureCode = "aborted"
	BackupFailureTimedOut         BackupFailureCode = "timed_out"
)

type BackupOrphanState string

const (
	BackupOrphanInspect BackupOrphanState = "inspect"
	BackupOrphanDelete  BackupOrphanState = "delete"
)

type BackupRetentionState string

const (
	BackupRetentionPending   BackupRetentionState = "pending"
	BackupRetentionScanning  BackupRetentionState = "scanning"
	BackupRetentionCompleted BackupRetentionState = "completed"
)

type BackupPruneState string

const (
	BackupPrunePending  BackupPruneState = "pending"
	BackupPruneAssigned BackupPruneState = "assigned"
)

type BackupRestoreState string

const (
	BackupRestoreQueued            BackupRestoreState = "queued"
	BackupRestoreDownloading       BackupRestoreState = "downloading"
	BackupRestoreArtifactVerified  BackupRestoreState = "artifact_verified"
	BackupRestoreConsumersStopped  BackupRestoreState = "consumers_stopped"
	BackupRestoreRestoring         BackupRestoreState = "restoring"
	BackupRestoreTreeValidated     BackupRestoreState = "tree_validated"
	BackupRestoreExchangeReady     BackupRestoreState = "exchange_ready"
	BackupRestoreExchanged         BackupRestoreState = "exchanged"
	BackupRestoreConsumersRestored BackupRestoreState = "consumers_restored"
	BackupRestoreReceiving         BackupRestoreState = "receiving"
	BackupRestoreStaged            BackupRestoreState = "staged"
	BackupRestoreApplyingDeletes   BackupRestoreState = "applying_deletes"
	BackupRestoreApplyingUpserts   BackupRestoreState = "applying_upserts"
	BackupRestoreCanonicalComplete BackupRestoreState = "canonical_complete"
	BackupRestoreMaterializing     BackupRestoreState = "materializing"
	BackupRestoreVerified          BackupRestoreState = "verified"
	BackupRestoreCompleted         BackupRestoreState = "completed"
	BackupRestoreFailedSafe        BackupRestoreState = "failed_safe"
	BackupRestoreRecoveryRequired  BackupRestoreState = "recovery_required"
)

type BackupVerificationState string

const (
	BackupVerificationPending BackupVerificationState = "pending"
	BackupVerificationPassed  BackupVerificationState = "passed"
	BackupVerificationFailed  BackupVerificationState = "failed"
)

type BackupServiceRuntimeIntent string

const (
	BackupServiceIntentRunning BackupServiceRuntimeIntent = "running"
	BackupServiceIntentStopped BackupServiceRuntimeIntent = "stopped"
	BackupServiceIntentAbsent  BackupServiceRuntimeIntent = "absent"
)

type BackupKeyRotationState string

const (
	BackupKeyRotationPrepared BackupKeyRotationState = "prepared"
	BackupKeyRotationApplied  BackupKeyRotationState = "applied"
)

type BackupScheduleCursorRecord struct {
	EnvironmentID   string    `json:"environment_id"`
	PolicyRevision  int64     `json:"policy_revision"`
	Frequency       string    `json:"frequency"`
	EnabledAt       time.Time `json:"enabled_at"`
	LastEvaluatedAt time.Time `json:"last_evaluated_at"`
	NextDueAt       time.Time `json:"next_due_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// EnvironmentMutationEpochRecord deliberately carries no counter. Rewriting
// this canonical value makes the etcd modification revision the mutation epoch.
type EnvironmentMutationEpochRecord struct {
	EnvironmentID string `json:"environment_id"`
}

type BackupDueOutcomeRecord struct {
	EnvironmentID  string           `json:"environment_id"`
	PolicyRevision int64            `json:"policy_revision"`
	ScheduledAt    time.Time        `json:"scheduled_at"`
	Outcome        BackupDueOutcome `json:"outcome"`
	TaskID         string           `json:"task_id,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
	RetainUntil    time.Time        `json:"retain_until"`
}

type BackupOperationLockRecord struct {
	EnvironmentID string              `json:"environment_id"`
	OperationID   string              `json:"operation_id"`
	TaskID        string              `json:"task_id"`
	Kind          BackupOperationKind `json:"kind"`
	CreatedAt     time.Time           `json:"created_at"`
	UpdatedAt     time.Time           `json:"updated_at"`
}

type BackupSourceTargetExclusionRecord struct {
	EnvironmentID string                 `json:"environment_id"`
	OperationID   string                 `json:"operation_id"`
	TaskID        string                 `json:"task_id"`
	OperationKind BackupOperationKind    `json:"operation_kind"`
	TargetKind    BackupSourceTargetKind `json:"target_kind"`
	TargetID      string                 `json:"target_id"`
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
}

type BackupPostgresSourceSnapshot struct {
	ConsumerEnvironmentID      string `json:"consumer_environment_id"`
	AttachID                   string `json:"attach_id"`
	AttachRevision             int64  `json:"attach_revision"`
	BackingProjectID           string `json:"backing_project_id"`
	BackingProjectRevision     int64  `json:"backing_project_revision"`
	BackingEnvironmentID       string `json:"backing_environment_id"`
	BackingEnvironmentRevision int64  `json:"backing_environment_revision"`
	BackingServiceID           string `json:"backing_service_id"`
	BackingServiceRevision     int64  `json:"backing_service_revision"`
	AttachFactsRevision        int64  `json:"attach_facts_revision"`
}

type BackupVolumeSourceSnapshot struct {
	EnvironmentID  string `json:"environment_id"`
	VolumeID       string `json:"volume_id"`
	VolumeRevision int64  `json:"volume_revision"`
}

type BackupConfigSourceSnapshot struct {
	ConfigSnapshotTaskID string `json:"config_snapshot_task_id"`
}

type BackupRunSourceSnapshot struct {
	Postgres *BackupPostgresSourceSnapshot `json:"postgres,omitempty"`
	Volume   *BackupVolumeSourceSnapshot   `json:"volume,omitempty"`
	Config   *BackupConfigSourceSnapshot   `json:"config,omitempty"`
}

type BackupRunSourceAttemptRecord struct {
	Ordinal         uint32                   `json:"ordinal"`
	SourceID        string                   `json:"source_id"`
	Kind            BackupRuntimeSourceKind  `json:"kind"`
	TargetID        string                   `json:"target_id"`
	SourceRevision  int64                    `json:"source_revision"`
	TargetRevision  int64                    `json:"target_revision"`
	Snapshot        BackupRunSourceSnapshot  `json:"snapshot"`
	Format          BackupRuntimeFormat      `json:"format"`
	RecoveryPointID string                   `json:"recovery_point_id"`
	ObjectKey       string                   `json:"object_key"`
	ServiceCount    uint32                   `json:"service_count"`
	State           BackupSourceAttemptState `json:"state"`
	SizeBytes       int64                    `json:"size_bytes,omitempty"`
	SHA256          string                   `json:"sha256,omitempty"`
	FailureCode     BackupFailureCode        `json:"failure_code,omitempty"`
}

type BackupRunRecord struct {
	TaskID                       string                         `json:"task_id"`
	OperationID                  string                         `json:"operation_id"`
	RetryOfTaskID                string                         `json:"retry_of_task_id,omitempty"`
	EnvironmentID                string                         `json:"environment_id"`
	PolicyRevision               int64                          `json:"policy_revision"`
	Initiator                    BackupRunInitiator             `json:"initiator"`
	ScheduledAt                  *time.Time                     `json:"scheduled_at,omitempty"`
	ConnectorID                  string                         `json:"connector_id"`
	ConnectorRevision            int64                          `json:"connector_revision"`
	ConnectorCredentialsRevision int64                          `json:"connector_credentials_revision"`
	Encryption                   BackupRuntimeEncryption        `json:"encryption"`
	BackupKeyRecordRevision      int64                          `json:"backup_key_record_revision,omitempty"`
	BackupKeyValueRevision       int64                          `json:"backup_key_value_revision,omitempty"`
	KeyEra                       int                            `json:"key_era,omitempty"`
	Recipient                    string                         `json:"recipient,omitempty"`
	State                        BackupRunState                 `json:"state"`
	Sources                      []BackupRunSourceAttemptRecord `json:"sources"`
	CreatedAt                    time.Time                      `json:"created_at"`
	UpdatedAt                    time.Time                      `json:"updated_at"`
}

type BackupRunVolumeServiceRecord struct {
	TaskID          string                     `json:"task_id"`
	EnvironmentID   string                     `json:"environment_id"`
	SourceID        string                     `json:"source_id"`
	SourceOrdinal   uint32                     `json:"source_ordinal"`
	ServiceOrdinal  uint32                     `json:"service_ordinal"`
	ServiceID       string                     `json:"service_id"`
	ServiceRevision int64                      `json:"service_revision"`
	PriorIntent     BackupServiceRuntimeIntent `json:"prior_intent"`
}

type BackupArtifactEvidence struct {
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type BackupRecoveryPointSnapshot struct {
	ID            string                  `json:"id"`
	EnvironmentID string                  `json:"environment_id"`
	SourceID      string                  `json:"source_id"`
	SourceKind    BackupRuntimeSourceKind `json:"source_kind"`
	TargetID      string                  `json:"target_id"`
	ConnectorID   string                  `json:"connector_id"`
	ObjectKey     string                  `json:"object_key"`
	SourceFormat  BackupRuntimeFormat     `json:"source_format"`
	Encryption    BackupRuntimeEncryption `json:"encryption"`
	KeyEra        int                     `json:"key_era,omitempty"`
	Recipient     string                  `json:"recipient,omitempty"`
	SizeBytes     int64                   `json:"size_bytes"`
	SHA256        string                  `json:"sha256"`
	CreatedAt     time.Time               `json:"created_at"`
}

type BackupRecoveryPointRecord struct {
	BackupRecoveryPointSnapshot
	VerifiedAt time.Time `json:"verified_at"`
}

type BackupOrphanRecord struct {
	Point     BackupRecoveryPointSnapshot `json:"point"`
	TaskID    string                      `json:"task_id"`
	State     BackupOrphanState           `json:"state"`
	CreatedAt time.Time                   `json:"created_at"`
	UpdatedAt time.Time                   `json:"updated_at"`
}

type BackupRetentionSweepRecord struct {
	SourceID               string               `json:"source_id"`
	TriggerRecoveryPointID string               `json:"trigger_recovery_point_id"`
	Keep                   int                  `json:"keep"`
	Revision               int64                `json:"revision"`
	Cursor                 string               `json:"cursor,omitempty"`
	State                  BackupRetentionState `json:"state"`
	CreatedAt              time.Time            `json:"created_at"`
	UpdatedAt              time.Time            `json:"updated_at"`
}

type BackupRecoveryPointPruneRecord struct {
	Point     BackupRecoveryPointSnapshot `json:"point"`
	State     BackupPruneState            `json:"state"`
	TaskID    string                      `json:"task_id,omitempty"`
	CreatedAt time.Time                   `json:"created_at"`
	UpdatedAt time.Time                   `json:"updated_at"`
}

type BackupRecoveryPointPruneDispatchRecord struct {
	TaskID           string    `json:"task_id"`
	ConnectorID      string    `json:"connector_id"`
	RecoveryPointIDs []string  `json:"recovery_point_ids"`
	CreatedAt        time.Time `json:"created_at"`
}

type BackupRestoreTargetSnapshot struct {
	Postgres *BackupPostgresSourceSnapshot `json:"postgres,omitempty"`
	Volume   *BackupVolumeSourceSnapshot   `json:"volume,omitempty"`
	Config   *BackupRestoreConfigTarget    `json:"config,omitempty"`
}

type BackupRestoreConfigTarget struct {
	EnvironmentID       string `json:"environment_id"`
	EnvironmentRevision int64  `json:"environment_revision"`
}

type BackupRestoreConfigProgress struct {
	CurrentEntryOrdinal        uint32 `json:"current_entry_ordinal"`
	NextDescriptorChunkOrdinal uint32 `json:"next_descriptor_chunk_ordinal"`
	NextValueChunkOrdinal      uint32 `json:"next_value_chunk_ordinal"`
	FinalizedEntryCount        uint32 `json:"finalized_entry_count"`
	DescriptorChunkCount       uint32 `json:"descriptor_chunk_count"`
	ValueChunkCount            uint32 `json:"value_chunk_count"`
	PlainValueBytes            uint64 `json:"plain_value_bytes"`
	DescriptorChainSHA256      string `json:"descriptor_chain_sha256,omitempty"`
	StoredValueChainSHA256     string `json:"stored_value_chain_sha256,omitempty"`
	StoredManifestSHA256       string `json:"stored_manifest_sha256,omitempty"`
	DeleteCursor               string `json:"delete_cursor,omitempty"`
	UpsertEntryOrdinal         uint32 `json:"upsert_entry_ordinal"`
	MaterializationGeneration  uint64 `json:"materialization_generation"`
}

type BackupRestoreRecord struct {
	TaskID                       string                       `json:"task_id"`
	OperationID                  string                       `json:"operation_id"`
	EnvironmentID                string                       `json:"environment_id"`
	RecoveryPointRevision        int64                        `json:"recovery_point_revision"`
	Point                        BackupRecoveryPointSnapshot  `json:"point"`
	SourceRevision               int64                        `json:"source_revision"`
	CurrentTarget                BackupRestoreTargetSnapshot  `json:"current_target"`
	ConnectorRevision            int64                        `json:"connector_revision"`
	ConnectorCredentialsRevision int64                        `json:"connector_credentials_revision"`
	ExpectedKeyRecordRevision    int64                        `json:"expected_key_record_revision,omitempty"`
	ExpectedKeyValueRevision     int64                        `json:"expected_key_value_revision,omitempty"`
	UsesOldIdentity              bool                         `json:"uses_old_identity"`
	Artifact                     *BackupArtifactEvidence      `json:"artifact,omitempty"`
	StagedTreeManifestSHA256     string                       `json:"staged_tree_manifest_sha256,omitempty"`
	State                        BackupRestoreState           `json:"state"`
	MutationStarted              bool                         `json:"mutation_started"`
	ServiceCount                 uint32                       `json:"service_count"`
	ConfigProgress               *BackupRestoreConfigProgress `json:"config_progress,omitempty"`
	Verification                 BackupVerificationState      `json:"verification"`
	CreatedAt                    time.Time                    `json:"created_at"`
	UpdatedAt                    time.Time                    `json:"updated_at"`
}

type BackupRestoreServiceRecord struct {
	TaskID          string                     `json:"task_id"`
	Ordinal         uint32                     `json:"ordinal"`
	ServiceID       string                     `json:"service_id"`
	ServiceRevision int64                      `json:"service_revision"`
	PriorIntent     BackupServiceRuntimeIntent `json:"prior_intent"`
}

type BackupKeyRotationRecord struct {
	TaskID                        string                 `json:"task_id"`
	OperationID                   string                 `json:"operation_id"`
	EnvironmentID                 string                 `json:"environment_id"`
	ExpectedCurrentRecordRevision int64                  `json:"expected_current_record_revision"`
	ExpectedCurrentValueRevision  int64                  `json:"expected_current_value_revision"`
	CurrentKeyEra                 int                    `json:"current_key_era"`
	NextKeyEra                    int                    `json:"next_key_era"`
	NextRecipient                 string                 `json:"next_recipient"`
	NextEncryptedIdentity         []byte                 `json:"next_encrypted_identity"`
	State                         BackupKeyRotationState `json:"state"`
	CreatedAt                     time.Time              `json:"created_at"`
	UpdatedAt                     time.Time              `json:"updated_at"`
}

func validateBackupScheduleCursorRecord(record BackupScheduleCursorRecord) error {
	if err := validateStableID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if record.PolicyRevision <= 0 || validateBackupPolicyFrequency(record.Frequency) != nil ||
		!validBackupRuntimeInstant(record.EnabledAt) || !validBackupRuntimeInstant(record.LastEvaluatedAt) ||
		!validBackupRuntimeInstant(record.NextDueAt) || !validBackupRuntimeInstant(record.UpdatedAt) ||
		record.LastEvaluatedAt.Before(record.EnabledAt) || !record.NextDueAt.After(record.LastEvaluatedAt) ||
		record.UpdatedAt.Before(record.EnabledAt) {
		return invalidBackupRuntimeRecord("backup schedule cursor is invalid")
	}
	return nil
}

func validateEnvironmentMutationEpochRecord(record EnvironmentMutationEpochRecord) error {
	if validateStableID(ids.KindEnvironment, record.EnvironmentID) != nil {
		return invalidBackupRuntimeRecord("environment mutation epoch is invalid")
	}
	return nil
}

func validateBackupDueOutcomeRecord(record BackupDueOutcomeRecord) error {
	if err := validateStableID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if record.PolicyRevision <= 0 || !validBackupRuntimeInstant(record.ScheduledAt) ||
		!validBackupRuntimeInstant(record.CreatedAt) || !validBackupRuntimeInstant(record.RetainUntil) ||
		record.ScheduledAt.After(record.CreatedAt) || !record.RetainUntil.After(record.CreatedAt) {
		return invalidBackupRuntimeRecord("backup due outcome lifecycle is invalid")
	}
	switch record.Outcome {
	case BackupDueDispatched:
		if validateStableID(ids.KindTask, record.TaskID) != nil {
			return invalidBackupRuntimeRecord("dispatched backup due outcome requires a task id")
		}
	case BackupDueSkippedOverlap:
		if record.TaskID != "" {
			return invalidBackupRuntimeRecord("skipped backup due outcome cannot carry a task id")
		}
	default:
		return invalidBackupRuntimeRecord("backup due outcome is invalid")
	}
	return nil
}

func validateBackupOperationLockRecord(record BackupOperationLockRecord) error {
	if validateStableID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		validateStableID(ids.KindOperation, record.OperationID) != nil ||
		validateStableID(ids.KindTask, record.TaskID) != nil || !validBackupOperationKind(record.Kind) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup operation lock is invalid")
	}
	return nil
}

func validateBackupSourceTargetExclusionRecord(record BackupSourceTargetExclusionRecord) error {
	if validateStableID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		validateStableID(ids.KindOperation, record.OperationID) != nil ||
		validateStableID(ids.KindTask, record.TaskID) != nil ||
		(record.OperationKind != BackupOperationBackup && record.OperationKind != BackupOperationRestore) ||
		validateBackupSourceTargetIdentity(record.TargetKind, record.TargetID) != nil ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup source-target exclusion is invalid")
	}
	return nil
}

func validateBackupRunRecord(record BackupRunRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindOperation, record.OperationID) != nil ||
		validateStableID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		validateStableID(ids.KindConnector, record.ConnectorID) != nil || record.PolicyRevision <= 0 ||
		record.ConnectorRevision <= 0 || record.ConnectorCredentialsRevision <= 0 ||
		!validBackupRunState(record.State) || !validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup run identity or lifecycle is invalid")
	}
	if record.RetryOfTaskID != "" {
		if validateStableID(ids.KindTask, record.RetryOfTaskID) != nil || record.RetryOfTaskID == record.TaskID {
			return invalidBackupRuntimeRecord("backup run retry task is invalid")
		}
	}
	if err := validateBackupRunInitiator(record); err != nil {
		return err
	}
	if err := validateBackupRuntimeEncryption(
		record.Encryption,
		record.BackupKeyRecordRevision,
		record.BackupKeyValueRevision,
		record.KeyEra,
		record.Recipient,
	); err != nil {
		return err
	}
	if len(record.Sources) == 0 || len(record.Sources) > MaximumBackupPolicySources {
		return invalidBackupRuntimeRecord("backup run source count is invalid")
	}
	seenSources := make(map[string]struct{}, len(record.Sources))
	seenPoints := make(map[string]struct{}, len(record.Sources))
	allSucceeded := true
	for index, source := range record.Sources {
		if source.Ordinal != uint32(index) {
			return invalidBackupRuntimeRecord("backup run source ordinals are not contiguous")
		}
		if err := validateBackupRunSourceAttempt(record.EnvironmentID, source); err != nil {
			return err
		}
		if _, exists := seenSources[source.SourceID]; exists {
			return invalidBackupRuntimeRecord("backup run source ids are not unique")
		}
		if _, exists := seenPoints[source.RecoveryPointID]; exists {
			return invalidBackupRuntimeRecord("backup run recovery point ids are not unique")
		}
		seenSources[source.SourceID] = struct{}{}
		seenPoints[source.RecoveryPointID] = struct{}{}
		allSucceeded = allSucceeded && source.State == BackupSourceAttemptSucceeded
	}
	if record.State == BackupRunCompleted && !allSucceeded {
		return invalidBackupRuntimeRecord("completed backup run has an incomplete source")
	}
	return validateBackupRunAttemptTable(record)
}

func validateBackupRunAttemptTable(record BackupRunRecord) error {
	if record.State == BackupRunCompleted {
		return nil
	}
	boundary := -1
	for index, source := range record.Sources {
		if source.State == BackupSourceAttemptSucceeded {
			if boundary >= 0 {
				return invalidBackupRuntimeRecord("backup run violates fail-fast source order")
			}
			continue
		}
		if boundary < 0 {
			boundary = index
		}
	}
	if boundary < 0 {
		return invalidBackupRuntimeRecord("non-completed backup run has no remaining source")
	}
	boundarySource := record.Sources[boundary]
	suffix := record.Sources[boundary+1:]
	suffixAll := func(state BackupSourceAttemptState) bool {
		for _, source := range suffix {
			if source.State != state || source.FailureCode != "" {
				return false
			}
		}
		return true
	}
	switch record.State {
	case BackupRunQueued:
		if boundarySource.FailureCode != "" || !suffixAll(BackupSourceAttemptPending) ||
			(boundarySource.State != BackupSourceAttemptPending &&
				boundarySource.State != BackupSourceAttemptPointCommitted) {
			return invalidBackupRuntimeRecord("queued backup run checkpoint is invalid")
		}
	case BackupRunRunning:
		if boundarySource.FailureCode != "" || !suffixAll(BackupSourceAttemptPending) ||
			!activeBackupSourceAttemptState(boundarySource.State) {
			return invalidBackupRuntimeRecord("running backup run checkpoint is invalid")
		}
	case BackupRunFailed:
		if boundarySource.FailureCode == "" || !suffixAll(BackupSourceAttemptUnstarted) ||
			!failedBackupSourceCheckpoint(boundarySource.State, boundarySource.FailureCode) ||
			boundarySource.FailureCode == BackupFailureAborted ||
			boundarySource.FailureCode == BackupFailureTimedOut {
			return invalidBackupRuntimeRecord("failed backup run checkpoint is invalid")
		}
	case BackupRunAborted:
		if boundarySource.State != BackupSourceAttemptFailed || boundarySource.FailureCode != BackupFailureAborted ||
			!suffixAll(BackupSourceAttemptUnstarted) {
			return invalidBackupRuntimeRecord("aborted backup run checkpoint is invalid")
		}
	case BackupRunTimedOut:
		if boundarySource.State != BackupSourceAttemptFailed || boundarySource.FailureCode != BackupFailureTimedOut ||
			!suffixAll(BackupSourceAttemptUnstarted) {
			return invalidBackupRuntimeRecord("timed-out backup run checkpoint is invalid")
		}
	}
	return nil
}

func validateBackupRunInitiator(record BackupRunRecord) error {
	switch record.Initiator {
	case BackupRunInitiatorOperator:
		if record.ScheduledAt != nil {
			return invalidBackupRuntimeRecord("operator backup run cannot carry a scheduled time")
		}
	case BackupRunInitiatorSchedule:
		if record.ScheduledAt == nil || !validBackupRuntimeInstant(*record.ScheduledAt) ||
			record.ScheduledAt.After(record.CreatedAt) {
			return invalidBackupRuntimeRecord("scheduled backup run time is invalid")
		}
	default:
		return invalidBackupRuntimeRecord("backup run initiator is invalid")
	}
	return nil
}

func validateBackupRunSourceAttempt(
	environmentID string,
	record BackupRunSourceAttemptRecord,
) error {
	if validateStableID(ids.KindBackupSource, record.SourceID) != nil || record.SourceRevision <= 0 ||
		record.TargetRevision <= 0 || validateStableID(ids.KindRecoveryPoint, record.RecoveryPointID) != nil ||
		!validBackupSourceAttemptState(record.State) ||
		!validBackupObjectKey(record.ObjectKey, environmentID, record.SourceID, record.RecoveryPointID) {
		return invalidBackupRuntimeRecord("backup run source attempt is invalid")
	}
	if err := validateBackupSourceIdentity(record.Kind, record.TargetID, record.Format); err != nil {
		return err
	}
	if record.Kind == BackupRuntimeSourceConfig && record.TargetID != environmentID {
		return invalidBackupRuntimeRecord("config backup source target does not match its Environment")
	}
	if err := validateBackupRunSourceSnapshot(
		record.Kind,
		environmentID,
		record.TargetID,
		record.TargetRevision,
		record.Snapshot,
	); err != nil {
		return err
	}
	if (record.SizeBytes == 0) != (record.SHA256 == "") || record.SizeBytes < 0 ||
		(record.SHA256 != "" && !validSHA256(record.SHA256)) {
		return invalidBackupRuntimeRecord("backup run source artifact evidence is invalid")
	}
	if sourceAttemptRequiresArtifact(record.State) && (record.SizeBytes <= 0 || record.SHA256 == "") {
		return invalidBackupRuntimeRecord("backup run source state requires artifact evidence")
	}
	if sourceAttemptForbidsArtifact(record.State) && (record.SizeBytes != 0 || record.SHA256 != "") {
		return invalidBackupRuntimeRecord("backup run source state cannot carry artifact evidence")
	}
	if record.Kind != BackupRuntimeSourceVolume && record.ServiceCount != 0 {
		return invalidBackupRuntimeRecord("non-volume backup source cannot carry service checkpoints")
	}
	if !validBackupFailureCodeForAttempt(record.State, record.FailureCode) {
		return invalidBackupRuntimeRecord("backup source failure checkpoint is invalid")
	}
	return nil
}

func validateBackupRunSourceSnapshot(
	kind BackupRuntimeSourceKind,
	environmentID string,
	targetID string,
	targetRevision int64,
	snapshot BackupRunSourceSnapshot,
) error {
	count := pointerCount(snapshot.Postgres != nil, snapshot.Volume != nil, snapshot.Config != nil)
	if count != 1 {
		return invalidBackupRuntimeRecord("backup source snapshot must contain exactly one kind")
	}
	switch kind {
	case BackupRuntimeSourceAttach:
		if snapshot.Postgres == nil || validateBackupPostgresSnapshot(*snapshot.Postgres) != nil ||
			snapshot.Postgres.ConsumerEnvironmentID != environmentID || snapshot.Postgres.AttachID != targetID ||
			snapshot.Postgres.AttachRevision != targetRevision {
			return invalidBackupRuntimeRecord("postgres backup source snapshot is invalid")
		}
	case BackupRuntimeSourceVolume:
		if snapshot.Volume == nil || validateBackupVolumeSnapshot(*snapshot.Volume) != nil ||
			snapshot.Volume.EnvironmentID != environmentID || snapshot.Volume.VolumeID != targetID ||
			snapshot.Volume.VolumeRevision != targetRevision {
			return invalidBackupRuntimeRecord("volume backup source snapshot is invalid")
		}
	case BackupRuntimeSourceConfig:
		if snapshot.Config == nil || validateStableID(
			ids.KindTask,
			snapshot.Config.ConfigSnapshotTaskID,
		) != nil {
			return invalidBackupRuntimeRecord("config backup source snapshot is invalid")
		}
	default:
		return invalidBackupRuntimeRecord("backup source snapshot kind is invalid")
	}
	return nil
}

func validateBackupRunVolumeServiceRecord(record BackupRunVolumeServiceRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		validateStableID(ids.KindBackupSource, record.SourceID) != nil ||
		validateStableID(ids.KindService, record.ServiceID) != nil || record.ServiceRevision <= 0 ||
		!validBackupServiceRuntimeIntent(record.PriorIntent) {
		return invalidBackupRuntimeRecord("backup run Volume service checkpoint is invalid")
	}
	return nil
}

func validateBackupRecoveryPointSnapshot(record BackupRecoveryPointSnapshot) error {
	if validateStableID(ids.KindRecoveryPoint, record.ID) != nil ||
		validateStableID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		validateStableID(ids.KindBackupSource, record.SourceID) != nil ||
		validateStableID(ids.KindConnector, record.ConnectorID) != nil || record.SizeBytes <= 0 ||
		!validSHA256(record.SHA256) || !validBackupRuntimeInstant(record.CreatedAt) ||
		!validBackupObjectKey(record.ObjectKey, record.EnvironmentID, record.SourceID, record.ID) {
		return invalidBackupRuntimeRecord("recovery point snapshot is invalid")
	}
	if err := validateBackupSourceIdentity(record.SourceKind, record.TargetID, record.SourceFormat); err != nil {
		return err
	}
	if record.SourceKind == BackupRuntimeSourceConfig && record.TargetID != record.EnvironmentID {
		return invalidBackupRuntimeRecord("config recovery point target does not match its Environment")
	}
	switch record.Encryption {
	case BackupRuntimeEncryptionAge:
		return validateBackupRuntimeEncryption(record.Encryption, 1, 1, record.KeyEra, record.Recipient)
	case BackupRuntimeEncryptionNone:
		return validateBackupRuntimeEncryption(record.Encryption, 0, 0, record.KeyEra, record.Recipient)
	default:
		return invalidBackupRuntimeRecord("recovery point encryption is invalid")
	}
}

func validateBackupRecoveryPointRecord(record BackupRecoveryPointRecord) error {
	if err := validateBackupRecoveryPointSnapshot(record.BackupRecoveryPointSnapshot); err != nil {
		return err
	}
	if !validBackupRuntimeInstant(record.VerifiedAt) || record.VerifiedAt.Before(record.CreatedAt) {
		return invalidBackupRuntimeRecord("recovery point verification time is invalid")
	}
	return nil
}

func validateBackupOrphanRecord(record BackupOrphanRecord) error {
	if err := validateBackupRecoveryPointSnapshot(record.Point); err != nil {
		return err
	}
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		(record.State != BackupOrphanInspect && record.State != BackupOrphanDelete) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) ||
		record.CreatedAt.Before(record.Point.CreatedAt) {
		return invalidBackupRuntimeRecord("backup orphan is invalid")
	}
	return nil
}

func validateBackupRetentionSweepRecord(record BackupRetentionSweepRecord) error {
	if validateStableID(ids.KindBackupSource, record.SourceID) != nil ||
		validateStableID(ids.KindRecoveryPoint, record.TriggerRecoveryPointID) != nil || record.Keep <= 0 ||
		record.Revision <= 0 || !validBackupRetentionState(record.State) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup retention sweep is invalid")
	}
	if record.Cursor != "" && validateStableID(ids.KindRecoveryPoint, record.Cursor) != nil {
		return invalidBackupRuntimeRecord("backup retention cursor is invalid")
	}
	if record.State == BackupRetentionPending && record.Cursor != "" {
		return invalidBackupRuntimeRecord("pending backup retention sweep cannot carry a cursor")
	}
	return nil
}

func validateBackupRecoveryPointPruneRecord(record BackupRecoveryPointPruneRecord) error {
	if err := validateBackupRecoveryPointSnapshot(record.Point); err != nil {
		return err
	}
	if !validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("recovery point prune lifecycle is invalid")
	}
	switch record.State {
	case BackupPrunePending:
		if record.TaskID != "" {
			return invalidBackupRuntimeRecord("pending recovery point prune cannot carry a task id")
		}
	case BackupPruneAssigned:
		if validateStableID(ids.KindTask, record.TaskID) != nil {
			return invalidBackupRuntimeRecord("assigned recovery point prune requires a task id")
		}
	default:
		return invalidBackupRuntimeRecord("recovery point prune state is invalid")
	}
	return nil
}

func validateBackupRecoveryPointPruneDispatchRecord(record BackupRecoveryPointPruneDispatchRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindConnector, record.ConnectorID) != nil ||
		!validBackupRuntimeInstant(record.CreatedAt) || len(record.RecoveryPointIDs) == 0 ||
		len(record.RecoveryPointIDs) > maximumBackupPruneDispatchPoints {
		return invalidBackupRuntimeRecord("recovery point prune dispatch is invalid")
	}
	seen := make(map[string]struct{}, len(record.RecoveryPointIDs))
	for _, recoveryPointID := range record.RecoveryPointIDs {
		if validateStableID(ids.KindRecoveryPoint, recoveryPointID) != nil {
			return invalidBackupRuntimeRecord("recovery point prune dispatch id is invalid")
		}
		if _, exists := seen[recoveryPointID]; exists {
			return invalidBackupRuntimeRecord("recovery point prune dispatch ids are not unique")
		}
		seen[recoveryPointID] = struct{}{}
	}
	return nil
}

func validateBackupRestoreRecord(record BackupRestoreRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindOperation, record.OperationID) != nil ||
		validateStableID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		record.RecoveryPointRevision <= 0 || record.SourceRevision <= 0 || record.ConnectorRevision <= 0 ||
		record.ConnectorCredentialsRevision <= 0 || !validBackupRestoreState(record.State) ||
		!validBackupVerificationState(record.Verification) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup restore identity or lifecycle is invalid")
	}
	if err := validateBackupRecoveryPointSnapshot(record.Point); err != nil {
		return err
	}
	if record.Point.EnvironmentID != record.EnvironmentID {
		return invalidBackupRuntimeRecord("backup restore point belongs to another environment")
	}
	if err := validateBackupRestoreTarget(record.Point, record.CurrentTarget); err != nil {
		return err
	}
	if err := validateBackupRestoreKeyReferences(record); err != nil {
		return err
	}
	if record.Artifact != nil {
		if !validBackupArtifact(*record.Artifact) || record.Artifact.SizeBytes != record.Point.SizeBytes ||
			record.Artifact.SHA256 != record.Point.SHA256 {
			return invalidBackupRuntimeRecord("backup restore artifact evidence does not match its point")
		}
	} else if restoreStateRequiresArtifact(record.State) {
		return invalidBackupRuntimeRecord("backup restore state requires verified artifact evidence")
	}
	if err := validateBackupRestoreStateTable(record); err != nil {
		return err
	}
	if err := validateBackupRestoreVolumeManifest(record); err != nil {
		return err
	}
	if err := validateBackupRestoreProgress(record); err != nil {
		return err
	}
	return validateBackupRestoreVerification(record)
}

func validateBackupRestoreTarget(
	point BackupRecoveryPointSnapshot,
	target BackupRestoreTargetSnapshot,
) error {
	if pointerCount(target.Postgres != nil, target.Volume != nil, target.Config != nil) != 1 {
		return invalidBackupRuntimeRecord("backup restore target must contain exactly one kind")
	}
	switch point.SourceKind {
	case BackupRuntimeSourceAttach:
		if target.Postgres == nil || validateBackupPostgresSnapshot(*target.Postgres) != nil ||
			target.Postgres.ConsumerEnvironmentID != point.EnvironmentID || target.Postgres.AttachID != point.TargetID {
			return invalidBackupRuntimeRecord("postgres restore target is invalid")
		}
	case BackupRuntimeSourceVolume:
		if target.Volume == nil || validateBackupVolumeSnapshot(*target.Volume) != nil ||
			target.Volume.EnvironmentID != point.EnvironmentID || target.Volume.VolumeID != point.TargetID {
			return invalidBackupRuntimeRecord("volume restore target is invalid")
		}
	case BackupRuntimeSourceConfig:
		if target.Config == nil || validateStableID(ids.KindEnvironment, target.Config.EnvironmentID) != nil ||
			target.Config.EnvironmentRevision <= 0 || target.Config.EnvironmentID != point.TargetID {
			return invalidBackupRuntimeRecord("config restore target is invalid")
		}
	default:
		return invalidBackupRuntimeRecord("backup restore target kind is invalid")
	}
	return nil
}

func validateBackupRestoreStateTable(record BackupRestoreRecord) error {
	allowed := false
	expectedMutation := false
	switch record.Point.SourceKind {
	case BackupRuntimeSourceAttach:
		switch record.State {
		case BackupRestoreQueued, BackupRestoreDownloading, BackupRestoreArtifactVerified,
			BackupRestoreConsumersStopped, BackupRestoreFailedSafe:
			allowed = true
		case BackupRestoreRestoring, BackupRestoreConsumersRestored, BackupRestoreVerified,
			BackupRestoreCompleted, BackupRestoreRecoveryRequired:
			allowed, expectedMutation = true, true
		}
	case BackupRuntimeSourceVolume:
		switch record.State {
		case BackupRestoreQueued, BackupRestoreDownloading, BackupRestoreArtifactVerified,
			BackupRestoreConsumersStopped, BackupRestoreRestoring, BackupRestoreTreeValidated,
			BackupRestoreExchangeReady, BackupRestoreFailedSafe:
			allowed = true
		case BackupRestoreExchanged, BackupRestoreConsumersRestored, BackupRestoreVerified,
			BackupRestoreCompleted, BackupRestoreRecoveryRequired:
			allowed, expectedMutation = true, true
		}
	case BackupRuntimeSourceConfig:
		switch record.State {
		case BackupRestoreQueued, BackupRestoreDownloading, BackupRestoreArtifactVerified,
			BackupRestoreReceiving, BackupRestoreStaged, BackupRestoreApplyingDeletes,
			BackupRestoreApplyingUpserts, BackupRestoreFailedSafe:
			allowed = true
		case BackupRestoreCanonicalComplete, BackupRestoreMaterializing, BackupRestoreVerified,
			BackupRestoreCompleted, BackupRestoreRecoveryRequired:
			allowed, expectedMutation = true, true
		}
	}
	if !allowed || record.MutationStarted != expectedMutation {
		return invalidBackupRuntimeRecord("backup restore source state or mutation checkpoint is invalid")
	}
	return nil
}

func validateBackupRestoreVolumeManifest(record BackupRestoreRecord) error {
	if record.Point.SourceKind != BackupRuntimeSourceVolume {
		if record.StagedTreeManifestSHA256 != "" {
			return invalidBackupRuntimeRecord("non-Volume restore cannot carry a staged-tree manifest")
		}
		return nil
	}
	if record.StagedTreeManifestSHA256 != "" && !validSHA256(record.StagedTreeManifestSHA256) {
		return invalidBackupRuntimeRecord("Volume restore staged-tree manifest is invalid")
	}
	switch record.State {
	case BackupRestoreTreeValidated, BackupRestoreExchangeReady, BackupRestoreExchanged,
		BackupRestoreConsumersRestored, BackupRestoreVerified, BackupRestoreCompleted,
		BackupRestoreRecoveryRequired:
		if record.StagedTreeManifestSHA256 == "" {
			return invalidBackupRuntimeRecord("Volume restore state requires a staged-tree manifest")
		}
	}
	return nil
}

func validateBackupRestoreKeyReferences(record BackupRestoreRecord) error {
	if record.Point.Encryption == BackupRuntimeEncryptionNone {
		if record.UsesOldIdentity || record.ExpectedKeyRecordRevision != 0 ||
			record.ExpectedKeyValueRevision != 0 {
			return invalidBackupRuntimeRecord("unencrypted restore cannot carry age key references")
		}
		return nil
	}
	if record.UsesOldIdentity {
		if record.ExpectedKeyRecordRevision != 0 || record.ExpectedKeyValueRevision != 0 {
			return invalidBackupRuntimeRecord("old-identity restore cannot carry current key revisions")
		}
		return nil
	}
	if record.ExpectedKeyRecordRevision <= 0 || record.ExpectedKeyValueRevision <= 0 {
		return invalidBackupRuntimeRecord("current-era restore requires current key revisions")
	}
	return nil
}

func validateBackupRestoreProgress(record BackupRestoreRecord) error {
	if record.Point.SourceKind == BackupRuntimeSourceConfig {
		if record.ServiceCount != 0 || record.ConfigProgress == nil {
			return invalidBackupRuntimeRecord("config restore progress is invalid")
		}
		return validateBackupRestoreConfigProgress(record.State, *record.ConfigProgress)
	}
	if record.ConfigProgress != nil {
		return invalidBackupRuntimeRecord("non-config restore cannot carry config progress")
	}
	return nil
}

func validateBackupRestoreConfigProgress(
	state BackupRestoreState,
	progress BackupRestoreConfigProgress,
) error {
	if progress.NextDescriptorChunkOrdinal > progress.DescriptorChunkCount ||
		progress.NextValueChunkOrdinal > progress.ValueChunkCount {
		return invalidBackupRuntimeRecord("config restore chunk cursor exceeds received chunks")
	}
	if progress.MaterializationGeneration == 0 || progress.CurrentEntryOrdinal != progress.FinalizedEntryCount ||
		(progress.FinalizedEntryCount > 0 && progress.DescriptorChunkCount < progress.FinalizedEntryCount) ||
		!optionalDigestMatchesCount(progress.DescriptorChainSHA256, progress.DescriptorChunkCount) ||
		!optionalDigestMatchesCount(progress.StoredValueChainSHA256, progress.ValueChunkCount) ||
		(progress.StoredManifestSHA256 != "" && !validSHA256(progress.StoredManifestSHA256)) {
		return invalidBackupRuntimeRecord("config restore progress digest is invalid")
	}
	if progress.DeleteCursor != "" && validateStableID(ids.KindEnvEntry, progress.DeleteCursor) != nil {
		return invalidBackupRuntimeRecord("config restore delete cursor is invalid")
	}
	if configRestoreStateRequiresManifest(state) && progress.StoredManifestSHA256 == "" {
		return invalidBackupRuntimeRecord("config restore state requires a manifest digest")
	}
	preManifest := state == BackupRestoreQueued || state == BackupRestoreDownloading ||
		state == BackupRestoreArtifactVerified || state == BackupRestoreReceiving
	if preManifest && progress.StoredManifestSHA256 != "" {
		return invalidBackupRuntimeRecord("config restore has a premature manifest digest")
	}
	if state != BackupRestoreReceiving &&
		(progress.NextDescriptorChunkOrdinal != 0 || progress.NextValueChunkOrdinal != 0) {
		return invalidBackupRuntimeRecord("config restore has an out-of-phase chunk cursor")
	}
	if (state == BackupRestoreQueued || state == BackupRestoreDownloading || state == BackupRestoreArtifactVerified) &&
		(progress.FinalizedEntryCount != 0 || progress.DescriptorChunkCount != 0 || progress.ValueChunkCount != 0 ||
			progress.PlainValueBytes != 0 || progress.DescriptorChainSHA256 != "" ||
			progress.StoredValueChainSHA256 != "" || progress.DeleteCursor != "" ||
			progress.UpsertEntryOrdinal != 0) {
		return invalidBackupRuntimeRecord("config restore has progress before receiving")
	}
	if (state == BackupRestoreReceiving || state == BackupRestoreStaged) &&
		(progress.DeleteCursor != "" || progress.UpsertEntryOrdinal != 0) {
		return invalidBackupRuntimeRecord("config restore has an out-of-phase apply cursor")
	}
	if state == BackupRestoreApplyingDeletes && progress.UpsertEntryOrdinal != 0 {
		return invalidBackupRuntimeRecord("config restore started upserts before deletes completed")
	}
	if progress.UpsertEntryOrdinal > progress.FinalizedEntryCount {
		return invalidBackupRuntimeRecord("config restore upsert cursor exceeds staged Entries")
	}
	if (state == BackupRestoreCanonicalComplete || state == BackupRestoreMaterializing ||
		state == BackupRestoreVerified || state == BackupRestoreCompleted ||
		state == BackupRestoreRecoveryRequired) && progress.UpsertEntryOrdinal != progress.FinalizedEntryCount {
		return invalidBackupRuntimeRecord("config restore canonical switch precedes complete upserts")
	}
	return nil
}

func validateBackupRestoreVerification(record BackupRestoreRecord) error {
	switch record.Verification {
	case BackupVerificationPassed:
		if record.State != BackupRestoreVerified && record.State != BackupRestoreCompleted {
			return invalidBackupRuntimeRecord("passed restore verification has an invalid state")
		}
	case BackupVerificationFailed:
		if record.State != BackupRestoreFailedSafe && record.State != BackupRestoreRecoveryRequired {
			return invalidBackupRuntimeRecord("failed restore verification has an invalid state")
		}
	case BackupVerificationPending:
		if record.State == BackupRestoreVerified || record.State == BackupRestoreCompleted {
			return invalidBackupRuntimeRecord("completed restore requires passed verification")
		}
	}
	if record.State == BackupRestoreRecoveryRequired && !record.MutationStarted {
		return invalidBackupRuntimeRecord("recovery-required restore must have started mutation")
	}
	return nil
}

func validateBackupRestoreServiceRecord(record BackupRestoreServiceRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindService, record.ServiceID) != nil || record.ServiceRevision <= 0 ||
		!validBackupServiceRuntimeIntent(record.PriorIntent) {
		return invalidBackupRuntimeRecord("backup restore service is invalid")
	}
	return nil
}

func validateBackupKeyRotationRecord(record BackupKeyRotationRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindOperation, record.OperationID) != nil ||
		validateStableID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		record.ExpectedCurrentRecordRevision <= 0 || record.ExpectedCurrentValueRevision <= 0 ||
		record.CurrentKeyEra <= 0 || record.NextKeyEra != record.CurrentKeyEra+1 ||
		!validBackupRecipient(record.NextRecipient) ||
		(record.State != BackupKeyRotationPrepared && record.State != BackupKeyRotationApplied) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup key rotation is invalid")
	}
	if record.State == BackupKeyRotationPrepared && (len(record.NextEncryptedIdentity) == 0 ||
		len(record.NextEncryptedIdentity) > maximumBackupKeyCiphertextLen) {
		return invalidBackupRuntimeRecord("prepared backup key rotation requires a wrapped identity")
	}
	if record.State == BackupKeyRotationApplied && len(record.NextEncryptedIdentity) != 0 {
		return invalidBackupRuntimeRecord("applied backup key rotation retains a wrapped identity")
	}
	return nil
}

func validateBackupPostgresSnapshot(snapshot BackupPostgresSourceSnapshot) error {
	if validateStableID(ids.KindEnvironment, snapshot.ConsumerEnvironmentID) != nil ||
		validateStableID(ids.KindAttach, snapshot.AttachID) != nil || snapshot.AttachRevision <= 0 ||
		validateStableID(ids.KindProject, snapshot.BackingProjectID) != nil ||
		snapshot.BackingProjectRevision <= 0 ||
		validateStableID(ids.KindEnvironment, snapshot.BackingEnvironmentID) != nil ||
		snapshot.BackingEnvironmentRevision <= 0 ||
		validateStableID(ids.KindService, snapshot.BackingServiceID) != nil ||
		snapshot.BackingServiceRevision <= 0 || snapshot.AttachFactsRevision <= 0 {
		return invalidBackupRuntimeRecord("postgres source snapshot is invalid")
	}
	return nil
}

func validateBackupVolumeSnapshot(snapshot BackupVolumeSourceSnapshot) error {
	if validateStableID(ids.KindEnvironment, snapshot.EnvironmentID) != nil ||
		validateStableID(ids.KindVolume, snapshot.VolumeID) != nil || snapshot.VolumeRevision <= 0 {
		return invalidBackupRuntimeRecord("volume source snapshot is invalid")
	}
	return nil
}

func validateBackupSourceIdentity(
	kind BackupRuntimeSourceKind,
	targetID string,
	format BackupRuntimeFormat,
) error {
	switch kind {
	case BackupRuntimeSourceAttach:
		if validateStableID(ids.KindAttach, targetID) != nil || format != BackupRuntimeFormatPostgres {
			return invalidBackupRuntimeRecord("postgres backup source identity is invalid")
		}
	case BackupRuntimeSourceVolume:
		if validateStableID(ids.KindVolume, targetID) != nil || format != BackupRuntimeFormatVolume {
			return invalidBackupRuntimeRecord("volume backup source identity is invalid")
		}
	case BackupRuntimeSourceConfig:
		if validateStableID(ids.KindEnvironment, targetID) != nil || format != BackupRuntimeFormatConfig {
			return invalidBackupRuntimeRecord("config backup source identity is invalid")
		}
	default:
		return invalidBackupRuntimeRecord("backup source kind is invalid")
	}
	return nil
}

func validateBackupRuntimeEncryption(
	encryption BackupRuntimeEncryption,
	keyRecordRevision int64,
	keyValueRevision int64,
	keyEra int,
	recipient string,
) error {
	switch encryption {
	case BackupRuntimeEncryptionAge:
		if keyRecordRevision <= 0 || keyValueRevision <= 0 || keyEra <= 0 || !validBackupRecipient(recipient) {
			return invalidBackupRuntimeRecord("age backup encryption evidence is invalid")
		}
	case BackupRuntimeEncryptionNone:
		if keyRecordRevision != 0 || keyValueRevision != 0 || keyEra != 0 || recipient != "" {
			return invalidBackupRuntimeRecord("unencrypted backup cannot carry age key evidence")
		}
	default:
		return invalidBackupRuntimeRecord("backup encryption is invalid")
	}
	return nil
}

func validBackupRecipient(value string) bool {
	recipient, err := age.ParseX25519Recipient(value)
	return err == nil && recipient.String() == value
}

func validBackupObjectKey(value string, environmentID string, sourceID string, recoveryPointID string) bool {
	if value == "" || len(value) > maximumBackupObjectKeyBytes || !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') || strings.Contains(value, `\`) || strings.HasPrefix(value, "/") ||
		path.Clean(value) != value {
		return false
	}
	suffix := environmentID + "/" + sourceID + "/" + recoveryPointID + "/artifact.bin"
	return value == suffix || strings.HasSuffix(value, "/"+suffix)
}

func validBackupFailureCodeForAttempt(state BackupSourceAttemptState, code BackupFailureCode) bool {
	if code == "" {
		return state != BackupSourceAttemptFailed
	}
	return failedBackupSourceCheckpoint(state, code)
}

func failedBackupSourceCheckpoint(state BackupSourceAttemptState, code BackupFailureCode) bool {
	switch state {
	case BackupSourceAttemptFailed:
		return code == BackupFailureCapture || code == BackupFailureStaging || code == BackupFailureUpload ||
			code == BackupFailureHeadVerification || code == BackupFailurePointCommit ||
			code == BackupFailureAborted || code == BackupFailureTimedOut
	case BackupSourceAttemptOrphaned:
		return code == BackupFailureHeadVerification || code == BackupFailurePointCommit
	case BackupSourceAttemptPointCommitted:
		return code == BackupFailureRetention
	case BackupSourceAttemptCleanupPending:
		return code == BackupFailureCleanup
	default:
		return false
	}
}

func validBackupArtifact(value BackupArtifactEvidence) bool {
	return value.SizeBytes > 0 && validSHA256(value.SHA256)
}

func validBackupRuntimeInstant(value time.Time) bool {
	if value.IsZero() || value.Location() != time.UTC {
		return false
	}
	nanoseconds := value.UnixNano()
	return nanoseconds > 0 && time.Unix(0, nanoseconds).UTC().Equal(value)
}

func validBackupRuntimeLifecycle(createdAt time.Time, updatedAt time.Time) bool {
	return validBackupRuntimeInstant(createdAt) && validBackupRuntimeInstant(updatedAt) &&
		!updatedAt.Before(createdAt)
}

func validBackupOperationKind(kind BackupOperationKind) bool {
	return kind == BackupOperationBackup || kind == BackupOperationRestore || kind == BackupOperationRotation ||
		kind == BackupOperationDeletion
}

func validateBackupSourceTargetIdentity(kind BackupSourceTargetKind, stableID string) error {
	var idKind ids.Kind
	switch kind {
	case BackupSourceTargetAttach:
		idKind = ids.KindAttach
	case BackupSourceTargetVolume:
		idKind = ids.KindVolume
	case BackupSourceTargetBackingProject:
		idKind = ids.KindProject
	case BackupSourceTargetBackingEnvironment:
		idKind = ids.KindEnvironment
	case BackupSourceTargetBackingService:
		idKind = ids.KindService
	default:
		return invalidBackupRuntimeRecord("backup source-target kind is invalid")
	}
	if validateStableID(idKind, stableID) != nil {
		return invalidBackupRuntimeRecord("backup source-target identity is invalid")
	}
	return nil
}

func validBackupRunState(state BackupRunState) bool {
	switch state {
	case BackupRunQueued, BackupRunRunning, BackupRunFailed, BackupRunCompleted,
		BackupRunAborted, BackupRunTimedOut:
		return true
	default:
		return false
	}
}

func validBackupSourceAttemptState(state BackupSourceAttemptState) bool {
	switch state {
	case BackupSourceAttemptPending, BackupSourceAttemptCapturing, BackupSourceAttemptReady,
		BackupSourceAttemptStaged, BackupSourceAttemptPointCommitted, BackupSourceAttemptCleanupPending,
		BackupSourceAttemptSucceeded, BackupSourceAttemptFailed, BackupSourceAttemptUnstarted,
		BackupSourceAttemptOrphaned:
		return true
	default:
		return false
	}
}

func sourceAttemptRequiresArtifact(state BackupSourceAttemptState) bool {
	switch state {
	case BackupSourceAttemptStaged, BackupSourceAttemptPointCommitted, BackupSourceAttemptCleanupPending,
		BackupSourceAttemptSucceeded, BackupSourceAttemptOrphaned:
		return true
	default:
		return false
	}
}

func sourceAttemptForbidsArtifact(state BackupSourceAttemptState) bool {
	return state == BackupSourceAttemptPending || state == BackupSourceAttemptCapturing ||
		state == BackupSourceAttemptReady || state == BackupSourceAttemptUnstarted
}

func activeBackupSourceAttemptState(state BackupSourceAttemptState) bool {
	switch state {
	case BackupSourceAttemptPending, BackupSourceAttemptCapturing, BackupSourceAttemptReady,
		BackupSourceAttemptStaged, BackupSourceAttemptPointCommitted, BackupSourceAttemptCleanupPending,
		BackupSourceAttemptOrphaned:
		return true
	default:
		return false
	}
}

func validBackupRetentionState(state BackupRetentionState) bool {
	return state == BackupRetentionPending || state == BackupRetentionScanning || state == BackupRetentionCompleted
}

func validBackupRestoreState(state BackupRestoreState) bool {
	switch state {
	case BackupRestoreQueued, BackupRestoreDownloading, BackupRestoreArtifactVerified,
		BackupRestoreConsumersStopped, BackupRestoreRestoring, BackupRestoreTreeValidated,
		BackupRestoreExchangeReady, BackupRestoreExchanged, BackupRestoreConsumersRestored,
		BackupRestoreReceiving, BackupRestoreStaged, BackupRestoreApplyingDeletes,
		BackupRestoreApplyingUpserts, BackupRestoreCanonicalComplete, BackupRestoreMaterializing,
		BackupRestoreVerified, BackupRestoreCompleted, BackupRestoreFailedSafe,
		BackupRestoreRecoveryRequired:
		return true
	default:
		return false
	}
}

func restoreStateRequiresArtifact(state BackupRestoreState) bool {
	switch state {
	case BackupRestoreArtifactVerified, BackupRestoreConsumersStopped, BackupRestoreRestoring,
		BackupRestoreTreeValidated, BackupRestoreExchangeReady, BackupRestoreExchanged,
		BackupRestoreConsumersRestored, BackupRestoreReceiving, BackupRestoreStaged,
		BackupRestoreApplyingDeletes, BackupRestoreApplyingUpserts, BackupRestoreCanonicalComplete,
		BackupRestoreMaterializing, BackupRestoreVerified, BackupRestoreCompleted,
		BackupRestoreRecoveryRequired:
		return true
	default:
		return false
	}
}

func configRestoreStateRequiresManifest(state BackupRestoreState) bool {
	switch state {
	case BackupRestoreStaged, BackupRestoreApplyingDeletes, BackupRestoreApplyingUpserts,
		BackupRestoreCanonicalComplete, BackupRestoreMaterializing, BackupRestoreVerified,
		BackupRestoreCompleted, BackupRestoreRecoveryRequired:
		return true
	default:
		return false
	}
}

func validBackupVerificationState(state BackupVerificationState) bool {
	return state == BackupVerificationPending || state == BackupVerificationPassed ||
		state == BackupVerificationFailed
}

func validBackupServiceRuntimeIntent(intent BackupServiceRuntimeIntent) bool {
	return intent == BackupServiceIntentRunning || intent == BackupServiceIntentStopped ||
		intent == BackupServiceIntentAbsent
}

func optionalDigestMatchesCount(digest string, count uint32) bool {
	if count == 0 {
		return digest == ""
	}
	return validSHA256(digest)
}

func pointerCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func invalidBackupRuntimeRecord(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}

func corruptBackupRuntimeRecord() error {
	return errs.New(errs.KindInternal, "backup runtime durable record is corrupt")
}

func encodeBackupRuntimeRecord[T any](kind string, record T, validate func(T) error) ([]byte, error) {
	if err := validate(record); err != nil {
		return nil, err
	}
	value, err := encodeEnvelope(kind, record)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumBackupRuntimeRecordBytes {
		return nil, invalidBackupRuntimeRecord("backup runtime durable record exceeds 256 KiB")
	}
	return value, nil
}

func decodeBackupRuntimeRecord[T any](value []byte, kind string, validate func(T) error) (T, error) {
	var zero T
	if len(value) == 0 || len(value) > maximumBackupRuntimeRecordBytes {
		return zero, corruptBackupRuntimeRecord()
	}
	record, err := decodeEnvelope[T](value, kind)
	if err != nil {
		return zero, err
	}
	if validate(record) != nil {
		return zero, corruptBackupRuntimeRecord()
	}
	return record, nil
}

func encodeBackupScheduleCursorRecord(record BackupScheduleCursorRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-schedule-cursor", record, validateBackupScheduleCursorRecord)
}

func encodeEnvironmentMutationEpochRecord(record EnvironmentMutationEpochRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"environment-mutation-epoch",
		record,
		validateEnvironmentMutationEpochRecord,
	)
}

func decodeEnvironmentMutationEpochRecord(value []byte) (EnvironmentMutationEpochRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"environment-mutation-epoch",
		validateEnvironmentMutationEpochRecord,
	)
}

func decodeBackupScheduleCursorRecord(value []byte) (BackupScheduleCursorRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-schedule-cursor", validateBackupScheduleCursorRecord)
}

func encodeBackupDueOutcomeRecord(record BackupDueOutcomeRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-due-outcome", record, validateBackupDueOutcomeRecord)
}

func decodeBackupDueOutcomeRecord(value []byte) (BackupDueOutcomeRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-due-outcome", validateBackupDueOutcomeRecord)
}

func encodeBackupOperationLockRecord(record BackupOperationLockRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-operation-lock", record, validateBackupOperationLockRecord)
}

func decodeBackupOperationLockRecord(value []byte) (BackupOperationLockRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-operation-lock", validateBackupOperationLockRecord)
}

func encodeBackupSourceTargetExclusionRecord(record BackupSourceTargetExclusionRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"backup-source-target-exclusion",
		record,
		validateBackupSourceTargetExclusionRecord,
	)
}

func decodeBackupSourceTargetExclusionRecord(value []byte) (BackupSourceTargetExclusionRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"backup-source-target-exclusion",
		validateBackupSourceTargetExclusionRecord,
	)
}

func encodeBackupRunRecord(record BackupRunRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-run", record, validateBackupRunRecord)
}

func decodeBackupRunRecord(value []byte) (BackupRunRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-run", validateBackupRunRecord)
}

func encodeBackupRunVolumeServiceRecord(record BackupRunVolumeServiceRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"backup-run-volume-service",
		record,
		validateBackupRunVolumeServiceRecord,
	)
}

func decodeBackupRunVolumeServiceRecord(value []byte) (BackupRunVolumeServiceRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"backup-run-volume-service",
		validateBackupRunVolumeServiceRecord,
	)
}

func encodeBackupRecoveryPointRecord(record BackupRecoveryPointRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("recovery-point", record, validateBackupRecoveryPointRecord)
}

func decodeBackupRecoveryPointRecord(value []byte) (BackupRecoveryPointRecord, error) {
	return decodeBackupRuntimeRecord(value, "recovery-point", validateBackupRecoveryPointRecord)
}

func encodeBackupOrphanRecord(record BackupOrphanRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-orphan", record, validateBackupOrphanRecord)
}

func decodeBackupOrphanRecord(value []byte) (BackupOrphanRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-orphan", validateBackupOrphanRecord)
}

func encodeBackupRetentionSweepRecord(record BackupRetentionSweepRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-retention-sweep", record, validateBackupRetentionSweepRecord)
}

func decodeBackupRetentionSweepRecord(value []byte) (BackupRetentionSweepRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-retention-sweep", validateBackupRetentionSweepRecord)
}

func encodeBackupRecoveryPointPruneRecord(record BackupRecoveryPointPruneRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("recovery-point-prune", record, validateBackupRecoveryPointPruneRecord)
}

func decodeBackupRecoveryPointPruneRecord(value []byte) (BackupRecoveryPointPruneRecord, error) {
	return decodeBackupRuntimeRecord(value, "recovery-point-prune", validateBackupRecoveryPointPruneRecord)
}

func encodeBackupRecoveryPointPruneDispatchRecord(
	record BackupRecoveryPointPruneDispatchRecord,
) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"recovery-point-prune-dispatch",
		record,
		validateBackupRecoveryPointPruneDispatchRecord,
	)
}

func decodeBackupRecoveryPointPruneDispatchRecord(
	value []byte,
) (BackupRecoveryPointPruneDispatchRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"recovery-point-prune-dispatch",
		validateBackupRecoveryPointPruneDispatchRecord,
	)
}

func encodeBackupRestoreRecord(record BackupRestoreRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-restore", record, validateBackupRestoreRecord)
}

func decodeBackupRestoreRecord(value []byte) (BackupRestoreRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-restore", validateBackupRestoreRecord)
}

func encodeBackupRestoreServiceRecord(record BackupRestoreServiceRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-restore-service", record, validateBackupRestoreServiceRecord)
}

func decodeBackupRestoreServiceRecord(value []byte) (BackupRestoreServiceRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-restore-service", validateBackupRestoreServiceRecord)
}

func encodeBackupKeyRotationRecord(record BackupKeyRotationRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-key-rotation", record, validateBackupKeyRotationRecord)
}

func decodeBackupKeyRotationRecord(value []byte) (BackupKeyRotationRecord, error) {
	if len(value) == 0 || len(value) > maximumBackupRuntimeRecordBytes {
		return BackupKeyRotationRecord{}, corruptBackupRuntimeRecord()
	}
	record, err := decodeEnvelope[BackupKeyRotationRecord](value, "backup-key-rotation")
	if err != nil || validateBackupKeyRotationRecord(record) != nil {
		clear(record.NextEncryptedIdentity)
		return BackupKeyRotationRecord{}, corruptBackupRuntimeRecord()
	}
	return record, nil
}
