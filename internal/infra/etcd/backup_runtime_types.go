package etcd

const (
	maximumBackupRuntimeRecordBytes  = 256 * 1024
	maximumBackupObjectKeyBytes      = 1024
	maximumBackupPruneDispatchPoints = 11
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
	BackupOperationPrune    BackupOperationKind = "recovery_point_prune"
	BackupOperationDeletion BackupOperationKind = "environment_deletion"
)

type BackupSourceTargetKind string

const (
	BackupSourceTargetAttach BackupSourceTargetKind = "attach"
	BackupSourceTargetVolume BackupSourceTargetKind = "volume"
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

// BackupSourceAttemptPhase is the protected execution checkpoint within the
// coarser operator-visible attempt State. It is intentionally closed so a
// durable failure identifies the exact checkpoint that failed.
type BackupSourceAttemptPhase string

const (
	BackupSourcePhaseCapture          BackupSourceAttemptPhase = "capture"
	BackupSourcePhaseStaging          BackupSourceAttemptPhase = "staging"
	BackupSourcePhaseUpload           BackupSourceAttemptPhase = "upload"
	BackupSourcePhaseHeadVerification BackupSourceAttemptPhase = "head_verification"
	BackupSourcePhasePointCommit      BackupSourceAttemptPhase = "point_commit"
	BackupSourcePhaseCleanup          BackupSourceAttemptPhase = "cleanup"
	BackupSourcePhaseRetention        BackupSourceAttemptPhase = "retention"
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
	BackupPrunePending        BackupPruneState = "pending"
	BackupPruneAssigned       BackupPruneState = "assigned"
	BackupPruneVerifiedAbsent BackupPruneState = "verified_absent"
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
