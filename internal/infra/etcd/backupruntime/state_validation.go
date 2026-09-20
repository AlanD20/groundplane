package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"
)

func ValidBackupFailureCodeForAttempt(
	state BackupSourceAttemptState,
	phase BackupSourceAttemptPhase,
	code BackupFailureCode,
) bool {
	if code == "" {
		return state != BackupSourceAttemptFailed
	}
	return failedBackupSourceCheckpoint(state, phase, code)
}

func failedBackupSourceCheckpoint(
	state BackupSourceAttemptState,
	phase BackupSourceAttemptPhase,
	code BackupFailureCode,
) bool {
	switch state {
	case BackupSourceAttemptFailed:
		return code == BackupFailureAborted || code == BackupFailureTimedOut ||
			backupFailureCodeMatchesPhase(code, phase)
	case BackupSourceAttemptOrphaned:
		return code == BackupFailureAborted || code == BackupFailureTimedOut ||
			backupFailureCodeMatchesPhase(code, phase)
	case BackupSourceAttemptPointCommitted:
		return phase == BackupSourcePhaseRetention && code == BackupFailureRetention
	case BackupSourceAttemptCleanupPending:
		return phase == BackupSourcePhaseCleanup && code == BackupFailureCleanup
	default:
		return false
	}
}

func backupFailureCodeMatchesPhase(code BackupFailureCode, phase BackupSourceAttemptPhase) bool {
	return (code == BackupFailureCapture && phase == BackupSourcePhaseCapture) ||
		(code == BackupFailureStaging && phase == BackupSourcePhaseStaging) ||
		(code == BackupFailureUpload && phase == BackupSourcePhaseUpload) ||
		(code == BackupFailureHeadVerification && phase == BackupSourcePhaseHeadVerification) ||
		(code == BackupFailurePointCommit && phase == BackupSourcePhasePointCommit) ||
		(code == BackupFailureCleanup && phase == BackupSourcePhaseCleanup) ||
		(code == BackupFailureRetention && phase == BackupSourcePhaseRetention)
}

func validBackupArtifact(value BackupArtifactEvidence) bool {
	return value.SizeBytes > 0 && recordcodec.ValidSHA256(value.SHA256)
}

func ValidBackupRuntimeInstant(value time.Time) bool {
	if value.IsZero() || value.Location() != time.UTC {
		return false
	}
	nanoseconds := value.UnixNano()
	return nanoseconds > 0 && time.Unix(0, nanoseconds).UTC().Equal(value)
}

func validBackupRuntimeLifecycle(createdAt time.Time, updatedAt time.Time) bool {
	return ValidBackupRuntimeInstant(createdAt) && ValidBackupRuntimeInstant(updatedAt) &&
		!updatedAt.Before(createdAt)
}

func validBackupOperationKind(kind BackupOperationKind) bool {
	return kind == BackupOperationBackup || kind == BackupOperationRestore ||
		kind == BackupOperationRotation ||
		kind == BackupOperationPrune ||
		kind == BackupOperationDeletion
}

func validateBackupSourceTargetIdentity(kind BackupSourceTargetKind, stableID string) error {
	var idKind ids.Kind
	switch kind {
	case BackupSourceTargetAttach:
		idKind = ids.KindAttach
	case BackupSourceTargetVolume:
		idKind = ids.KindVolume
	default:
		return invalidBackupRuntimeRecord("backup source-target kind is invalid")
	}
	if recordcodec.ValidateID(idKind, stableID) != nil {
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

func ValidBackupSourceAttemptState(state BackupSourceAttemptState) bool {
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

func ValidBackupSourceAttemptPhase(phase BackupSourceAttemptPhase) bool {
	switch phase {
	case BackupSourcePhaseCapture, BackupSourcePhaseStaging, BackupSourcePhaseUpload,
		BackupSourcePhaseHeadVerification, BackupSourcePhasePointCommit,
		BackupSourcePhaseCleanup, BackupSourcePhaseRetention:
		return true
	default:
		return false
	}
}

func validBackupPhaseForAttemptState(
	state BackupSourceAttemptState,
	phase BackupSourceAttemptPhase,
) bool {
	switch state {
	case BackupSourceAttemptPending, BackupSourceAttemptCapturing, BackupSourceAttemptUnstarted:
		return phase == BackupSourcePhaseCapture
	case BackupSourceAttemptReady:
		return phase == BackupSourcePhaseStaging
	case BackupSourceAttemptStaged:
		return phase == BackupSourcePhaseUpload ||
			phase == BackupSourcePhaseHeadVerification || phase == BackupSourcePhasePointCommit
	case BackupSourceAttemptOrphaned:
		return phase == BackupSourcePhaseUpload || phase == BackupSourcePhaseHeadVerification ||
			phase == BackupSourcePhasePointCommit
	case BackupSourceAttemptPointCommitted:
		return phase == BackupSourcePhaseRetention
	case BackupSourceAttemptCleanupPending, BackupSourceAttemptSucceeded:
		return phase == BackupSourcePhaseCleanup
	case BackupSourceAttemptFailed:
		return ValidBackupSourceAttemptPhase(phase)
	default:
		return false
	}
}

func SourceAttemptRequiresArtifact(
	state BackupSourceAttemptState,
	phase BackupSourceAttemptPhase,
) bool {
	switch state {
	case BackupSourceAttemptStaged,
		BackupSourceAttemptPointCommitted,
		BackupSourceAttemptCleanupPending,
		BackupSourceAttemptSucceeded,
		BackupSourceAttemptOrphaned:
		return true
	case BackupSourceAttemptFailed:
		return phase == BackupSourcePhaseUpload || phase == BackupSourcePhaseHeadVerification ||
			phase == BackupSourcePhasePointCommit
	default:
		return false
	}
}

func sourceAttemptForbidsArtifact(state BackupSourceAttemptState) bool {
	return state == BackupSourceAttemptPending || state == BackupSourceAttemptCapturing ||
		state == BackupSourceAttemptReady || state == BackupSourceAttemptUnstarted
}

func ActiveBackupSourceAttemptState(state BackupSourceAttemptState) bool {
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
	return state == BackupRetentionPending || state == BackupRetentionScanning ||
		state == BackupRetentionCompleted
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
	return recordcodec.ValidSHA256(digest)
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
