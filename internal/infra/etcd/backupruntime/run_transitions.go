package backupruntime

import (
	"bytes"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type backupRunTransitionMode uint8

const (
	BackupRunTransitionOrdinary backupRunTransitionMode = iota + 1
	BackupRunTransitionOrphanCreate
	BackupRunTransitionOrphanDelete
	BackupRunTransitionOrphanTerminal
	BackupRunTransitionPointCommit
	BackupRunTransitionRetentionComplete
	BackupRunTransitionCaptureComplete
)

func ValidateBackupRunTransition(
	current BackupRunRecord,
	next BackupRunRecord,
	mode backupRunTransitionMode,
) error {
	if ValidateBackupRunRecord(current) != nil || ValidateBackupRunRecord(next) != nil ||
		!next.UpdatedAt.After(current.UpdatedAt) || !backupRunImmutableEqual(current, next) ||
		!validBackupRunStateTransition(current.State, next.State) {
		return errs.New(errs.KindValidationFailed, "backup run transition is invalid")
	}
	changed, hasChanged := ChangedBackupSourceOrdinal(current, next)
	if !hasChanged {
		for index := range current.Sources {
			if !backupRunSourceMutableEqual(current.Sources[index], next.Sources[index]) {
				return errs.New(
					errs.KindValidationFailed,
					"backup run transition changes multiple sources",
				)
			}
		}
		if current.State == next.State {
			return errs.New(errs.KindValidationFailed, "backup run transition is a no-op")
		}
		return nil
	}
	from := current.Sources[changed]
	to := next.Sources[changed]
	if !validBackupSourceTransition(from, to, mode) {
		return errs.New(errs.KindValidationFailed, "backup source transition is invalid")
	}
	return nil
}

func validBackupRunStateTransition(current BackupRunState, next BackupRunState) bool {
	if current == next {
		return current == BackupRunQueued || current == BackupRunRunning
	}
	switch current {
	case BackupRunQueued:
		return next == BackupRunRunning || next == BackupRunFailed || next == BackupRunAborted ||
			next == BackupRunTimedOut
	case BackupRunRunning:
		return next == BackupRunFailed || next == BackupRunCompleted || next == BackupRunAborted ||
			next == BackupRunTimedOut
	default:
		return false
	}
}

func validBackupSourceTransition(
	current BackupRunSourceAttemptRecord,
	next BackupRunSourceAttemptRecord,
	mode backupRunTransitionMode,
) bool {
	if current.ConfigArchive != next.ConfigArchive &&
		!(current.Kind == BackupRuntimeSourceConfig && current.ConfigArchive == (BackupConfigArchiveEvidence{}) &&
			current.Phase == BackupSourcePhaseCapture &&
			(current.State == BackupSourceAttemptPending || current.State == BackupSourceAttemptCapturing) &&
			next.State == BackupSourceAttemptReady && next.Phase == BackupSourcePhaseStaging &&
			validateSelectedConfigArchive(next.Kind, next.ConfigArchive, next.Evidence) == nil) {
		return false
	}
	if current.VolumeArchive != next.VolumeArchive &&
		!(current.Kind == BackupRuntimeSourceVolume && current.VolumeArchive == (BackupVolumeArchiveEvidence{}) &&
			current.Phase == BackupSourcePhaseCapture &&
			(current.State == BackupSourceAttemptPending || current.State == BackupSourceAttemptCapturing) &&
			next.State == BackupSourceAttemptStaged && next.Phase == BackupSourcePhaseUpload &&
			validateSelectedVolumeArchive(next.Kind, next.VolumeArchive, next.Evidence) == nil) {
		return false
	}
	if current.MySQLArchive != next.MySQLArchive &&
		!(current.Format == BackupRuntimeFormatMySQL && current.MySQLArchive == (BackupMySQLArchiveEvidence{}) &&
			current.Phase == BackupSourcePhaseCapture && current.State == BackupSourceAttemptCapturing &&
			next.State == BackupSourceAttemptReady && next.Phase == BackupSourcePhaseStaging &&
			validateSelectedMySQLArchive(next.Kind, next.Format, next.MySQLArchive) == nil) {
		return false
	}
	if current.PostgresArchive != next.PostgresArchive &&
		!(current.Format == BackupRuntimeFormatPostgres &&
			current.PostgresArchive == (BackupPostgresArchiveEvidence{}) &&
			current.Phase == BackupSourcePhaseCapture && current.State == BackupSourceAttemptCapturing &&
			next.State == BackupSourceAttemptReady && next.Phase == BackupSourcePhaseStaging &&
			validateSelectedPostgresArchive(next.Kind, next.Format, next.PostgresArchive) == nil) {
		return false
	}
	if current.Evidence != (BackupArtifactEvidence{}) && current.Evidence != next.Evidence {
		return false
	}
	if current.Upload != next.Upload && current.Upload != (BackupUploadOutcome{}) &&
		!(current.Upload.Kind == BackupUploadPrepared && current.Phase == BackupSourcePhaseUpload &&
			next.Phase == BackupSourcePhaseHeadVerification && current.Upload.Target == next.Upload.Target &&
			(next.Upload.Kind == BackupUploadReturned || next.Upload.Kind == BackupUploadUnknown)) {
		return false
	}
	if current.Object != next.Object &&
		!(current.Object == (BackupObjectIdentity{}) && current.Phase == BackupSourcePhaseHeadVerification &&
			next.Phase == BackupSourcePhasePointCommit) {
		return false
	}
	if mode == BackupRunTransitionOrphanCreate {
		return current.State == BackupSourceAttemptStaged &&
			(current.Phase == BackupSourcePhaseUpload ||
				current.Phase == BackupSourcePhaseHeadVerification ||
				current.Phase == BackupSourcePhasePointCommit) &&
			next.State == BackupSourceAttemptOrphaned && next.Phase == current.Phase
	}
	if mode == BackupRunTransitionOrphanDelete {
		return current.State == BackupSourceAttemptOrphaned &&
			(current.Phase == BackupSourcePhaseUpload ||
				current.Phase == BackupSourcePhaseHeadVerification ||
				current.Phase == BackupSourcePhasePointCommit) &&
			next.State == BackupSourceAttemptFailed && next.Phase == current.Phase
	}
	if mode == BackupRunTransitionOrphanTerminal {
		return current.State == BackupSourceAttemptOrphaned &&
			next.State == BackupSourceAttemptOrphaned && next.Phase == current.Phase &&
			current.FailureCode == "" && next.FailureCode != ""
	}
	if mode == BackupRunTransitionPointCommit {
		return ((current.State == BackupSourceAttemptStaged && current.Phase == BackupSourcePhasePointCommit) ||
			(current.State == BackupSourceAttemptOrphaned && current.Phase == BackupSourcePhasePointCommit)) &&
			next.State == BackupSourceAttemptPointCommitted &&
			next.Phase == BackupSourcePhaseRetention
	}
	if mode == BackupRunTransitionRetentionComplete {
		return current.State == BackupSourceAttemptPointCommitted &&
			current.Phase == BackupSourcePhaseRetention &&
			next.State == BackupSourceAttemptCleanupPending &&
			next.Phase == BackupSourcePhaseCleanup
	}
	if mode == BackupRunTransitionCaptureComplete {
		// Capture does not stop consumers or modify live source state.
		// Its source-cleanup receipt already proved staging removal;
		// only the native retention sweep remains before source completion.
		return current.State == BackupSourceAttemptPointCommitted &&
			current.Phase == BackupSourcePhaseRetention && next.State == BackupSourceAttemptSucceeded &&
			next.Phase == BackupSourcePhaseCleanup && current.FailureCode == "" && next.FailureCode == ""
	}
	if next.State == BackupSourceAttemptFailed {
		return ActiveBackupSourceAttemptState(current.State) &&
			current.State != BackupSourceAttemptOrphaned && next.Phase == current.Phase
	}
	switch current.State {
	case BackupSourceAttemptPending:
		return current.Phase == BackupSourcePhaseCapture &&
			((next.State == BackupSourceAttemptCapturing && next.Phase == current.Phase) ||
				(next.State == BackupSourceAttemptReady && next.Phase == BackupSourcePhaseStaging) ||
				(current.Kind == BackupRuntimeSourceVolume && next.State == BackupSourceAttemptStaged && next.Phase == BackupSourcePhaseUpload))
	case BackupSourceAttemptCapturing:
		return current.Phase == BackupSourcePhaseCapture &&
			(next.State == BackupSourceAttemptReady && next.Phase == BackupSourcePhaseStaging ||
				current.Kind == BackupRuntimeSourceVolume && next.State == BackupSourceAttemptStaged && next.Phase == BackupSourcePhaseUpload)
	case BackupSourceAttemptReady:
		return current.Phase == BackupSourcePhaseStaging &&
			next.State == BackupSourceAttemptStaged && next.Phase == BackupSourcePhaseUpload
	case BackupSourceAttemptStaged:
		return next.State == current.State &&
			((current.Phase == BackupSourcePhaseUpload && next.Phase == BackupSourcePhaseHeadVerification) ||
				(current.Phase == BackupSourcePhaseHeadVerification && next.Phase == BackupSourcePhasePointCommit))
	case BackupSourceAttemptOrphaned:
		return next.State == current.State &&
			((current.Phase == BackupSourcePhaseUpload &&
				next.Phase == BackupSourcePhaseHeadVerification) ||
				(current.Phase == BackupSourcePhaseHeadVerification &&
					next.Phase == BackupSourcePhasePointCommit))
	case BackupSourceAttemptPointCommitted:
		return next.State == current.State && next.Phase == current.Phase &&
			next.FailureCode == BackupFailureRetention
	case BackupSourceAttemptCleanupPending:
		return next.Phase == current.Phase && (next.State == BackupSourceAttemptSucceeded ||
			(next.State == current.State && next.FailureCode == BackupFailureCleanup))
	default:
		return false
	}
}

func backupRunImmutableEqual(current BackupRunRecord, next BackupRunRecord) bool {
	if len(current.Sources) != len(next.Sources) {
		return false
	}
	normalized := next
	normalized.State = current.State
	normalized.UpdatedAt = current.UpdatedAt
	normalized.Sources = append([]BackupRunSourceAttemptRecord(nil), next.Sources...)
	for index := range normalized.Sources {
		normalized.Sources[index].State = current.Sources[index].State
		normalized.Sources[index].Phase = current.Sources[index].Phase
		normalized.Sources[index].Evidence = current.Sources[index].Evidence
		normalized.Sources[index].ConfigArchive = current.Sources[index].ConfigArchive
		normalized.Sources[index].VolumeArchive = current.Sources[index].VolumeArchive
		normalized.Sources[index].PostgresArchive = current.Sources[index].PostgresArchive
		normalized.Sources[index].MySQLArchive = current.Sources[index].MySQLArchive
		normalized.Sources[index].Upload = current.Sources[index].Upload
		normalized.Sources[index].Object = current.Sources[index].Object
		normalized.Sources[index].FailureCode = current.Sources[index].FailureCode
	}
	return BackupRunRecordsEqual(current, normalized)
}

func backupRunSourceMutableEqual(
	left BackupRunSourceAttemptRecord,
	right BackupRunSourceAttemptRecord,
) bool {
	return left.State == right.State && left.Phase == right.Phase &&
		left.ConfigArchive == right.ConfigArchive && left.VolumeArchive == right.VolumeArchive &&
		left.PostgresArchive == right.PostgresArchive && left.MySQLArchive == right.MySQLArchive &&
		left.Evidence == right.Evidence && left.Upload == right.Upload && left.Object == right.Object &&
		left.FailureCode == right.FailureCode
}

func BackupRunRecordsEqual(left BackupRunRecord, right BackupRunRecord) bool {
	leftValue, leftErr := EncodeBackupRunRecord(left)
	rightValue, rightErr := EncodeBackupRunRecord(right)
	defer clear(leftValue)
	defer clear(rightValue)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftValue, rightValue)
}

func TerminalBackupRunState(state BackupRunState) bool {
	return state == BackupRunFailed || state == BackupRunCompleted || state == BackupRunAborted ||
		state == BackupRunTimedOut
}
