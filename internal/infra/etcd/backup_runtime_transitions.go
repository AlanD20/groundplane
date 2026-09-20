package etcd

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) TransitionBackupRun(
	ctx context.Context,
	authority BackupAssignmentInput,
	current etcdstore.Versioned[BackupRunRecord],
	next BackupRunRecord,
) (etcdstore.Versioned[BackupRunRecord], error) {
	if terminalBackupRunState(next.State) {
		return etcdstore.Versioned[BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"terminal backup run requires atomic Task completion",
		)
	}
	if err := validateBackupRunTransition(
		current.Record,
		next,
		backupRunTransitionOrdinary,
	); err != nil {
		return etcdstore.Versioned[BackupRunRecord]{}, err
	}
	if backupRunTransitionRequiresCheckpoint(current.Record, next) {
		return etcdstore.Versioned[BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup source transition requires a checkpoint",
		)
	}
	return repository.replaceBackupRun(ctx, current, next, nil, nil, nil, &authority, nil)
}

func (repository *BackupRuntimeRepository) CheckpointBackupRun(
	ctx context.Context,
	checkpoint BackupCheckpointInput,
	current etcdstore.Versioned[BackupRunRecord],
	next BackupRunRecord,
) (etcdstore.Versioned[BackupRunRecord], error) {
	if terminalBackupRunState(next.State) || validateBackupRunTransition(
		current.Record,
		next,
		backupRunTransitionOrdinary,
	) != nil {
		return etcdstore.Versioned[BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"checkpointed backup run transition is invalid",
		)
	}
	ordinal, changed := changedBackupSourceOrdinal(current.Record, next)
	if !changed || !backupRunCheckpointMatchesTransition(
		checkpoint.Payload,
		current.Record.Sources[ordinal],
		next.Sources[ordinal],
	) {
		return etcdstore.Versioned[BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup run checkpoint does not match its source transition",
		)
	}
	return repository.replaceBackupRun(ctx, current, next, nil, nil, nil, nil, &checkpoint)
}

func backupRunTransitionRequiresCheckpoint(current BackupRunRecord, next BackupRunRecord) bool {
	ordinal, changed := changedBackupSourceOrdinal(current, next)
	if !changed {
		return false
	}
	from := current.Sources[ordinal]
	to := next.Sources[ordinal]
	return (from.State == BackupSourceAttemptReady && to.State == BackupSourceAttemptStaged) ||
		((from.State == BackupSourceAttemptStaged || from.State == BackupSourceAttemptOrphaned) &&
			to.State == from.State &&
			from.Phase == BackupSourcePhaseUpload && to.Phase == BackupSourcePhaseHeadVerification) ||
		((from.State == BackupSourceAttemptStaged || from.State == BackupSourceAttemptOrphaned) &&
			to.State == from.State && from.Phase == BackupSourcePhaseHeadVerification &&
			to.Phase == BackupSourcePhasePointCommit) ||
		(from.State == BackupSourceAttemptCleanupPending && to.State == BackupSourceAttemptSucceeded)
}

func backupRunCheckpointMatchesTransition(
	payload BackupCheckpointPayload,
	current BackupRunSourceAttemptRecord,
	next BackupRunSourceAttemptRecord,
) bool {
	if current.State == BackupSourceAttemptReady && next.State == BackupSourceAttemptStaged {
		return payload.Kind == BackupCheckpointArtifactPrepared &&
			payload.PointID == next.RecoveryPointID &&
			payload.StoredSizeBytes == uint64(next.SizeBytes) &&
			payload.StoredSHA256 == next.SHA256
	}
	if (current.State == BackupSourceAttemptStaged || current.State == BackupSourceAttemptOrphaned) &&
		next.State == current.State &&
		current.Phase == BackupSourcePhaseUpload &&
		next.Phase == BackupSourcePhaseHeadVerification {
		return payload.Kind == BackupCheckpointUploadCompleted &&
			payload.PointID == next.RecoveryPointID &&
			payload.StoredSizeBytes == uint64(next.SizeBytes) &&
			payload.StoredSHA256 == next.SHA256
	}
	if (current.State == BackupSourceAttemptStaged || current.State == BackupSourceAttemptOrphaned) &&
		next.State == current.State &&
		current.Phase == BackupSourcePhaseHeadVerification &&
		next.Phase == BackupSourcePhasePointCommit {
		return payload.Kind == BackupCheckpointUploadVerified &&
			payload.PointID == next.RecoveryPointID &&
			payload.StoredSizeBytes == uint64(next.SizeBytes) &&
			payload.StoredSHA256 == next.SHA256
	}
	if current.State == BackupSourceAttemptCleanupPending &&
		next.State == BackupSourceAttemptSucceeded {
		return payload.Kind == BackupCheckpointSourceCleanupCompleted &&
			payload.PointID == next.RecoveryPointID
	}
	return false
}

type backupRunTransitionMode uint8

const (
	backupRunTransitionOrdinary backupRunTransitionMode = iota + 1
	backupRunTransitionOrphanCreate
	backupRunTransitionOrphanDelete
	backupRunTransitionOrphanTerminal
	backupRunTransitionPointCommit
	backupRunTransitionRetentionComplete
)

func validateBackupRunTransition(
	current BackupRunRecord,
	next BackupRunRecord,
	mode backupRunTransitionMode,
) error {
	if validateBackupRunRecord(current) != nil || validateBackupRunRecord(next) != nil ||
		!next.UpdatedAt.After(current.UpdatedAt) || !backupRunImmutableEqual(current, next) ||
		!validBackupRunStateTransition(current.State, next.State) {
		return errs.New(errs.KindValidationFailed, "backup run transition is invalid")
	}
	changed, hasChanged := changedBackupSourceOrdinal(current, next)
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
	if sourceAttemptRequiresArtifact(current.State, current.Phase) &&
		(current.SizeBytes != next.SizeBytes || current.SHA256 != next.SHA256) {
		return false
	}
	if mode == backupRunTransitionOrphanCreate {
		return current.State == BackupSourceAttemptStaged &&
			(current.Phase == BackupSourcePhaseUpload ||
				current.Phase == BackupSourcePhaseHeadVerification ||
				current.Phase == BackupSourcePhasePointCommit) &&
			next.State == BackupSourceAttemptOrphaned && next.Phase == current.Phase
	}
	if mode == backupRunTransitionOrphanDelete {
		return current.State == BackupSourceAttemptOrphaned &&
			(current.Phase == BackupSourcePhaseUpload ||
				current.Phase == BackupSourcePhaseHeadVerification ||
				current.Phase == BackupSourcePhasePointCommit) &&
			next.State == BackupSourceAttemptFailed && next.Phase == current.Phase
	}
	if mode == backupRunTransitionOrphanTerminal {
		return current.State == BackupSourceAttemptOrphaned &&
			next.State == BackupSourceAttemptOrphaned && next.Phase == current.Phase &&
			current.FailureCode == "" && next.FailureCode != ""
	}
	if mode == backupRunTransitionPointCommit {
		return ((current.State == BackupSourceAttemptStaged && current.Phase == BackupSourcePhasePointCommit) ||
			(current.State == BackupSourceAttemptOrphaned && current.Phase == BackupSourcePhasePointCommit)) &&
			next.State == BackupSourceAttemptPointCommitted &&
			next.Phase == BackupSourcePhaseRetention
	}
	if mode == backupRunTransitionRetentionComplete {
		return current.State == BackupSourceAttemptPointCommitted &&
			current.Phase == BackupSourcePhaseRetention &&
			next.State == BackupSourceAttemptCleanupPending &&
			next.Phase == BackupSourcePhaseCleanup
	}
	if next.State == BackupSourceAttemptFailed {
		return activeBackupSourceAttemptState(current.State) &&
			current.State != BackupSourceAttemptOrphaned && next.Phase == current.Phase
	}
	switch current.State {
	case BackupSourceAttemptPending:
		return current.Phase == BackupSourcePhaseCapture &&
			next.State == BackupSourceAttemptCapturing && next.Phase == current.Phase
	case BackupSourceAttemptCapturing:
		return current.Phase == BackupSourcePhaseCapture &&
			next.State == BackupSourceAttemptReady && next.Phase == BackupSourcePhaseStaging
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

func backupRunCheckpointBinding(run BackupRunRecord, ordinal uint32) backupCheckpointBinding {
	if int(ordinal) >= len(run.Sources) {
		return backupCheckpointBinding{}
	}
	return backupCheckpointBinding{
		taskType: TaskBackup, ordinal: ordinal, pointID: run.Sources[ordinal].RecoveryPointID,
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
		normalized.Sources[index].SizeBytes = current.Sources[index].SizeBytes
		normalized.Sources[index].SHA256 = current.Sources[index].SHA256
		normalized.Sources[index].FailureCode = current.Sources[index].FailureCode
	}
	return backupRunRecordsEqual(current, normalized)
}

func backupRunSourceMutableEqual(
	left BackupRunSourceAttemptRecord,
	right BackupRunSourceAttemptRecord,
) bool {
	return left.State == right.State && left.Phase == right.Phase &&
		left.SizeBytes == right.SizeBytes &&
		left.SHA256 == right.SHA256 &&
		left.FailureCode == right.FailureCode
}

func backupRunRecordsEqual(left BackupRunRecord, right BackupRunRecord) bool {
	leftValue, leftErr := encodeBackupRunRecord(left)
	rightValue, rightErr := encodeBackupRunRecord(right)
	defer clear(leftValue)
	defer clear(rightValue)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftValue, rightValue)
}

func terminalBackupRunState(state BackupRunState) bool {
	return state == BackupRunFailed || state == BackupRunCompleted || state == BackupRunAborted ||
		state == BackupRunTimedOut
}
