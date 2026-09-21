package etcd

import (
	"bytes"
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) TransitionBackupRun(
	ctx context.Context,
	authority BackupAssignmentInput,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	if terminalBackupRunState(next.State) {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"terminal backup run requires atomic Task completion",
		)
	}
	if err := validateBackupRunTransition(
		current.Record,
		next,
		backupRunTransitionOrdinary,
	); err != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, err
	}
	if backupRunTransitionRequiresCheckpoint(current.Record, next) {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup source transition requires a checkpoint",
		)
	}
	return repository.replaceBackupRun(ctx, current, next, nil, nil, nil, &authority, nil)
}

func (repository *BackupRuntimeRepository) CheckpointBackupRun(
	ctx context.Context,
	checkpoint BackupCheckpointInput,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
) (etcdstore.Versioned[backupruntime.BackupRunRecord], error) {
	if terminalBackupRunState(next.State) || validateBackupRunTransition(
		current.Record,
		next,
		backupRunTransitionOrdinary,
	) != nil {
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
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
		return etcdstore.Versioned[backupruntime.BackupRunRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backup run checkpoint does not match its source transition",
		)
	}
	return repository.replaceBackupRun(ctx, current, next, nil, nil, nil, nil, &checkpoint)
}

func backupRunTransitionRequiresCheckpoint(current backupruntime.BackupRunRecord, next backupruntime.BackupRunRecord) bool {
	ordinal, changed := changedBackupSourceOrdinal(current, next)
	if !changed {
		return false
	}
	from := current.Sources[ordinal]
	to := next.Sources[ordinal]
	return (from.State == backupruntime.BackupSourceAttemptReady && to.State == backupruntime.BackupSourceAttemptStaged) ||
		((from.State == backupruntime.BackupSourceAttemptStaged || from.State == backupruntime.BackupSourceAttemptOrphaned) &&
			to.State == from.State &&
			from.Phase == backupruntime.BackupSourcePhaseUpload && to.Phase == backupruntime.BackupSourcePhaseHeadVerification) ||
		((from.State == backupruntime.BackupSourceAttemptStaged || from.State == backupruntime.BackupSourceAttemptOrphaned) &&
			to.State == from.State && from.Phase == backupruntime.BackupSourcePhaseHeadVerification &&
			to.Phase == backupruntime.BackupSourcePhasePointCommit) ||
		(from.State == backupruntime.BackupSourceAttemptCleanupPending && to.State == backupruntime.BackupSourceAttemptSucceeded)
}

func backupRunCheckpointMatchesTransition(
	payload BackupCheckpointPayload,
	current backupruntime.BackupRunSourceAttemptRecord,
	next backupruntime.BackupRunSourceAttemptRecord,
) bool {
	if current.State == backupruntime.BackupSourceAttemptReady && next.State == backupruntime.BackupSourceAttemptStaged {
		return payload.Kind == BackupCheckpointArtifactPrepared &&
			payload.PointID == next.RecoveryPointID &&
			payload.StoredSizeBytes == uint64(next.SizeBytes) &&
			payload.StoredSHA256 == next.SHA256
	}
	if (current.State == backupruntime.BackupSourceAttemptStaged || current.State == backupruntime.BackupSourceAttemptOrphaned) &&
		next.State == current.State &&
		current.Phase == backupruntime.BackupSourcePhaseUpload &&
		next.Phase == backupruntime.BackupSourcePhaseHeadVerification {
		return payload.Kind == BackupCheckpointUploadCompleted &&
			payload.PointID == next.RecoveryPointID &&
			payload.StoredSizeBytes == uint64(next.SizeBytes) &&
			payload.StoredSHA256 == next.SHA256
	}
	if (current.State == backupruntime.BackupSourceAttemptStaged || current.State == backupruntime.BackupSourceAttemptOrphaned) &&
		next.State == current.State &&
		current.Phase == backupruntime.BackupSourcePhaseHeadVerification &&
		next.Phase == backupruntime.BackupSourcePhasePointCommit {
		return payload.Kind == BackupCheckpointUploadVerified &&
			payload.PointID == next.RecoveryPointID &&
			payload.StoredSizeBytes == uint64(next.SizeBytes) &&
			payload.StoredSHA256 == next.SHA256
	}
	if current.State == backupruntime.BackupSourceAttemptCleanupPending &&
		next.State == backupruntime.BackupSourceAttemptSucceeded {
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
	current backupruntime.BackupRunRecord,
	next backupruntime.BackupRunRecord,
	mode backupRunTransitionMode,
) error {
	if backupruntime.ValidateBackupRunRecord(current) != nil || backupruntime.ValidateBackupRunRecord(next) != nil ||
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

func validBackupRunStateTransition(current backupruntime.BackupRunState, next backupruntime.BackupRunState) bool {
	if current == next {
		return current == backupruntime.BackupRunQueued || current == backupruntime.BackupRunRunning
	}
	switch current {
	case backupruntime.BackupRunQueued:
		return next == backupruntime.BackupRunRunning || next == backupruntime.BackupRunFailed || next == backupruntime.BackupRunAborted ||
			next == backupruntime.BackupRunTimedOut
	case backupruntime.BackupRunRunning:
		return next == backupruntime.BackupRunFailed || next == backupruntime.BackupRunCompleted || next == backupruntime.BackupRunAborted ||
			next == backupruntime.BackupRunTimedOut
	default:
		return false
	}
}

func validBackupSourceTransition(
	current backupruntime.BackupRunSourceAttemptRecord,
	next backupruntime.BackupRunSourceAttemptRecord,
	mode backupRunTransitionMode,
) bool {
	if backupruntime.SourceAttemptRequiresArtifact(current.State, current.Phase) &&
		(current.SizeBytes != next.SizeBytes || current.SHA256 != next.SHA256) {
		return false
	}
	if mode == backupRunTransitionOrphanCreate {
		return current.State == backupruntime.BackupSourceAttemptStaged &&
			(current.Phase == backupruntime.BackupSourcePhaseUpload ||
				current.Phase == backupruntime.BackupSourcePhaseHeadVerification ||
				current.Phase == backupruntime.BackupSourcePhasePointCommit) &&
			next.State == backupruntime.BackupSourceAttemptOrphaned && next.Phase == current.Phase
	}
	if mode == backupRunTransitionOrphanDelete {
		return current.State == backupruntime.BackupSourceAttemptOrphaned &&
			(current.Phase == backupruntime.BackupSourcePhaseUpload ||
				current.Phase == backupruntime.BackupSourcePhaseHeadVerification ||
				current.Phase == backupruntime.BackupSourcePhasePointCommit) &&
			next.State == backupruntime.BackupSourceAttemptFailed && next.Phase == current.Phase
	}
	if mode == backupRunTransitionOrphanTerminal {
		return current.State == backupruntime.BackupSourceAttemptOrphaned &&
			next.State == backupruntime.BackupSourceAttemptOrphaned && next.Phase == current.Phase &&
			current.FailureCode == "" && next.FailureCode != ""
	}
	if mode == backupRunTransitionPointCommit {
		return ((current.State == backupruntime.BackupSourceAttemptStaged && current.Phase == backupruntime.BackupSourcePhasePointCommit) ||
			(current.State == backupruntime.BackupSourceAttemptOrphaned && current.Phase == backupruntime.BackupSourcePhasePointCommit)) &&
			next.State == backupruntime.BackupSourceAttemptPointCommitted &&
			next.Phase == backupruntime.BackupSourcePhaseRetention
	}
	if mode == backupRunTransitionRetentionComplete {
		return current.State == backupruntime.BackupSourceAttemptPointCommitted &&
			current.Phase == backupruntime.BackupSourcePhaseRetention &&
			next.State == backupruntime.BackupSourceAttemptCleanupPending &&
			next.Phase == backupruntime.BackupSourcePhaseCleanup
	}
	if next.State == backupruntime.BackupSourceAttemptFailed {
		return backupruntime.ActiveBackupSourceAttemptState(current.State) &&
			current.State != backupruntime.BackupSourceAttemptOrphaned && next.Phase == current.Phase
	}
	switch current.State {
	case backupruntime.BackupSourceAttemptPending:
		return current.Phase == backupruntime.BackupSourcePhaseCapture &&
			next.State == backupruntime.BackupSourceAttemptCapturing && next.Phase == current.Phase
	case backupruntime.BackupSourceAttemptCapturing:
		return current.Phase == backupruntime.BackupSourcePhaseCapture &&
			next.State == backupruntime.BackupSourceAttemptReady && next.Phase == backupruntime.BackupSourcePhaseStaging
	case backupruntime.BackupSourceAttemptReady:
		return current.Phase == backupruntime.BackupSourcePhaseStaging &&
			next.State == backupruntime.BackupSourceAttemptStaged && next.Phase == backupruntime.BackupSourcePhaseUpload
	case backupruntime.BackupSourceAttemptStaged:
		return next.State == current.State &&
			((current.Phase == backupruntime.BackupSourcePhaseUpload && next.Phase == backupruntime.BackupSourcePhaseHeadVerification) ||
				(current.Phase == backupruntime.BackupSourcePhaseHeadVerification && next.Phase == backupruntime.BackupSourcePhasePointCommit))
	case backupruntime.BackupSourceAttemptOrphaned:
		return next.State == current.State &&
			((current.Phase == backupruntime.BackupSourcePhaseUpload &&
				next.Phase == backupruntime.BackupSourcePhaseHeadVerification) ||
				(current.Phase == backupruntime.BackupSourcePhaseHeadVerification &&
					next.Phase == backupruntime.BackupSourcePhasePointCommit))
	case backupruntime.BackupSourceAttemptPointCommitted:
		return next.State == current.State && next.Phase == current.Phase &&
			next.FailureCode == backupruntime.BackupFailureRetention
	case backupruntime.BackupSourceAttemptCleanupPending:
		return next.Phase == current.Phase && (next.State == backupruntime.BackupSourceAttemptSucceeded ||
			(next.State == current.State && next.FailureCode == backupruntime.BackupFailureCleanup))
	default:
		return false
	}
}

func backupRunCheckpointBinding(run backupruntime.BackupRunRecord, ordinal uint32) backupCheckpointBinding {
	if int(ordinal) >= len(run.Sources) {
		return backupCheckpointBinding{}
	}
	return backupCheckpointBinding{
		taskType: taskjournal.TaskBackup, ordinal: ordinal, pointID: run.Sources[ordinal].RecoveryPointID,
	}
}

func backupRunImmutableEqual(current backupruntime.BackupRunRecord, next backupruntime.BackupRunRecord) bool {
	if len(current.Sources) != len(next.Sources) {
		return false
	}
	normalized := next
	normalized.State = current.State
	normalized.UpdatedAt = current.UpdatedAt
	normalized.Sources = append([]backupruntime.BackupRunSourceAttemptRecord(nil), next.Sources...)
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
	left backupruntime.BackupRunSourceAttemptRecord,
	right backupruntime.BackupRunSourceAttemptRecord,
) bool {
	return left.State == right.State && left.Phase == right.Phase &&
		left.SizeBytes == right.SizeBytes &&
		left.SHA256 == right.SHA256 &&
		left.FailureCode == right.FailureCode
}

func backupRunRecordsEqual(left backupruntime.BackupRunRecord, right backupruntime.BackupRunRecord) bool {
	leftValue, leftErr := backupruntime.EncodeBackupRunRecord(left)
	rightValue, rightErr := backupruntime.EncodeBackupRunRecord(right)
	defer clear(leftValue)
	defer clear(rightValue)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftValue, rightValue)
}

func terminalBackupRunState(state backupruntime.BackupRunState) bool {
	return state == backupruntime.BackupRunFailed || state == backupruntime.BackupRunCompleted || state == backupruntime.BackupRunAborted ||
		state == backupruntime.BackupRunTimedOut
}
