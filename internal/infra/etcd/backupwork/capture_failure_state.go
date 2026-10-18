package backupwork

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
)

func failedCaptureRun(current backupruntime.BackupRunRecord, outcome workexecution.Outcome,
	reason executionplan.BackupFailureReason, at time.Time,
) (backupruntime.BackupRunRecord, error) {
	if (current.State != backupruntime.BackupRunRunning && current.State != backupruntime.BackupRunQueued) || !at.After(current.UpdatedAt) {
		return backupruntime.BackupRunRecord{}, conflict("Backup failure requires its running source record")
	}
	next := backupruntime.CloneBackupRunPublicationRecord(current)
	next.State, next.UpdatedAt = backupruntime.BackupRunFailed, at
	if outcome == workexecution.Cancelled || outcome == workexecution.Superseded {
		next.State = backupruntime.BackupRunAborted
	} else if reason == executionplan.BackupDeadlineExpired {
		next.State = backupruntime.BackupRunTimedOut
	}
	boundary := -1
	for index, source := range next.Sources {
		if source.State != backupruntime.BackupSourceAttemptSucceeded {
			boundary = index
			break
		}
	}
	if boundary < 0 {
		return backupruntime.BackupRunRecord{}, conflict("Backup failure has no unfinished source")
	}
	source := &next.Sources[boundary]
	if source.State == backupruntime.BackupSourceAttemptStaged &&
		(source.Phase == backupruntime.BackupSourcePhaseUpload || source.Phase == backupruntime.BackupSourcePhaseHeadVerification ||
			source.Phase == backupruntime.BackupSourcePhasePointCommit) {
		source.State = backupruntime.BackupSourceAttemptOrphaned
	} else if source.State != backupruntime.BackupSourceAttemptOrphaned {
		source.State = backupruntime.BackupSourceAttemptFailed
	}
	var err error
	switch next.State {
	case backupruntime.BackupRunAborted:
		source.FailureCode = backupruntime.BackupFailureAborted
	case backupruntime.BackupRunTimedOut:
		source.FailureCode = backupruntime.BackupFailureTimedOut
	default:
		source.FailureCode, err = captureFailureCode(source.Phase)
	}
	if err != nil {
		return backupruntime.BackupRunRecord{}, err
	}
	for index := boundary + 1; index < len(next.Sources); index++ {
		// Validation rejects any suffix with effects. Never erase evidence to
		// manufacture a fail-fast sequence.
		next.Sources[index].State = backupruntime.BackupSourceAttemptUnstarted
	}
	mode := backupruntime.BackupRunTransitionOrdinary
	if backupruntime.BackupRunNeedsTerminalOrphan(current, next) {
		mode = backupruntime.BackupRunTransitionOrphanCreate
	} else if backupruntime.BackupRunRetainsTerminalOrphan(current, next) {
		mode = backupruntime.BackupRunTransitionOrphanTerminal
	}
	if err := backupruntime.ValidateBackupRunTransition(current, next, mode); err != nil {
		return backupruntime.BackupRunRecord{}, err
	}
	return next, nil
}

func captureFailureCode(phase backupruntime.BackupSourceAttemptPhase) (backupruntime.BackupFailureCode, error) {
	switch phase {
	case backupruntime.BackupSourcePhaseCapture:
		return backupruntime.BackupFailureCapture, nil
	case backupruntime.BackupSourcePhaseStaging:
		return backupruntime.BackupFailureStaging, nil
	case backupruntime.BackupSourcePhaseUpload:
		return backupruntime.BackupFailureUpload, nil
	case backupruntime.BackupSourcePhaseHeadVerification:
		return backupruntime.BackupFailureHeadVerification, nil
	case backupruntime.BackupSourcePhasePointCommit:
		return backupruntime.BackupFailurePointCommit, nil
	case backupruntime.BackupSourcePhaseCleanup:
		return backupruntime.BackupFailureCleanup, nil
	case backupruntime.BackupSourcePhaseRetention:
		return backupruntime.BackupFailureRetention, nil
	default:
		return "", conflict("Backup failure source phase is invalid")
	}
}
