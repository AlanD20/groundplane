package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *InputRepository) prepareCaptureRecoverySettlement(ctx context.Context, view workqueue.View,
	input *agentpb.BackupWorkInput, sources []*agentpb.BackupStepResume,
	failure executionplan.BackupWorkRecovery, at time.Time,
) (workqueue.Change, error) {
	run, guard, err := repository.LoadStagingCapture(ctx, StagingSource{View: view, Input: input})
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(sources) != len(run.Sources) {
		return workqueue.Change{}, conflict("Backup cleanup source progress is incomplete")
	}
	allSucceeded := true
	for ordinal, source := range run.Sources {
		if source.State != backupruntime.BackupSourceAttemptSucceeded {
			allSucceeded = false
			continue
		}
		cleanup := sources[ordinal].GetCapture().GetSourceCleanupCompleted()
		if cleanup == nil || cleanup.PointId != source.RecoveryPointID ||
			!backupruntime.BackupArtifactEvidenceMatchesWire(source.Evidence, cleanup.Evidence) {
			return workqueue.Change{}, conflict("Backup completed source lacks its cleanup checkpoint")
		}
	}
	next := run
	if run.State == backupruntime.BackupRunRunning {
		if allSucceeded {
			// Source results are authoritative even if the executor failed while
			// reporting completion. The visible Task still retains that failure.
			next = backupruntime.CloneBackupRunPublicationRecord(run)
			next.State, next.UpdatedAt = backupruntime.BackupRunCompleted, at
			err = backupruntime.ValidateBackupRunTransition(run, next, backupruntime.BackupRunTransitionOrdinary)
		} else {
			next, err = failedCaptureRun(run, failure.Forward.Outcome, failure.Failure.Reason, at)
		}
		if err != nil {
			return workqueue.Change{}, err
		}
	} else if !backupruntime.TerminalBackupRunState(run.State) {
		return workqueue.Change{}, conflict("Backup cleanup requires its started or failed resource record")
	}
	orphan, err := repository.captureFailureOrphan(ctx, run, next, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	// A failure recorded earlier already created the orphan. Recheck its exact
	// ownership before releasing source locks; cleanup must not lose that debt.
	for ordinal, source := range run.Sources {
		if source.State != backupruntime.BackupSourceAttemptOrphaned || run.State == backupruntime.BackupRunRunning {
			continue
		}
		guards, err := repository.retainedCaptureOrphan(ctx, run, uint32(ordinal), view.ReadRevision)
		if err != nil {
			keyvalue.ClearMutationValues(orphan.Mutations)
			return workqueue.Change{}, err
		}
		orphan.Conditions = append(orphan.Conditions, guards.Conditions...)
	}
	change, err := repository.prepareCaptureRelease(ctx, view, run, next, guard, orphan)
	if err != nil {
		keyvalue.ClearMutationValues(orphan.Mutations)
	}
	return change, err
}
