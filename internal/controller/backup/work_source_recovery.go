package backup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *AgentWork) ResolveSourceRecovery(ctx context.Context, identity workexecution.Identity,
	entry *agentpb.BackupRecoveredStage,
) (*agentpb.BackupStagingDisposition, []keyvalue.Condition, error) {
	if executionplan.ValidateBackupStagingInventory(
		&agentpb.BackupStagingInventory{Entries: []*agentpb.BackupRecoveredStage{entry}},
	) != nil {
		return nil, nil, invalidCaptureStaging()
	}
	source, err := owner.inputs.ReadStagingSource(ctx, owner.queue, identity, entry.RecoveryKeySha256)
	if err != nil {
		return nil, nil, err
	}
	step := source.Input.Steps[source.Ordinal]
	switch {
	case step.GetRestore().GetVolume() != nil:
		return owner.volumeRestoreStaging(ctx, identity, entry, source)
	case step.GetRestore() != nil:
		return owner.restoreSourceStaging(ctx, identity, entry, source)
	case step.GetCapture().GetVolume() != nil:
		return owner.volumeCaptureStaging(ctx, identity, entry, source)
	case step.GetCapture() != nil:
		return owner.captureSourceStaging(ctx, identity, entry, source)
	default:
		return nil, nil, invalidCaptureStaging()
	}
}

func (owner *AgentWork) captureSourceStaging(ctx context.Context, identity workexecution.Identity,
	entry *agentpb.BackupRecoveredStage, source backupwork.StagingSource,
) (*agentpb.BackupStagingDisposition, []keyvalue.Condition, error) {
	run, guard, err := owner.inputs.LoadStagingCapture(ctx, source)
	if err != nil {
		return nil, nil, err
	}
	step := source.Input.Steps[source.Ordinal]
	capture := step.GetCapture()
	database := capture.GetPostgres() != nil || capture.GetMysql() != nil
	if !database && (capture.GetConfig() == nil || entry.DatabaseExecution) {
		return nil, nil, invalidCaptureStaging()
	}
	guards := append(append([]keyvalue.Condition(nil), source.Conditions...), guard)
	item := source.View.Item.Item
	settled := item.Phase == workexecution.Settled &&
		(item.Cleanup == workexecution.CleanupNone || item.Cleanup == workexecution.CleanupComplete)
	terminal := run.State == backupruntime.BackupRunCompleted || run.State == backupruntime.BackupRunFailed ||
		run.State == backupruntime.BackupRunAborted || run.State == backupruntime.BackupRunTimedOut
	var decision *agentpb.BackupStagingDisposition
	if settled && terminal {
		decision = discardCapturedFiles(entry)
	} else {
		if terminal {
			return nil, nil, invalidCaptureStaging()
		}
		retained := source.Sources[source.Ordinal].GetCapture()
		if retained == nil {
			return nil, nil, invalidCaptureStaging()
		}
		var liveGuards []keyvalue.Condition
		switch {
		case database && entry.DatabaseExecution && len(entry.Files) == 0 &&
			(retained.GetSourceCleanupCompleted() != nil || retained.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT ||
				retained.Phase == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_SOURCE_CLEANUP):
			_, liveGuards, err = owner.stagingAssignment(ctx, identity, source)
			decision = discardCapturedFiles(entry)
		case retained.PreparedArtifact != nil:
			decision, liveGuards, err = owner.resumePreparedCapture(ctx, identity, entry, source)
		case database:
			var assignment *agentpb.BackupWorkAssignment
			assignment, liveGuards, err = owner.stagingAssignment(ctx, identity, source)
			if err == nil {
				var digest []byte
				digest, err = executionplan.BackupWorkStagingResumeSHA256(assignment, identity)
				if err == nil {
					decision, err = databaseCapturePrefix(entry, step, retained, digest)
				}
			}
		default:
			decision, liveGuards, err = owner.configCapturePrefix(ctx, identity, entry, source, run)
		}
		if err != nil {
			return nil, nil, err
		}
		guards = append(guards, liveGuards...)
	}
	if database {
		if err := attachDatabaseRecoveryGuard(source, nil, decision); err != nil {
			return nil, nil, err
		}
	}
	return decision, guards, nil
}
