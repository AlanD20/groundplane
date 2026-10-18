package backup

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"time"
)

// Database state and the native execution receipt are committed together.
// Apply-start remains a spent-effect boundary, never permission to retry SQL.
func (owner *AgentWork) prepareDatabaseRestoreProgress(ctx context.Context,
	view workqueue.ResumeView, admitted backupwork.CheckpointAdmission, at time.Time,
) (workqueue.Change, error) {
	current, guard, err := owner.inputs.LoadStagingRestore(ctx, backupwork.StagingSource{
		View: view.Work, Input: admitted.Input(), Sources: admitted.Sources(),
		Ordinal: admitted.Request.SourceOrdinal,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	if !at.After(current.UpdatedAt) {
		at = current.UpdatedAt.Add(time.Nanosecond)
	}
	return owner.configRuntime.PrepareDatabaseRestoreProgress(ctx, view, admitted,
		keyvalue.Versioned[backupruntime.BackupRestoreRecord]{
			Record: current, Revision: guard.ModRevision, ReadRevision: view.Work.ReadRevision,
		}, at)
}
