package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Prepare immutable values/files before taking the checkpoint's MVCC snapshot.
// Publication still waits for the atomically committed native receipt.
func (owner *AgentWork) prepareRestorePublication(ctx context.Context, view workqueue.ResumeView,
	admitted backupwork.CheckpointAdmission,
) (*ConfigRestorePublication, error) {
	report := admitted.Request
	if report.GetConfig().GetTransferCompleted() == nil && report.GetConfig().GetMaterializationVerified() == nil {
		return nil, nil
	}
	binding, err := admitted.ConfigBinding(view.Attempt.Attempt.Executor)
	if err != nil {
		return nil, err
	}
	step := admitted.Input().Steps[report.SourceOrdinal]
	transfer, err := configRestoreTransferOwner(
		backupconfiguration.ConfigTransferOwner{Binding: binding},
		step.GetRestore(),
	)
	if err != nil {
		return nil, err
	}
	current, _, err := owner.inputs.LoadStagingRestore(ctx, backupwork.StagingSource{
		View: view.Work, Input: admitted.Input(), Sources: admitted.Sources(),
		Ordinal: report.SourceOrdinal,
	})
	if err != nil {
		return nil, err
	}
	content := step.GetRestore().GetConfig().GetExpectedArchive().GetContent()
	if current.ConfigProgress.RevisionRootRevision == 0 {
		return owner.configRestores.PreparePublished(ctx, transfer, content, step.GetRestore().PointId)
	}
	var candidate *ConfigRestorePublication
	if admitted.Recovery != nil {
		candidate, err = owner.configRestores.PrepareRecoveryCandidate(
			ctx,
			transfer,
			content,
			step.GetRestore().PointId,
			admitted.Recovery,
			view.Attempt.Attempt.Executor,
		)
	} else {
		candidate, err = owner.configRestores.PrepareCandidate(ctx, transfer, content, step.GetRestore().PointId)
	}
	if err != nil {
		return nil, err
	}
	if err := owner.configRestores.ProveCandidateFiles(ctx, candidate); err != nil {
		candidate.Clear()
		return nil, err
	}
	candidate.RootRevision = current.ConfigProgress.RevisionRootRevision
	return candidate, nil
}

func (owner *AgentWork) prepareConfigRestoreProgress(ctx context.Context, view workqueue.ResumeView,
	admitted backupwork.CheckpointAdmission, candidate *ConfigRestorePublication, at time.Time,
) (workqueue.Change, error) {
	current, guard, err := owner.inputs.LoadStagingRestore(ctx, backupwork.StagingSource{
		View: view.Work, Input: admitted.Input(), Sources: admitted.Sources(),
		Ordinal: admitted.Request.SourceOrdinal,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	var publication *etcd.ConfigRestoreCheckpointPublication
	if candidate != nil {
		publication = &etcd.ConfigRestoreCheckpointPublication{
			Generation: candidate.SourceSeal, Root: candidate.Revision.Seal,
			RootRevision: candidate.RootRevision, DeleteEntryCount: uint32(len(candidate.DeleteEntries)),
			Projection: candidate.Projection, Identities: candidate.Identities, FileProof: candidate.FileProof,
		}
	}
	if !at.After(current.UpdatedAt) {
		at = current.UpdatedAt.Add(time.Nanosecond)
	}
	return owner.configRuntime.PrepareConfigRestoreProgress(ctx, view, admitted,
		keyvalue.Versioned[backupruntime.BackupRestoreRecord]{
			Record: current, Revision: guard.ModRevision, ReadRevision: view.Work.ReadRevision,
		}, publication, at)
}
