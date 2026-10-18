package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareRestoreRecoveryCheckpoint records continuation under the recovery
// attempt while advancing the original work's source cursor. The caller must
// join the corresponding Restore resource transition in the same transaction.
func (repository *InputRepository) PrepareRestoreRecoveryCheckpoint(ctx context.Context, view workqueue.ResumeView,
	request *agentpb.BackupWorkCheckpointRequest, evidence CheckpointEvidence, at time.Time,
) (workqueue.Change, *agentpb.BackupWorkCheckpointAck, error) {
	admitted, err := repository.InspectRestoreRecoveryCheckpoint(ctx, view, request)
	if err != nil {
		return workqueue.Change{}, nil, err
	}
	if admitted.Replay != nil {
		return workqueue.Change{Conditions: admitted.Guards}, admitted.Replay, nil
	}
	attempt := view.Attempt.Attempt
	if at.IsZero() || attempt.StartedAt == nil || at.Before(*attempt.StartedAt) || !at.Before(attempt.Deadline) {
		return workqueue.Change{}, nil, conflict("Restore recovery checkpoint is outside its execution deadline")
	}
	// Recovery is allowed after forward cancellation, but only with its own
	// current start grant and original transfer evidence. It cannot capture a
	// new Volume manifest or choose a different Restore source.
	if evidence.VolumeManifest != nil {
		return workqueue.Change{}, nil, conflict("Restore recovery cannot capture a new Volume manifest")
	}
	if evidence.Config != nil {
		matched := false
		for _, guard := range evidence.Conditions {
			if guard.Key == "" || guard.ModRevision <= 0 || guard.ModRevision > view.Work.ReadRevision {
				return workqueue.Change{}, nil, conflict("Restore recovery transfer evidence is outside its snapshot")
			}
			matched = matched || guard.ModRevision == evidence.Config.Revision
		}
		if !matched {
			return workqueue.Change{}, nil, conflict("Restore recovery Config transfer revision is not fenced")
		}
	}
	next, err := executionplan.AdvanceBackupRestoreRecovery(admitted.Recovery, admitted.Request, attempt.Executor, evidence.Config)
	if err != nil {
		return workqueue.Change{}, nil, err
	}
	change, err := repository.prepareCheckpointWrite(ctx, view, admitted, admitted.Recovery.Input,
		len(admitted.Recovery.Sources), next, evidence.Conditions, at)
	return change, nil, err
}
