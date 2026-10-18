package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// VolumeTransferGuard binds each durable credit to its live native execution.
// Stored bindings identify an owner; they never substitute for current start,
// deadline, revocation, Agent generation or Environment ownership checks.
func (repository *InputRepository) VolumeTransferGuard(queue *workqueue.Repository) backupvolumemanifest.Guard {
	return func(ctx context.Context, owner backupvolumemanifest.Owner, revision int64) ([]keyvalue.Condition, error) {
		if queue == nil || owner.Validate() != nil || revision <= 0 {
			return nil, conflict("Volume transfer requires native execution ownership")
		}
		binding := owner.Binding
		original := binding.Attempt
		work, err := queue.ReadWork(ctx, original.TaskID, original.WorkItemID, revision)
		if err != nil {
			return nil, err
		}
		_, active, err := queue.ReadAttempt(ctx, original.TaskID, original.WorkItemID, work.Item.Item.AttemptNumber, revision)
		if err != nil {
			return nil, err
		}
		attempt := active.Attempt
		view, err := queue.ReadResumeAt(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor, revision)
		if err != nil {
			return nil, err
		}
		live := view.Attempt.Attempt
		task, item := view.Work.Task.Task, view.Work.Item.Item
		at := time.Now().UTC()
		if !workexecution.SameAssignment(live, attempt) || live.StartedAt == nil || live.FinishedAt != nil ||
			!view.ActiveAttempt || !view.CurrentExecutor || !at.Before(live.Deadline) {
			return nil, conflict("Volume transfer attempt is stopped, expired or replaced")
		}
		var expected backupvolumetransfer.Binding
		var guarded workqueue.Change
		if live.Direction == workexecution.Recovery {
			if binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD {
				return nil, conflict("Volume recovery can publish only its stopped original tree")
			}
			assignment, change, loadErr := repository.LoadRecoveryAssignment(ctx, view)
			if loadErr != nil {
				return nil, loadErr
			}
			if assignment.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION {
				return nil, conflict("Volume recovery has no captured live mutation")
			}
			guarded = change
			expected, err = backupvolumetransfer.BindRecoverySource(assignment, live.Executor, binding.SourceOrdinal, binding.Direction)
		} else {
			if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) || !at.Before(task.Deadline) {
				return nil, conflict("Volume forward transfer is stopped or expired")
			}
			assignment, change, loadErr := repository.LoadAssignment(ctx, view)
			if loadErr != nil {
				return nil, loadErr
			}
			guarded = change
			expected, err = backupvolumetransfer.Bind(assignment, live.Executor, binding.SourceOrdinal, binding.Direction)
		}
		if err != nil {
			return nil, err
		}
		if expected != binding {
			return nil, conflict("Volume transfer differs from captured source authority")
		}
		return append(guarded.Conditions,
			keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
			keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
			keyvalue.Condition{Key: workqueue.AttemptKey(live.ID), ModRevision: view.Attempt.Revision},
			keyvalue.Condition{Key: workqueue.ExecutorKey(live.Executor), ModRevision: view.ExecutorRevision}), nil
	}
}
