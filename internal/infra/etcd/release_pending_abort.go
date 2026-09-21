package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func unassignedReleaseAbort(task TaskRecord, status taskjournal.TaskStatus) bool {
	return status == taskjournal.TaskStatusAborted && task.Status == taskjournal.TaskStatusAborted &&
		task.StartedAt == nil &&
		task.FinishedAt != nil &&
		task.Executor == taskjournal.TaskExecutorAgent &&
		(task.Type == taskjournal.TaskDeploy || task.Type == taskjournal.TaskRollback) &&
		task.Params[releaserender.TaskReleasePublicationParam] != ""
}

// Task terminalization wins its assignment race first. Only then may the
// existing bounded Release finalizer retire never-executed candidates and
// release their fence. Replaying Abort resumes this cleanup after interruption.
func (repository *TaskRepository) finishUnassignedReleaseAbort(
	ctx context.Context,
	current etcdstore.Versioned[TaskRecord],
) (etcdstore.Versioned[TaskRecord], error) {
	if !unassignedReleaseAbort(current.Record, current.Record.Status) {
		return current, nil
	}
	for batch := 0; batch < 32; batch++ {
		task := current.Record
		keys := []string{
			taskjournal.TaskAssignmentIndexKey(task.ID),
			releases.ReleaseFenceSetKey(task.Owner.EnvironmentID),
		}
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: current.ReadRevision})
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if read == nil || read.ReadRevision != current.ReadRevision || len(read.Values) != len(keys) ||
			read.Values[0] != nil {
			return etcdstore.Versioned[TaskRecord]{}, taskassignments.CorruptTaskAssignment()
		}
		if read.Values[1] == nil {
			return current, nil
		}
		fence, err := releases.DecodeReleaseRecord[releases.ReleaseFenceSet](read.Values[1].Value, "release-fence-set")
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if fence.OperationID != task.OperationID || fence.AttemptTaskID != task.ID {
			// A later operation owns this fence; the old Abort has no authority over it.
			return current, nil
		}
		processed, err := repository.finalizeReleaseTaskBatch(
			ctx,
			task,
			taskassignments.TaskAssignmentRecord{},
			taskjournal.TaskStatusAborted,
			taskjournal.TaskResultRecord{
				Kind:       taskjournal.TaskResultCompose,
				Diagnostic: taskjournal.TaskResultDiagnosticNone,
			},
			"",
			*task.FinishedAt,
			current.ReadRevision,
			etcdstore.Condition{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: current.Revision},
			etcdstore.Condition{Key: keys[0]},
		)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if !processed {
			return etcdstore.Versioned[TaskRecord]{}, releases.CorruptReleaseRecord()
		}
		current, err = repository.GetTask(ctx, task.ID)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if !unassignedReleaseAbort(current.Record, current.Record.Status) {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"unassigned Release abort authority changed",
			)
		}
	}
	return etcdstore.Versioned[TaskRecord]{}, errs.New(
		errs.KindStateConflict,
		"unassigned Release abort cleanup remains incomplete",
	)
}
