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

func unassignedReleaseTerminal(task TaskRecord, status taskjournal.TaskStatus) bool {
	preflightFailure := task.Result != nil && !task.Result.ReconciliationRequired &&
		(status == taskjournal.TaskStatusFailed &&
			(task.Result.Diagnostic == taskjournal.TaskResultDiagnosticReleaseNetworkFailed || task.Result.Diagnostic == taskjournal.TaskResultDiagnosticDNSActivationFailed) ||
			status == taskjournal.TaskStatusTimedOut && task.Result.Diagnostic == taskjournal.TaskResultDiagnosticDNSActivationTimedOut)
	return (status == taskjournal.TaskStatusAborted || preflightFailure) && task.Status == status &&
		task.StartedAt == nil &&
		task.FinishedAt != nil &&
		task.Executor == taskjournal.TaskExecutorAgent &&
		(task.Type == taskjournal.TaskDeploy || task.Type == taskjournal.TaskRollback) &&
		task.Params[releaserender.TaskReleasePublicationParam] != ""
}

// Task terminalization wins its assignment race first. Only then may the
// existing bounded Release finalizer retire never-executed candidates and
// release their fence. The durable timeout scan resumes interrupted cleanup.
func (repository *TaskRepository) finishUnassignedReleaseTerminal(
	ctx context.Context,
	current etcdstore.Versioned[TaskRecord],
) (etcdstore.Versioned[TaskRecord], error) {
	if !unassignedReleaseTerminal(current.Record, current.Record.Status) {
		return current, nil
	}
	for batch := 0; batch < 32; batch++ {
		task := current.Record
		dependency, applies, err := repository.readReleaseNetworkDependency(ctx, task, current.ReadRevision)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if applies && !releaseNetworkEffectsSettled(dependency.child.Record) {
			if dependency.child.Record.Status == taskjournal.TaskStatusPending {
				if _, err := repository.AbortPendingTask(ctx, dependency.child.Record.ID, *task.FinishedAt); err != nil {
					return etcdstore.Versioned[TaskRecord]{}, err
				}
				current, err = repository.GetTask(ctx, task.ID)
				if err != nil {
					return etcdstore.Versioned[TaskRecord]{}, err
				}
				continue
			}
			return current, nil // Assigned or uncertain bridge effects retain the release fence.
		}
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
		result := taskjournal.TaskResultRecord{
			Kind:       taskjournal.TaskResultCompose,
			Diagnostic: taskjournal.TaskResultDiagnosticNone,
		}
		if task.Result != nil {
			result = *task.Result
		}
		proofs := []etcdstore.Condition{
			{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: current.Revision},
			{Key: keys[0]},
		}
		if applies {
			proofs = append(proofs, dependency.conditions...)
		}
		processed, err := repository.finalizeReleaseTaskBatch(
			ctx,
			task,
			taskassignments.TaskAssignmentRecord{},
			task.Status,
			result,
			"",
			*task.FinishedAt,
			current.ReadRevision,
			proofs...,
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
		if !unassignedReleaseTerminal(current.Record, current.Record.Status) {
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
