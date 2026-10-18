package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareHierarchyEnvironmentCompletion records the exact final absence report
// as action proof. The shared queue owner must join attempt/slot/claim settlement
// in this transaction; this record alone cannot finish work or release ownership.
func (repository *HierarchyDeletionRepository) PrepareHierarchyEnvironmentCompletion(ctx context.Context, view workqueue.ResumeView,
	input hierarchyexecution.EnvironmentInput, at time.Time,
) (workqueue.Change, error) {
	return repository.prepareHierarchyEnvironmentCompletion(ctx, view, input, at, false)
}

func (repository *HierarchyDeletionRepository) PrepareInterruptedHierarchyEnvironmentCompletion(ctx context.Context, view workqueue.ResumeView,
	input hierarchyexecution.EnvironmentInput, at time.Time,
) (workqueue.Change, error) {
	return repository.prepareHierarchyEnvironmentCompletion(ctx, view, input, at, true)
}

func (repository *HierarchyDeletionRepository) prepareHierarchyEnvironmentCompletion(ctx context.Context, view workqueue.ResumeView,
	input hierarchyexecution.EnvironmentInput, at time.Time, interrupted bool,
) (workqueue.Change, error) {
	if !view.ActiveAttempt || view.CurrentExecutor == interrupted || view.ExecutorRevision <= 0 || view.Attempt.Attempt.FinishedAt != nil || view.Checkpoint.Record == nil ||
		view.Checkpoint.Record.Sequence != 4 || at.Before(view.Checkpoint.Record.RecordedAt) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Environment removal has no completed active checkpoint")
	}
	key := hierarchydeletion.EnvironmentReportKey(view.Attempt.Attempt.ID, 4)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].Key != key {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	report, err := hierarchyexecution.DecodeEnvironmentReport(input, read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	checkpoint, err := report.Checkpoint(input)
	if err != nil || report.Progress.Phase != hierarchyexecution.EnvironmentRemoved || checkpoint.Evidence != view.Checkpoint.Record.Evidence ||
		!checkpoint.RecordedAt.Equal(view.Checkpoint.Record.RecordedAt) {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	// This source check and the report CAS bind completion to the same cleanup
	// target even if a conflicting mutation is admitted concurrently.
	change, err := repository.PrepareHierarchyEnvironmentCheckpoint(ctx, view, input, report)
	if err != nil {
		return workqueue.Change{}, err
	}
	captured, err := repository.LoadHierarchyWork(ctx, view.Work)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	completionKey, err := hierarchydeletion.HierarchyDeletionCompletionKey(captured.Action.ParentOperationID, captured.Action.Ordinal)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	if input.Attempt.Direction == workexecution.Recovery {
		completionKey = hierarchydeletion.EnvironmentRecoveryResultKey(input.Attempt.ID)
	}
	// Native host actions retain their typed final report, not an old child-Task
	// terminal receipt. Controller actions retain their own finalizer proof.
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: completionKey},
		keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(input.Attempt.Executor), ModRevision: view.ExecutorRevision})
	value, err := hierarchydeletion.EncodeEnvironmentResult(hierarchydeletion.EnvironmentResult{Attempt: input.Attempt, Report: report, Recovery: input.Recovery})
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: completionKey, Value: value})
	return change, nil
}
