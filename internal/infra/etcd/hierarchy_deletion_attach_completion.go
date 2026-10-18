package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) PrepareHierarchyAttachCompletion(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, at time.Time,
) (workqueue.Change, error) {
	return repository.prepareHierarchyAttachCompletion(ctx, view, input, at, false)
}

func (repository *HierarchyDeletionRepository) PrepareInterruptedHierarchyAttachCompletion(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, at time.Time,
) (workqueue.Change, error) {
	return repository.prepareHierarchyAttachCompletion(ctx, view, input, at, true)
}

func (repository *HierarchyDeletionRepository) prepareHierarchyAttachCompletion(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, at time.Time, interrupted bool,
) (workqueue.Change, error) {
	if !view.ActiveAttempt || view.CanSettleResult() == interrupted || view.ExecutorRevision <= 0 || view.Attempt.Attempt.FinishedAt != nil ||
		view.Checkpoint.Record == nil || view.Checkpoint.Record.Sequence != uint64(input.CommandCount())*2 ||
		at.Before(view.Checkpoint.Record.RecordedAt) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach removal has no completed active checkpoint")
	}
	key := attachremoval.WorkReportKey(input.Attempt.ID, view.Checkpoint.Record.Sequence)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].Key != key {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	report, err := attachexecution.DecodeRemovalReport(input, read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	checkpoint, err := report.Checkpoint(input)
	if err != nil || !report.Progress.Complete(input) || checkpoint.Evidence != view.Checkpoint.Record.Evidence ||
		checkpoint.StageID != view.Checkpoint.Record.StageID || !checkpoint.RecordedAt.Equal(view.Checkpoint.Record.RecordedAt) {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	change, err := repository.PrepareHierarchyAttachCheckpoint(ctx, view, input, report)
	if err != nil {
		return workqueue.Change{}, err
	}
	captured, err := repository.LoadHierarchyWork(ctx, view.Work)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	completionKey, err := hierarchydeletion.HierarchyDeletionCompletionKey(captured.Action.ParentOperationID, captured.Action.Ordinal)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	authority, err := input.Authority()
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	raw, err := attachremoval.EncodeWorkResult(attachremoval.WorkResult{Attempt: input.Attempt, Authority: authority, Report: report})
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: completionKey},
		keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(input.Attempt.Executor), ModRevision: view.ExecutorRevision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: completionKey, Value: raw})
	return change, nil
}
