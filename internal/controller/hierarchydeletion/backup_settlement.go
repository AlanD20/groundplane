package hierarchydeletion

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RejectUnstarted has no external effect to undo. Settle records the refusal
// from the same native attempt and stored deletion evidence.
func (owner *BackupWork) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	attempt := delivery.Attempt.Attempt
	view, err := owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if !view.ActiveAttempt || view.ExecutorRevision <= 0 || view.Attempt.Attempt.StartedAt != nil ||
		view.Attempt.Attempt.FinishedAt != nil || view.Work.Item.Item.Operation != workexecution.RemovalBackupObject {
		return errs.New(errs.KindStateConflict, "backup removal attempt is no longer unstarted")
	}
	return nil
}

func (owner *BackupWork) Settle(
	ctx context.Context,
	queue *workqueue.Repository,
	delivery workqueue.Delivery,
	at time.Time,
) error {
	attempt := delivery.Attempt.Attempt
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if view.Attempt.Attempt.FinishedAt != nil {
		return nil
	}
	proof, result, err := owner.records.PrepareHierarchyBackupResult(ctx, view, at)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(proof.Mutations)
	var completion workqueue.Completion
	if view.CurrentExecutor {
		completion, err = queue.ReadCompletionAt(ctx, view)
	} else {
		completion, err = queue.ReadInterruptedBackupRemovalAt(ctx, view)
	}
	if err != nil {
		return err
	}
	completion.Result, completion.At = result, at
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	committed, err := queue.Commit(ctx, proof, change)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "backup removal result changed before settlement")
	}
	return nil
}
