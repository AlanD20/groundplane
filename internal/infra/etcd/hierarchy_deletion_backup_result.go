package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionbackupcleanup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type hierarchyBackupResult struct {
	AttemptID     string              `json:"attempt_id"`
	Input         workexecution.Input `json:"input"`
	Reason        string              `json:"reason"`
	DeleteGranted bool                `json:"delete_granted"`
	Absent        bool                `json:"absent"`
	RecordedAt    time.Time           `json:"recorded_at"`
}

func HierarchyBackupResultKey(attemptID string) string {
	return "/v1/runtime/removal-backup-results/" + attemptID
}

// PrepareHierarchyBackupResult joins exact remote absence, metadata retirement
// and completion. Without absence a delete grant retains claims, independently
// of cancellation, deadline or loss of the provider response.
func (repository *HierarchyDeletionRepository) PrepareHierarchyBackupResult(ctx context.Context, view workqueue.ResumeView, at time.Time) (workqueue.Change, workexecution.Completion, error) {
	item, attempt := view.Work.Item.Item, view.Attempt.Attempt
	if !view.ActiveAttempt || view.ExecutorRevision <= 0 ||
		attempt.Executor.Executor != workexecution.Controller ||
		item.Operation != workexecution.RemovalBackupObject || attempt.FinishedAt != nil ||
		(attempt.Direction != workexecution.Forward && attempt.Direction != workexecution.Recovery) || workexecution.ValidateDelivery(item, attempt) != nil ||
		at.IsZero() || at.Before(item.UpdatedAt) {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(errs.KindStateConflict, "backup removal no longer owns its result")
	}
	captured, err := repository.LoadHierarchyWork(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	observed, err := hierarchydeletionbackupcleanup.NewRepository(repository.store).
		ReadOutcome(ctx, captured.Operation, captured.Action, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change := workqueue.Change{Conditions: append(captured.Conditions, observed.Conditions...)}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	if observed.Absent && attempt.StartedAt != nil && attempt.Direction == workexecution.Forward {
		effects, err := hierarchydeletionfinalization.NewPreparer(repository.store).PrepareCaptured(ctx, captured.Operation, captured.Action)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		value, result, err := hierarchyActionCompletion(view.Work.Task.Task, item, captured, at)
		if err != nil {
			keyvalue.ClearByteSlices(effects.Values())
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		key, err := hierarchydeletion.HierarchyDeletionCompletionKey(captured.Action.ParentOperationID, captured.Action.Ordinal)
		if err != nil {
			clear(value)
			keyvalue.ClearByteSlices(effects.Values())
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		change.Conditions = append(change.Conditions, effects.Conditions()...)
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
		change.Mutations = append(change.Mutations, effects.Mutations()...)
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
		return change, workexecution.Completion{Outcome: workexecution.Succeeded, Evidence: result, EffectsSettled: true}, nil
	}
	reason := "object_absence_unconfirmed"
	if attempt.StartedAt == nil {
		reason = "execution_not_started"
	} else if !observed.DeleteGranted {
		reason = "delete_not_authorized"
	}
	if observed.Absent && attempt.StartedAt != nil {
		reason = "object_absence_confirmed"
	}
	failure := hierarchyBackupResult{AttemptID: attempt.ID, Input: attempt.Input, Reason: reason,
		DeleteGranted: observed.DeleteGranted, Absent: observed.Absent, RecordedAt: at}
	value, err := recordcodec.Encode("hierarchy-backup-result", failure)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	key := HierarchyBackupResultKey(attempt.ID)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}
	outcome := workexecution.Failed
	if attempt.Direction == workexecution.Recovery && observed.Absent && attempt.StartedAt != nil {
		outcome = workexecution.Succeeded
	} else if view.Work.Task.Task.Stop != nil {
		outcome = view.Work.Task.Task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	return change, workexecution.Completion{Outcome: outcome, EffectsSettled: observed.Absent || !observed.DeleteGranted,
		Evidence: workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, view.Work.Task.Task.CreatedAt, attempt.ID, "backup-removal-result"),
			SHA256: hierarchydeletion.HierarchyDeletionBytesDigest(value)}}, nil
}
