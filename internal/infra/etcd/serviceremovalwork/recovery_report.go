package serviceremovalwork

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func RecoveryReportPrefix(attemptID string) string {
	return "/v1/execution/service-removal/recovery-reports/" + attemptID + "/"
}

func RecoveryReportKey(attemptID string, sequence uint64) string {
	return RecoveryReportPrefix(attemptID) + strconv.FormatUint(sequence, 10)
}

// PrepareRecoveryCheckpoint persists at most ten bounded reports per attempt.
// The caller joins authenticated-session checks to this change. Restorative
// effects require acknowledgement of a new grant, not just an old replay.
func (repository *Repository) PrepareRecoveryCheckpoint(ctx context.Context, view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput, progress removalexecution.Progress, at time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if item.Operation != workexecution.ServiceRemoval || attempt.Direction != workexecution.Recovery ||
		attempt.Executor.Executor != workexecution.Agent || attempt.StartedAt == nil ||
		!task.PublicationComplete || task.CollectionStartedAt != nil ||
		at.IsZero() || at.Location() != time.UTC || progress.UpdatedAt.After(at) {
		return workqueue.Change{}, conflict("Service removal recovery report does not belong to a published recovery")
	}
	input, captureGuard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	saved, err := repository.LoadRecovery(ctx, view.Work, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	raw, err := removalexecution.EncodeProgress(attempt, delivery, saved.Capture, progress)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(raw)
	next, err := progress.Checkpoint(attempt, delivery, saved.Capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := RecoveryReportKey(attempt.ID, next.Sequence)
	keys := []string{key}
	if view.Checkpoint.Record != nil {
		keys = append(keys, RecoveryReportKey(attempt.ID, view.Checkpoint.Record.Sequence))
	}
	read, err := repository.read(ctx, keys, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := []keyvalue.Condition{captureGuard,
		{Key: RecoveryKey(item.ID), ModRevision: saved.Revision},
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
	}
	var previous *removalexecution.Progress
	if view.Checkpoint.Record != nil {
		value := read.Values[1]
		if value == nil {
			return workqueue.Change{}, conflict("Service removal recovery checkpoint has no saved report")
		}
		decoded, err := removalexecution.DecodeProgress(attempt, delivery, saved.Capture, value.Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		checkpoint, err := decoded.Checkpoint(attempt, delivery, saved.Capture)
		if err != nil || checkpoint != *view.Checkpoint.Record {
			return workqueue.Change{}, conflict("Service removal recovery report differs from its acknowledged checkpoint")
		}
		previous = &decoded
		conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	if existing := read.Values[0]; existing != nil {
		if !bytes.Equal(existing.Value, raw) || view.Checkpoint.Record == nil || next.Sequence > view.Checkpoint.Record.Sequence {
			return workqueue.Change{}, conflict("Service removal recovery sequence already contains different evidence")
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: existing.ModRevision},
			keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision})
		return workqueue.Change{Conditions: conditions, Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)},
		}}, nil
	}
	if !view.ActiveAttempt || task.FinishedAt != nil {
		return workqueue.Change{}, conflict("Service removal recovery no longer accepts new evidence")
	}
	if progress.InFlight {
		if !view.CurrentExecutor || !at.Before(attempt.Deadline) || item.Cleanup != workexecution.CleanupRunning {
			return workqueue.Change{}, conflict("Service removal recovery cannot authorize another effect on this executor")
		}
		runtime, err := repository.RuntimeGuards(ctx, input, view.Work.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		conditions = append(conditions, runtime...)
		conditions = append(conditions, keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	}
	if err := removalexecution.ValidateTransition(attempt, delivery, saved.Capture, previous, progress); err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, next)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)})
	return change, nil
}
