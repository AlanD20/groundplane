package resolverwork

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func ReportPrefix(attemptID string) string {
	return "/v1/execution/resolver/reports/" + attemptID + "/"
}
func ReportKey(attemptID string, sequence uint64) string {
	return ReportPrefix(attemptID) + strconv.FormatUint(sequence, 10)
}

func (repository *Repository) PrepareProgress(ctx context.Context, view workqueue.ResumeView,
	progress resolverexecution.Progress, at time.Time,
) (workqueue.Change, workexecution.Checkpoint, error) {
	stored, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	attempt := view.Attempt.Attempt
	layout, err := resolverexecution.CaptureLayout(attempt, stored.Input)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	raw, err := resolverexecution.EncodeProgress(attempt, layout, progress)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	next, err := progress.Checkpoint(attempt, layout)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	key := ReportKey(attempt.ID, progress.Sequence)
	keys := []string{key}
	if cursor := view.Checkpoint.Record; cursor != nil {
		keys = append(keys, ReportKey(attempt.ID, cursor.Sequence))
	}
	read, err := repository.read(ctx, keys, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	var previous *resolverexecution.Progress
	if cursor := view.Checkpoint.Record; cursor != nil {
		if read.Values[1] == nil {
			return workqueue.Change{}, workexecution.Checkpoint{}, invalid("DNS resolver acknowledged checkpoint lost its recorded observation")
		}
		progress, err := resolverexecution.DecodeProgress(attempt, layout, read.Values[1].Value)
		if err != nil {
			return workqueue.Change{}, workexecution.Checkpoint{}, err
		}
		saved, err := progress.Checkpoint(attempt, layout)
		if err != nil || saved != *cursor {
			return workqueue.Change{}, workexecution.Checkpoint{}, invalid("DNS resolver checkpoint differs from its recorded observation")
		}
		previous = &progress
	}
	if existing := read.Values[0]; existing != nil {
		if !bytes.Equal(existing.Value, raw) || view.Checkpoint.Record == nil || next.Sequence > view.Checkpoint.Record.Sequence {
			return workqueue.Change{}, workexecution.Checkpoint{}, invalid("DNS resolver checkpoint sequence already contains different evidence")
		}
		return workqueue.Change{Conditions: append(append([]keyvalue.Condition(nil), stored.Guards...),
			keyvalue.Condition{Key: key, ModRevision: existing.ModRevision},
			keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision})}, next, nil
	}
	if !view.ActiveAttempt || !view.CurrentExecutor || view.Work.Task.Task.FinishedAt != nil || at.Before(progress.UpdatedAt) ||
		progress.InFlight && (view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked || !at.Before(attempt.Deadline)) {
		return workqueue.Change{}, workexecution.Checkpoint{}, invalid("DNS resolver no longer permits this execution boundary")
	}
	if err := resolverexecution.ValidateTransition(attempt, layout, previous, progress); err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, next)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	change.Conditions = append(change.Conditions, stored.Guards...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	if progress.InFlight {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.HostForwardFenceKey(attempt.Executor.HostID)})
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: raw})
	return change, next, nil
}

func (repository *Repository) ReadProgress(ctx context.Context, view workqueue.ResumeView, input resolverexecution.Input, sequence uint64) (resolverexecution.Progress, error) {
	layout, err := resolverexecution.CaptureLayout(view.Attempt.Attempt, input)
	if err != nil {
		return resolverexecution.Progress{}, err
	}
	if view.Checkpoint.Record == nil || sequence == 0 || sequence > view.Checkpoint.Record.Sequence {
		return resolverexecution.Progress{}, invalid("DNS resolver progress is not covered by its acknowledged checkpoint")
	}
	read, err := repository.read(ctx, []string{ReportKey(view.Attempt.Attempt.ID, sequence)}, view.Work.ReadRevision)
	if err != nil {
		return resolverexecution.Progress{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return resolverexecution.Progress{}, invalid("DNS resolver recorded progress is unavailable")
	}
	progress, err := resolverexecution.DecodeProgress(view.Attempt.Attempt, layout, read.Values[0].Value)
	if err != nil {
		return resolverexecution.Progress{}, err
	}
	cursor, err := progress.Checkpoint(view.Attempt.Attempt, layout)
	if err != nil || cursor.Sequence != sequence || sequence == view.Checkpoint.Record.Sequence && cursor != *view.Checkpoint.Record {
		return resolverexecution.Progress{}, invalid("DNS resolver progress differs from its acknowledged checkpoint")
	}
	return progress, nil
}
