package hierarchydeletion

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func EnvironmentReportPrefix(attemptID string) string {
	return "/v1/hierarchy-removal-reports/" + attemptID + "/"
}

func EnvironmentReportKey(attemptID string, sequence uint64) string {
	return EnvironmentReportPrefix(attemptID) + strconv.FormatUint(sequence, 10)
}

type EnvironmentReportReader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

// PrepareEnvironmentCheckpoint joins the immutable report and shared cursor.
// The caller supplies the captured input and adds its source guards and the
// authenticated reporting session to the same transaction before acknowledging.
// Retaining at most four reports permits acknowledgement replay after advancement.
func PrepareEnvironmentCheckpoint(ctx context.Context, reader EnvironmentReportReader, view workqueue.ResumeView,
	input hierarchyexecution.EnvironmentInput, report hierarchyexecution.EnvironmentReport,
) (workqueue.Change, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if !workexecution.SameAssignment(input.Attempt, attempt) || input.Attempt.StartedAt == nil ||
		attempt.StartedAt == nil ||
		!input.Attempt.StartedAt.Equal(*attempt.StartedAt) ||
		item.Operation != workexecution.Hierarchy ||
		task.Action != workexecution.ActionRemove ||
		view.Work.ReadRevision <= 0 {
		return workqueue.Change{}, checkpointConflict("removal report does not match its captured execution")
	}
	raw, err := hierarchyexecution.EncodeEnvironmentReport(input, report)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(raw)
	next, err := report.Checkpoint(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := EnvironmentReportKey(attempt.ID, next.Sequence)
	keys := []string{key}
	if view.Checkpoint.Record != nil {
		keys = append(keys, EnvironmentReportKey(attempt.ID, view.Checkpoint.Record.Sequence))
	}
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, checkpointConflict("removal checkpoint read is incomplete")
	}
	var previous *hierarchyexecution.EnvironmentProgress
	if view.Checkpoint.Record != nil {
		value := read.Values[1]
		if value == nil || value.Key != keys[1] || value.ModRevision <= 0 {
			return workqueue.Change{}, checkpointConflict("removal checkpoint has no saved execution report")
		}
		saved, err := hierarchyexecution.DecodeEnvironmentReport(input, value.Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		cursor, err := saved.Checkpoint(input)
		if err != nil || cursor.AttemptID != view.Checkpoint.Record.AttemptID ||
			cursor.Sequence != view.Checkpoint.Record.Sequence ||
			cursor.StageID != view.Checkpoint.Record.StageID ||
			cursor.Evidence != view.Checkpoint.Record.Evidence ||
			!cursor.RecordedAt.Equal(view.Checkpoint.Record.RecordedAt) {
			return workqueue.Change{}, checkpointConflict("removal report differs from its acknowledged checkpoint")
		}
		previous = &saved.Progress
	}
	if existing := read.Values[0]; existing != nil {
		if existing.Key != key || existing.ModRevision <= 0 || !bytes.Equal(existing.Value, raw) ||
			view.Checkpoint.Record == nil || next.Sequence > view.Checkpoint.Record.Sequence {
			return workqueue.Change{}, checkpointConflict("removal report sequence already has different evidence")
		}
		return workqueue.Change{Conditions: []keyvalue.Condition{
			{Key: key, ModRevision: existing.ModRevision},
			{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)}}}, nil
	}
	if !view.ActiveAttempt || task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return workqueue.Change{}, checkpointConflict("removal execution no longer accepts new reports")
	}
	if report.Progress.InFlight && (!view.CurrentExecutor ||
		attempt.Direction == workexecution.Forward && (item.ForwardRevoked || task.Stop != nil) || !time.Now().UTC().Before(attempt.Deadline)) {
		return workqueue.Change{}, checkpointConflict(
			"removal cannot start another stage after execution was stopped or replaced",
		)
	}
	if err := hierarchyexecution.ValidateEnvironmentTransition(input, previous, report.Progress); err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, next)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision})
	if report.Progress.InFlight {
		change.Conditions = append(change.Conditions, keyvalue.Condition{
			Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision,
		})
	}
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)},
	)
	return change, nil
}

func checkpointConflict(detail string) error { return errs.New(errs.KindStateConflict, detail) }
