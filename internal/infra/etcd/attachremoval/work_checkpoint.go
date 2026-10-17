package attachremoval

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func WorkFailureKey(attemptID string) string { return "/v1/attach-removal-failures/" + attemptID }

func WorkRecoveryKey(workID string) string { return "/v1/attach-removal-recovery/" + workID }

func WorkInspectionKey(attemptID string) string {
	return "/v1/attach-removal-inspections/" + attemptID
}

func WorkReportPrefix(attemptID string) string {
	return "/v1/attach-removal-reports/" + attemptID + "/"
}
func WorkReportKey(attemptID string, sequence uint64) string {
	return WorkReportPrefix(attemptID) + strconv.FormatUint(sequence, 10)
}

// PrepareRemovalCheckpoint saves at most two reports per captured command.
// The caller joins the frozen capture and authenticated Agent guards before ack.
func PrepareRemovalCheckpoint(ctx context.Context, reader Reader, view workqueue.ResumeView,
	input attachexecution.RemovalInput, report attachexecution.RemovalReport,
) (workqueue.Change, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if !workexecution.SameAssignment(input.Attempt, attempt) || attempt.StartedAt == nil || input.Attempt.StartedAt == nil ||
		!input.Attempt.StartedAt.Equal(*attempt.StartedAt) || !removalOperation(item.Operation) ||
		view.Work.ReadRevision <= 0 {
		return workqueue.Change{}, checkpointConflict("Attach removal report has no matching started execution")
	}
	raw, err := attachexecution.EncodeRemovalReport(input, report)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(raw)
	next, err := report.Checkpoint(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := WorkReportKey(attempt.ID, next.Sequence)
	keys := []string{key}
	if view.Checkpoint.Record != nil {
		keys = append(keys, WorkReportKey(attempt.ID, view.Checkpoint.Record.Sequence))
	}
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, checkpointConflict("Attach removal checkpoint read is incomplete")
	}
	var previous *attachexecution.RemovalProgress
	if view.Checkpoint.Record != nil {
		value := read.Values[1]
		if value == nil || value.Key != keys[1] || value.ModRevision <= 0 {
			return workqueue.Change{}, checkpointConflict("Attach removal has no saved checkpoint report")
		}
		saved, err := attachexecution.DecodeRemovalReport(input, value.Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		cursor, err := saved.Checkpoint(input)
		if err != nil || !sameCheckpoint(cursor, *view.Checkpoint.Record) {
			return workqueue.Change{}, checkpointConflict("Attach removal report differs from its saved cursor")
		}
		previous = &saved.Progress
	}
	if existing := read.Values[0]; existing != nil {
		if existing.Key != key || existing.ModRevision <= 0 || !bytes.Equal(existing.Value, raw) ||
			view.Checkpoint.Record == nil || next.Sequence > view.Checkpoint.Record.Sequence {
			return workqueue.Change{}, checkpointConflict("Attach removal sequence has different saved evidence")
		}
		return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: existing.ModRevision},
			{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision}},
			Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)}}}, nil
	}
	if !view.ActiveAttempt || task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return workqueue.Change{}, checkpointConflict("Attach removal no longer accepts new reports")
	}
	if report.Progress.InFlight && (!view.CurrentExecutor || item.ForwardRevoked || task.Stop != nil || !time.Now().UTC().Before(attempt.Deadline)) {
		return workqueue.Change{}, checkpointConflict("Attach removal cannot begin a command after stop, replacement or deadline")
	}
	if err := attachexecution.ValidateRemovalTransition(input, previous, report.Progress); err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, next)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision})
	if report.Progress.InFlight {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)})
	return change, nil
}

func sameCheckpoint(left, right workexecution.Checkpoint) bool {
	return left.AttemptID == right.AttemptID && left.Sequence == right.Sequence && left.StageID == right.StageID &&
		left.Evidence == right.Evidence && left.RecordedAt.Equal(right.RecordedAt)
}

func checkpointConflict(detail string) error { return errs.New(errs.KindStateConflict, detail) }

func removalOperation(operation workexecution.Operation) bool {
	return operation == workexecution.Hierarchy || operation == workexecution.Detach
}
