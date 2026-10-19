package volumework

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ReportPrefix(attemptID string) string { return "/v1/volume-work-reports/" + attemptID + "/" }

func ReportKey(attemptID string, sequence uint64) string {
	return ReportPrefix(attemptID) + strconv.FormatUint(sequence, 10)
}

type ReportReader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

// PrepareCheckpoint persists the bounded stage report with the execution cursor.
// The publication owner must join captured-input and authenticated-session guards
// before committing. Each attempt retains at most six immutable reports.
func PrepareCheckpoint(ctx context.Context, reader ReportReader, view workqueue.ResumeView,
	input *agentpb.VolumeWorkInput, progress volumeexecution.Progress, at time.Time,
) (workqueue.Change, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if reader == nil || attempt.StartedAt == nil || item.Operation != workexecution.Volume ||
		view.Work.ReadRevision <= 0 || at.IsZero() || progress.UpdatedAt.After(at) ||
		!task.PublicationComplete || task.CollectionStartedAt != nil {
		return workqueue.Change{}, conflict("volume report does not match a published execution")
	}
	owned, captured, reference, err := executionplan.CaptureVolumeWorkInput(attempt.Input.ID, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(captured)
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward ||
		attempt.Executor.Executor != workexecution.Agent || reference != attempt.Input {
		return workqueue.Change{}, conflict("volume report input differs from its captured assignment")
	}
	raw, err := volumeexecution.EncodeProgress(attempt, owned.Action, owned.VolumeId, progress)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(raw)
	next, err := progress.Checkpoint(attempt, owned.Action, owned.VolumeId)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := ReportKey(attempt.ID, next.Sequence)
	keys := []string{key}
	if view.Checkpoint.Record != nil {
		keys = append(keys, ReportKey(attempt.ID, view.Checkpoint.Record.Sequence))
	}
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, conflict("volume report read is incomplete")
	}
	var previous *volumeexecution.Progress
	if view.Checkpoint.Record != nil {
		value := read.Values[1]
		if value == nil || value.Key != keys[1] || value.ModRevision <= 0 {
			return workqueue.Change{}, conflict("volume checkpoint has no saved report")
		}
		saved, err := volumeexecution.DecodeProgress(attempt, owned.Action, owned.VolumeId, value.Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		cursor, err := saved.Checkpoint(attempt, owned.Action, owned.VolumeId)
		ack := view.Checkpoint.Record
		if err != nil || cursor.AttemptID != ack.AttemptID || cursor.Sequence != ack.Sequence ||
			cursor.StageID != ack.StageID || cursor.Evidence != ack.Evidence || !cursor.RecordedAt.Equal(ack.RecordedAt) {
			return workqueue.Change{}, conflict("volume report differs from its saved checkpoint")
		}
		previous = &saved
	}
	if existing := read.Values[0]; existing != nil {
		if existing.Key != key || existing.ModRevision <= 0 || !bytes.Equal(existing.Value, raw) ||
			view.Checkpoint.Record == nil || next.Sequence > view.Checkpoint.Record.Sequence {
			return workqueue.Change{}, conflict("volume report sequence already has different evidence")
		}
		return workqueue.Change{Conditions: []keyvalue.Condition{
			{Key: key, ModRevision: existing.ModRevision},
			{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)}}}, nil
	}
	if !view.ActiveAttempt || task.FinishedAt != nil {
		return workqueue.Change{}, conflict("volume execution no longer accepts new reports")
	}
	if progress.InFlight &&
		(!view.CurrentExecutor || item.ForwardRevoked || task.Stop != nil || !at.Before(attempt.Deadline)) {
		return workqueue.Change{}, conflict("volume cannot start another stage after execution was stopped or replaced")
	}
	if err := volumeexecution.ValidateTransition(attempt, owned.Action, owned.VolumeId, previous, progress); err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, next)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision})
	if progress.InFlight {
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

func conflict(detail string) error { return errs.New(errs.KindStateConflict, detail) }
