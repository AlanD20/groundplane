package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func CheckpointKey(attemptID string) string { return root + "checkpoints/" + attemptID }

type CheckpointSnapshot struct {
	Record   *workexecution.Checkpoint
	Revision int64
}

func validateCheckpoint(record workexecution.Checkpoint) error {
	if ids.Validate(ids.KindAttempt, record.AttemptID) != nil || record.Sequence == 0 ||
		ids.Validate(ids.KindStep, record.StageID) != nil || record.RecordedAt.IsZero() {
		return invalid("execution checkpoint identity is invalid")
	}
	return record.Evidence.Validate()
}

func DecodeCheckpoint(value []byte) (workexecution.Checkpoint, error) {
	if len(value) > maximumRecordBytes {
		return workexecution.Checkpoint{}, corrupt()
	}
	record, err := recordcodec.Decode[workexecution.Checkpoint](value, "execution_checkpoint")
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	if err := validateCheckpoint(record); err != nil {
		return workexecution.Checkpoint{}, corrupt()
	}
	return record, nil
}

// PrepareStart persists permission before the executor can cross its first
// effect boundary. It is joined to current operation-specific admission checks.
func PrepareStart(task TaskSnapshot, item ItemSnapshot, attempt AttemptSnapshot, executor ExecutorSnapshot, at time.Time) (Change, error) {
	if task.Task.Stop != nil && attempt.Attempt.Direction == workexecution.Forward {
		return Change{}, errs.New(errs.KindStateConflict, "task no longer permits forward execution")
	}
	if item.Revision <= 0 || attempt.Revision <= 0 {
		return Change{}, invalid("execution start requires versioned ownership")
	}
	if err := executor.Record.Validate(); err != nil {
		return Change{}, err
	}
	if executor.Revision <= 0 || !executor.Record.Ready || executor.Record.Identity != attempt.Attempt.Executor {
		return Change{}, errs.New(errs.KindStateConflict, "execution belongs to an unavailable or replaced executor")
	}
	if attempt.Attempt.StartedAt != nil {
		return Change{}, errs.New(errs.KindStateConflict, "execution already started; resume from its saved checkpoint")
	}
	next, started, err := workexecution.Start(item.Item, attempt.Attempt, at)
	if err != nil {
		return Change{}, err
	}
	itemBytes, err := EncodeItem(next)
	if err != nil {
		return Change{}, err
	}
	attemptBytes, err := EncodeAttempt(started)
	if err != nil {
		return Change{}, err
	}
	change := Change{
		Conditions: []keyvalue.Condition{
			{Key: ExecutorKey(executor.Record.Identity), ModRevision: executor.Revision},
			{Key: ItemKey(next.ID), ModRevision: item.Revision},
			{Key: AttemptKey(started.ID), ModRevision: attempt.Revision},
		},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: ItemKey(next.ID), Value: itemBytes},
			{Type: keyvalue.MutationPut, Key: AttemptKey(started.ID), Value: attemptBytes},
		},
	}
	if item.Item.Placement.Executor == workexecution.Agent && started.Direction == workexecution.Forward {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: HostForwardFenceKey(item.Item.Placement.HostID)})
	}
	if err := countTransition(&change, task, item.Item, next, at); err != nil {
		return Change{}, err
	}
	return change, nil
}

// PrepareCheckpoint keeps the latest authoritative cursor outside disposable
// progress. An exact replay is accepted; the same sequence with different
// evidence is rejected. Callers must not execute the dependent next action
// until this transaction and its operation-owned evidence writes commit.
func PrepareCheckpoint(item ItemSnapshot, attempt AttemptSnapshot, previous CheckpointSnapshot,
	next workexecution.Checkpoint,
) (Change, error) {
	if err := item.Item.Validate(); err != nil {
		return Change{}, err
	}
	if err := attempt.Attempt.Validate(); err != nil {
		return Change{}, err
	}
	if err := validateCheckpoint(next); err != nil {
		return Change{}, err
	}
	if item.Revision <= 0 || attempt.Revision <= 0 || previous.Revision < 0 ||
		(previous.Record == nil) != (previous.Revision == 0) ||
		item.Item.Phase != workexecution.Running || item.Item.ActiveAttemptID != next.AttemptID ||
		attempt.Attempt.ID != next.AttemptID || attempt.Attempt.WorkItemID != item.Item.ID ||
		attempt.Attempt.TaskID != item.Item.TaskID || attempt.Attempt.Number != item.Item.AttemptNumber ||
		attempt.Attempt.FinishedAt != nil || attempt.Attempt.StartedAt == nil ||
		next.RecordedAt.Before(*attempt.Attempt.StartedAt) {
		return Change{}, invalid("checkpoint does not belong to the active execution")
	}
	if previous.Record == nil {
		if next.Sequence != 1 {
			return Change{}, invalid("first execution checkpoint must start at sequence one")
		}
	} else {
		if err := validateCheckpoint(*previous.Record); err != nil {
			return Change{}, err
		}
		if previous.Record.AttemptID != next.AttemptID || next.RecordedAt.Before(previous.Record.RecordedAt) {
			return Change{}, invalid("checkpoint changed execution identity or moved backwards")
		}
		if next.Sequence == previous.Record.Sequence {
			if next.StageID != previous.Record.StageID || next.Evidence != previous.Record.Evidence ||
				!next.RecordedAt.Equal(previous.Record.RecordedAt) {
				return Change{}, invalid("checkpoint sequence already records different evidence")
			}
		} else if next.Sequence <= previous.Record.Sequence || next.Sequence-previous.Record.Sequence != 1 {
			return Change{}, invalid("checkpoint sequence must advance exactly once")
		}
	}
	value, err := encode("execution_checkpoint", next)
	if err != nil {
		return Change{}, err
	}
	return Change{
		Conditions: []keyvalue.Condition{
			{Key: ItemKey(item.Item.ID), ModRevision: item.Revision},
			{Key: AttemptKey(attempt.Attempt.ID), ModRevision: attempt.Revision},
			{Key: CheckpointKey(next.AttemptID), ModRevision: previous.Revision},
		},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: CheckpointKey(next.AttemptID), Value: value},
		},
	}, nil
}
