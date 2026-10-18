package executiondiagnostics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct {
	store Store
	queue *workqueue.Repository
}

func New(store Store, queue *workqueue.Repository) (*Repository, error) {
	if store == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "execution diagnostics require storage and ownership")
	}
	return &Repository{store: store, queue: queue}, nil
}

type Snapshot struct {
	Attempt      workexecution.Attempt
	FinishedAt   *time.Time
	Outcome      workexecution.Outcome
	Incomplete   bool
	Record       *Record
	Revision     int64
	ReadRevision int64
}

// Read fixes membership, attempt and diagnostics to one snapshot. Absent
// diagnostics are explicit and do not imply that execution did not happen.
func (repository *Repository) Read(ctx context.Context, taskID, workID, attemptID string, revision int64) (Snapshot, error) {
	view, err := repository.queue.ReadWork(ctx, taskID, workID, revision)
	if err != nil {
		return Snapshot{}, err
	}
	return repository.read(ctx, view, attemptID)
}

func (repository *Repository) read(ctx context.Context, view workqueue.View, attemptID string) (Snapshot, error) {
	if ids.Validate(ids.KindAttempt, attemptID) != nil {
		return Snapshot{}, invalid()
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{workqueue.AttemptKey(attemptID), Key(attemptID), LossKey(attemptID)}, Revision: view.ReadRevision})
	if err != nil {
		return Snapshot{}, err
	}
	if read == nil {
		return Snapshot{}, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 3 || read.Values[0] == nil {
		return Snapshot{}, invalid()
	}
	attempt, err := workqueue.DecodeAttempt(read.Values[0].Value)
	if err != nil {
		return Snapshot{}, err
	}
	if attempt.ID != attemptID || attempt.TaskID != view.Task.Task.ID || attempt.WorkItemID != view.Item.Item.ID {
		return Snapshot{}, invalid()
	}
	result := Snapshot{Attempt: attempt, ReadRevision: read.ReadRevision, FinishedAt: attempt.FinishedAt, Outcome: attempt.Outcome}
	if read.Values[2] != nil {
		if string(read.Values[2].Value) != "incomplete" {
			return Snapshot{}, invalid()
		}
		result.Incomplete = true
	}
	if read.Values[1] == nil {
		return result, nil
	}
	record, err := decode(read.Values[1].Value)
	if err != nil {
		return Snapshot{}, err
	}
	if record.TaskID != attempt.TaskID || record.WorkItemID != attempt.WorkItemID || record.AttemptID != attempt.ID {
		return Snapshot{}, invalid()
	}
	result.Record, result.Revision = &record, read.Values[1].ModRevision
	return result, nil
}

// Report replaces a bounded stage snapshot under exact executor ownership.
// Failure is diagnostic incompleteness, not permission to repeat an effect or
// change a successful operation result. The caller reports it separately.
func (repository *Repository) Report(ctx context.Context, taskID, workID, attemptID string,
	identity workexecution.Identity, sequence uint64, stage workexecution.Stage, at time.Time,
) error {
	var err error
	for retry := 0; retry < 3; retry++ {
		err = repository.report(ctx, taskID, workID, attemptID, identity, sequence, stage, at, nil)
		if err == nil || ctx.Err() != nil || !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			return err
		}
	}
	return err
}

func (repository *Repository) report(ctx context.Context, taskID, workID, attemptID string,
	identity workexecution.Identity, sequence uint64, stage workexecution.Stage, at time.Time, resultEvidence *workexecution.Input,
) error {
	if stage.Validate() != nil || sequence == 0 {
		return invalid()
	}
	view, err := repository.queue.ReadResume(ctx, taskID, workID, attemptID, identity)
	if err != nil {
		return err
	}
	if resultEvidence == nil {
		if !view.CurrentExecutor || !view.ActiveAttempt || view.Attempt.Attempt.StartedAt == nil || view.Attempt.Attempt.FinishedAt != nil || at.Before(*view.Attempt.Attempt.StartedAt) {
			return errs.New(errs.KindStateConflict, "diagnostic report does not belong to the active executor")
		}
	} else {
		attempt := view.Attempt.Attempt
		if resultEvidence.Validate() != nil || attempt.Result == nil || *attempt.Result != *resultEvidence ||
			attempt.FinishedAt == nil || !at.Equal(*attempt.FinishedAt) || view.Work.Task.Task.CollectionStartedAt != nil {
			return errs.New(errs.KindStateConflict, "terminal diagnostic does not match the retained execution result")
		}
		if stage.StartedAt != nil && (attempt.StartedAt == nil || stage.StartedAt.Before(*attempt.StartedAt) || stage.StartedAt.After(at)) ||
			stage.FinishedAt != nil && (stage.FinishedAt.Before(attempt.AssignedAt) || stage.FinishedAt.After(at)) {
			return errs.New(errs.KindStateConflict, "terminal diagnostic timing falls outside the retained execution")
		}
	}
	previous, err := repository.read(ctx, view.Work, attemptID)
	if err != nil {
		return err
	}
	next := Record{TaskID: taskID, WorkItemID: workID, AttemptID: attemptID, Sequence: sequence, UpdatedAt: at, Stages: []workexecution.Stage{stage}}
	if previous.Record == nil {
		if sequence != 1 {
			return invalid()
		}
	} else {
		before := previous.Record
		if sequence == before.Sequence {
			// A sequence identifies the last report, not any stage which happens
			// to remain in the current snapshot. The atomic event tail proves it.
			events, _, err := repository.readEvents(ctx, taskID, workID, attemptID, view.Work.ReadRevision)
			if err != nil {
				return err
			}
			if events == nil {
				return invalid()
			}
			last := events.Items[len(events.Items)-1]
			left, leftErr := json.Marshal(last.Stage)
			right, rightErr := json.Marshal(stage)
			if leftErr != nil || rightErr != nil {
				return invalid()
			}
			if last.Sequence == sequence && bytes.Equal(left, right) && at.Equal(last.RecordedAt) && at.Equal(before.UpdatedAt) {
				return nil
			}
			return errs.New(errs.KindStateConflict, "diagnostic sequence already contains different evidence")
		}
		if before.Sequence == math.MaxUint64 || sequence != before.Sequence+1 || at.Before(before.UpdatedAt) {
			return invalid()
		}
		next.Stages = append([]workexecution.Stage(nil), before.Stages...)
		found := false
		for index, saved := range next.Stages {
			if saved.Key != stage.Key {
				continue
			}
			if err := workexecution.AdvanceStage(saved, stage); err != nil {
				return err
			}
			next.Stages[index], found = stage, true
			break
		}
		if !found {
			next.Stages = append(next.Stages, stage)
		}
	}
	trimmed := false
	for len(next.Stages) > maximumStages {
		// Retain active/unknown effects and the just-reported stage. Old
		// completed observations may expire; they are not recovery authority.
		remove := -1
		for index, saved := range next.Stages {
			if saved.Key != stage.Key && saved.FinishedAt != nil {
				remove = index
				break
			}
		}
		if remove < 0 {
			return invalid()
		}
		next.Stages = append(next.Stages[:remove], next.Stages[remove+1:]...)
		trimmed = true
	}
	value, err := encode(next)
	if err != nil {
		return err
	}
	defer clear(value)
	eventCondition, eventMutation, err := repository.prepareEvent(ctx, next, stage, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	defer clear(eventMutation.Value)
	taskEventCondition, taskEventMutation, err := repository.prepareTaskEvent(ctx, next, stage, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	defer clear(taskEventMutation.Value)
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: Key(attemptID), Value: value}, eventMutation, taskEventMutation}
	if trimmed {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: LossKey(attemptID), Value: []byte("incomplete")})
	}
	result, err := repository.store.Transact(ctx, []keyvalue.Condition{
		eventCondition,
		taskEventCondition,
		{Key: workqueue.ExecutorKey(identity), ModRevision: view.ExecutorRevision},
		{Key: workqueue.TaskKey(taskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(workID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attemptID), ModRevision: view.Attempt.Revision},
		{Key: Key(attemptID), ModRevision: previous.Revision},
	}, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "execution changed while recording stage diagnostics")
	}
	return nil
}
