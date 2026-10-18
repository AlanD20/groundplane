package softwarepreparation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type WorkSnapshot struct {
	Capture       Capture
	InputRevision int64
	Record        keyvalue.Versioned[Record]
}

func (snapshot WorkSnapshot) Guard() workqueue.Change {
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: InputKey(snapshot.Capture.WorkItemID), ModRevision: snapshot.InputRevision},
		{Key: Key(snapshot.Capture.TaskID), ModRevision: snapshot.Record.Revision},
	}}
}

func (repository *Repository) LoadWork(ctx context.Context, item workexecution.Item, revision int64) (WorkSnapshot, error) {
	if item.Validate() != nil || item.Operation != workexecution.SoftwarePreparation || revision < 0 {
		return WorkSnapshot{}, errs.New(errs.KindValidationFailed, "software preparation work is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{InputKey(item.ID), Key(item.TaskID)}, Revision: revision})
	if err != nil {
		return WorkSnapshot{}, err
	}
	if read == nil {
		return WorkSnapshot{}, errs.New(errs.KindStorageUnavailable, "software preparation input read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return WorkSnapshot{}, errs.New(errs.KindStateConflict, "software preparation input or progress is missing")
	}
	capture, err := recordcodec.Decode[Capture](read.Values[0].Value, "software_preparation_capture")
	if err != nil {
		return WorkSnapshot{}, err
	}
	record, err := Decode(read.Values[1].Value)
	if err != nil {
		return WorkSnapshot{}, err
	}
	if err := capture.Match(item, record); err != nil {
		return WorkSnapshot{}, err
	}
	return WorkSnapshot{Capture: capture, InputRevision: read.Values[0].ModRevision,
		Record: keyvalue.Versioned[Record]{Record: record, Revision: read.Values[1].ModRevision, ReadRevision: read.ReadRevision}}, nil
}

// CheckpointWork can publish forward build outputs only for the current,
// durably started Controller assignment. Replaced processes can only reconcile.
func (repository *Repository) CheckpointWork(ctx context.Context, queue *workqueue.Repository,
	attempt workexecution.Attempt, progress preparation.Progress, at time.Time,
) error {
	view, snapshot, err := repository.activeWork(ctx, queue, attempt)
	if err != nil {
		return err
	}
	if !view.CurrentExecutor || view.Attempt.Attempt.StartedAt == nil || attempt.Direction != workexecution.Forward ||
		view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked || !at.Before(attempt.Deadline) {
		return errs.New(errs.KindStateConflict, "software preparation no longer permits forward publication")
	}
	change, _, err := prepareProgress(snapshot, progress, at)
	if err != nil {
		return err
	}
	return repository.commitWork(ctx, view, change)
}

func (repository *Repository) activeWork(ctx context.Context, queue *workqueue.Repository, attempt workexecution.Attempt) (workqueue.ResumeView, WorkSnapshot, error) {
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return view, WorkSnapshot{}, err
	}
	if !view.ActiveAttempt || view.Attempt.Attempt.FinishedAt != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return view, WorkSnapshot{}, errs.New(errs.KindStateConflict, "software preparation attempt no longer owns execution")
	}
	snapshot, err := repository.LoadWork(ctx, view.Work.Item.Item, view.Work.ReadRevision)
	return view, snapshot, err
}

func prepareProgress(snapshot WorkSnapshot, progress preparation.Progress, at time.Time) (workqueue.Change, Record, error) {
	current := snapshot.Record.Record
	if err := validateTransition(current.Progress, progress, current.Source); err != nil {
		return workqueue.Change{}, Record{}, err
	}
	change := snapshot.Guard()
	if current.Progress.Equal(progress) {
		return change, current, nil
	}
	if !at.After(current.UpdatedAt) {
		at = current.UpdatedAt.Add(time.Nanosecond)
	}
	current.Progress, current.UpdatedAt = progress, at.UTC()
	raw, err := Encode(current)
	if err != nil {
		return workqueue.Change{}, Record{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: Key(current.TaskID), Value: raw})
	return change, current, nil
}

func (repository *Repository) commitWork(ctx context.Context, view workqueue.ResumeView, change workqueue.Change) error {
	defer keyvalue.ClearMutationValues(change.Mutations)
	attempt := view.Attempt.Attempt
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	result, err := repository.store.Transact(ctx, change.Conditions, change.Mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "software preparation ownership changed before publication")
	}
	return nil
}
