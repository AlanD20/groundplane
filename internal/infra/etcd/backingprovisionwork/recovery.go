package backingprovisionwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type RecoverySource struct {
	Recovery   backingprovision.Recovery
	Conditions []keyvalue.Condition
}

// LoadRecovery follows the current forward Result to its saved attempt and
// checkpoint. Cleanup never overwrites Result. FirstFailure is historical and
// may belong to an older, already-settled forward retry; it is not an effect grant.
func (repository *Repository) LoadRecovery(ctx context.Context, view workqueue.View) (RecoverySource, error) {
	number := view.Item.Item.AttemptNumber
	if view.Item.Item.ActiveAttemptID == "" {
		if number == ^uint64(0) {
			return RecoverySource{}, invalid("backing creation cleanup attempt sequence is exhausted")
		}
		number++
	}
	return repository.loadRecovery(ctx, view, number)
}

func (repository *Repository) LoadRecoveryForAttempt(ctx context.Context, view workqueue.View, attempt workexecution.Attempt) (RecoverySource, error) {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery || attempt.TaskID != view.Task.Task.ID ||
		attempt.WorkItemID != view.Item.Item.ID || attempt.Number > view.Item.Item.AttemptNumber {
		return RecoverySource{}, invalid("backing creation cleanup source requires its actual retained attempt")
	}
	return repository.loadRecovery(ctx, view, attempt.Number)
}

func (repository *Repository) loadRecovery(ctx context.Context, view workqueue.View, number uint64) (RecoverySource, error) {
	input, guard, err := repository.Load(ctx, view)
	if err != nil {
		return RecoverySource{}, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return RecoverySource{}, err
	}
	evidence := view.Item.Item.Result
	if evidence == nil || evidence.Validate() != nil {
		return RecoverySource{}, invalid("backing creation cleanup has no retained forward failure")
	}
	key := FailureKey(evidence.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return RecoverySource{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return RecoverySource{}, invalid("backing creation original failure is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision {
		return RecoverySource{}, invalid("backing creation failure ownership is invalid")
	}
	failure, err := jcs.Decode[backingprovision.Failure](value.Value)
	if err != nil {
		return RecoverySource{}, invalid("backing creation original failure cannot be decoded")
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return RecoverySource{}, err
	}
	attemptRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{workqueue.AttemptKey(failure.AttemptID)}, Revision: view.ReadRevision})
	if err != nil {
		return RecoverySource{}, err
	}
	if attemptRead == nil || attemptRead.ReadRevision != view.ReadRevision || len(attemptRead.Values) != 1 || attemptRead.Values[0] == nil {
		return RecoverySource{}, invalid("backing creation original attempt is unavailable")
	}
	defer keyvalue.ClearValues(attemptRead.Values)
	attemptValue := attemptRead.Values[0]
	if attemptValue.Key != workqueue.AttemptKey(failure.AttemptID) || attemptValue.ModRevision <= 0 || attemptValue.ModRevision > view.ReadRevision {
		return RecoverySource{}, invalid("backing creation original attempt revision is invalid")
	}
	forward, err := workqueue.DecodeAttempt(attemptValue.Value)
	if err != nil {
		return RecoverySource{}, err
	}
	if forward.ID != failure.AttemptID || forward.TaskID != view.Task.Task.ID || forward.WorkItemID != view.Item.Item.ID ||
		forward.Input != *view.Item.Item.Input || forward.Executor.HostID != procedure.HostId {
		return RecoverySource{}, invalid("backing creation failure belongs to another captured action")
	}
	layout, err := backingprovision.CaptureLayout(forward, procedure)
	if err != nil {
		return RecoverySource{}, err
	}
	failure, err = backingprovision.DecodeFailure(forward, layout, value.Value, *evidence)
	if err != nil {
		return RecoverySource{}, err
	}
	sourceView, err := queue.ReadResumeAt(ctx, forward.TaskID, forward.WorkItemID, forward.ID, forward.Executor, view.ReadRevision)
	if err != nil {
		return RecoverySource{}, err
	}
	boundary, err := repository.LoadProgress(ctx, sourceView)
	if err != nil {
		return RecoverySource{}, err
	}
	recovery := backingprovision.Recovery{Forward: forward, Failure: failure, Evidence: *evidence, Boundary: boundary}
	conditions := []keyvalue.Condition{guard,
		{Key: key, ModRevision: value.ModRevision},
		{Key: attemptValue.Key, ModRevision: attemptValue.ModRevision},
		{Key: workqueue.CheckpointKey(forward.ID), ModRevision: sourceView.Checkpoint.Revision},
		{Key: workqueue.TaskKey(view.Task.Task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(view.Item.Item.ID), ModRevision: view.Item.Revision}}
	if number <= forward.Number {
		return RecoverySource{}, invalid("backing creation cleanup cannot precede its original failed attempt")
	}
	if number > forward.Number+1 {
		_, previous, err := queue.ReadAttempt(ctx, forward.TaskID, forward.WorkItemID, number-1, view.ReadRevision)
		if err != nil {
			return RecoverySource{}, err
		}
		recovery.Previous = &previous.Attempt
		conditions = append(conditions, keyvalue.Condition{Key: workqueue.AttemptKey(previous.Attempt.ID), ModRevision: previous.Revision})
	}
	if err := recovery.Validate(layout); err != nil {
		return RecoverySource{}, err
	}
	return RecoverySource{Recovery: recovery, Conditions: conditions}, nil
}
