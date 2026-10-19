package releasegroups

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RestorationFailureSource struct {
	Interruption releaseexecution.RestorationInterruption
	Attempt      workqueue.AttemptSnapshot
	Conditions   []keyvalue.Condition
}

// LoadRestorationFailure follows an explicit numbered restoration attempt. A
// live delivery selects its immediate predecessor; Retry selects the precise
// last failed attempt being compared by admission, never a later Group failure.
func (repository *ExecutionRepository) LoadRestorationFailure(
	ctx context.Context,
	work workqueue.View,
	number uint64,
) (RestorationFailureSource, error) {
	stored, err := repository.LoadRestorationInput(ctx, work)
	if err != nil {
		return RestorationFailureSource{}, err
	}
	_, previous, err := repository.queue.ReadAttempt(
		ctx,
		work.Task.Task.ID,
		work.Item.Item.ID,
		number,
		work.ReadRevision,
	)
	if err != nil {
		return RestorationFailureSource{}, err
	}
	attempt := previous.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || attempt.Outcome != workexecution.Failed ||
		attempt.Input != *work.Item.Item.Input || attempt.WorkItemID != work.Item.Item.ID || attempt.TaskID != work.Task.Task.ID {
		return RestorationFailureSource{}, errs.New(
			errs.KindTaskNotRetryable,
			"Group restoration has no matching failed attempt to resume",
		)
	}
	key := releasework.FailureKey(attempt.Result.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: work.ReadRevision},
	)
	if err != nil {
		return RestorationFailureSource{}, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return RestorationFailureSource{}, errs.New(
			errs.KindStateConflict,
			"Group restoration failed attempt lost its exact failure receipt",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > work.ReadRevision {
		return RestorationFailureSource{}, errs.New(
			errs.KindStateConflict,
			"Group restoration failure receipt revision is invalid",
		)
	}
	failure, err := releaseexecution.DecodeFailure(attempt, value.Value, *attempt.Result)
	if err != nil {
		return RestorationFailureSource{}, err
	}
	interruption := releaseexecution.RestorationInterruption{Attempt: attempt, Failure: failure}
	if err := interruption.ValidatePrevious(stored.Input.Source, stored.Input.Authority); err != nil {
		return RestorationFailureSource{}, err
	}
	conditions := append(stored.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(work.Task.Task.ID), ModRevision: work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(work.Item.Item.ID), ModRevision: work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: previous.Revision},
		keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
	return RestorationFailureSource{Interruption: interruption, Attempt: previous, Conditions: conditions}, nil
}
