package workqueue

import (
	"context"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadJournalAttempt resolves a retained filename without trusting it to supply
// a Task, work item or host. The caller must authenticate the returned host and
// join the full execution view before acknowledging ownership.
func (repository *Repository) ReadJournalAttempt(ctx context.Context, attemptID string) (AttemptSnapshot, error) {
	if ids.Validate(ids.KindAttempt, attemptID) != nil {
		return AttemptSnapshot{}, invalid("journal attempt identity is invalid")
	}
	read, err := repository.read(ctx, []string{AttemptKey(attemptID)}, 0)
	if err != nil {
		return AttemptSnapshot{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return AttemptSnapshot{}, errs.New(
			errs.KindStateConflict,
			"local work journal has no retained Controller attempt; preserve it for operator reconciliation",
		)
	}
	attempt, err := DecodeAttempt(read.Values[0].Value)
	if err != nil || attempt.ID != attemptID {
		return AttemptSnapshot{}, corrupt()
	}
	return AttemptSnapshot{Attempt: attempt, Revision: read.Values[0].ModRevision}, nil
}

func journalInventoryKey(attemptID string) string { return root + "journal-inventory/" + attemptID }

// ConfirmJournalOwnership stores only the latest authenticated inventory sighting.
// Its acknowledgement never means effects stopped or cleanup succeeded. No
// execution record, reservation, checkpoint or partial local file is changed.
func (repository *Repository) ConfirmJournalOwnership(
	ctx context.Context,
	view ResumeView,
	observer workexecution.Identity,
	authentication []keyvalue.Condition,
) error {
	if len(authentication) == 0 || !view.CanSettleResult() || view.Attempt.Revision <= 0 ||
		view.Work.Task.Revision <= 0 ||
		observer.Validate() != nil ||
		observer.Executor != workexecution.Agent ||
		observer.HostID != view.Attempt.Attempt.Executor.HostID {
		return invalid("journal accounting requires retained ownership and current authentication")
	}
	conditions := append([]keyvalue.Condition(nil), authentication...)
	conditions = append(conditions,
		keyvalue.Condition{Key: TaskKey(view.Work.Task.Task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: ItemKey(view.Work.Item.Item.ID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: AttemptKey(view.Attempt.Attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: ExecutorKey(view.Attempt.Attempt.Executor), ModRevision: view.ExecutorRevision},
	)
	value, err := json.Marshal(struct {
		AttemptID string                 `json:"attempt_id"`
		Observer  workexecution.Identity `json:"observer"`
	}{AttemptID: view.Attempt.Attempt.ID, Observer: observer})
	if err != nil {
		return invalid("journal ownership observation cannot be encoded")
	}
	defer clear(value)
	result, err := repository.Commit(ctx, Change{Conditions: conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: journalInventoryKey(view.Attempt.Attempt.ID), Value: value},
	}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "journal ownership changed during startup accounting")
	}
	return nil
}
