package releasegroups

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRestorationInput derives executable authority only after captured
// policy selects this earlier successful member. The returned transaction
// compares the source, later failure, complete Group policy and native Task CAS.
func (repository *ExecutionRepository) PrepareRestorationInput(
	ctx context.Context,
	work workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	item, task := work.Item.Item, work.Task.Task
	if item.Operation != workexecution.ReleaseRestoration || item.Input != nil || item.AttemptNumber != 0 ||
		item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput || item.ForwardRevoked ||
		!task.PublicationComplete || task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		at.Before(task.UpdatedAt) || !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group restoration is not waiting for captured executable input",
		)
	}
	decision, err := repository.Decide(ctx, work.Task, work.ReadRevision, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	if decision.Decision.Action != domain.GroupWorkRestore || decision.Decision.WorkID != item.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Group policy has not selected this restoration")
	}
	source, err := repository.LoadRestorationSource(ctx, work, decision.Decision.FailureWorkID)
	if err != nil {
		return workqueue.Change{}, err
	}
	input := RestorationInput{
		Preparation: source.Preparation,
		Authority:   source.Authority,
		Source:      source.SourceAttempt.Attempt,
	}
	value, reference, err := EncodeRestorationInput(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(value)
	change, err := workqueue.PrepareInput(work.Task, work.Item, reference, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := RestorationInputKey(item.ID)
	change.Conditions = append(change.Conditions, source.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), value...)},
	)
	return change, nil
}
