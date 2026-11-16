package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareInputDiagnostic changes display metadata only. It neither grants an
// attempt nor changes work readiness, outcomes, resource claims or Task counts.
// Both authority records fence publication racing Stop or input preparation.
func PrepareInputDiagnostic(view View, code, detail string, at time.Time) (Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if view.Task.Revision <= 0 || view.Item.Revision <= 0 || item.TaskID != task.ID ||
		at.IsZero() || at.Location() != time.UTC || at.Before(item.UpdatedAt) || at.Before(task.UpdatedAt) {
		return Change{}, invalid("input diagnostic requires current revisioned work")
	}
	if !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		task.Stop != nil || !at.Before(task.Deadline) || !at.Before(item.Deadline) ||
		!waitingInput(item) || item.AttemptNumber != 0 || item.ActiveAttemptID != "" {
		return Change{}, nil
	}
	if item.Wait.Code == code && item.Wait.Detail == detail {
		return Change{}, nil
	}
	next := workexecution.Clone(item)
	next.Wait.Code, next.Wait.Detail, next.UpdatedAt = code, detail, at
	value, err := EncodeItem(next)
	if err != nil {
		return Change{}, err
	}
	return Change{
		Conditions: []keyvalue.Condition{
			{Key: TaskKey(task.ID), ModRevision: view.Task.Revision},
			{Key: ItemKey(item.ID), ModRevision: view.Item.Revision},
		},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ItemKey(item.ID), Value: value}},
	}, nil
}
