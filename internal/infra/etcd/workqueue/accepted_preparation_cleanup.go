package workqueue

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func acceptedPreparationCleanupKey(taskID string) string {
	return root + "accepted-preparation-cleanup/" + taskID
}

func acceptedPreparationCleanupReady(task workexecution.Task) bool {
	return task.AcceptedIntent != nil && !task.PendingPreparation()
}

// AcceptedPreparationCleanup discovers retired pure requests whose typed owners
// may still hold inert drafts, including a crash after successful promotion.
func (repository *Repository) AcceptedPreparationCleanup(ctx context.Context, after string) (TaskIndexPage, error) {
	return repository.indexedTasks(ctx, after, root+"accepted-preparation-cleanup/", acceptedPreparationCleanupReady)
}

func PrepareAcceptedPreparationCleanupComplete(task TaskSnapshot) (Change, error) {
	if task.Revision <= 0 || !acceptedPreparationCleanupReady(task.Task) {
		return Change{}, invalid("accepted preparation cleanup requires a retired pure request")
	}
	return Change{
		Conditions: []keyvalue.Condition{{Key: TaskKey(task.Task.ID), ModRevision: task.Revision}},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: acceptedPreparationCleanupKey(task.Task.ID)},
		},
	}, nil
}
