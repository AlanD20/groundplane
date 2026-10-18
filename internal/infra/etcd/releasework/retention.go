package releasework

import (
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareAttemptRetirement removes only execution evidence. Applied Release
// records, runtime/configuration heads and resource artifacts are not Task data.
// The collector must join its captured-input retirement proof in this write.
func PrepareAttemptRetirement(view workqueue.View, attempt workqueue.AttemptSnapshot) (workqueue.Change, error) {
	if view.Item.Item.Operation != workexecution.Release {
		return workqueue.Change{}, invalid()
	}
	guard, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, attempt)
	if err != nil {
		return workqueue.Change{}, err
	}
	id := attempt.Attempt.ID
	failureID, err := releaseexecution.FailureEvidenceID(id)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: guard.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: ProgressKey(id)},
		{Type: keyvalue.MutationDelete, Key: RecoveryKey(id)},
		{Type: keyvalue.MutationDelete, Key: FailureKey(failureID)},
		{Type: keyvalue.MutationDelete, Key: EffectEvidencePrefix(id), Prefix: true},
	}}, nil
}
