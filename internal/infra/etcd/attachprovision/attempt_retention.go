package attachprovision

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareAttemptRetirement removes only command and inspection history after
// collection has established settled effects. Hook outputs can expire with the
// attempt because accepted facts were copied atomically into resource state.
func PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	item, attempt := view.Item.Item, snapshot.Attempt
	if item.Operation != workexecution.Attach || item.Placement.Executor != workexecution.Agent ||
		item.Input == nil || attempt.Input != *item.Input || attempt.Executor.Executor != workexecution.Agent ||
		attempt.Executor.HostID != item.Placement.HostID {
		return workqueue.Change{}, checkpointConflict("Attach attempt history differs from its captured host or input")
	}
	collection, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: collection.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: ReportPrefix(attempt.ID), Prefix: true},
		{Type: keyvalue.MutationDelete, Key: FailureKey(attempt.ID)},
		{Type: keyvalue.MutationDelete, Key: RecoveryResultKey(attempt.ID)},
		{Type: keyvalue.MutationDelete, Key: blueprintattachoutputs.Key(item.TargetID, attempt.ID)},
	}}, nil
}
