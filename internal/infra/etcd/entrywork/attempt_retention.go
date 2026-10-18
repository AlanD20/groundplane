package entrywork

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareAttemptRetirement removes only execution evidence. Captured input is
// kept until all attempts retire; applied runtime and configuration are separate
// resource records and never belong to diagnostic collection.
func PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	item, attempt := view.Item.Item, snapshot.Attempt
	if item.Operation != workexecution.EntryConfiguration || item.Placement.Executor != workexecution.Agent ||
		item.Input == nil || attempt.Input != *item.Input || attempt.Executor.Executor != workexecution.Agent ||
		attempt.Executor.HostID != item.Placement.HostID {
		return workqueue.Change{}, conflict("Entry attempt history differs from its captured host or input")
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: prepared.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: ReportPrefix(attempt.ID), Prefix: true},
		{Type: keyvalue.MutationDelete, Key: FailureKey(attempt.ID)},
		{Type: keyvalue.MutationDelete, Key: RecoveryResultKey(attempt.ID)},
	}}, nil
}
