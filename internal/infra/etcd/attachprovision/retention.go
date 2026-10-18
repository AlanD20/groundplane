package attachprovision

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// PrepareRetention collects execution-owned captures only. Applied bindings and
// their encrypted facts belong to resources and are never deleted here.
func (repository *InputRepository) PrepareRetention(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return false, workqueue.Change{}, err
	}
	if repository == nil || repository.store == nil || task.Validate() != nil || item.Validate() != nil ||
		view.Task.Revision <= 0 || view.Item.Revision <= 0 || view.ReadRevision <= 0 ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		item.Operation != workexecution.Attach || item.TaskID != task.ID ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Result == nil ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, checkpointConflict("Attach history requires expired, fully settled execution")
	}
	source, found, err := blueprintattachinputs.ReadAtRevision(ctx, repository.store, task.ID, item.TargetID, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer blueprintattachinputs.Clear(&source.Record)
	capture := source.Record
	if !found || !capture.MatchesWork(task, item) {
		return false, workqueue.Change{}, checkpointConflict("Attach history has lost its captured ownership")
	}
	reference, err := blueprintattachinputs.WorkInput(capture)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if item.Input != nil && *item.Input != reference || item.Input == nil && item.AttemptNumber != 0 {
		return false, workqueue.Change{}, checkpointConflict("Attach history input differs from its captured ownership")
	}
	guards := []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: blueprintattachinputs.Key(task.ID, item.TargetID), ModRevision: source.Revision},
	}
	if !final {
		return true, workqueue.Change{Conditions: guards}, nil
	}
	collection, err := workqueue.PrepareWorkCollection(view.Task, view.Item)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	guards = append(guards, collection.Conditions...)
	retired, pinGuards, err := repository.retirePins(ctx, view.ReadRevision, capture, guards)
	if err != nil || !retired {
		return false, workqueue.Change{}, err
	}
	guards = append(guards, pinGuards...)
	return true, workqueue.Change{Conditions: guards, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: blueprintattachinputs.Key(task.ID, item.TargetID)},
		{Type: keyvalue.MutationDelete, Key: RecoveryKey(item.ID)},
		{Type: keyvalue.MutationDelete, Key: ResultKey(item.ID)},
		{Type: keyvalue.MutationDelete, Key: ConsumerSettlementKey(item.ID)},
		{Type: keyvalue.MutationDelete, Key: DirectControllerResultKey(item.ID)},
		{Type: keyvalue.MutationDelete, Key: UnstartedKey(item.ID)},
		{Type: keyvalue.MutationDelete, Key: blueprintunits.ControllerAttachResultKey(item.ID)},
	}}, nil
}

func (repository *InputRepository) retirePins(ctx context.Context, revision int64, capture blueprintattachinputs.Generation, guards []keyvalue.Condition) (bool, []keyvalue.Condition, error) {
	absence := []keyvalue.Condition{
		{Key: tasksecretpins.RootKey(capture.OperationID)},
		{Key: tasksecretpins.PreparationKey(capture.OperationID)},
		{Key: tasksecretpins.ReversePrefix(capture.OperationID), Prefix: true},
	}
	if capture.SecretPins == nil {
		return true, absence, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, capture.BackingProjectID)
	if err != nil {
		return false, nil, err
	}
	root, found, err := pins.LoadActive(ctx, capture.OperationID, revision)
	if err != nil {
		return false, nil, err
	}
	if !found {
		return true, absence, nil
	}
	_, releasing, err := root.RetentionCondition(*capture.SecretPins)
	if err != nil {
		return false, nil, err
	}
	if releasing {
		_, _, err := pins.ReleaseBatch(ctx, capture.OperationID)
		return false, nil, err
	}
	release, err := tasksecretpins.BeginRelease(root)
	if err != nil {
		return false, nil, err
	}
	defer release.Clear()
	conditions, mutations, err := tasksecretpins.EtcdFragment(release)
	if err != nil {
		return false, nil, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return false, nil, err
	}
	result, err := queue.Commit(ctx, workqueue.Change{Conditions: append(guards, conditions...), Mutations: mutations})
	keyvalue.ClearValues(result.FailureReads)
	return false, nil, err
}
