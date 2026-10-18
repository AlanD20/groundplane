package etcd

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// An uncertain publication is never interpreted as failure to publish. Observe
// native Task absence first; Abandon also fences Task/work/root absence in each
// cleanup transaction, so publication racing this read retains its protection.
func (repository *ServiceRepository) abandonRemovalSecretPreparation(ctx context.Context, input serviceremovalwork.Input, cause error) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	key := workqueue.TaskKey(input.TaskID)
	read, err := repository.store.GetMany(cleanup, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return errors.Join(cause, err)
	}
	if read == nil {
		return errors.Join(cause, errs.New(errs.KindStorageUnavailable, "Cannot establish removal publication outcome for Secret cleanup"))
	}
	defer keyvalue.ClearValues(read.Values)
	if len(read.Values) != 1 || read.ReadRevision <= 0 {
		return errors.Join(cause, errs.New(errs.KindStorageUnavailable, "Removal publication observation is incomplete"))
	}
	if read.Values[0] != nil {
		return cause
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, input.Scope.ProjectID)
	if err != nil {
		return errors.Join(cause, err)
	}
	return errors.Join(cause, pins.Abandon(cleanup, input.ConfigurationOperationID()))
}

// Apply already owns a durable Task. Absence of its immutable input, compared
// with the exact current work revision, distinguishes an unpublished preparation
// from an uncertain successful commit. Published pins are never abandoned.
func (repository *ServiceRepository) abandonBlueprintRemovalPreparation(ctx context.Context, input serviceremovalwork.Input, cause error) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return errors.Join(cause, err)
	}
	view, err := queue.ReadWork(cleanup, input.TaskID, input.WorkItemID, 0)
	if err != nil {
		return errors.Join(cause, err)
	}
	if view.Item.Item.Input != nil {
		return cause
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, input.Scope.ProjectID)
	if err != nil {
		return errors.Join(cause, err)
	}
	guards := []tasksecretpins.Condition{
		{Key: workqueue.TaskKey(input.TaskID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(input.WorkItemID), ModRevision: view.Item.Revision},
		{Key: serviceremovalwork.InputKey(input.WorkItemID)},
	}
	for {
		if err := cleanup.Err(); err != nil {
			return errors.Join(cause, err)
		}
		done, err := pins.AbandonOwned(cleanup, input.ConfigurationOperationID(), input.TaskID, input.WorkItemID, guards)
		if err != nil || done {
			return errors.Join(cause, err)
		}
	}
}
