package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareBlueprintServiceRemoval publishes the captured removal input and its
// protections with readiness. No Agent may observe ready work without the
// deletion fence and retained configuration Secrets in the same transaction.
func (repository *ServiceRepository) PrepareBlueprintServiceRemoval(ctx context.Context, taskID, workID string) (resultErr error) {
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return err
	}
	view, err := queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	if !at.Before(view.Task.Task.Deadline) || !at.Before(view.Item.Item.Deadline) {
		return errs.New(errs.KindStateConflict, "Blueprint removal input preparation deadline expired")
	}
	capture, err := repository.captureBlueprintServiceRemoval(ctx, view)
	if err != nil {
		return err
	}
	if capture.waiting {
		return nil
	}
	if capture.rejection != "" {
		change, err := workqueue.PrepareInputRejection(view, capture.rejection, time.Now().UTC())
		if err != nil {
			return err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		result, err := queue.Commit(ctx, change, workqueue.Change{Conditions: capture.guards})
		keyvalue.ClearValues(result.FailureReads)
		return err
	}
	guards := append(capture.guards,
		keyvalue.Condition{Key: workqueue.TaskKey(taskID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(workID), ModRevision: view.Item.Revision},
		keyvalue.Condition{Key: serviceremovalwork.InputKey(workID)})
	for _, id := range view.Item.Item.Dependencies {
		dependency, err := queue.ReadWork(ctx, taskID, id, view.ReadRevision)
		if err != nil {
			return err
		}
		if dependency.Item.Item.Phase != workexecution.Settled || dependency.Item.Item.Outcome != workexecution.Succeeded {
			return errs.New(errs.KindStateConflict, "Blueprint removal is waiting for its dependencies")
		}
		guards = append(guards, keyvalue.Condition{Key: workqueue.ItemKey(id), ModRevision: dependency.Item.Revision})
	}
	dns, err := dnsrecords.NewReader(repository.store).RequireDNSUnreferenced(ctx, capture.input.ServiceID, "", view.ReadRevision)
	if err != nil {
		return err
	}
	scripts, err := scriptsourceevidence.PrepareServiceScriptAbsence(ctx, repository.store, capture.input.ServiceID, view.ReadRevision)
	if err != nil {
		return err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, capture.input.Scope.EnvironmentID, view.ReadRevision)
	if err != nil {
		return err
	}
	guards = append(guards, dns...)
	guards = append(guards, scripts...)
	references, err := repository.prepareBlueprintRemovalReferences(ctx, capture.input, view.ReadRevision)
	if err != nil {
		return err
	}
	guards = append(guards, references...)
	guards = append(guards, fence.TransactionConditions()...)
	defer func() {
		if resultErr != nil {
			resultErr = repository.abandonBlueprintRemovalPreparation(ctx, capture.input, resultErr)
		}
	}()
	input, pins, err := repository.prepareRemovalSecretPins(ctx, capture.input, view.ReadRevision, guards)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(pins.Mutations)
	resource, err := serviceremovalwork.PrepareResource(input, capture.service)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(resource.Mutations)
	ref, err := input.Reference()
	if err != nil {
		return err
	}
	ready, err := workqueue.PrepareInput(view.Task, view.Item, ref, time.Now().UTC())
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(ready.Mutations)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return err
	}
	defer clear(epoch.Value)
	result, err := queue.Commit(ctx, ready, resource, pins, workqueue.Change{Conditions: guards, Mutations: []keyvalue.Mutation{epoch}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint removal preparation changed before publication")
	}
	return nil
}
