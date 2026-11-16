package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceLifecyclePublication is captured only after whole-Task prerequisites.
// Its authority must join promotion of the same accepted Task.
type ServiceLifecyclePublication struct {
	Input    servicelifecyclework.Input
	Item     workexecution.Item
	Change   workqueue.Change
	prepared *servicelifecyclework.Publication
}

func (publication ServiceLifecyclePublication) Finish(ctx context.Context, applied bool) error {
	if applied || publication.prepared == nil {
		return nil
	}
	return publication.prepared.ReconcilePreparation(ctx)
}

func (repository *ServiceRepository) PrepareServiceLifecycle(ctx context.Context,
	tenant *keyvalue.Versioned[hierarchy.TenantRecord], project keyvalue.Versioned[hierarchy.ProjectRecord],
	environment keyvalue.Versioned[hierarchy.EnvironmentRecord], current keyvalue.Versioned[services.ServiceRecord],
	input servicelifecyclework.Input, envelope *taskconfiguration.BackingHookEncryptedInputs,
	deadline time.Time, accepted workqueue.TaskSnapshot,
) (result ServiceLifecyclePublication, resultErr error) {
	if err := validateServiceLifecycleHierarchy(tenant, project, environment, current); err != nil {
		return result, err
	}
	if input.Scope != (workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID}) ||
		accepted.Revision <= 0 ||
		!accepted.Task.PendingPreparation() ||
		accepted.Task.ID != input.TaskID ||
		accepted.Task.TargetID != input.ServiceID ||
		accepted.Task.Action != input.Action ||
		accepted.Task.Scope != input.Scope {
		return result, errs.New(errs.KindValidationFailed, "Service lifecycle capture differs from its accepted action")
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, environment.Record.ID, current.ReadRevision)
	if err != nil {
		return result, err
	}
	inputs, err := servicelifecyclework.NewRepository(repository.store)
	if err != nil {
		return result, err
	}
	publication, err := inputs.PrepareInput(ctx, input, envelope, accepted)
	if err != nil {
		return result, err
	}
	defer keyvalue.ClearMutationValues(publication.Change.Mutations)
	defer func() {
		if result.prepared != nil {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if err := publication.ReconcilePreparation(cleanup); err != nil {
			resultErr = errs.WrapJoined(errs.KindInternal, resultErr, err)
		}
	}()
	item, resource, err := servicelifecyclework.PrepareCapture(publication.Input, current, deadline)
	if err != nil {
		return result, err
	}
	defer keyvalue.ClearMutationValues(resource.Mutations)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return result, err
	}
	defer clear(epoch.Value)
	conditions := append(resource.Conditions, publication.Change.Conditions...)
	conditions = append(conditions,
		keyvalue.Condition{Key: hierarchy.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		keyvalue.Condition{Key: hierarchy.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		keyvalue.Condition{Key: deletions.TombstoneKey("service", input.ServiceID)},
		keyvalue.Condition{Key: workqueue.TaskKey(input.TaskID), ModRevision: accepted.Revision})
	conditions, err = environmentfence.AppendConditions(conditions, fence)
	if err != nil {
		return result, err
	}
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return result, err
	}
	conditions, _, err = prepareTaskInitiationFences(
		initiation,
		conditions,
		func(_ int64, _ []*keyvalue.KeyValue) error {
			return errs.New(errs.KindStateConflict, "Service or Environment changed during lifecycle preparation")
		},
	)
	if err != nil {
		return result, err
	}
	mutations := append(resource.Mutations, publication.Change.Mutations...)
	mutations = append(mutations, epoch)
	owned := make([]keyvalue.Mutation, len(mutations))
	for index, mutation := range mutations {
		owned[index] = mutation
		owned[index].Value = append([]byte(nil), mutation.Value...)
	}
	return ServiceLifecyclePublication{Input: publication.Input, Item: item,
		Change: workqueue.Change{Conditions: conditions, Mutations: owned}, prepared: &publication}, nil
}
