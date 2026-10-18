package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyRepository) prepareRouteWorkExecution(ctx context.Context, request RouteWorkPublication,
	guards []keyvalue.Condition, revision int64,
) (workqueue.Change, error) {
	task, item := request.Task, request.Item
	if item.Placement.Executor == workexecution.Controller {
		if request.Input != nil || request.InputID != "" || !request.Secrets.IsZero() || len(request.InactiveRouterKinds) == 0 {
			return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Controller Route decision cannot contain host execution input")
		}
		for _, component := range request.Projection.Components {
			if component.Desired.Enabled && slices.Contains(request.InactiveRouterKinds, component.Desired.Kind) {
				return workqueue.Change{}, errs.New(errs.KindStateConflict, "Enabled router requires Route host execution")
			}
		}
		inactive, err := componentplanning.InactiveRouterConditions(ctx, repository.store, task.Scope.EnvironmentID,
			request.InactiveRouterKinds, revision)
		if err != nil {
			return workqueue.Change{}, err
		}
		markerKey, err := idempotency.IdempotencyMarkerKey(request.Marker.Locator)
		if err != nil {
			return workqueue.Change{}, err
		}
		markerValue, err := idempotency.EncodeIdempotencyMarker(request.Marker)
		if err != nil {
			return workqueue.Change{}, err
		}
		change, err := componentwork.PrepareRouteDefinition(task, item, request.Marker, time.Now().UTC())
		if err != nil {
			clear(markerValue)
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: markerKey})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: markerKey, Value: markerValue})
		change.Conditions = append(change.Conditions, guards...)
		change.Conditions = append(change.Conditions, inactive...)
		return change, nil
	}
	if len(request.InactiveRouterKinds) != 0 {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Route execution cannot also declare a metadata-only decision")
	}
	guards = append(guards,
		keyvalue.Condition{Key: components.RecordKey(item.TargetID), ModRevision: request.Capture.CurrentRevision},
		keyvalue.Condition{Key: componentplanning.RuntimeKey(item.TargetID), ModRevision: request.Capture.RuntimeRevision})
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	configuration, err := sources.WorkConditions(ctx, request.Capture.Configuration, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	for _, condition := range configuration {
		guards = append(guards, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision})
	}
	inputs, err := componentwork.NewInputRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	_, change, err := inputs.PrepareRoutePublication(ctx, task, item, request.InputID, request.Input,
		request.Capture, request.Secrets, request.Marker, guards, time.Now().UTC())
	return change, err
}
