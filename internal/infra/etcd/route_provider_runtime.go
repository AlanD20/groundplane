package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RouteRepository) GetRouteProviderRuntimeAtRevision(
	ctx context.Context, componentID string, revision int64,
) (keyvalue.Versioned[componentplanning.Runtime], bool, error) {
	if repository == nil || revision <= 0 {
		return keyvalue.Versioned[componentplanning.Runtime]{}, false,
			errs.New(errs.KindValidationFailed, "Route router runtime requires a fixed snapshot")
	}
	return componentplanning.ReadRuntime(ctx, repository.store, componentID, revision)
}

func prepareRouteRuntimeAcknowledgement(
	task TaskRecord, provider *environmentchanges.RouteProviderPin,
	projection *projectionrecord.EnvironmentComposeProjection, sealed []byte, removal bool,
) (routeTaskChange, error) {
	if provider == nil {
		return routeTaskChange{}, nil
	}
	if projection == nil {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route runtime projection is missing")
	}
	plan, err := environmentchanges.OpenRouteExecutionPlan(
		sealed,
		provider,
		projection.EnvironmentID,
		task.Target,
		projection.RenderGeneration,
		removal,
	)
	if err != nil {
		return routeTaskChange{}, err
	}
	if task.PlanID != plan.GetPlanId() || task.PlanHash != hex.EncodeToString(plan.GetPlanHash()) ||
		task.Owner.EnvironmentID != projection.EnvironmentID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Route runtime plan differs from its Task")
	}
	condition, err := routeProviderRuntimeCondition(provider, projection.EnvironmentID)
	if err != nil {
		return routeTaskChange{}, err
	}
	value, err := componentplanning.EncodeRouteAcknowledgedRuntime(task.ID, plan, provider, *projection)
	if err != nil {
		return routeTaskChange{}, err
	}
	return routeTaskChange{
		applies: true, conditions: []keyvalue.Condition{condition}, values: [][]byte{value},
		mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: condition.Key, Value: value}},
	}, nil
}

func routeProviderRuntimeCondition(
	provider *environmentchanges.RouteProviderPin,
	environmentID string,
) (keyvalue.Condition, error) {
	if _, err := environmentchanges.DecodeRouteProviderRuntime(provider); err != nil {
		return keyvalue.Condition{}, err
	}
	if provider.Runtime.EnvironmentID != environmentID {
		return keyvalue.Condition{}, errs.New(
			errs.KindValidationFailed,
			"Route captured runtime belongs to another Environment",
		)
	}
	return keyvalue.Condition{
		Key: componentplanning.RuntimeKey(provider.ComponentID), ModRevision: provider.Runtime.Revision,
	}, nil
}
