package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func prepareDirectDesiredPublication(ctx context.Context, store hierarchyStore, environmentID string,
	marker idempotencyrecord.IdempotencyMarker, mutate func(*core.BlueprintDesiredInput) error,
) (routeHeadPublication, error) {
	return prepareDirectDesiredProjectionPublication(ctx, store, environmentID, marker,
		func(input *core.BlueprintDesiredInput, _ *projectionrecord.EnvironmentComposeProjection) error {
			return mutate(input)
		})
}

func prepareDirectDesiredProjectionPublication(ctx context.Context, store hierarchyStore, environmentID string,
	marker idempotencyrecord.IdempotencyMarker,
	mutate func(*core.BlueprintDesiredInput, *projectionrecord.EnvironmentComposeProjection) error,
) (routeHeadPublication, error) {
	publication, err := desiredauthoring.Prepare(ctx, store, environmentID, marker, mutate)
	if err != nil {
		return routeHeadPublication{}, err
	}
	result := routeHeadPublication{conditions: publication.Conditions, mutations: publication.Mutations}
	for _, mutation := range publication.Mutations {
		result.values = append(result.values, mutation.Value)
	}
	return result, nil
}

func (publication routeHeadPublication) bindDirectDesired(conditions []etcdstore.Condition, mutations []etcdstore.Mutation, previous idempotencyPlanClassifier) ([]etcdstore.Condition, []etcdstore.Mutation, idempotencyPlanClassifier, error) {
	conditions, mutations, classify, err := desiredauthoring.Bind(desiredauthoring.Publication{Conditions: publication.conditions, Mutations: publication.mutations}, conditions, mutations, desiredauthoring.Classifier(previous))
	return conditions, mutations, idempotencyPlanClassifier(classify), err
}
