package componentwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// AdmissionConditions preserves the exact applied predecessor across the gap
// between input preparation and execution. It never substitutes fresh sources.
func (repository *InputRepository) AdmissionConditions(ctx context.Context, view workqueue.View) ([]keyvalue.Condition, error) {
	_, capture, guards, err := repository.Load(ctx, view)
	if err != nil {
		return nil, err
	}
	key := componentplanning.RuntimeKey(view.Item.Item.TargetID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	revision := keyvalue.RevisionOf(read.Values[0])
	keyvalue.ClearValues(read.Values)
	if revision != capture.RuntimeRevision {
		return nil, conflict("Component runtime changed after execution input was captured")
	}
	guards = append(guards, keyvalue.Condition{Key: key, ModRevision: revision})
	routeGuards, err := repository.routeConditions(ctx, view, capture.Route)
	if err != nil {
		return nil, err
	}
	guards = append(guards, routeGuards...)
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return nil, err
	}
	conditions, err := sources.WorkConditions(ctx, capture.Configuration, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	for _, condition := range conditions {
		guards = append(guards, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision})
	}
	pins, err := repository.ExecutionSecretConditions(ctx, view)
	if err != nil {
		return nil, err
	}
	guards = append(guards, pins...)
	return guards, nil
}
