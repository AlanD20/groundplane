package entrywork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// ReadDesiredMatch fences new effects. Receipts of already-authorized effects
// remain valid even if a later operator action has requested supersession.
func (repository *InputRepository) ReadDesiredMatch(ctx context.Context, view workqueue.View) (bool, []keyvalue.Condition, error) {
	_, capture, guards, err := repository.Load(ctx, view)
	if err != nil {
		return false, nil, err
	}
	key := blueprints.EnvironmentBlueprintHeadKey(view.Task.Task.Scope.EnvironmentID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return false, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	guards = append(guards, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)})
	if value == nil {
		return false, guards, nil
	}
	current, err := idempotency.DecodeTaskReference(value.Value)
	if err != nil {
		return false, nil, err
	}
	if capture.Removal != nil {
		return current == capture.Removal.BaseRevisionID && value.ModRevision == capture.Removal.HeadRevision, guards, nil
	}
	return current == view.Task.Task.ID, guards, nil
}

// AdmissionConditions binds the original consumers, file predecessor and retained
// Secret values. No source is refreshed to make an obsolete assignment runnable.
func (repository *InputRepository) AdmissionConditions(ctx context.Context, view workqueue.View) ([]keyvalue.Condition, error) {
	_, capture, guards, err := repository.Load(ctx, view)
	if err != nil {
		return nil, err
	}
	consumers, err := repository.ConsumerConditions(ctx, capture, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	guards = append(guards, consumers...)
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
	return append(guards, pins...), nil
}
