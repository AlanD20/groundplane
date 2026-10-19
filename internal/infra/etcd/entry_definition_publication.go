package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func entryDefinitionConflict() error {
	return errs.New(errs.KindStateConflict, "Entry has acknowledged configuration that requires host cleanup")
}

func (repository *HierarchyRepository) prepareEntryDefinitionPublication(
	ctx context.Context,
	request EntryWorkPublication,
	inputs *entrywork.InputRepository,
	sources *runtimeconfiguration.Repository,
	revision int64,
	guards []keyvalue.Condition,
) (workqueue.Change, error) {
	snapshot, head, err := sources.ReadAcknowledged(ctx, request.Task.Scope.EnvironmentID, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if head != request.DefinitionConfigurationRevision {
		return workqueue.Change{}, entryDefinitionConflict()
	}
	entryID := request.Capture.Removal.EntryID
	for _, file := range snapshot.Files {
		if file.Source.EntryValue != nil && file.Source.EntryValue.EntryID == entryID {
			return workqueue.Change{}, entryDefinitionConflict()
		}
		if file.Source.GeneratedEnvironment != nil {
			for _, value := range file.Source.GeneratedEnvironment.Values {
				if value.Value.EntryID == entryID {
					return workqueue.Change{}, entryDefinitionConflict()
				}
			}
		}
	}
	removal, err := inputs.PrepareDefinitionRemoval(ctx, workqueue.View{
		Task: workqueue.TaskSnapshot{
			Task: request.Task,
		}, Item: workqueue.ItemSnapshot{Item: request.Item}, ReadRevision: revision,
	}, *request.Capture.Removal, request.Claim.RenderGeneration, request.Projection)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := entrywork.PrepareDefinitionPublication(
		request.Task,
		request.Item,
		*request.Capture.Removal,
		head,
		request.Marker,
		time.Now().UTC(),
	)
	if err != nil {
		keyvalue.ClearMutationValues(removal.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, removal.Conditions...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: runtimeconfiguration.HeadKey(request.Task.Scope.EnvironmentID), ModRevision: head},
	)
	change.Mutations = append(change.Mutations, removal.Mutations...)
	markerConditions, markerMutations, err := idempotency.PreparePublication(request.Marker)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, markerConditions...)
	change.Mutations = append(change.Mutations, markerMutations...)
	return change, nil
}
