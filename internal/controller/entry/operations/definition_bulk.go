package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (service *entryBulkUpsertService) bulkEntryDefinitions(ctx context.Context,
	input entryBulkUpsertInput, locator idempotency.IdempotencyLocator, intent idempotency.ProtectedIntentRecord,
	source etcd.EntryMutationSource, project keyvalue.Versioned[hierarchy.ProjectRecord],
	environment keyvalue.Versioned[hierarchy.EnvironmentRecord], accepted workqueue.TaskSnapshot, at time.Time,
) (taskdependencies.Prepared, error) {
	claim := entryMetadataClaim(source, locator, intent, accepted.Task.CreatedAt, accepted)
	candidate, err := buildEntryBulkCandidate(environment.Record.ID, source.Entries, input, claim.TaskID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	changed := make([]string, 0, len(candidate.changes))
	for _, change := range candidate.changes {
		if err := service.desired.prepareEntryGeneration(ctx, project.Record.ID, environment.Record.ID,
			change.desired, change.record, at); err != nil {
			return taskdependencies.Prepared{}, err
		}
		changed = append(changed, change.record.Entry.ID)
	}
	task, item, err := metadataEntryWork(project.Record, environment.Record, claim,
		workexecution.ActionUpdate, environment.Record.ID, "Entries", accepted, at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	response, err := entryTaskResponse(task.ID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(response.Body)
	marker := idempotency.IdempotencyMarker{
		Kind: idempotency.IdempotencyMarkerTask, State: idempotency.IdempotencyMarkerPending,
		Locator: locator, Intent: claim.Intent, Response: response, TaskID: task.ID,
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	projection, err := entryDefinitionProjection(source, candidate.entries, task.ID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	capture, err := service.desired.repository.PrepareEntryDefinitionWork(ctx, etcd.EntryMetadataPublication{
		Source: source, Records: candidate.entries, Projection: projection, Changed: changed, Task: task, Item: item, Marker: marker, At: at, Accepted: accepted,
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return entryCapturePrepared(capture), nil
}
