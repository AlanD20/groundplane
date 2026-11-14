package operations

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (service *entryBulkUpsertService) bulkUnrenderedEntries(ctx context.Context,
	input entryBulkUpsertInput, locator idempotency.IdempotencyLocator, evidence entryBulkUpsertEvidence,
	source etcd.EntryMutationSource, project keyvalue.Versioned[hierarchy.ProjectRecord],
	environment keyvalue.Versioned[hierarchy.EnvironmentRecord],
) (idempotency.IdempotencyResponse, error) {
	claim := entryMetadataClaim(source, locator, evidence.durable, service.desired.now().UTC())
	candidate, err := buildEntryBulkCandidate(environment.Record.ID, source.Entries, input, claim.TaskID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	changed := make([]string, 0, len(candidate.changes))
	for _, change := range candidate.changes {
		if err := service.desired.prepareEntryGeneration(ctx, project.Record.ID, environment.Record.ID,
			change.desired, change.record, claim.CreatedAt); err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		changed = append(changed, change.record.Entry.ID)
	}
	task, item, err := metadataEntryWork(project.Record, environment.Record, claim,
		workexecution.ActionUpdate, environment.Record.ID, "Entries")
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	response, err := entryBulkUpsertResponse(candidate.changes, task.ID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(response.Body)
	marker := idempotency.IdempotencyMarker{
		Kind: idempotency.IdempotencyMarkerTask, State: idempotency.IdempotencyMarkerPending,
		Locator: locator, Intent: claim.Intent, Response: response, TaskID: task.ID,
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	result, publicationErr := service.desired.repository.PublishUnrenderedEntryWork(ctx, etcd.EntryMetadataPublication{
		Source: source, Records: candidate.entries, Changed: changed,
		Task: task, Item: item, Marker: marker, At: claim.CreatedAt,
	})
	if publicationErr != nil {
		if !isUnknownEntryCreationOutcome(publicationErr) {
			return idempotency.IdempotencyResponse{}, publicationErr
		}
		resolution, err := service.idempotency.ResolveUnknown(ctx, locator, evidence, publicationErr)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		return resolvedEntryMetadataResponse(resolution, response)
	}
	resolution, err := service.idempotency.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	return resolvedEntryMetadataResponse(resolution, response)
}
