package operations

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func entryMetadataClaim(source etcd.EntryMutationSource, locator idempotency.IdempotencyLocator,
	intent idempotency.ProtectedIntentRecord, at time.Time,
) blueprints.EnvironmentBlueprintStageClaim {
	taskID := ids.New(ids.KindTask)
	return blueprints.EnvironmentBlueprintStageClaim{
		DescriptorID: strings.TrimPrefix(taskID, "task_"), TaskID: taskID, RevisionID: taskID,
		EnvironmentID: source.Desired.Record.EnvironmentID, Locator: locator, Intent: intent,
		BaselineHeadRevision: source.Desired.Revision, SourceKind: blueprints.EnvironmentBlueprintSourceMutation,
		RenderGeneration: source.Desired.Record.RenderGeneration, ProjectionSchema: blueprints.EnvironmentDesiredInputSchema,
		CreatedAt: at,
	}
}

func metadataEntryWork(project hierarchy.ProjectRecord, environment hierarchy.EnvironmentRecord,
	claim blueprints.EnvironmentBlueprintStageClaim, action workexecution.Action, targetID, targetName string,
) (workexecution.Task, workexecution.Item, error) {
	return taskplanning.NewEntryConfigurationWork(workexecution.Scope{
		TenantID: project.TenantID, ProjectID: project.ID, EnvironmentID: environment.ID,
	}, workexecution.Placement{Executor: workexecution.Controller}, claim, action, targetID, targetName)
}

func (service *entryDesiredMutationService) mutateUnrenderedEntry(ctx context.Context,
	request entryDesiredMutationRequest, source etcd.EntryMutationSource,
	project keyvalue.Versioned[hierarchy.ProjectRecord], environment keyvalue.Versioned[hierarchy.EnvironmentRecord],
) (idempotency.IdempotencyResponse, error) {
	claim := entryMetadataClaim(source, request.locator, request.evidence.durable, service.now().UTC())
	next, previous, err := entryDesiredCandidateRecord(environment.Record.ID, source.Entries, request, claim.TaskID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if next != nil {
		if err := service.prepareEntryGeneration(ctx, project.Record.ID, environment.Record.ID,
			request.desired, *next, claim.CreatedAt); err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
	}
	targetID, targetName, action := request.entryID, "", workexecution.ActionUpdate
	if request.action == entryDesiredMutationCreate {
		action = workexecution.ActionCreate
	} else if request.action == entryDesiredMutationRemove {
		action = workexecution.ActionRemove
	}
	if next != nil {
		targetID, targetName = next.Entry.ID, next.Entry.Key
		if targetName == "" {
			targetName = next.Entry.Path
		}
	} else if previous != nil {
		targetName = previous.Entry.Key
		if targetName == "" {
			targetName = previous.Entry.Path
		}
	}
	task, item, err := metadataEntryWork(project.Record, environment.Record, claim, action, targetID, targetName)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	response, err := entryDesiredResponse(next, task.ID, request.status)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(response.Body)
	replayTarget := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetEntry, ID: targetID}
	marker := idempotency.IdempotencyMarker{
		Kind: idempotency.IdempotencyMarkerTask, State: idempotency.IdempotencyMarkerPending,
		Locator: request.locator, Intent: claim.Intent, Response: response, ReplayTarget: &replayTarget,
		TaskID: task.ID, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	result, publicationErr := service.repository.PublishUnrenderedEntryWork(ctx, etcd.EntryMetadataPublication{
		Source: source, Records: replaceProjectedEntry(source.Entries, previous, next), Changed: []string{targetID},
		Task: task, Item: item, Marker: marker, At: claim.CreatedAt,
	})
	if publicationErr != nil {
		if !isUnknownEntryCreationOutcome(publicationErr) {
			return idempotency.IdempotencyResponse{}, publicationErr
		}
		resolution, err := request.evidence.resolveUnknown(ctx, request.locator, publicationErr)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		return resolvedEntryMetadataResponse(resolution, response)
	}
	resolution, err := request.evidence.resolveKnown(ctx, result)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	return resolvedEntryMetadataResponse(resolution, response)
}

func resolvedEntryMetadataResponse(resolution requestidempotency.Resolution,
	accepted idempotency.IdempotencyResponse,
) (idempotency.IdempotencyResponse, error) {
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(accepted), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry metadata acceptance is unresolved")
	}
}
