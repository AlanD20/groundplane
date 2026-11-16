package operations

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func entryMetadataClaim(source etcd.EntryMutationSource, locator idempotency.IdempotencyLocator,
	intent idempotency.ProtectedIntentRecord, at time.Time, accepted workqueue.TaskSnapshot,
) blueprints.EnvironmentBlueprintStageClaim {
	taskID := accepted.Task.ID
	return blueprints.EnvironmentBlueprintStageClaim{
		DescriptorID: strings.TrimPrefix(taskID, "task_"), TaskID: taskID, RevisionID: taskID,
		EnvironmentID: source.Desired.Record.EnvironmentID, Locator: locator, Intent: intent,
		BaselineHeadRevision: source.Desired.Revision, SourceKind: blueprints.EnvironmentBlueprintSourceMutation,
		RenderGeneration: source.Desired.Record.RenderGeneration, ProjectionSchema: blueprints.EnvironmentDesiredInputSchema,
		CreatedAt: at,
	}
}

func metadataEntryWork(
	project hierarchy.ProjectRecord,
	environment hierarchy.EnvironmentRecord,
	claim blueprints.EnvironmentBlueprintStageClaim,
	action workexecution.Action,
	targetID, targetName string,
	accepted workqueue.TaskSnapshot,
	at time.Time,
) (workexecution.Task, workexecution.Item, error) {
	return taskplanning.NewEntryConfigurationWork(workexecution.Scope{
		TenantID: project.TenantID, ProjectID: project.ID, EnvironmentID: environment.ID,
	}, workexecution.Placement{Executor: workexecution.Controller}, claim, action, targetID, targetName, accepted, at)
}

func (service *entryDesiredMutationService) prepareUnrenderedEntry(ctx context.Context,
	request entryDesiredMutationRequest, source etcd.EntryMutationSource,
	project keyvalue.Versioned[hierarchy.ProjectRecord], environment keyvalue.Versioned[hierarchy.EnvironmentRecord],
) (taskdependencies.Prepared, error) {
	claim := entryMetadataClaim(
		source,
		request.locator,
		request.evidence.durable,
		request.accepted.Task.CreatedAt,
		request.accepted,
	)
	next, previous, err := entryDesiredCandidateRecord(environment.Record.ID, source.Entries, request, claim.TaskID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if next != nil {
		if err := service.prepareEntryGeneration(ctx, project.Record.ID, environment.Record.ID,
			request.desired, *next, request.at); err != nil {
			return taskdependencies.Prepared{}, err
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
	task, item, err := metadataEntryWork(
		project.Record,
		environment.Record,
		claim,
		action,
		targetID,
		targetName,
		request.accepted,
		request.at,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	response, err := entryTaskResponse(task.ID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(response.Body)
	replayTarget := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetEntry, ID: targetID}
	marker := idempotency.IdempotencyMarker{
		Kind: idempotency.IdempotencyMarkerTask, State: idempotency.IdempotencyMarkerPending,
		Locator: request.locator, Intent: claim.Intent, Response: response, ReplayTarget: &replayTarget,
		TaskID: task.ID, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	capture, publicationErr := service.repository.PrepareUnrenderedEntryWork(ctx, etcd.EntryMetadataPublication{
		Source: source, Records: replaceProjectedEntry(source.Entries, previous, next), Changed: []string{targetID},
		Task: task, Item: item, Marker: marker, At: request.at, Accepted: request.accepted,
	})
	if publicationErr != nil {
		return taskdependencies.Prepared{}, publicationErr
	}
	return entryCapturePrepared(capture), nil
}
