package services

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"net/http"
)

func (service *serviceMutationService) RemoveService(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindService, serviceID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Service removal input is invalid",
		)
	}
	for attempt := 0; attempt < maximumServiceMutationAttempts; attempt++ {
		response, err := service.removeServiceOnce(ctx, serviceID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumServiceMutationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Service removal retry bound was not enforced",
	)
}

func (service *serviceMutationService) removeServiceOnce(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetService,
		ID:   serviceID,
	}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, serviceEditRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayIndexedServiceRemoval(ctx, serviceID, locator)
	}
	current, err := service.repository.GetService(ctx, serviceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if err := service.rejectComponentGeneratedServiceMutation(ctx, current.Record); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intentValue := serviceMutationIntent{
		method: http.MethodDelete, route: serviceEditRoute,
		environmentID: current.Record.EnvironmentID, serviceID: serviceID,
	}
	evidence, err := service.idempotency.Prepare(ctx, intentValue)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator = serviceMutationLocator(intentValue, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return serviceReplayResponse(resolution)
	}
	environment, project, err := service.serviceHierarchy(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Service removal",
		)
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	projection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	expectedHeadRevision, generation, err := controllerrevision.NextGeneration(
		environment.Record.ID, head, hasHead, projection, hasProjection,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !hasProjection || generation > math.MaxInt32 {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Service removal requires initialized desired state",
		)
	}
	if err := service.repository.ValidateServiceRemovalReferences(ctx, current, projection); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidateRevisionID := ids.New(ids.KindTask)
	candidate, err := buildServiceRemovalProjection(
		tenant.Record.ID, project.Record.ID, projection.Record, current.Record, candidateRevisionID, generation,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	claim, err := service.claimServiceDesiredRevision(
		ctx, candidate, candidateRevisionID, expectedHeadRevision, locator, evidence, service.now().UTC(),
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidate, err = buildServiceRemovalProjection(
		tenant.Record.ID, project.Record.ID, projection.Record, current.Record, claim.RevisionID, generation,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &blueprints.EnvironmentDesiredMutationAudit{Service: &blueprints.EnvironmentServiceMutationAudit{
			Action: blueprints.EnvironmentServiceMutationRemove, BaseRevisionID: projection.Record.RevisionID,
			ServiceID: serviceID,
		}},
		Projection: candidate, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	owner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	taskID := claim.TaskID
	task := etcd.TaskRecord{
		ID: taskID, OperationID: serviceStableIDFromRevision(ids.KindOperation, taskID),
		IdempotencyKey: idempotencyKey, Owner: owner, Actor: taskjournal.TaskActorOperator,
		Executor: taskjournal.TaskExecutorAgent, PlanID: serviceStableIDFromRevision(ids.KindPlan, taskID),
		Type: taskjournal.TaskRemove, Target: serviceID, TimeoutSeconds: serviceLifecycleAgentTimeoutSeconds,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	removalIntent, err := environmentchanges.NewServiceRemovalIntent(
		taskID, current, projection, expectedHeadRevision, claim, candidate, claim.CreatedAt,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if service.lifecycle == nil || service.lifecycle.plans == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Service removal planner is not configured",
		)
	}
	task, err = service.lifecycle.plans.PrepareServiceRemovalTask(
		ctx, task, removalIntent,
		serviceStableIDFromRevision(ids.KindConfig, taskID), serviceStableIDFromRevision(ids.KindStep, taskID),
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: claim.Intent, Response: response, TaskID: taskID,
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetService, TargetID: serviceID, TargetRevision: current.Revision,
		TaskID: taskID, Phase: deletionrecord.DeletionPhaseHostEffects, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	result, mutationErr := service.repository.BeginServiceRemovalWithTask(
		ctx, tenant, project, environment, current, projection, tombstone, removalIntent, task, marker,
	)
	return service.resolveServiceMutation(ctx, locator, evidence, result, mutationErr, response)
}

func (service *serviceMutationService) replayIndexedServiceRemoval(
	ctx context.Context,
	serviceID string,
	locator idempotencyrecord.IdempotencyLocator,
) (idempotencyrecord.IdempotencyResponse, error) {
	intent := serviceMutationIntent{
		method: http.MethodDelete, route: serviceEditRoute,
		environmentID: locator.ScopeID, serviceID: serviceID,
	}
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Service removal replay target is inconsistent",
		)
	}
	return serviceReplayResponse(resolution)
}
