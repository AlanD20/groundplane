package services

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"net/http"
)

func (service *serviceMutationService) RemoveService(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindService, serviceID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Service removal input is invalid")
	}
	for attempt := 0; attempt < maximumServiceMutationAttempts; attempt++ {
		response, err := service.removeServiceOnce(ctx, serviceID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumServiceMutationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service removal retry bound was not enforced")
}

func (service *serviceMutationService) removeServiceOnce(
	ctx context.Context,
	serviceID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetService, ID: serviceID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, serviceEditRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayIndexedServiceRemoval(ctx, serviceID, locator)
	}
	current, err := service.repository.GetService(ctx, serviceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if err := service.rejectComponentGeneratedServiceMutation(ctx, current.Record); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intentValue := serviceMutationIntent{
		method: http.MethodDelete, route: serviceEditRoute,
		environmentID: current.Record.EnvironmentID, serviceID: serviceID,
	}
	evidence, err := service.idempotency.Prepare(ctx, intentValue)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator = serviceMutationLocator(intentValue, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return serviceReplayResponse(resolution)
	}
	environment, project, err := service.serviceHierarchy(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Service removal",
		)
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	expectedHeadRevision, generation, err := serviceDesiredState(
		environment.Record.ID, head, hasHead, projection, hasProjection,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !hasProjection || generation > math.MaxInt32 {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Service removal requires initialized desired state",
		)
	}
	if err := service.repository.ValidateServiceRemovalReferences(ctx, current, projection); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidateRevisionID := ids.New(ids.KindTask)
	candidate, err := buildServiceRemovalProjection(
		tenant.Record.ID, project.Record.ID, projection.Record, current.Record, candidateRevisionID, generation,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	claim, err := service.claimServiceDesiredRevision(
		ctx, candidate, candidateRevisionID, expectedHeadRevision, locator, evidence, service.now().UTC(),
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidate, err = buildServiceRemovalProjection(
		tenant.Record.ID, project.Record.ID, projection.Record, current.Record, claim.RevisionID, generation,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &etcd.EnvironmentDesiredMutationAudit{Service: &etcd.EnvironmentServiceMutationAudit{
			Action: etcd.EnvironmentServiceMutationRemove, BaseRevisionID: projection.Record.RevisionID,
			ServiceID: serviceID,
		}},
		Projection: candidate, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	owner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskID := claim.TaskID
	task := etcd.TaskRecord{
		ID: taskID, OperationID: serviceStableIDFromRevision(ids.KindOperation, taskID),
		IdempotencyKey: idempotencyKey, Owner: owner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: serviceStableIDFromRevision(ids.KindPlan, taskID),
		Type: etcd.TaskRemove, Target: serviceID, TimeoutSeconds: serviceLifecycleAgentTimeoutSeconds,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	removalIntent, err := etcd.NewServiceRemovalIntent(
		taskID, current, projection, expectedHeadRevision, claim, candidate, claim.CreatedAt,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if service.lifecycle == nil || service.lifecycle.plans == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service removal planner is not configured")
	}
	task, err = service.lifecycle.plans.PrepareServiceRemovalTask(
		ctx, task, removalIntent,
		serviceStableIDFromRevision(ids.KindConfig, taskID), serviceStableIDFromRevision(ids.KindStep, taskID),
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: claim.Intent, Response: response, TaskID: taskID,
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	tombstone := etcd.DeletionTombstoneRecord{
		TargetKind: etcd.DeletionTargetService, TargetID: serviceID, TargetRevision: current.Revision,
		TaskID: taskID, Phase: etcd.DeletionPhaseHostEffects, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	result, mutationErr := service.repository.BeginServiceRemovalWithTask(
		ctx, tenant, project, environment, current, projection, tombstone, removalIntent, task, marker,
	)
	return service.resolveServiceMutation(ctx, locator, evidence, result, mutationErr, response)
}

func (service *serviceMutationService) replayIndexedServiceRemoval(
	ctx context.Context,
	serviceID string,
	locator etcd.IdempotencyLocator,
) (etcd.IdempotencyResponse, error) {
	intent := serviceMutationIntent{
		method: http.MethodDelete, route: serviceEditRoute,
		environmentID: locator.ScopeID, serviceID: serviceID,
	}
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !existing {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service removal replay target is inconsistent")
	}
	return serviceReplayResponse(resolution)
}
