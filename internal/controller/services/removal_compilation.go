package services

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"net/http"
	"time"
)

func (service *serviceMutationService) prepareAcceptedRemoval(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, request acceptedServiceRemoval,
) (taskdependencies.Prepared, error) {
	defer clear(request.Intent.Ciphertext)
	task := accepted.Task
	serviceID := task.TargetID
	current, err := service.repository.GetService(ctx, serviceID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if err := service.rejectComponentGeneratedServiceMutation(ctx, current.Record); err != nil {
		return taskdependencies.Prepared{}, err
	}
	environment, project, err := service.serviceHierarchy(ctx, current.Record.EnvironmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Service removal",
		)
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	projection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	expectedHeadRevision, generation, err := controllerrevision.NextGeneration(
		environment.Record.ID, head, hasHead, projection, hasProjection,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if !hasProjection || generation > math.MaxInt32 {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Service removal requires initialized desired state",
		)
	}
	if err := service.repository.ValidateServiceRemovalReferences(ctx, current, projection); err != nil {
		return taskdependencies.Prepared{}, err
	}
	candidateRevisionID := task.ID
	candidate, err := buildServiceRemovalProjection(
		tenant.Record.ID, project.Record.ID, projection.Record, current.Record, candidateRevisionID, generation,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	claim, err := service.repository.ClaimEnvironmentBlueprintStage(
		ctx,
		blueprints.EnvironmentBlueprintStageClaimRequest{
			AcceptedTaskRevision: accepted.Revision,
			TaskLocator:          &request.Locator,
			EnvironmentID:        environment.Record.ID, CandidateRevisionID: task.ID, CandidateTaskID: task.ID,
			Locator: request.Locator, Intent: request.Intent, BaselineHeadRevision: expectedHeadRevision,
			SourceKind: blueprints.EnvironmentBlueprintSourceMutation, RenderGeneration: generation,
			ProjectionSchema: blueprints.EnvironmentDesiredInputSchema, CreatedAt: task.CreatedAt,
		},
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if claim.TaskID != task.ID || claim.RevisionID != task.ID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Service removal staging belongs to another accepted Task",
		)
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	desiredInput, err := controllerrevision.DeriveCurrentMutationDesiredInput(
		ctx, service.repository, environment.Record.ID, projection.Record.RevisionID, candidate,
		environment.Record.NetworkPool, func(*core.BlueprintDesiredInput) error { return nil },
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	seal, err := service.repository.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &blueprints.EnvironmentDesiredMutationAudit{Service: &blueprints.EnvironmentServiceMutationAudit{
			Action: blueprints.EnvironmentServiceMutationRemove, BaseRevisionID: projection.Record.RevisionID,
			ServiceID: serviceID,
		}},
		DesiredInput: desiredInput, DependencyDigest: projectionEvidence.DependencyDigest,
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	taskID := claim.TaskID
	runtimeSource, err := service.repository.GetServiceRemovalRuntime(
		ctx,
		task.Scope.EnvironmentID,
		serviceID,
		projection.ReadRevision,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	input := serviceremovalwork.Input{
		TaskID: taskID, WorkItemID: serviceStableIDFromRevision(ids.KindWorkItem, taskID),
		PlanID: serviceStableIDFromRevision(ids.KindPlan, taskID),
		Scope: workexecution.Scope{
			TenantID:      tenant.Record.ID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		},
		ServiceID: serviceID, ServiceName: current.Record.Desired.Name, ServiceRevision: current.Revision,
		RuntimeRevision: servicerecord.ServiceRuntimeRevision(current), RuntimeSourceRevision: runtimeSource.Revision,
		DesiredRevisionID: claim.RevisionID, DesiredSHA256: hex.EncodeToString(seal.ProjectionSHA256[:]),
		BaseRevisionID: projection.Record.RevisionID, HeadRevision: expectedHeadRevision,
		Generation: generation, CreatedAt: at,
	}
	timeout := serviceLifecycleControlTimeoutSeconds
	if runtimeSource.Revision != 0 {
		input.Runtime = &runtimeSource.Record
		input.HostID = runtimeSource.Record.Source.Executor.HostID
		timeout = serviceLifecycleAgentTimeoutSeconds
	}

	if task.Scope != input.Scope {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Service ownership changed after removal was accepted",
		)
	}
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return taskdependencies.Prepared{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetService,
		ID:   serviceID,
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: request.Locator, ReplayTarget: &target, Intent: request.Intent,
		Response: idempotencyrecord.IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        body,
		},
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	publication, err := service.repository.PrepareServiceRemoval(ctx, etcd.ServiceRemovalPublication{
		Tenant: tenant, Project: project, Environment: environment, Service: current,
		Baseline: projection, Candidate: candidate, Claim: claim, Input: input,
		Deadline: acceptedaction.ExecutionDeadline(
			task,
			at,
			time.Duration(timeout)*time.Second,
		), Marker: marker, Accepted: accepted,
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, err = publication.Input.Reference()
	if err != nil {
		return taskdependencies.Prepared{Authority: publication.Change, Finalize: publication.Finish}, err
	}
	task.ExpectedItems, task.UpdatedAt = 1, at
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{publication.Item},
		Authority: publication.Change, Finalize: publication.Finish}, nil
}
