package services

import (
	"context"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"sort"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *serviceMutationService) publishServiceDesiredMutation(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current *etcdstore.Versioned[servicerecord.ServiceRecord],
	record servicerecord.ServiceRecord,
	references etcd.ServiceMutationReferences,
	request etcd.EnvironmentServiceMutationRequest,
	action etcd.EnvironmentServiceMutationAction,
	status int,
	locator idempotencyrecord.IdempotencyLocator,
	evidence serviceMutationEvidence,
) (idempotencyrecord.IdempotencyResponse, error) {
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
		environment.Record.ID,
		head,
		hasHead,
		projection,
		hasProjection,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidateRevisionID := ids.New(ids.KindTask)
	if current == nil {
		desired := record.Desired
		desired.ID = serviceStableIDFromRevision(ids.KindService, candidateRevisionID)
		record, err = servicerecord.NewServiceRecord(environment.Record.ID, desired, "")
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		references, err = service.resolveServiceReferences(ctx, record)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	candidate, err := buildServiceDesiredProjection(
		tenant.Record.ID, project.Record.ID, environment.Record, projection.Record, hasProjection,
		record, references, current == nil,
		candidateRevisionID, generation,
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
	if current == nil && claim.RevisionID != candidateRevisionID {
		desired := record.Desired
		desired.ID = serviceStableIDFromRevision(ids.KindService, claim.RevisionID)
		record, err = servicerecord.NewServiceRecord(environment.Record.ID, desired, "")
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		references, err = service.resolveServiceReferences(ctx, record)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	candidate, err = buildServiceDesiredProjection(
		tenant.Record.ID, project.Record.ID, environment.Record, projection.Record, hasProjection,
		record, references, current == nil,
		claim.RevisionID, generation,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	audit := &etcd.EnvironmentServiceMutationAudit{
		Action: action, BaseRevisionID: projection.Record.RevisionID,
		ServiceID: record.Desired.ID, Request: &request,
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: claim, Mutation: &etcd.EnvironmentDesiredMutationAudit{Service: audit},
		Projection: candidate, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	response, marker, err := service.serviceResponseMarker(
		locator, claim.Intent, record, status, claim.CreatedAt,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	change := etcd.EnvironmentBlueprintServiceChange{Record: record}
	if current != nil {
		currentCopy := *current
		change.Current = &currentCopy
	}
	result, publicationErr := service.repository.PublishEnvironmentServiceDesiredRevisionDirect(
		ctx, etcd.EnvironmentServiceDesiredPublication{
			Project: project, Environment: environment, ExpectedHeadRevision: expectedHeadRevision,
			Claim: claim,
			Revision: etcd.EnvironmentDesiredRevisionIdentity{
				EnvironmentID: environment.Record.ID, RevisionID: claim.RevisionID,
			},
			Projection: candidate, Change: change, References: references, Marker: marker,
		},
	)
	return service.resolveServiceMutation(ctx, locator, evidence, result, publicationErr, response)
}

func (service *serviceMutationService) claimServiceDesiredRevision(
	ctx context.Context,
	projection projectionrecord.EnvironmentComposeProjection,
	candidateRevisionID string,
	expectedHeadRevision int64,
	locator idempotencyrecord.IdempotencyLocator,
	evidence serviceMutationEvidence,
	createdAt time.Time,
) (etcd.EnvironmentBlueprintStageClaim, error) {
	if _, err := controllerrevision.PreflightProjection(projection); err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	claim, err := service.repository.ClaimEnvironmentBlueprintStage(ctx, etcd.EnvironmentBlueprintStageClaimRequest{
		EnvironmentID: projection.EnvironmentID, CandidateRevisionID: candidateRevisionID,
		CandidateTaskID: candidateRevisionID, Locator: locator, Intent: evidence.durable,
		BaselineHeadRevision: expectedHeadRevision, SourceKind: etcd.EnvironmentBlueprintSourceMutation,
		RenderGeneration: projection.RenderGeneration,
		ProjectionSchema: etcd.EnvironmentDesiredProjectionSchema, CreatedAt: createdAt,
	})
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	if claim.Existing {
		matched, matchErr := service.idempotency.MatchesStaged(ctx, evidence, claim.Intent)
		if matchErr != nil {
			return etcd.EnvironmentBlueprintStageClaim{}, matchErr
		}
		if !matched {
			return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
				errs.KindIdempotencyMismatch, "idempotency key was used for a different Service mutation",
			)
		}
	}
	if claim.EnvironmentID != projection.EnvironmentID || claim.RevisionID != claim.TaskID ||
		claim.Locator != locator || claim.BaselineHeadRevision != expectedHeadRevision ||
		claim.SourceKind != etcd.EnvironmentBlueprintSourceMutation ||
		claim.RenderGeneration != projection.RenderGeneration ||
		claim.ProjectionSchema != etcd.EnvironmentDesiredProjectionSchema {
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindStateConflict,
			"Service staged baseline changed",
		)
	}
	return claim, nil
}

func buildServiceDesiredProjection(
	tenantID string,
	projectID string,
	environment hierarchyrecord.EnvironmentRecord,
	current projectionrecord.EnvironmentComposeProjection,
	hasCurrent bool,
	record servicerecord.ServiceRecord,
	references etcd.ServiceMutationReferences,
	create bool,
	revisionID string,
	generation uint64,
) (projectionrecord.EnvironmentComposeProjection, error) {
	if hasCurrent {
		if err := rejectComponentGeneratedServiceTarget(current, record.Desired.ID); err != nil {
			return projectionrecord.EnvironmentComposeProjection{}, err
		}
	}
	candidate := controllerrevision.CloneProjection(current)
	candidate.EnvironmentID = environment.ID
	candidate.RevisionID = revisionID
	candidate.RenderGeneration = generation
	if create {
		candidate.DesiredServices = append(candidate.DesiredServices, servicerecord.EnvironmentServiceProjection{
			EnvironmentID: record.EnvironmentID, BackingNetworkID: record.BackingNetworkID, Desired: record.Desired,
		})
	} else {
		replaced := false
		for index := range candidate.DesiredServices {
			if candidate.DesiredServices[index].Desired.ID == record.Desired.ID {
				candidate.DesiredServices[index] = servicerecord.EnvironmentServiceProjection{
					EnvironmentID: record.EnvironmentID, BackingNetworkID: record.BackingNetworkID, Desired: record.Desired,
				}
				replaced = true
				break
			}
		}
		if !replaced {
			return projectionrecord.EnvironmentComposeProjection{}, errs.New(errs.KindStateConflict, "Service desired record is absent")
		}
	}
	sort.Slice(candidate.DesiredServices, func(left, right int) bool {
		return candidate.DesiredServices[left].Desired.Name < candidate.DesiredServices[right].Desired.Name
	})
	artifact := &agentpb.ComposeArtifact{
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             environment.ID,
		ProjectName:         "gp-" + strings.ToLower(environment.ID),
		AuthorizedVolumeDir: environment.VolumeDir,
		CanonicalYaml:       []byte("services: {}\nnetworks: {}\n"),
	}
	if hasCurrent {
		if err := proto.Unmarshal(current.ComposeArtifact, artifact); err != nil {
			return projectionrecord.EnvironmentComposeProjection{}, errs.New(
				errs.KindInternal,
				"Service baseline artifact is corrupt",
			)
		}
	}
	action := composerender.ServiceArtifactEdit
	if create {
		action = composerender.ServiceArtifactCreate
	}
	mutated, err := composerender.MutateEnvironmentServiceArtifact(artifact, composerender.ServiceArtifactMutation{
		Action: action, Desired: record.Desired,
		Zones:      serviceArtifactZones(references),
		ArtifactID: serviceStableIDFromRevision(ids.KindConfig, revisionID),
		PlanID:     serviceStableIDFromRevision(ids.KindPlan, revisionID), TenantID: tenantID, ProjectID: projectID,
		RenderGeneration: generation,
	})
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	normalizedArtifact := proto.Clone(artifact).(*agentpb.ComposeArtifact)
	if hasCurrent {
		normalizedArtifact, err = composerender.NormalizedEnvironmentArtifact(current)
		if err != nil {
			return projectionrecord.EnvironmentComposeProjection{}, err
		}
	}
	normalizedArtifact, err = composerender.MutateEnvironmentServiceArtifact(
		normalizedArtifact,
		composerender.ServiceArtifactMutation{
			Action: action, Desired: record.Desired,
			Zones:      serviceArtifactZones(references),
			ArtifactID: serviceStableIDFromRevision(ids.KindConfig, revisionID),
			PlanID:     serviceStableIDFromRevision(ids.KindPlan, revisionID), TenantID: tenantID, ProjectID: projectID,
			RenderGeneration: generation,
		},
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	candidate.NormalizedCompose = append([]byte(nil), normalizedArtifact.GetCanonicalYaml()...)
	candidate.ServiceExtensions = controllerrevision.CloneServiceExtensions(current.ServiceExtensions)
	setDirectServiceExtension(candidate.ServiceExtensions, record.Desired)
	candidate.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(mutated)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, errs.Wrap(errs.KindInternal, err)
	}
	return candidate, nil
}

func serviceArtifactZones(references etcd.ServiceMutationReferences) []composerender.ServiceArtifactZone {
	zones := make([]composerender.ServiceArtifactZone, len(references.Zones))
	for index, zone := range references.Zones {
		zones[index] = composerender.ServiceArtifactZone{
			ID: zone.Record.Desired.ID, Name: zone.Record.Desired.Name,
			Subnet: zone.Record.Desired.Subnet, Internal: zone.Record.Desired.Internal,
		}
	}
	return zones
}

func buildServiceRemovalProjection(
	tenantID string,
	projectID string,
	current projectionrecord.EnvironmentComposeProjection,
	record servicerecord.ServiceRecord,
	revisionID string,
	generation uint64,
) (projectionrecord.EnvironmentComposeProjection, error) {
	if err := rejectComponentGeneratedServiceTarget(current, record.Desired.ID); err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	candidate := controllerrevision.CloneProjection(current)
	candidate.RevisionID = revisionID
	candidate.RenderGeneration = generation
	candidate.DesiredServices = make([]servicerecord.EnvironmentServiceProjection, 0, len(current.DesiredServices)-1)
	for _, desired := range current.DesiredServices {
		if desired.Desired.ID != record.Desired.ID {
			candidate.DesiredServices = append(candidate.DesiredServices, desired)
		}
	}
	if len(candidate.DesiredServices) != len(current.DesiredServices)-1 {
		return projectionrecord.EnvironmentComposeProjection{}, errs.New(errs.KindStateConflict, "Service desired record is absent")
	}
	candidate.VolumeMounts = candidate.VolumeMounts[:0]
	for _, mount := range current.VolumeMounts {
		if mount.ServiceID != record.Desired.ID {
			candidate.VolumeMounts = append(candidate.VolumeMounts, mount)
		}
	}
	candidate.ServiceDependencyPlans = current.ServiceDependencyPlans.Clone().WithoutService(record.Desired.Name)
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(current.ComposeArtifact, artifact); err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, errs.New(errs.KindInternal, "Service baseline artifact is corrupt")
	}
	mutated, err := composerender.MutateEnvironmentServiceArtifact(artifact, composerender.ServiceArtifactMutation{
		Action: composerender.ServiceArtifactRemove, Desired: record.Desired,
		ArtifactID: serviceStableIDFromRevision(ids.KindConfig, revisionID),
		PlanID:     serviceStableIDFromRevision(ids.KindPlan, revisionID), TenantID: tenantID, ProjectID: projectID,
		RenderGeneration: generation,
	})
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	normalizedArtifact, err := composerender.NormalizedEnvironmentArtifact(current)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	normalizedArtifact, err = composerender.MutateEnvironmentServiceArtifact(
		normalizedArtifact,
		composerender.ServiceArtifactMutation{
			Action: composerender.ServiceArtifactRemove, Desired: record.Desired,
			ArtifactID: serviceStableIDFromRevision(ids.KindConfig, revisionID),
			PlanID:     serviceStableIDFromRevision(ids.KindPlan, revisionID), TenantID: tenantID, ProjectID: projectID,
			RenderGeneration: generation,
		},
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	candidate.NormalizedCompose = append([]byte(nil), normalizedArtifact.GetCanonicalYaml()...)
	candidate.ServiceExtensions = controllerrevision.CloneServiceExtensions(current.ServiceExtensions)
	delete(candidate.ServiceExtensions, record.Desired.Name)
	if current.ServiceExtensions == nil {
		candidate.ServiceExtensions = nil
	}
	candidate.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(mutated)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, errs.Wrap(errs.KindInternal, err)
	}
	return candidate, nil
}

func rejectComponentGeneratedServiceTarget(projection projectionrecord.EnvironmentComposeProjection, serviceID string) error {
	for _, component := range projection.Components {
		for _, generatedServiceID := range component.Runtime.GeneratedServices {
			if generatedServiceID == serviceID {
				return errs.New(errs.KindResourceInUse, "Service is managed by its owning Component")
			}
		}
	}
	return nil
}

func setDirectServiceExtension(extensions map[string]core.ServiceExtensionSpec, service core.Service) {
	extension := core.ServiceExtensionSpec{}
	if service.Strategy != "" || service.OnFailure != "" {
		extension.Release = &core.ServiceReleaseSpec{
			DefaultStrategy: service.Strategy,
			OnFailure:       service.OnFailure,
		}
	}
	if len(service.DependsOn) != 0 {
		extension.DependsOn = make(map[string]core.ServiceDependency, len(service.DependsOn))
		for dependency, decision := range service.DependsOn {
			decision.Phases = append([]core.ServiceDependencyPhase(nil), decision.Phases...)
			extension.DependsOn[dependency] = decision
		}
	}
	if extension.Release == nil && len(extension.DependsOn) == 0 {
		delete(extensions, service.Name)
		return
	}
	extensions[service.Name] = extension
}

func serviceStableIDFromRevision(kind ids.Kind, revisionID string) string {
	if ids.Validate(ids.KindTask, revisionID) != nil {
		return ""
	}
	return string(kind) + "_" + strings.TrimPrefix(revisionID, "task_")
}

func serviceMutationAuditFromCreate(input apiTypes.ServiceCreate) etcd.EnvironmentServiceMutationRequest {
	return etcd.EnvironmentServiceMutationRequest{
		EnvironmentID: input.EnvironmentID, Name: input.Name, Image: input.Image,
		Zones: append([]string(nil), input.Zones...), Strategy: core.Strategy(input.Strategy),
		OnFailure: core.OnFailure(input.OnFailure), Healthcheck: serviceHealthcheckToCore(input.Healthcheck),
		Resources: core.Resources{Mem: input.Resources.Mem, CPUs: input.Resources.CPUs},
		Expose:    append([]string(nil), input.Expose...), Restart: input.Restart, Replicas: input.Replicas,
	}
}

func serviceMutationAuditFromEdit(input apiTypes.ServiceEdit) etcd.EnvironmentServiceMutationRequest {
	return etcd.EnvironmentServiceMutationRequest{
		Image: input.Image, Zones: append([]string(nil), input.Zones...), Strategy: core.Strategy(input.Strategy),
		OnFailure: core.OnFailure(input.OnFailure), Healthcheck: serviceHealthcheckToCore(input.Healthcheck),
		Resources: core.Resources{Mem: input.Resources.Mem, CPUs: input.Resources.CPUs},
		Expose:    append([]string(nil), input.Expose...), Restart: input.Restart, Replicas: input.Replicas,
	}
}
