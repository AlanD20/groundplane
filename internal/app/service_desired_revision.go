package app

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
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
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	current *etcd.Versioned[etcd.ServiceRecord],
	record etcd.ServiceRecord,
	references etcd.ServiceMutationReferences,
	request etcd.EnvironmentServiceMutationRequest,
	action etcd.EnvironmentServiceMutationAction,
	status int,
	locator etcd.IdempotencyLocator,
	evidence serviceMutationEvidence,
) (etcd.IdempotencyResponse, error) {
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	expectedHeadRevision, generation, err := serviceDesiredState(environment.Record.ID, head, hasHead, projection, hasProjection)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidateRevisionID := ids.New(ids.KindTask)
	if current == nil {
		desired := record.Desired
		desired.ID = serviceStableIDFromRevision(ids.KindService, candidateRevisionID)
		record, err = etcd.NewServiceRecord(environment.Record.ID, desired, "")
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		references, err = service.resolveServiceReferences(ctx, record)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	candidate, err := buildServiceDesiredProjection(
		tenant.Record.ID, project.Record.ID, environment.Record, projection.Record, hasProjection,
		record, references, current == nil,
		candidateRevisionID, generation,
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
	if current == nil && claim.RevisionID != candidateRevisionID {
		desired := record.Desired
		desired.ID = serviceStableIDFromRevision(ids.KindService, claim.RevisionID)
		record, err = etcd.NewServiceRecord(environment.Record.ID, desired, "")
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		references, err = service.resolveServiceReferences(ctx, record)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	candidate, err = buildServiceDesiredProjection(
		tenant.Record.ID, project.Record.ID, environment.Record, projection.Record, hasProjection,
		record, references, current == nil,
		claim.RevisionID, generation,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	audit := &etcd.EnvironmentServiceMutationAudit{
		Action: action, BaseRevisionID: projection.Record.RevisionID,
		ServiceID: record.Desired.ID, Request: &request,
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: claim, Mutation: &etcd.EnvironmentDesiredMutationAudit{Service: audit},
		Projection: candidate, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, marker, err := service.serviceResponseMarker(
		locator, claim.Intent, record, status, claim.CreatedAt,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
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
	projection etcd.EnvironmentComposeProjection,
	candidateRevisionID string,
	expectedHeadRevision int64,
	locator etcd.IdempotencyLocator,
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
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(errs.KindStateConflict, "Service staged baseline changed")
	}
	return claim, nil
}

func serviceDesiredState(
	environmentID string,
	head etcd.Versioned[etcd.EnvironmentBlueprintHead],
	hasHead bool,
	projection etcd.Versioned[etcd.EnvironmentComposeProjection],
	hasProjection bool,
) (int64, uint64, error) {
	if hasHead != hasProjection {
		return 0, 0, errs.New(errs.KindInternal, "Environment desired-state pointers are inconsistent")
	}
	if !hasHead {
		return 0, 1, nil
	}
	if head.Record.EnvironmentID != environmentID || projection.Record.EnvironmentID != environmentID ||
		head.Record.RevisionID != projection.Record.RevisionID || head.Revision <= 0 ||
		projection.Revision != head.Revision || projection.Record.RenderGeneration == math.MaxUint64 {
		return 0, 0, errs.New(errs.KindInternal, "Environment desired-state pointers are corrupt")
	}
	return head.Revision, projection.Record.RenderGeneration + 1, nil
}

func buildServiceDesiredProjection(
	tenantID string,
	projectID string,
	environment etcd.EnvironmentRecord,
	current etcd.EnvironmentComposeProjection,
	hasCurrent bool,
	record etcd.ServiceRecord,
	references etcd.ServiceMutationReferences,
	create bool,
	revisionID string,
	generation uint64,
) (etcd.EnvironmentComposeProjection, error) {
	candidate := current
	candidate.EnvironmentID = environment.ID
	candidate.RevisionID = revisionID
	candidate.RenderGeneration = generation
	candidate.Services = append([]etcd.EnvironmentComposeIdentity(nil), current.Services...)
	networks, err := serviceZoneNetworkIdentities(current.Networks, references.Zones)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	candidate.Networks = networks
	candidate.Volumes = append([]etcd.EnvironmentVolumeIdentity(nil), current.Volumes...)
	candidate.VolumeMounts = append([]etcd.EnvironmentServiceVolumeMount(nil), current.VolumeMounts...)
	candidate.Routes = append([]etcd.EnvironmentRouteIdentity(nil), current.Routes...)
	candidate.SuppressedRoutes = append([]etcd.EnvironmentRouteIdentity(nil), current.SuppressedRoutes...)
	candidate.Components = append([]etcd.ComponentRecord(nil), current.Components...)
	candidate.Entries = append([]etcd.EntryRecord(nil), current.Entries...)
	candidate.ServiceDependencyPlans = current.ServiceDependencyPlans.Clone()
	if create {
		candidate.Services = append(candidate.Services, etcd.EnvironmentComposeIdentity{
			ID: record.Desired.ID, Name: record.Desired.Name,
		})
		sort.Slice(candidate.Services, func(left, right int) bool {
			return candidate.Services[left].Name < candidate.Services[right].Name
		})
	}
	artifact := &agentpb.ComposeArtifact{
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             environment.ID,
		ProjectName:         "gp-" + strings.ToLower(environment.ID),
		AuthorizedVolumeDir: environment.VolumeDir,
		CanonicalYaml:       []byte("services: {}\nnetworks: {}\n"),
	}
	if hasCurrent {
		if err := proto.Unmarshal(current.ComposeArtifact, artifact); err != nil {
			return etcd.EnvironmentComposeProjection{}, errs.New(errs.KindInternal, "Service baseline artifact is corrupt")
		}
	}
	action := controller.ServiceArtifactEdit
	if create {
		action = controller.ServiceArtifactCreate
	}
	mutated, err := controller.MutateEnvironmentServiceArtifact(artifact, controller.ServiceArtifactMutation{
		Action: action, Desired: record.Desired,
		Zones:      serviceArtifactZones(references),
		ArtifactID: serviceStableIDFromRevision(ids.KindConfig, revisionID),
		PlanID:     serviceStableIDFromRevision(ids.KindPlan, revisionID), TenantID: tenantID, ProjectID: projectID,
		RenderGeneration: generation,
	})
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	normalizedArtifact := proto.Clone(artifact).(*agentpb.ComposeArtifact)
	if hasCurrent {
		normalizedArtifact, err = controller.NormalizedEnvironmentArtifact(current)
		if err != nil {
			return etcd.EnvironmentComposeProjection{}, err
		}
	}
	normalizedArtifact, err = controller.MutateEnvironmentServiceArtifact(normalizedArtifact, controller.ServiceArtifactMutation{
		Action: action, Desired: record.Desired,
		Zones:      serviceArtifactZones(references),
		ArtifactID: serviceStableIDFromRevision(ids.KindConfig, revisionID),
		PlanID:     serviceStableIDFromRevision(ids.KindPlan, revisionID), TenantID: tenantID, ProjectID: projectID,
		RenderGeneration: generation,
	})
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	candidate.NormalizedCompose = append([]byte(nil), normalizedArtifact.GetCanonicalYaml()...)
	candidate.ServiceExtensions = cloneDirectServiceExtensions(current.ServiceExtensions)
	setDirectServiceExtension(candidate.ServiceExtensions, record.Desired)
	candidate.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(mutated)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, errs.Wrap(errs.KindInternal, err)
	}
	return candidate, nil
}

func serviceArtifactZones(references etcd.ServiceMutationReferences) []controller.ServiceArtifactZone {
	zones := make([]controller.ServiceArtifactZone, len(references.Zones))
	for index, zone := range references.Zones {
		zones[index] = controller.ServiceArtifactZone{
			ID: zone.Record.Desired.ID, Name: zone.Record.Desired.Name,
			Subnet: zone.Record.Desired.Subnet, Internal: zone.Record.Desired.Internal,
		}
	}
	return zones
}

func serviceZoneNetworkIdentities(
	current []etcd.EnvironmentComposeIdentity,
	zones []etcd.Versioned[etcd.ZoneRecord],
) ([]etcd.EnvironmentComposeIdentity, error) {
	result := append([]etcd.EnvironmentComposeIdentity(nil), current...)
	for _, zone := range zones {
		identity := etcd.EnvironmentComposeIdentity{ID: zone.Record.Desired.ID, Name: zone.Record.Desired.Name}
		found := false
		for _, existing := range result {
			if existing.ID == identity.ID || existing.Name == identity.Name {
				if existing != identity {
					return nil, errs.New(errs.KindInternal, "Service Zone identity conflicts with the Environment projection")
				}
				found = true
				break
			}
		}
		if !found {
			result = append(result, identity)
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })
	return result, nil
}

func buildServiceRemovalProjection(
	tenantID string,
	projectID string,
	current etcd.EnvironmentComposeProjection,
	record etcd.ServiceRecord,
	revisionID string,
	generation uint64,
) (etcd.EnvironmentComposeProjection, error) {
	candidate := current
	candidate.RevisionID = revisionID
	candidate.RenderGeneration = generation
	candidate.Services = make([]etcd.EnvironmentComposeIdentity, 0, len(current.Services)-1)
	for _, identity := range current.Services {
		if identity.ID != record.Desired.ID {
			candidate.Services = append(candidate.Services, identity)
		}
	}
	if len(candidate.Services) != len(current.Services)-1 {
		return etcd.EnvironmentComposeProjection{}, errs.New(errs.KindStateConflict, "Service desired identity is absent")
	}
	candidate.Networks = append([]etcd.EnvironmentComposeIdentity(nil), current.Networks...)
	candidate.Volumes = append([]etcd.EnvironmentVolumeIdentity(nil), current.Volumes...)
	candidate.VolumeMounts = candidate.VolumeMounts[:0]
	for _, mount := range current.VolumeMounts {
		if mount.ServiceID != record.Desired.ID {
			candidate.VolumeMounts = append(candidate.VolumeMounts, mount)
		}
	}
	candidate.Routes = append([]etcd.EnvironmentRouteIdentity(nil), current.Routes...)
	candidate.SuppressedRoutes = append([]etcd.EnvironmentRouteIdentity(nil), current.SuppressedRoutes...)
	candidate.Components = append([]etcd.ComponentRecord(nil), current.Components...)
	candidate.Entries = append([]etcd.EntryRecord(nil), current.Entries...)
	candidate.ServiceDependencyPlans = current.ServiceDependencyPlans.Clone().WithoutService(record.Desired.Name)
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(current.ComposeArtifact, artifact); err != nil {
		return etcd.EnvironmentComposeProjection{}, errs.New(errs.KindInternal, "Service baseline artifact is corrupt")
	}
	mutated, err := controller.MutateEnvironmentServiceArtifact(artifact, controller.ServiceArtifactMutation{
		Action: controller.ServiceArtifactRemove, Desired: record.Desired,
		ArtifactID: serviceStableIDFromRevision(ids.KindConfig, revisionID),
		PlanID:     serviceStableIDFromRevision(ids.KindPlan, revisionID), TenantID: tenantID, ProjectID: projectID,
		RenderGeneration: generation,
	})
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	normalizedArtifact, err := controller.NormalizedEnvironmentArtifact(current)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	normalizedArtifact, err = controller.MutateEnvironmentServiceArtifact(normalizedArtifact, controller.ServiceArtifactMutation{
		Action: controller.ServiceArtifactRemove, Desired: record.Desired,
		ArtifactID: serviceStableIDFromRevision(ids.KindConfig, revisionID),
		PlanID:     serviceStableIDFromRevision(ids.KindPlan, revisionID), TenantID: tenantID, ProjectID: projectID,
		RenderGeneration: generation,
	})
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	candidate.NormalizedCompose = append([]byte(nil), normalizedArtifact.GetCanonicalYaml()...)
	candidate.ServiceExtensions = cloneDirectServiceExtensions(current.ServiceExtensions)
	delete(candidate.ServiceExtensions, record.Desired.Name)
	if current.ServiceExtensions == nil {
		candidate.ServiceExtensions = nil
	}
	candidate.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(mutated)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, errs.Wrap(errs.KindInternal, err)
	}
	return candidate, nil
}

func cloneDirectServiceExtensions(source map[string]core.ServiceExtensionSpec) map[string]core.ServiceExtensionSpec {
	result := make(map[string]core.ServiceExtensionSpec, len(source)+1)
	for name, extension := range source {
		result[name] = cloneEnvironmentBlueprintServiceExtension(extension)
	}
	return result
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
