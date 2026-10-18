package blueprint

import (
	"context"
	"net/http"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/entry"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// applyAuthoredOnce accepts the authored head without claiming rendered runtime.
// The coordinator seals effective files after Custom Attaches produce their facts.
func (service *Service) applyAuthoredOnce(
	ctx context.Context,
	environmentID, taskTarget string,
	bundle core.BlueprintBundle,
	expectedRevision, idempotencyKey string,
	preserveRoutes bool,
) (response idempotencyrecord.IdempotencyResponse, resultErr error) {
	evidence, err := service.idempotency.Prepare(ctx, desiredrevision.IntentAddress{
		Method: http.MethodPut, Route: environmentBlueprintRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: environmentID}},
	}, bundle)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.Durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPut, Route: environmentBlueprintRoute, Key: idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal, "Environment Blueprint replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	baseline, err := service.loadAuthoredApplyBaseline(ctx, environmentID, expectedRevision)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	preflight, err := service.prepareAuthoredApplyPreflight(
		ctx, environmentID, bundle, baseline, preserveRoutes,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	bundle, parsed := preflight.bundle, preflight.parsed
	normalizedCompose, err := composerender.MarshalNormalizedEnvironmentProject(parsed.Project)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	previousFiles := baseline.previousInput.Record.Input.RuntimeFiles
	runtimeFiles, err := blueprintparser.SelectRuntimeFiles(parsed.Project, bundle.Files, previousFiles)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	operatorInput, err := blueprintparser.BuildDesiredInput(parsed, normalizedCompose, runtimeFiles)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidateTaskID := ids.New(ids.KindTask)
	claim, err := desiredrevision.Claim(ctx, service.repository, desiredrevision.ClaimInput{
		EnvironmentID: environmentID, CandidateTaskID: candidateTaskID,
		Locator: locator, Intent: evidence.Durable,
		BaselineHeadRevision: baseline.expectedHeadRevision,
		SourceKind:           blueprints.EnvironmentBlueprintSourceApply,
		MatchExistingIntent: func(ctx context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
			return service.idempotency.MatchesStaged(ctx, evidence, existing)
		},
		RenderGeneration: baseline.generation, CreatedAt: service.now().UTC(),
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	claimHeld := true
	defer func() {
		if claimHeld && resultErr != nil {
			resultErr = desiredrevision.AbandonClaim(ctx, service.repository, claim, resultErr)
		}
	}()
	prerequisiteAuthority, err := service.prepareAuthoredPrerequisiteAuthority(
		ctx, environmentID, claim.TaskID, preflight.requirements,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	allocator, err := desiredrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	changes, err := composeidentity.ReconcileOwned(parsed.Project, baseline.previous, allocator.New)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if len(changes.RemovedVolumeIDs) != 0 {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse, "Blueprint omits a persistent Volume; remove it separately before Apply",
		)
	}
	previousVolumes := make(
		[]projectionrecord.EnvironmentVolumeIdentity,
		len(baseline.previousIdentities.Record.Volumes),
	)
	for index, identity := range baseline.previousIdentities.Record.Volumes {
		previousVolumes[index] = projectionrecord.EnvironmentVolumeIdentity{
			ID: identity.ID, Key: identity.Name, Slug: identity.Slug,
		}
	}
	volumeSlugs, err := environmentBlueprintVolumeSlugsFromVolumes(
		parsed.Project, previousVolumes, baseline.hasHead,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	serviceExtensions := cloneEnvironmentBlueprintServiceExtensions(parsed.ServiceExtensions)
	if preserveRoutes {
		serviceExtensions, err = preserveEnvironmentBlueprintServiceExtensions(
			parsed.ServiceExtensions, preflight.submittedServiceNames,
			baseline.previous.Services, baseline.previousInput.Record.Input.ServiceExtensions,
		)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	operatorInput.ServiceExtensions = cloneEnvironmentBlueprintServiceExtensions(serviceExtensions)
	desiredServices, err := taskplanning.ProjectServiceProjection(parsed.Project, changes.Current, serviceExtensions)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if err := service.validateAuthoredExistingAttaches(
		ctx, environmentID, claim.TaskID, parsed.Extensions.Attachments,
		desiredServices, preflight.currentAttaches,
	); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	currentEntries, err := service.listBlueprintEntries(ctx, environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	pinnedEntries, err := entry.BlueprintProjection(currentEntries)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	entries, err := taskplanning.ReconcileBlueprintEntries(
		environmentID, parsed.Extensions.Entries, pinnedEntries, allocator.Named,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	// A fact is resolved only after its Attach child settles. Give this Apply a
	// fresh immutable value identity even when the Entry syntax is unchanged;
	// reusing the preceding generation would bind the new receipt to old facts.
	for index := range entries.Current {
		key := entries.Current[index].BlueprintKey
		if parsed.Extensions.Entries[key].Source.Fact != nil {
			entries.Current[index].CurrentValueGenerationID = allocator.Named(ids.KindConfig, "entry-generation/"+key)
		}
	}
	previousRoutes := make([]taskplanning.RouteIdentity, len(baseline.previousIdentities.Record.Routes))
	for index, identity := range baseline.previousIdentities.Record.Routes {
		host, path, ok := strings.Cut(identity.Name, "\x00")
		if !ok {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal, "Blueprint Route identity is corrupt",
			)
		}
		previousRoutes[index] = taskplanning.RouteIdentity{ID: identity.ID, Host: host, Path: path}
	}
	routes, err := taskplanning.ReconcileBlueprintRoutes(
		parsed.Extensions.Routes, desiredServices, previousRoutes, allocator.New,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	routeProjections := make([]projectionrecord.EnvironmentRouteProjection, len(routes.Current))
	for index, route := range routes.Current {
		routeProjections[index] = projectionrecord.EnvironmentRouteProjection{
			EnvironmentID: environmentID, Desired: route, DesiredGeneration: baseline.generation,
		}
	}
	previous := baseline.previousIdentities.Record
	attaches, err := selectAuthoredAttachIdentities(
		parsed.Extensions.Attachments,
		previous.Attaches,
		preflight.currentAttaches,
		allocator.Named,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	components, err := service.authoredComponentIdentities(
		ctx, environmentID, parsed.Extensions.Components, previous.Components,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	reconciledScripts, scriptPublication, err := service.prepareApplyScripts(
		ctx, environmentID, claim.RevisionID, parsed.Extensions.Scripts, desiredServices,
		desiredrevision.BlueprintScriptResources{Volumes: changes.Current.Volumes, Entries: entries.Current},
		allocator.Named,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer scriptPublication.Clear()
	scripts := make([]composeidentity.Resource, len(reconciledScripts.Current))
	for index, record := range reconciledScripts.Current {
		name, err := scriptrecord.BlueprintAuthoringKey(record)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		scripts[index] = composeidentity.Resource{ID: record.Desired.ID, Name: name}
	}
	owned, err := authoredOwnedIdentities(
		environmentID, claim.RevisionID, baseline.generation, changes.Current, previous,
		volumeSlugs, entries.Current, routeProjections, attaches, components, scripts,
		selectAuthoredNamedResources(parsed.Extensions.DNS, previous.DNSRecords, ids.KindDNSRecord, allocator.Named),
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	desired, err := projectionrecord.NewEnvironmentDesiredInput(
		environmentID, claim.RevisionID, baseline.generation, operatorInput,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	zoneReservations, err := authoredZoneReservations(ctx, desired, owned, baseline.project.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	attachInputGenerations, err := service.prepareAuthoredAttachInputGenerations(
		ctx, environmentID, claim.RevisionID, parsed.Extensions.Attachments,
		desiredServices, preflight.currentAttaches, owned, allocator,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clearAuthoredAttachInputGenerations(attachInputGenerations)
	admission, err := service.planAuthoredAdmission(ctx, baseline, desired, owned, attachInputGenerations)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	plan := admission.Plan
	parent, err := authoredApplyParent(
		claim, baseline.scope, taskTarget, desired, uint32(len(plan.Units)), service.executionTimeout,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	graph, err := service.workAdmission.Prepare(ctx, parent, plan, baseline.ledger, desired.Input.Components)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if err := captureAuthoredWorkNames(&graph, baseline.previousIdentities.Record, owned); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	staged, err := desiredrevision.Stage(ctx, service.repository, desiredrevision.StageInput{
		Claim: claim, Blueprint: desiredrevision.BlueprintRevision(environmentID, claim.TaskID, claim.CreatedAt, bundle),
		DesiredInput: desired,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	claimHeld = false
	return desiredrevision.PublishAuthored(
		ctx,
		service.repository,
		service.idempotency,
		desiredrevision.PublishAuthoredInput{
			Project: baseline.project, Environment: baseline.environment,
			ExpectedHeadRevision: baseline.expectedHeadRevision,
			Baseline:             baseline.ledger,
			Graph:                graph,
			Staged:               staged, OwnedIdentities: owned,
			ZoneReservations:         zoneReservations,
			PrerequisiteAuthority:    prerequisiteAuthority,
			AttachInputGenerations:   attachInputGenerations,
			RetainedAttachConditions: admission.Conditions,
			Evidence:                 evidence, Locator: locator, Parent: parent,
			ScriptPublication: scriptPublication,
		},
	)
}

func selectAuthoredNamedResources[T any](
	authored map[string]T,
	previous []projectionrecord.OwnedIdentity,
	kind ids.Kind,
	allocate func(ids.Kind, string) string,
) []composeidentity.Resource {
	prior := make(map[string]string, len(previous))
	for _, identity := range previous {
		prior[identity.Name] = identity.ID
	}
	resources := make([]composeidentity.Resource, 0, len(authored))
	for name := range authored {
		id := prior[name]
		if id == "" {
			id = allocate(kind, "owned/"+string(kind)+"/"+name)
		}
		resources = append(resources, composeidentity.Resource{ID: id, Name: name})
	}
	return resources
}
