package blueprint

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareAuthoredOnce captures a desired revision after the whole-Task gate.
// Desired-head publication joins promotion of the original pure accepted Task.
func (service *Service) prepareAuthoredOnce(
	ctx context.Context,
	accepted workqueue.TaskSnapshot,
	input acceptedBlueprint,
) (response taskdependencies.Prepared, resultErr error) {
	environmentID, bundle, expectedRevision, preserveRoutes := accepted.Task.Scope.EnvironmentID, input.Bundle, input.ExpectedRevision, input.PreserveRoutes
	locator := input.Locator
	baseline, err := service.loadAuthoredApplyBaseline(ctx, environmentID, expectedRevision)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	preflight, err := service.prepareAuthoredApplyPreflight(
		ctx, environmentID, bundle, baseline, preserveRoutes,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	bundle, parsed := preflight.bundle, preflight.parsed
	normalizedCompose, err := composerender.MarshalNormalizedEnvironmentProject(parsed.Project)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	previousFiles := baseline.previousInput.Record.Input.RuntimeFiles
	runtimeFiles, err := blueprintparser.SelectRuntimeFiles(parsed.Project, bundle.Files, previousFiles)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	operatorInput, err := blueprintparser.BuildDesiredInput(parsed, normalizedCompose, runtimeFiles)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	candidateTaskID := accepted.Task.ID
	claim, err := desiredrevision.Claim(ctx, service.repository, desiredrevision.ClaimInput{
		EnvironmentID: environmentID, CandidateTaskID: candidateTaskID, AcceptedTaskRevision: accepted.Revision,
		Locator: locator, TaskLocator: &locator, Intent: input.Intent,
		BaselineHeadRevision: baseline.expectedHeadRevision,
		SourceKind:           blueprints.EnvironmentBlueprintSourceApply,
		MatchExistingIntent: func(_ context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
			return blueprints.SameBlueprintProtectedIntent(input.Intent, existing), nil
		},
		RenderGeneration: baseline.generation, CreatedAt: accepted.Task.CreatedAt,
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
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
		return taskdependencies.Prepared{}, err
	}
	allocator, err := desiredrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	changes, err := composeidentity.ReconcileOwned(parsed.Project, baseline.previous, allocator.New)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if len(changes.RemovedVolumeIDs) != 0 {
		return taskdependencies.Prepared{}, errs.New(
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
		return taskdependencies.Prepared{}, err
	}
	serviceExtensions := cloneEnvironmentBlueprintServiceExtensions(parsed.ServiceExtensions)
	if preserveRoutes {
		serviceExtensions, err = preserveEnvironmentBlueprintServiceExtensions(
			parsed.ServiceExtensions, preflight.submittedServiceNames,
			baseline.previous.Services, baseline.previousInput.Record.Input.ServiceExtensions,
		)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
	}
	operatorInput.ServiceExtensions = cloneEnvironmentBlueprintServiceExtensions(serviceExtensions)
	desiredServices, err := taskplanning.ProjectServiceProjection(parsed.Project, changes.Current, serviceExtensions)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if err := service.validateAuthoredExistingAttaches(
		ctx, environmentID, claim.TaskID, parsed.Extensions.Attachments,
		desiredServices, preflight.currentAttaches,
	); err != nil {
		return taskdependencies.Prepared{}, err
	}
	currentEntries, err := service.listBlueprintEntries(ctx, environmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	pinnedEntries, err := entry.BlueprintProjection(currentEntries)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	entries, err := taskplanning.ReconcileBlueprintEntries(
		environmentID, parsed.Extensions.Entries, pinnedEntries, allocator.Named,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
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
			return taskdependencies.Prepared{}, errs.New(
				errs.KindInternal, "Blueprint Route identity is corrupt",
			)
		}
		previousRoutes[index] = taskplanning.RouteIdentity{ID: identity.ID, Host: host, Path: path}
	}
	routes, err := taskplanning.ReconcileBlueprintRoutes(
		parsed.Extensions.Routes, desiredServices, previousRoutes, allocator.New,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
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
		return taskdependencies.Prepared{}, err
	}
	components, err := service.authoredComponentIdentities(
		ctx, environmentID, parsed.Extensions.Components, previous.Components,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	reconciledScripts, scriptPublication, err := service.prepareApplyScripts(
		ctx, environmentID, claim.RevisionID, parsed.Extensions.Scripts, desiredServices,
		desiredrevision.BlueprintScriptResources{Volumes: changes.Current.Volumes, Entries: entries.Current},
		allocator.Named,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer scriptPublication.Clear()
	scripts := make([]composeidentity.Resource, len(reconciledScripts.Current))
	for index, record := range reconciledScripts.Current {
		name, err := scriptrecord.BlueprintAuthoringKey(record)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		scripts[index] = composeidentity.Resource{ID: record.Desired.ID, Name: name}
	}
	owned, err := authoredOwnedIdentities(
		environmentID, claim.RevisionID, baseline.generation, changes.Current, previous,
		volumeSlugs, entries.Current, routeProjections, attaches, components, scripts,
		selectAuthoredNamedResources(parsed.Extensions.DNS, previous.DNSRecords, ids.KindDNSRecord, allocator.Named),
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	desired, err := projectionrecord.NewEnvironmentDesiredInput(
		environmentID, claim.RevisionID, baseline.generation, operatorInput,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	zoneReservations, err := authoredZoneReservations(ctx, desired, owned, baseline.project.Record)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	attachInputGenerations, err := service.prepareAuthoredAttachInputGenerations(
		ctx, environmentID, claim.RevisionID, parsed.Extensions.Attachments,
		desiredServices, preflight.currentAttaches, owned, allocator,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clearAuthoredAttachInputGenerations(attachInputGenerations)
	admission, err := service.planAuthoredAdmission(ctx, baseline, desired, owned, attachInputGenerations)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	plan := admission.Plan
	parent, err := authoredApplyParent(
		claim,
		baseline.scope,
		baseline.environment.Record.Name,
		desired,
		uint32(len(plan.Units)),
		service.executionTimeout,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}

	parent.AcceptedIntent = accepted.Task.AcceptedIntent
	parent.After = accepted.Task.After
	parent.Deadline = accepted.Task.Deadline
	parent.TargetName = accepted.Task.TargetName
	detachInputs, err := service.prepareAuthoredDetachInputs(ctx, parent, plan, owned, baseline.ledger)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clearAuthoredDetachInputs(detachInputs)
	attachCaptures := make([]attachinputs.Generation, len(attachInputGenerations))
	for index, capture := range attachInputGenerations {
		attachCaptures[index] = capture.Generation
	}
	graph, err := service.workAdmission.Prepare(
		ctx,
		parent,
		plan,
		baseline.ledger,
		desired.Input.Components,
		attachCaptures, detachInputs,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if err := captureAuthoredWorkNames(&graph, baseline.previousIdentities.Record, owned); err != nil {
		return taskdependencies.Prepared{}, err
	}
	staged, err := desiredrevision.Stage(ctx, service.repository, desiredrevision.StageInput{
		Claim: claim, Blueprint: desiredrevision.BlueprintRevision(environmentID, claim.TaskID, claim.CreatedAt, bundle),
		DesiredInput: desired,
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	claimHeld = false
	return desiredrevision.PrepareAuthored(
		ctx,
		service.repository,
		desiredrevision.PublishAuthoredInput{
			Project: baseline.project, Environment: baseline.environment,
			ExpectedHeadRevision: baseline.expectedHeadRevision,
			Baseline:             baseline.ledger,
			Graph:                graph,
			Staged:               staged, OwnedIdentities: owned,
			ZoneReservations:         zoneReservations,
			PrerequisiteAuthority:    prerequisiteAuthority,
			AttachInputGenerations:   attachInputGenerations,
			DetachInputs:             detachInputs,
			RetainedAttachConditions: admission.Conditions,
			Locator:                  locator, Parent: parent,
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
