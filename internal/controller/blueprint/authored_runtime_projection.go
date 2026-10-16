package blueprint

import (
	"context"
	"math"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

type authoredRuntimeProjectionSource struct {
	desired     etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]
	identities  etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]
	tenant      etcdstore.Versioned[hierarchyrecord.TenantRecord]
	project     etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
}

// sealAuthoredRuntimeProjection waits for acknowledged input effects, compiles
// one complete runtime projection and atomically settles its Backup policy.
func (service *Service) sealAuthoredRuntimeProjection(
	ctx context.Context,
	parent etcd.TaskRecord,
	snapshot blueprintunits.Snapshot,
) error {
	if service == nil || service.repository == nil || ctx == nil ||
		validateAuthoredRuntimeProjectionParent(parent, snapshot) != nil {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection authority changed")
	}
	if current, found, err := service.repository.GetEnvironmentComposeProjectionRevision(
		ctx, parent.Owner.EnvironmentID, parent.ID,
	); err == nil && found {
		if current.Record.EnvironmentID != parent.Owner.EnvironmentID ||
			current.Record.RevisionID != parent.ID ||
			current.Record.RenderGeneration != uint64(parent.RenderGeneration) {
			return errs.New(errs.KindStateConflict, "Blueprint runtime projection changed")
		}
		return nil
	} else if err != nil {
		kind, known := errs.KindOf(err)
		if !known || kind != errs.KindStateConflict {
			return err
		}
	}
	source, err := service.loadAuthoredRuntimeProjectionSource(ctx, parent)
	if err != nil {
		return err
	}
	if err := requireAuthoredRuntimeProjectionInputs(
		source.desired.Record, source.identities.Record, snapshot,
	); err != nil {
		return err
	}
	components, err := service.loadAuthoredRuntimeComponents(ctx, parent, source, snapshot)
	if err != nil {
		return err
	}
	attaches, err := service.loadAuthoredRuntimeAttaches(
		ctx, parent.Owner.EnvironmentID, source.identities.Record, snapshot,
	)
	if err != nil {
		return err
	}
	requirements, err := service.loadAuthoredRuntimeRequirements(
		ctx, source.desired, snapshot.HeadRevision,
	)
	if err != nil {
		return err
	}
	projection, err := service.compileAuthoredRuntimeProjection(
		ctx, parent, source, snapshot, attaches, components, requirements,
	)
	if err != nil {
		return err
	}
	backup, err := service.prepareAuthoredRuntimeBackup(ctx, parent, source, snapshot, projection, attaches)
	if err != nil {
		return err
	}
	defer backup.Clear()
	projection.Backup = backup.Projection()
	return service.repository.SealEnvironmentBlueprintRuntimeProjection(
		ctx,
		etcd.BlueprintRuntimeProjectionSeal{
			Parent: parent, SnapshotReadRevision: snapshot.ReadRevision,
			ExpectedHeadRevision:    snapshot.HeadRevision,
			ExpectedEpochRevision:   snapshot.EpochRevision,
			DesiredRootRevision:     source.desired.Revision,
			OwnedIdentitiesRevision: source.identities.Revision,
			Tenant:                  source.tenant, Project: source.project, Environment: source.environment,
			Projection: projection, Attaches: attaches,
			Components: components.authorities, ComponentReceipts: components.receipts,
			BackupPreparation: backup,
		},
	)
}

func validateAuthoredRuntimeProjectionParent(
	parent etcd.TaskRecord,
	snapshot blueprintunits.Snapshot,
) error {
	if etcd.ValidateTaskRecord(parent) != nil ||
		parent.Executor != taskjournal.TaskExecutorBlueprint ||
		parent.Status != taskjournal.TaskStatusRunning ||
		parent.RenderGeneration <= 0 ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID ||
		snapshot.EnvironmentID != parent.Owner.EnvironmentID ||
		snapshot.HeadTaskID != parent.ID || snapshot.HeadRevision <= 0 ||
		snapshot.ReadRevision < snapshot.HeadRevision || snapshot.EpochRevision < 0 ||
		(snapshot.EpochRevision == 0 && snapshot.Epoch.Sequence != 0) ||
		(snapshot.EpochRevision > 0 &&
			(snapshot.Epoch.EnvironmentID != parent.Owner.EnvironmentID ||
				snapshot.Epoch.Sequence == 0 || snapshot.Epoch.Sequence == math.MaxUint64)) {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection parent changed")
	}
	return nil
}

func (service *Service) loadAuthoredRuntimeProjectionSource(
	ctx context.Context,
	parent etcd.TaskRecord,
) (authoredRuntimeProjectionSource, error) {
	environmentID := parent.Owner.EnvironmentID
	desired, found, err := service.repository.GetEnvironmentDesiredInputRevision(ctx, environmentID, parent.ID)
	if err != nil {
		return authoredRuntimeProjectionSource{}, err
	}
	if !found {
		return authoredRuntimeProjectionSource{}, errs.New(
			errs.KindStateConflict, "Blueprint parent desired input is missing",
		)
	}
	identities, found, err := service.repository.GetEnvironmentOwnedIdentitiesRevision(
		ctx, environmentID, parent.ID,
	)
	if err != nil {
		return authoredRuntimeProjectionSource{}, err
	}
	if !found || desired.Revision <= 0 || identities.Revision <= 0 ||
		desired.Record.EnvironmentID != environmentID || desired.Record.RevisionID != parent.ID ||
		identities.Record.EnvironmentID != environmentID || identities.Record.RevisionID != parent.ID ||
		desired.Record.RenderGeneration != identities.Record.RenderGeneration ||
		desired.Record.RenderGeneration > math.MaxInt32 ||
		int32(desired.Record.RenderGeneration) != parent.RenderGeneration {
		return authoredRuntimeProjectionSource{}, errs.New(
			errs.KindStateConflict, "Blueprint parent desired input changed",
		)
	}
	if _, err := authoredOwnedIdentitySnapshot(ctx, desired.Record, identities.Record); err != nil {
		return authoredRuntimeProjectionSource{}, err
	}
	if _, err := projectionrecord.AuthoredEntryRecords(desired.Record, identities.Record); err != nil {
		return authoredRuntimeProjectionSource{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return authoredRuntimeProjectionSource{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return authoredRuntimeProjectionSource{}, err
	}
	expectedOwner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return authoredRuntimeProjectionSource{}, err
	}
	if expectedOwner != parent.Owner ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return authoredRuntimeProjectionSource{}, errs.New(
			errs.KindStateConflict, "Blueprint runtime projection hierarchy changed",
		)
	}
	tenant, err := service.authoredOwnerTenant(ctx, project.Record)
	if err != nil {
		return authoredRuntimeProjectionSource{}, err
	}
	return authoredRuntimeProjectionSource{
		desired: desired, identities: identities,
		tenant: tenant, project: project, environment: environment,
	}, nil
}

func (service *Service) compileAuthoredRuntimeProjection(
	ctx context.Context,
	parent etcd.TaskRecord,
	source authoredRuntimeProjectionSource,
	unitSnapshot blueprintunits.Snapshot,
	attaches []etcdstore.Versioned[attachrecord.Record],
	components authoredRuntimeComponents,
	requirements core.BlueprintRequirements,
) (projectionrecord.EnvironmentComposeProjection, error) {
	if err := requireAuthoredRuntimeProjectionInputs(source.desired.Record, source.identities.Record, unitSnapshot); err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	return service.compileAuthoredEnvironmentInput(ctx, parent, source, attaches,
		authoredComponentCompileInput{
			records: components.records, projected: components.projected, sources: components.sources,
		}, requirements)
}

// compileAuthoredEnvironmentInput only renders exact input configuration. It
// neither establishes acknowledged runtime nor publishes a projection; those
// decisions belong to the receipt-checking caller and atomic runtime seal.
func (service *Service) compileAuthoredEnvironmentInput(
	ctx context.Context,
	parent etcd.TaskRecord,
	source authoredRuntimeProjectionSource,
	attaches []etcdstore.Versioned[attachrecord.Record],
	components authoredComponentCompileInput,
	requirements core.BlueprintRequirements,
) (projectionrecord.EnvironmentComposeProjection, error) {
	desired, identities := source.desired.Record, source.identities.Record
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, desired, identities)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	snapshot, err := authoredOwnedIdentitySnapshot(ctx, desired, identities)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	zoneOwnerKind, zoneOwnerID := core.ZoneOwnerEnvironment, desired.EnvironmentID
	renderOwnerKind := composerender.ComposeProjectOwnerTenant
	if source.project.Record.Kind == hierarchyrecord.ProjectKindBacking {
		zoneOwnerKind, zoneOwnerID = core.ZoneOwnerBackingProject, source.project.Record.ID
		renderOwnerKind = composerender.ComposeProjectOwnerBacking
	}
	zones, err := taskplanning.ProjectZoneProjection(project, snapshot, zoneOwnerKind, zoneOwnerID)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	services, err := taskplanning.ProjectServiceProjection(
		project, snapshot, desired.Input.ServiceExtensions,
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	routes, err := projectAuthoredRuntimeRoutes(desired, identities, services)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	entries, err := projectionrecord.AuthoredEntryRecords(desired, identities)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	componentRoutes := make([]core.Route, len(routes))
	for index, route := range routes {
		componentRoutes[index] = route.Desired
	}
	componentEnvironment := blueprintComponentEnvironment(
		source.environment.Record, zones, services, componentRoutes, components.projected, entries,
	)
	componentProjection, err := composerender.ProjectEnvironmentComponents(
		project, componentEnvironment, service.componentCatalog,
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	renderIdentities := environmentComponentComposeIdentities(snapshot, componentProjection.Services)
	entryProjection, err := composerender.ProjectEnvironmentEntries(
		componentProjection.Project, desired.EnvironmentID, source.environment.Record.VolumeDir,
		snapshot.Services, entries,
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	zoneProjection := make([]projectionrecord.EnvironmentZoneProjection, len(zones))
	for index, zone := range zones {
		zoneProjection[index] = projectionrecord.EnvironmentZoneProjection{
			EnvironmentID: desired.EnvironmentID, Desired: zone,
		}
	}
	serviceProjection := make([]servicerecord.EnvironmentServiceProjection, len(services))
	for index, service := range services {
		serviceProjection[index] = servicerecord.EnvironmentServiceProjection{
			EnvironmentID: desired.EnvironmentID, Desired: service,
		}
	}
	externalNetworks, err := taskplanning.ProjectEnvironmentAttachNetworks(
		entryProjection.Project, desired.EnvironmentID, zoneProjection, serviceProjection, attaches,
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	volumeMounts, err := environmentBlueprintVolumeMounts(entryProjection.Project, renderIdentities)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	dependencyPlans, err := buildEnvironmentDependencyPlans(snapshot.Services, desired.Input.ServiceExtensions)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: entryProjection.Project,
		ArtifactID: ids.DeriveAt(
			ids.KindConfig, parent.CreatedAt, parent.ID, "blueprint/compose-artifact",
		),
		ProjectOwnerKind: renderOwnerKind,
		TenantID:         source.tenant.Record.ID, ProjectID: source.project.Record.ID,
		EnvironmentID: desired.EnvironmentID,
		PlanID: ids.DeriveAt(
			ids.KindPlan, parent.CreatedAt, parent.ID, "blueprint/execution-plan",
		),
		RenderGeneration:    desired.RenderGeneration,
		AuthorizedVolumeDir: source.environment.Record.VolumeDir,
		Identities:          renderIdentities, ExternalNetworks: externalNetworks,
	})
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	artifactValue, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, errs.Wrap(errs.KindInternal, err)
	}
	volumeSlugs := make(map[string]string, len(identities.Volumes))
	for _, identity := range identities.Volumes {
		volumeSlugs[identity.Name] = identity.Slug
	}
	projection := desiredrevision.ComposeProjection(
		desired.EnvironmentID, desired.RevisionID, desired.RenderGeneration,
		renderIdentities, volumeSlugs, volumeMounts, artifactValue,
		desired.Input.NormalizedCompose, desired.Input.RuntimeFiles,
		desired.Input.ServiceExtensions, nil, authoredComponentInputRecords(components.records), entries,
	)
	projection.ManagedComponentRuntimeSources = append(
		[]projectionrecord.ManagedComponentRuntimeSource(nil), components.sources...,
	)
	projection = desiredrevision.WithDesiredTopology(
		projection, zoneProjection, serviceProjection, routes,
	)
	projection.ServiceDependencyPlans = dependencyPlans
	projection.BlueprintRequirements = requirements.Clone()
	if err := projectionrecord.ValidateEnvironmentComposeProjection(projection); err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	return projection, nil
}

func requireAuthoredRuntimeProjectionInputs(
	desired projectionrecord.EnvironmentDesiredInput,
	identities projectionrecord.EnvironmentOwnedIdentities,
	snapshot blueprintunits.Snapshot,
) error {
	return requireAuthoredProjectionInputReceipts(desired, identities, snapshot, "")
}

// Only pre-switch planning may omit the one pending proxy's receipt. Runtime
// publication always calls the wrapper above with no omitted receipt.
func requireAuthoredProjectionInputReceipts(
	desired projectionrecord.EnvironmentDesiredInput,
	identities projectionrecord.EnvironmentOwnedIdentities,
	snapshot blueprintunits.Snapshot,
	pendingProxyID string,
) error {
	if pendingProxyID != "" && (ids.Validate(ids.KindComponent, pendingProxyID) != nil ||
		!slices.ContainsFunc(identities.Components, func(identity projectionrecord.OwnedIdentity) bool {
			return identity.ID == pendingProxyID && identity.Name == string(core.ComponentCapabilityHTTPRouter)
		})) {
		return errs.New(errs.KindValidationFailed, "Blueprint pending proxy identity is invalid")
	}
	if err := requireAuthoredBackupInputs(desired, identities, snapshot); err != nil {
		return err
	}
	if !sameOwnedNames(sortedAuthoredMapKeys(desired.Input.Attachments), identities.Attaches) ||
		!sameOwnedNames(sortedAuthoredMapKeys(desired.Input.Components), identities.Components) ||
		!sameOwnedNames(sortedAuthoredMapKeys(desired.Input.Scripts), identities.Scripts) {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection desired identities changed")
	}
	for _, applied := range snapshot.Applied {
		if applied.Record.Target.Kind != ids.KindComponent || applied.Record.State == blueprintunits.Absent {
			continue
		}
		if !slices.ContainsFunc(identities.Components, func(identity projectionrecord.OwnedIdentity) bool {
			return identity.ID == applied.Record.Target.ID
		}) {
			return errs.New(
				errs.KindResourceInUse,
				"Blueprint runtime projection is waiting for omitted Component teardown",
			)
		}
	}
	for _, group := range []struct {
		kind       ids.Kind
		identities []projectionrecord.OwnedIdentity
	}{
		{ids.KindAttach, identities.Attaches},
		{ids.KindComponent, identities.Components},
		{ids.KindEnvEntry, identities.Entries},
	} {
		for _, identity := range group.identities {
			if group.kind == ids.KindComponent && identity.ID == pendingProxyID {
				continue
			}
			target := blueprintunits.ResourceKey{Kind: group.kind, ID: identity.ID}
			if !authoredRuntimeInputApplied(snapshot, target) {
				return errs.New(errs.KindResourceInUse, "Blueprint runtime projection inputs are pending")
			}
		}
	}
	return nil
}

// Live health is observation, not a reproducible configuration input. Keep it
// out of both prepared and acknowledged rendering snapshots without changing
// the actual Component record or its execution receipt.
func authoredComponentInputRecords(records []componentrecord.Record) []componentrecord.Record {
	result := make([]componentrecord.Record, len(records))
	for index, record := range records {
		result[index] = componentrecord.CloneRecord(record)
		result[index].Runtime.Healthy = false
	}
	return result
}

func authoredRuntimeInputApplied(
	snapshot blueprintunits.Snapshot,
	target blueprintunits.ResourceKey,
) bool {
	_, _, found := authoredRuntimeAppliedReceipt(snapshot, target)
	return found
}

func projectAuthoredRuntimeRoutes(
	desired projectionrecord.EnvironmentDesiredInput,
	identities projectionrecord.EnvironmentOwnedIdentities,
	services []core.Service,
) ([]projectionrecord.EnvironmentRouteProjection, error) {
	previous := make([]taskplanning.RouteIdentity, len(identities.Routes))
	for index, identity := range identities.Routes {
		host, routePath, found := strings.Cut(identity.Name, "\x00")
		if !found {
			return nil, errs.New(errs.KindInternal, "Blueprint Route identity is corrupt")
		}
		previous[index] = taskplanning.RouteIdentity{ID: identity.ID, Host: host, Path: routePath}
	}
	reconciled, err := taskplanning.ReconcileBlueprintRoutes(
		desired.Input.Routes, services, previous, func(ids.Kind) string { return "" },
	)
	if err != nil {
		return nil, err
	}
	if len(reconciled.RemovedRouteIDs) != 0 || len(reconciled.Current) != len(identities.Routes) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Route desired identities changed")
	}
	routes := make([]projectionrecord.EnvironmentRouteProjection, len(reconciled.Current))
	for index, route := range reconciled.Current {
		routes[index] = projectionrecord.EnvironmentRouteProjection{
			EnvironmentID: desired.EnvironmentID, Desired: route,
			DesiredGeneration: desired.RenderGeneration,
		}
	}
	return routes, nil
}
