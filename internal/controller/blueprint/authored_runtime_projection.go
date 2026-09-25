package blueprint

import (
	"context"
	"math"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
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

// sealAuthoredRuntimeProjection compiles and publishes one complete runtime
// projection. It deliberately supports only revisions whose runtime inputs are
// already present in immutable authored state; fact-producing dependencies stay
// pending until their separate authority is available.
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
	projection, err := compileAuthoredRuntimeProjection(ctx, parent, source, snapshot)
	if err != nil {
		return err
	}
	return service.repository.SealEnvironmentBlueprintRuntimeProjection(
		ctx,
		etcd.BlueprintRuntimeProjectionSeal{
			Parent: parent, ExpectedHeadRevision: snapshot.HeadRevision,
			ExpectedEpochRevision:   snapshot.EpochRevision,
			DesiredRootRevision:     source.desired.Revision,
			OwnedIdentitiesRevision: source.identities.Revision,
			Tenant:                  source.tenant, Project: source.project, Environment: source.environment,
			Projection: projection,
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
	if _, err := authoredEntryRecords(desired.Record, identities.Record); err != nil {
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
	if expectedOwner != parent.Owner || environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return authoredRuntimeProjectionSource{}, errs.New(
			errs.KindStateConflict, "Blueprint runtime projection hierarchy changed",
		)
	}
	var tenant etcdstore.Versioned[hierarchyrecord.TenantRecord]
	switch project.Record.Kind {
	case hierarchyrecord.ProjectKindTenant:
		tenant, err = service.repository.GetTenant(ctx, project.Record.TenantID)
		if err != nil {
			return authoredRuntimeProjectionSource{}, err
		}
		if project.Record.TenantID != tenant.Record.ID {
			return authoredRuntimeProjectionSource{}, errs.New(
				errs.KindStateConflict, "Blueprint runtime projection Tenant changed",
			)
		}
	case hierarchyrecord.ProjectKindBacking:
		if project.Record.TenantID != "" {
			return authoredRuntimeProjectionSource{}, errs.New(
				errs.KindStateConflict, "Blueprint runtime projection backing owner changed",
			)
		}
	default:
		return authoredRuntimeProjectionSource{}, errs.New(
			errs.KindStateConflict, "Blueprint runtime projection Project kind changed",
		)
	}
	return authoredRuntimeProjectionSource{
		desired: desired, identities: identities,
		tenant: tenant, project: project, environment: environment,
	}, nil
}

func compileAuthoredRuntimeProjection(
	ctx context.Context,
	parent etcd.TaskRecord,
	source authoredRuntimeProjectionSource,
	unitSnapshot blueprintunits.Snapshot,
) (projectionrecord.EnvironmentComposeProjection, error) {
	desired, identities := source.desired.Record, source.identities.Record
	if err := requireAuthoredRuntimeProjectionInputs(desired, identities, unitSnapshot); err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
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
	entries, err := authoredEntryRecords(desired, identities)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	entryProjection, err := composerender.ProjectEnvironmentEntries(
		project, desired.EnvironmentID, source.environment.Record.VolumeDir, snapshot.Services, entries,
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	volumeMounts, err := environmentBlueprintVolumeMounts(entryProjection.Project, snapshot)
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
		Identities:          snapshot,
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
		snapshot, volumeSlugs, volumeMounts, artifactValue,
		desired.Input.NormalizedCompose, desired.Input.RuntimeFiles,
		desired.Input.ServiceExtensions, nil, nil, entries,
	)
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
	projection = desiredrevision.WithDesiredTopology(
		projection, zoneProjection, serviceProjection, routes,
	)
	projection.ServiceDependencyPlans = dependencyPlans
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
	if len(desired.Input.Components) != 0 ||
		desired.Input.Backup != nil || len(desired.Input.Requires) != 0 {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection prerequisites are pending")
	}
	if !sameOwnedNames(sortedAuthoredMapKeys(desired.Input.Attachments), identities.Attaches) ||
		!sameOwnedNames(sortedAuthoredMapKeys(desired.Input.Components), identities.Components) ||
		!sameOwnedNames(sortedAuthoredMapKeys(desired.Input.Scripts), identities.Scripts) {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection desired identities changed")
	}
	for _, group := range []struct {
		kind       ids.Kind
		identities []projectionrecord.OwnedIdentity
	}{
		{ids.KindAttach, identities.Attaches},
		{ids.KindEnvEntry, identities.Entries},
	} {
		for _, identity := range group.identities {
			target := blueprintunits.ResourceKey{Kind: group.kind, ID: identity.ID}
			if !authoredRuntimeInputApplied(snapshot, target) {
				return errs.New(errs.KindStateConflict, "Blueprint runtime projection inputs are pending")
			}
		}
	}
	return nil
}

func authoredRuntimeInputApplied(
	snapshot blueprintunits.Snapshot,
	target blueprintunits.ResourceKey,
) bool {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID {
		return false
	}
	for _, unit := range snapshot.Desired.Record.Units {
		if unit.Target != target || unit.Removal {
			continue
		}
		for _, applied := range snapshot.Applied {
			if applied.Record.Target == target && applied.Record.State == blueprintunits.Applied &&
				applied.Record.Fingerprint == unit.Fingerprint {
				return true
			}
		}
		return false
	}
	return false
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
