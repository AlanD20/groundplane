package blueprint

import (
	"context"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// prepareAuthoredComponentChild renders only this singleton's generated
// workload. Authored Services provide routing context, never mutation authority.
// The sealed recipe is published with its candidate rather than rebuilt from
// whichever Environment projection happens to exist when the Agent dispatches.
func (service *Service) prepareAuthoredComponentChild(ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit, timeoutSeconds int64) (blueprintcoordinator.PreparedChild, error) {
	source, err := service.loadAuthoredRuntimeProjectionSource(ctx, parent)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	capability, spec, found := authoredComponentSpec(source.identities.Record.Components, source.desired.Record.Input.Components, unit.Target.ID)
	if !found && !unit.Removal || found && unit.Removal {
		return blueprintcoordinator.PreparedChild{}, errs.New(errs.KindStateConflict, "Blueprint Component is absent from the sealed input")
	}
	currentSet, err := service.listBlueprintComponents(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	var current keyvalue.Versioned[componentrecord.Record]
	for _, selected := range currentSet {
		if selected.Record.Desired.ID == unit.Target.ID {
			current = selected
		}
	}
	if current.Revision <= 0 {
		return blueprintcoordinator.PreparedChild{}, errs.New(errs.KindStateConflict, "Blueprint Component singleton is unavailable")
	}
	active, err := componentrecord.ProjectRecord(current.Record)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if unit.Removal {
		capability, spec, err = omittedComponentSelection(current.Record)
		if err != nil {
			return blueprintcoordinator.PreparedChild{}, err
		}
	}
	changes, err := taskplanning.ReconcileBlueprintComponent(capability, spec, active, func(kind ids.Kind) string { return ids.New(kind) })
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if len(changes.Effective) != 1 {
		return blueprintcoordinator.PreparedChild{}, errs.New(errs.KindStateConflict, "Blueprint Component projection is not a singleton")
	}
	candidate := changes.Effective[0]
	candidate.PinnedIPv4, candidate.Healthy = "", false
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, source.desired.Record, source.identities.Record)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	identities, err := authoredOwnedIdentitySnapshot(ctx, source.desired.Record, source.identities.Record)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	zoneOwnerKind, zoneOwnerID := core.ZoneOwnerEnvironment, parent.Owner.EnvironmentID
	renderOwnerKind := composerender.ComposeProjectOwnerTenant
	if source.project.Record.Kind == hierarchyrecord.ProjectKindBacking {
		zoneOwnerKind, zoneOwnerID = core.ZoneOwnerBackingProject, source.project.Record.ID
		renderOwnerKind = composerender.ComposeProjectOwnerBacking
	}
	zones, err := taskplanning.ProjectZoneProjection(project, identities, zoneOwnerKind, zoneOwnerID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	zoneVersions := make([]keyvalue.Versioned[zonerecord.Record], len(zones))
	zoneChanges := make([]blueprints.EnvironmentBlueprintZoneChange, len(zones))
	for index, zone := range zones {
		record, err := zonerecord.NewRecord(parent.Owner.EnvironmentID, zone)
		if err != nil {
			return blueprintcoordinator.PreparedChild{}, err
		}
		zoneVersions[index] = keyvalue.Versioned[zonerecord.Record]{Record: record, Revision: source.desired.Revision, ReadRevision: current.ReadRevision}
		zoneChanges[index] = blueprints.EnvironmentBlueprintZoneChange{Current: &zoneVersions[index], Record: record}
	}
	head, found, err := service.repository.GetEnvironmentBlueprintHead(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found || head.Record.RevisionID != parent.ID {
		return blueprintcoordinator.PreparedChild{}, errs.New(errs.KindStateConflict, "Blueprint Component parent was superseded")
	}
	authority, err := service.repository.PrepareFrozenComponentUnitAuthority(ctx, componentplanning.FrozenProjectionAuthorityInput{
		EnvironmentID: parent.Owner.EnvironmentID, ReadRevision: current.ReadRevision,
		DesiredHeadRevision: head.Revision, DesiredRevisionID: parent.ID,
		DesiredZones: zoneVersions, RuntimeSourceAuthority: componentplanning.ManagedRuntimeSourceAuthorityApplied,
	}, current)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	predecessorZones, err := authority.ComponentPredecessorZones()
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	for _, prior := range predecessorZones {
		if slices.ContainsFunc(zoneVersions, func(zone keyvalue.Versioned[zonerecord.Record]) bool {
			return zone.Record.Desired.ID == prior.Record.Desired.ID
		}) {
			continue
		}
		zoneChanges = append(zoneChanges, blueprints.EnvironmentBlueprintZoneChange{Current: &prior, Record: prior.Record})
	}
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
		Type: taskjournal.TaskUpdate, Target: parent.Owner.EnvironmentID, RenderGeneration: parent.RenderGeneration,
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam:      parent.ID,
			taskjournal.TaskBlueprintParentParam:            parent.ID,
			taskjournal.TaskBlueprintComponentUnitParam:     unit.Target.ID,
			taskjournal.TaskMaterializationEnvironmentParam: parent.Owner.EnvironmentID,
		},
		TimeoutSeconds: timeoutSeconds, Status: taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	preparation, err := service.repository.PrepareEnvironmentComponentTask(ctx, child.ID, parent.Owner.EnvironmentID, zoneChanges,
		[]componentplanning.EnvironmentComponentCandidateInput{{Current: current, Candidate: candidate}}, createdAt, authority)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	candidateRecord := preparation.Intent.Candidates[0].Candidate
	candidate, err = componentrecord.ProjectRecord(candidateRecord)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	services, err := taskplanning.ProjectServiceProjection(project, identities, source.desired.Record.Input.ServiceExtensions)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	routeProjection, err := projectAuthoredRuntimeRoutes(source.desired.Record, source.identities.Record, services)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	routes := make([]core.Route, len(routeProjection))
	for index, route := range routeProjection {
		routes[index] = route.Desired
	}
	entries, err := authoredEntryRecords(source.desired.Record, source.identities.Record)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	environment := blueprintComponentEnvironment(source.environment.Record, zones, services, routes, []core.Component{candidate}, entries)
	projected, err := composerender.ProjectEnvironmentComponents(project, environment, service.componentCatalog)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	// A component recipe must not accidentally become an Environment-wide up.
	generated := make(composetypes.Services, len(projected.Services))
	for _, identity := range projected.Services {
		generated[identity.Name] = projected.Project.Services[identity.Name]
	}
	projected.Project.Services, projected.Project.DisabledServices = generated, nil
	projected.Project.Volumes = nil
	renderIdentities := composeidentity.Snapshot{Services: projected.Services, Networks: identities.Networks}
	artifactID := ids.New(ids.KindConfig)
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: projected.Project, ArtifactID: artifactID, ProjectOwnerKind: renderOwnerKind,
		TenantID: source.tenant.Record.ID, ProjectID: source.project.Record.ID, EnvironmentID: parent.Owner.EnvironmentID,
		PlanID: child.PlanID, RenderGeneration: uint64(parent.RenderGeneration), AuthorizedVolumeDir: source.environment.Record.VolumeDir,
		Identities: renderIdentities,
	})
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	allocate := func(kind ids.Kind, name string) string { return ids.DeriveAt(kind, createdAt, child.PlanID, name) }
	materializations, steps, err := service.environmentComponentMaterializations(ctx, parent.Owner.EnvironmentID, source.project.Record.ID,
		parent.ID, artifactID, uint64(parent.RenderGeneration), allocate, nil, projected, nil, entries)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	child.Materializations = materializations
	child, err = service.prepareAuthoredComponentConfiguration(ctx, child, current.ReadRevision)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	for _, step := range steps {
		step.TimeoutSeconds = uint32(timeoutSeconds)
	}
	prerequisite := ""
	if len(steps) > 0 {
		prerequisite = steps[len(steps)-1].GetStepId()
	}
	// Predecessor teardown is captured during preparation; it is never loaded
	// through the latest desired head by the later execution resolver.
	child.ManagedComponentTeardownSources = preparation.ManagedComponentTeardownSources()
	for index := len(child.ManagedComponentTeardownSources) - 1; index >= 0; index-- {
		if child.ManagedComponentTeardownSources[index].ComponentID != unit.Target.ID {
			child.ManagedComponentTeardownSources = append(child.ManagedComponentTeardownSources[:index], child.ManagedComponentTeardownSources[index+1:]...)
		}
	}
	// Reuse the existing exact predecessor decoder during sealing only. Dispatch
	// consumes the resulting complete sealed recipe, including source artifacts.
	projection := projectionrecord.EnvironmentComposeProjection{EnvironmentID: parent.Owner.EnvironmentID, RevisionID: parent.ID, RenderGeneration: uint64(parent.RenderGeneration)}
	pinnedSources := map[string]projectionrecord.EnvironmentComposeProjection{}
	if len(child.ManagedComponentTeardownSources) != 0 {
		predecessor, found := authority.ComponentPredecessor()
		if !found || predecessor.EnvironmentID != parent.Owner.EnvironmentID {
			return blueprintcoordinator.PreparedChild{}, errs.New(errs.KindStateConflict, "Blueprint Component predecessor is unavailable")
		}
		pinnedSources[unit.Target.ID] = predecessor
	}
	teardown, err := service.plans.BlueprintManagedComponentTeardown(ctx, child, projection, artifact, prerequisite, false, pinnedSources)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	steps = append(steps, teardown.Steps...)
	if len(steps) > 0 {
		prerequisite = steps[len(steps)-1].GetStepId()
	}
	managedSteps, err := taskplanning.BlueprintManagedServiceSteps(child, artifact, prerequisite, false)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	steps = append(steps, managedSteps...)
	componentSteps, _, err := taskplanning.BuildEnvironmentComponentTaskContribution(taskplanning.EnvironmentComponentTaskContributionInput{
		Apply: taskplanning.EnvironmentManagedConfigApplyInput{
			RevisionID: parent.ID, RenderGeneration: uint64(parent.RenderGeneration), Components: []componentrecord.Record{candidateRecord},
			ComponentCatalog: service.componentCatalog, Materializations: materializations, Artifact: artifact,
		}, AllocateStep: func() string { return allocate(ids.KindStep, "component-config-activate") }, TimeoutSeconds: uint32(timeoutSeconds),
	})
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	steps = append(steps, componentSteps...)
	steps, componentRecovery, err := service.plans.PrepareBlueprintComponentRecovery(
		ctx, child, artifact, teardown.Artifacts, teardown.Procedure, steps,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	plan, err := taskplan.Build(taskplan.BuildInput{VolumeRoot: service.volumeRoot, PlanID: child.PlanID, RenderGeneration: uint64(child.RenderGeneration),
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, TargetID: child.Target, Artifacts: teardown.Artifacts,
		Steps: steps, ManagedComponentProcedure: teardown.Procedure, ComponentRecoveryProcedure: componentRecovery})
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	child.PlanHash = hex.EncodeToString(plan.GetPlanHash())
	child.ComponentActionStepIDs, err = executionplan.ComponentActionStepIDs(plan)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	child.Steps = make([]taskjournal.TaskStepRecord, len(plan.GetSteps()))
	for index, step := range plan.GetSteps() {
		child.Steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: step.GetStepId()}
	}
	child.Steps = taskjournal.CaptureStepDescriptions(child.Steps, plan.GetSteps())
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(plan)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	publication, err := service.repository.PrepareComponentTaskPublication(ctx, source.environment, componentplanning.TaskIdentity{
		ID: child.ID, Target: child.Target, Executor: child.Executor, Type: child.Type, CreatedAt: child.CreatedAt,
	}, zoneChanges, preparation)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	record := componentplanning.ChildExecution{TaskID: child.ID, ParentTaskID: parent.ID, EnvironmentID: parent.Owner.EnvironmentID,
		Component: candidateRecord, Plan: encoded, Configuration: taskconfiguration.CloneTaskConfiguration(child.Configuration),
		Materializations: taskmaterialization.Clone(child.Materializations)}
	for _, zone := range zoneVersions {
		record.Zones = append(record.Zones, zone.Record)
	}
	for _, prior := range predecessorZones {
		if !slices.ContainsFunc(record.Zones, func(zone zonerecord.Record) bool { return zone.Desired.ID == prior.Record.Desired.ID }) {
			record.Zones = append(record.Zones, prior.Record)
		}
	}
	slices.SortFunc(record.Zones, func(left, right zonerecord.Record) int {
		if left.Desired.ID < right.Desired.ID {
			return -1
		}
		if left.Desired.ID > right.Desired.ID {
			return 1
		}
		return 0
	})
	sealedPublication, err := publication.WithChildExecution(record, child.PlanID, child.PlanHash)
	if err != nil {
		publication.Clear()
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: child, ComponentPublication: sealedPublication}, nil
}

func authoredComponentSpec(identities []projectionrecord.OwnedIdentity, specs map[string]core.ComponentSpec, componentID string) (core.ComponentCapability, core.ComponentSpec, bool) {
	for _, identity := range identities {
		if identity.ID == componentID {
			spec, found := specs[identity.Name]
			return core.ComponentCapability(identity.Name), spec, found
		}
	}
	return "", core.ComponentSpec{}, false
}

func omittedComponentSelection(record componentrecord.Record) (core.ComponentCapability, core.ComponentSpec, error) {
	var capability core.ComponentCapability
	switch record.Desired.Kind {
	case core.ComponentKindIngressCaddy:
		capability = core.ComponentCapabilityHTTPRouter
	case core.ComponentKindEdgeCloudflare:
		capability = core.ComponentCapabilityEdgeTunnel
	default:
		return "", core.ComponentSpec{}, errs.New(errs.KindStateConflict, "omitted Component is not an Environment singleton")
	}
	return capability, core.ComponentSpec{Implementation: record.Desired.Kind, Enabled: false}, nil
}
