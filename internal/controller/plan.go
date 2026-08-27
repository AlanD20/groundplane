// plan.go: the Controller's ONE typed ExecutionPlan — architecture.md,
// "Compose superset and execution boundary": "The Controller creates one
// typed ExecutionPlan and sends an execution bundle containing its
// plan_id and plan_hash, render generation, one canonical Compose file
// per generated project, generated env/file materializations, typed
// steps, expected labels, and dependency ordering." Protobuf carries
// this over the live Agent channel (see proto/agent.proto's
// TaskAssignment.plan_id/plan_hash/render_generation); x-gp-execution is
// the same plan's file-local serialization for inspection/replay.
package controller

import (
	"context"
	"math"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

const (
	EnvironmentCreateVolumeDirectoryParam      = taskcontract.EnvironmentCreateVolumeDirectoryParam
	EnvironmentBlueprintArtifactParam          = taskcontract.EnvironmentBlueprintArtifactParam
	EnvironmentBlueprintIntroducedVolumesParam = "introduced_volume_ids"
	EnvironmentRemoveVolumeDirectoryParam      = taskcontract.EnvironmentRemoveVolumeDirectoryParam
)

// ExecutionPlan is what internal/controller/renderer.go ultimately
// produces for one operation — the render plan turned into something
// dispatchable. Both sides of the Agent channel carry plan_id and
// plan_hash; the Agent rejects a bundle whose two projections don't
// match (blueprint.md, "x-gp-execution").
type ExecutionPlan = agentpb.ExecutionPlan

// PlanBuildInput is the complete typed output of rendering and procedure
// sequencing. It deliberately contains no persisted rendered-artifact handle:
// a resolver rebuilds these values from retained desired-state inputs.
type PlanBuildInput struct {
	VolumeRoot       string
	PlanID           string
	RenderGeneration uint64
	Operation        agentpb.PlanOperation
	TargetID         string
	Artifacts        []*agentpb.ComposeArtifact
	Steps            []*agentpb.ExecutionStep
}

// BuildPlan owns schema selection, defensive copying, deterministic hashing,
// and closed-shape validation for every Controller-produced execution plan.
func BuildPlan(input PlanBuildInput) (*ExecutionPlan, error) {
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: 1, PlanId: input.PlanID, RenderGeneration: input.RenderGeneration,
		Operation: input.Operation, TargetId: input.TargetID,
		Artifacts: input.Artifacts, Steps: input.Steps,
	})
	if err != nil {
		return nil, err
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, input.VolumeRoot); err != nil {
		return nil, err
	}
	return plan, nil
}

// TaskPlanResolver rebuilds ephemeral plans exclusively from closed durable
// Task inputs and daemon-owned path policy. Rendered plans are never stored.
type TaskPlanResolver struct {
	volumeRoot       string
	blueprints       blueprintPlanStateReader
	attaches         attachPlanRecordReader
	services         attachPlanServiceReader
	attachIdentities attachPlanIdentityResolver
	componentCatalog []components.Registration
	releases         *etcd.ReleaseLedger
	backupRuns       backupRunPlanReader
}

type blueprintPlanStateReader interface {
	GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(
		context.Context,
		string,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
}

func NewTaskPlanResolver(volumeRoot string) (*TaskPlanResolver, error) {
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return nil, err
	}
	return &TaskPlanResolver{volumeRoot: volumeRoot, componentCatalog: components.All()}, nil
}

func NewTaskPlanResolverWithBlueprints(
	volumeRoot string,
	blueprints blueprintPlanStateReader,
) (*TaskPlanResolver, error) {
	resolver, err := NewTaskPlanResolver(volumeRoot)
	if err != nil {
		return nil, err
	}
	if blueprints == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint plan state reader is required")
	}
	resolver.blueprints = blueprints
	return resolver, nil
}

func (resolver *TaskPlanResolver) ResolveExecutionPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if task.Type == etcd.TaskDeploy || task.Type == etcd.TaskRollback {
		return resolver.resolveReleasePlan(ctx, task)
	}
	if task.Type == etcd.TaskStart || task.Type == etcd.TaskStop || task.Type == etcd.TaskDestroy {
		return resolver.resolveServiceLifecyclePlan(ctx, task)
	}
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "execution plan resolution context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resolver == nil || resolver.volumeRoot == "" {
		return nil, errs.New(errs.KindInternal, "execution plan resolver is not configured")
	}
	if task.Type == etcd.TaskBackup {
		return resolver.resolveBackupRunPlan(ctx, task)
	}
	if task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceVolume {
		return resolver.resolveVolumePlan(ctx, task)
	}
	if task.Type == etcd.TaskAttach || task.Type == etcd.TaskDetach {
		return resolver.resolveAttachPlan(ctx, task)
	}
	if task.Type == etcd.TaskUpdate {
		return resolver.resolveEnvironmentBlueprintPlan(ctx, task)
	}
	if task.Type == etcd.TaskRemove {
		if ids.Validate(ids.KindEnvironment, task.Target) == nil {
			return resolver.resolveEnvironmentRemovalPlan(ctx, task)
		}
		if ids.Validate(ids.KindNetwork, task.Target) == nil {
			return resolver.resolveZoneRemovalPlan(task)
		}
		if ids.Validate(ids.KindRoute, task.Target) == nil {
			return resolver.resolveRouteRemovalPlan(ctx, task)
		}
		if ids.Validate(ids.KindEnvEntry, task.Target) == nil {
			return resolver.resolveEntryRemovalPlan(ctx, task)
		}
		return nil, errs.New(errs.KindInternal, "durable removal Task target is invalid")
	}
	if task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskCreate ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || len(task.Params) != 1 ||
		len(task.Steps) != 1 || task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 {
		return nil, errs.New(errs.KindInternal, "durable Environment creation Task shape is invalid")
	}
	volumeDirectory, exists := task.Params[EnvironmentCreateVolumeDirectoryParam]
	if !exists || ids.Validate(ids.KindStep, task.Steps[0].ID) != nil {
		return nil, errs.New(errs.KindInternal, "durable Environment creation Task procedure is invalid")
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE,
		TargetID:         task.Target,
		Steps: []*agentpb.ExecutionStep{{
			StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId: task.Target, ExpectedVolumeDir: volumeDirectory,
				},
			},
		}},
	})
}

func (resolver *TaskPlanResolver) resolveZoneRemovalPlan(
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	environmentID := task.Params[etcd.TaskZoneEnvironmentParam]
	if task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskRemove ||
		ids.Validate(ids.KindNetwork, task.Target) != nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || len(task.Params) != 1 ||
		len(task.Materializations) != 0 || len(task.Steps) != 1 ||
		ids.Validate(ids.KindStep, task.Steps[0].ID) != nil || task.TimeoutSeconds <= 0 ||
		task.TimeoutSeconds > math.MaxUint32 {
		return nil, errs.New(errs.KindInternal, "durable Zone removal Task shape is invalid")
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE,
		TargetID:         task.Target,
		Steps: []*agentpb.ExecutionStep{{
			StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedNetworkRemove{
				ManagedNetworkRemove: &agentpb.ManagedNetworkRemove{
					NetworkId: task.Target, EnvironmentId: environmentID,
					DockerName: "gp_net_" + strings.ToLower(task.Target),
				},
			},
		}},
	})
}

func (resolver *TaskPlanResolver) resolveEnvironmentBlueprintPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	introducedVolumeIDs, volumeIntentDigest, err := blueprintIntroducedVolumeProcedure(task.Params)
	if err != nil {
		return nil, err
	}
	expectedParams := 3
	if len(introducedVolumeIDs) != 0 {
		expectedParams = 5
	}
	if resolver.blueprints == nil || task.Executor != etcd.TaskExecutorAgent ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || len(task.Params) != expectedParams ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Task shape is invalid")
	}
	revisionID := task.Params[etcd.EnvironmentDesiredRevisionParam]
	artifactID := task.Params[EnvironmentBlueprintArtifactParam]
	if task.Params[etcd.TaskMaterializationEnvironmentParam] != task.Target ||
		ids.Validate(ids.KindTask, revisionID) != nil || ids.Validate(ids.KindConfig, artifactID) != nil {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Task parameters are invalid")
	}
	artifact, err := resolver.renderPinnedEnvironmentBlueprintArtifact(ctx, task, revisionID, artifactID)
	if err != nil {
		return nil, err
	}
	expectedSteps := len(task.Materializations) + 1
	if len(introducedVolumeIDs) != 0 {
		expectedSteps++
	}
	if len(task.Steps) != expectedSteps {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Task step count is invalid")
	}
	steps := make([]*agentpb.ExecutionStep, 0, expectedSteps)
	stepIndex := 0
	materializations := make(map[string]etcd.TaskMaterializationRecord, len(task.Materializations))
	for _, reference := range task.Materializations {
		materializations[reference.StepID] = reference
	}
	for ; stepIndex < len(task.Materializations); stepIndex++ {
		reference, exists := materializations[task.Steps[stepIndex].ID]
		if !exists {
			return nil, errs.New(errs.KindInternal, "durable Blueprint materialization order is invalid")
		}
		step, stepErr := BuildTaskMaterializationStep(reference, artifactID, uint32(task.TimeoutSeconds))
		if stepErr != nil {
			return nil, stepErr
		}
		steps = append(steps, step)
		delete(materializations, reference.StepID)
	}
	if len(materializations) != 0 {
		return nil, errs.New(errs.KindInternal, "durable Blueprint materialization steps are incomplete")
	}
	if len(introducedVolumeIDs) != 0 {
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
				ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
					ArtifactId: artifactID, VolumeIds: introducedVolumeIDs, IntentSha256: volumeIntentDigest,
				},
			},
		})
		stepIndex++
	}
	steps = append(steps, &agentpb.ExecutionStep{
		StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifactID, FullReconcile: true,
		}},
	})
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
	})
}

func blueprintIntroducedVolumeProcedure(params map[string]string) ([]string, []byte, error) {
	encodedIDs, hasIDs := params[EnvironmentBlueprintIntroducedVolumesParam]
	encodedDigest, hasDigest := params[VolumeTaskIntentSHA256Param]
	if !hasIDs && !hasDigest {
		return nil, nil, nil
	}
	if !hasIDs || !hasDigest || encodedIDs == "" {
		return nil, nil, errs.New(errs.KindInternal, "durable Blueprint Volume procedure is incomplete")
	}
	volumeIDs := strings.Split(encodedIDs, ",")
	previous := ""
	for _, volumeID := range volumeIDs {
		if ids.Validate(ids.KindVolume, volumeID) != nil || volumeID <= previous {
			return nil, nil, errs.New(errs.KindInternal, "durable Blueprint introduced Volume ids are invalid")
		}
		previous = volumeID
	}
	digest, err := decodeVolumeTaskDigest(encodedDigest)
	if err != nil {
		return nil, nil, err
	}
	return volumeIDs, digest, nil
}

func (resolver *TaskPlanResolver) resolveEnvironmentRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver.blueprints == nil || task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskRemove ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || task.TimeoutSeconds <= 0 ||
		task.TimeoutSeconds > math.MaxUint32 || len(task.Materializations) != 0 ||
		(len(task.Params) != 1 && len(task.Params) != 4) {
		return nil, errs.New(errs.KindInternal, "durable Environment removal Task shape is invalid")
	}
	volumeDirectory, exists := task.Params[EnvironmentRemoveVolumeDirectoryParam]
	if !exists {
		return nil, errs.New(errs.KindInternal, "durable Environment removal directory is missing")
	}
	environment, err := resolver.blueprints.GetEnvironment(ctx, task.Target)
	if err != nil {
		return nil, err
	}
	if environment.Record.VolumeDir != volumeDirectory {
		return nil, errs.New(errs.KindInternal, "durable Environment removal directory changed")
	}
	directoryStep := func(stepID string) *agentpb.ExecutionStep {
		return &agentpb.ExecutionStep{
			StepId: stepID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryRemove{
				EnvironmentDirectoryRemove: &agentpb.EnvironmentDirectoryRemove{
					EnvironmentId: task.Target, ExpectedVolumeDir: volumeDirectory,
				},
			},
		}
	}
	if len(task.Params) == 1 {
		if len(task.Steps) != 1 || ids.Validate(ids.KindStep, task.Steps[0].ID) != nil {
			return nil, errs.New(errs.KindInternal, "artifact-free Environment removal procedure is invalid")
		}
		_, found, err := resolver.blueprints.GetEnvironmentComposeProjection(ctx, task.Target)
		if err != nil {
			return nil, err
		}
		if found {
			return nil, errs.New(errs.KindInternal, "artifact-free Environment removal lost its pinned desired state")
		}
		return BuildPlan(PlanBuildInput{
			VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
			RenderGeneration: uint64(task.RenderGeneration),
			Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: task.Target,
			Steps: []*agentpb.ExecutionStep{directoryStep(task.Steps[0].ID)},
		})
	}
	if len(task.Steps) != 2 || ids.Validate(ids.KindStep, task.Steps[0].ID) != nil ||
		ids.Validate(ids.KindStep, task.Steps[1].ID) != nil {
		return nil, errs.New(errs.KindInternal, "Environment removal procedure is invalid")
	}
	revisionID := task.Params[etcd.EnvironmentDesiredRevisionParam]
	artifactID := task.Params[EnvironmentBlueprintArtifactParam]
	if task.Params[etcd.TaskMaterializationEnvironmentParam] != task.Target ||
		ids.Validate(ids.KindTask, revisionID) != nil || ids.Validate(ids.KindConfig, artifactID) != nil {
		return nil, errs.New(errs.KindInternal, "Environment removal Blueprint parameters are invalid")
	}
	artifact, err := resolver.renderPinnedEnvironmentBlueprintArtifact(ctx, task, revisionID, artifactID)
	if err != nil {
		return nil, err
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{
			{
				StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
					ArtifactId: artifactID, WholeProject: true,
				}},
			},
			directoryStep(task.Steps[1].ID),
		},
	})
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentBlueprintArtifact(
	ctx context.Context,
	task etcd.TaskRecord,
	revisionID string,
	artifactID string,
) (*agentpb.ComposeArtifact, error) {
	environment, err := resolver.blueprints.GetEnvironment(ctx, task.Target)
	if err != nil {
		return nil, err
	}
	if environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady {
		return nil, errs.New(errs.KindInternal, "Blueprint Task Environment is not ready")
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return nil, err
	}
	if project.Record.Kind != etcd.ProjectKindTenant {
		return nil, errs.New(errs.KindInternal, "Blueprint Task Project is not tenant-owned")
	}
	projection, found, err := resolver.blueprints.GetEnvironmentComposeProjectionRevision(
		ctx,
		task.Target,
		revisionID,
	)
	if err != nil {
		return nil, err
	}
	if !found || projection.Record.RevisionID != revisionID ||
		projection.Record.RenderGeneration != uint64(task.RenderGeneration) {
		return nil, errs.New(errs.KindInternal, "Blueprint Task Compose projection is stale")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		projection.Record.ComposeArtifact,
		artifact,
	); err != nil || artifact.GetArtifactId() != artifactID || artifact.GetOwnerId() != task.Target {
		return nil, errs.New(errs.KindInternal, "Blueprint Task normalized Compose artifact is corrupt")
	}
	return proto.Clone(artifact).(*agentpb.ComposeArtifact), nil
}

type pinnedEnvironmentIdentity struct {
	TenantID            string
	TenantSlug          string
	ProjectID           string
	ProjectSlug         string
	EnvironmentID       string
	EnvironmentName     string
	AuthorizedVolumeDir string
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentArtifact(
	ctx context.Context,
	task etcd.TaskRecord,
	identity pinnedEnvironmentIdentity,
	revisionID string,
	artifactID string,
	projection etcd.EnvironmentComposeProjection,
	transform environmentComposeTransform,
) (*agentpb.ComposeArtifact, error) {
	return resolver.renderPinnedEnvironmentArtifactForPhase(
		ctx,
		task,
		identity,
		revisionID,
		artifactID,
		projection,
		"",
		transform,
	)
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentArtifactForPhase(
	ctx context.Context,
	task etcd.TaskRecord,
	identity pinnedEnvironmentIdentity,
	revisionID string,
	artifactID string,
	projection etcd.EnvironmentComposeProjection,
	phase core.ServiceLifecyclePhase,
	transform environmentComposeTransform,
) (*agentpb.ComposeArtifact, error) {
	return resolver.renderPinnedEnvironmentArtifactForPhaseWithReleases(
		ctx, task, identity, revisionID, artifactID, projection, phase, transform, nil,
	)
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentArtifactWithReleases(
	ctx context.Context,
	task etcd.TaskRecord,
	identity pinnedEnvironmentIdentity,
	revisionID string,
	artifactID string,
	projection etcd.EnvironmentComposeProjection,
	transform environmentComposeTransform,
	releases map[string]ComposeReleaseIdentity,
) (*agentpb.ComposeArtifact, error) {
	return resolver.renderPinnedEnvironmentArtifactForPhaseWithReleases(
		ctx, task, identity, revisionID, artifactID, projection, "", transform, releases,
	)
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentArtifactForPhaseWithReleases(
	ctx context.Context,
	task etcd.TaskRecord,
	identity pinnedEnvironmentIdentity,
	revisionID string,
	artifactID string,
	projection etcd.EnvironmentComposeProjection,
	phase core.ServiceLifecyclePhase,
	transform environmentComposeTransform,
	releases map[string]ComposeReleaseIdentity,
) (*agentpb.ComposeArtifact, error) {
	parsed, err := resolver.parsePinnedEnvironmentBlueprint(ctx, identity, revisionID)
	if err != nil {
		return nil, err
	}
	if len(parsed.Extensions.Requires) != 0 || len(parsed.Extensions.Attachments) != 0 ||
		len(parsed.Extensions.Entries) != 0 || parsed.Extensions.Backup != nil ||
		(len(parsed.Extensions.ReleaseGroups) != 0 && releases == nil) || len(parsed.Project.Configs) != 0 ||
		len(parsed.Project.Secrets) != 0 {
		return nil, errs.New(errs.KindNotImplemented, "Blueprint materialized resources are not yet executable")
	}
	if phase != "" {
		if err := applyServiceDependencyPhase(parsed.Project, parsed.ServiceExtensions, phase); err != nil {
			return nil, err
		}
	}
	externalNetworks := []ComposeResourceIdentity(nil)
	if transform != nil {
		externalNetworks, err = transform(parsed.Project, projection)
		if err != nil {
			return nil, err
		}
	}
	renderProject := parsed.Project
	if len(projection.Components) != 0 {
		componentProjection, componentErr := projectPinnedEnvironmentComponents(
			parsed.Project,
			parsed.ServiceExtensions,
			identity,
			projection,
			parsed.Extensions.Routes,
			parsed.Extensions.Components,
			projectedEnvironmentEntries(projection.Entries),
			resolver.componentCatalog,
		)
		if componentErr != nil {
			return nil, componentErr
		}
		renderProject = componentProjection.Project
	}
	return RenderCompose(ComposeRenderInput{
		Project: renderProject, ArtifactID: artifactID,
		TenantID: identity.TenantID, ProjectID: identity.ProjectID, EnvironmentID: identity.EnvironmentID,
		PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		AuthorizedVolumeDir: identity.AuthorizedVolumeDir,
		Identities:          composeIdentitySnapshotFromProjection(projection),
		ExternalNetworks:    externalNetworks,
		Releases:            releases,
	})
}

func applyServiceDependencyPhase(
	project *types.Project,
	extensions map[string]core.ServiceExtensionSpec,
	phase core.ServiceLifecyclePhase,
) error {
	if project == nil {
		return errs.New(errs.KindInternal, "Service dependency planning requires a parsed Compose project")
	}
	names := make([]string, 0, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		names = append(names, name)
	}
	for name := range project.DisabledServices {
		names = append(names, name)
	}
	sort.Strings(names)
	plan, err := core.BuildServiceDependencyPhasePlan(names, extensions, phase)
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	for _, edge := range plan.Edges {
		service, enabled := project.Services[edge.Service]
		if !enabled {
			service = project.DisabledServices[edge.Service]
		}
		if service.DependsOn == nil {
			service.DependsOn = make(map[string]types.ServiceDependency)
		}
		dependency := types.ServiceDependency{Condition: edge.Condition.String(), Required: true}
		if current, exists := service.DependsOn[edge.Dependency]; exists {
			if current.Condition != dependency.Condition || current.Required != dependency.Required {
				return errs.New(errs.KindValidationFailed, "Service dependency phase conflicts with native Compose")
			}
		} else {
			service.DependsOn[edge.Dependency] = dependency
		}
		if enabled {
			project.Services[edge.Service] = service
		} else {
			project.DisabledServices[edge.Service] = service
		}
	}
	return nil
}

func (resolver *TaskPlanResolver) parsePinnedEnvironmentBlueprint(
	ctx context.Context,
	identity pinnedEnvironmentIdentity,
	revisionID string,
) (blueprintparser.Result, error) {
	revision, found, err := resolver.blueprints.GetEnvironmentBlueprintRevision(
		ctx, identity.EnvironmentID, revisionID,
	)
	if err != nil {
		return blueprintparser.Result{}, err
	}
	if !found {
		return blueprintparser.Result{}, errs.New(errs.KindInternal, "Blueprint Task immutable revision is missing")
	}
	bundle := core.BlueprintBundle{
		RootPath:       revision.Record.RootPath,
		ComposeSources: append([]string(nil), revision.Record.ComposeSources...),
		Interpolation:  cloneBlueprintInterpolation(revision.Record.Interpolation),
		Files:          make([]core.BlueprintFile, len(revision.Record.Files)),
	}
	for index, file := range revision.Record.Files {
		bundle.Files[index] = core.BlueprintFile{Path: file.Path, Content: append([]byte(nil), file.Content...)}
		defer clear(bundle.Files[index].Content)
	}
	return blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{
		EnvironmentID: identity.EnvironmentID,
		Tenant:        identity.TenantSlug, Project: identity.ProjectSlug, Environment: identity.EnvironmentName,
	}, bundle)
}

func projectedEnvironmentEntries(records []etcd.EntryRecord) []core.EnvEntry {
	entries := make([]core.EnvEntry, len(records))
	for index, record := range records {
		entries[index] = record.Entry
	}
	return entries
}

func composeIdentitySnapshotFromProjection(
	projection etcd.EnvironmentComposeProjection,
) ComposeIdentitySnapshot {
	convert := func(values []etcd.EnvironmentComposeIdentity) []ComposeResourceIdentity {
		result := make([]ComposeResourceIdentity, len(values))
		for index, value := range values {
			result[index] = ComposeResourceIdentity{ID: value.ID, Name: value.Name}
		}
		return result
	}
	return ComposeIdentitySnapshot{
		Services: convert(projection.Services),
		Networks: convert(projection.Networks),
		Volumes:  composeVolumeResourceIdentities(projection.Volumes),
	}
}
