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
	"github.com/AlanD20/groundplane/internal/common/hierarchyplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
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
	EnvironmentCreateVolumeDirectoryParam   = taskcontract.EnvironmentCreateVolumeDirectoryParam
	EnvironmentBlueprintArtifactParam       = taskcontract.EnvironmentBlueprintArtifactParam
	EnvironmentBlueprintManagedVolumesParam = "managed_volume_ids"
	EnvironmentRemoveVolumeDirectoryParam   = taskcontract.EnvironmentRemoveVolumeDirectoryParam
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
	componentCatalog []EnvironmentComponentRegistration
	releases         *etcd.ReleaseLedger
	backupRuns       backupRunPlanReader
	componentPlans   ComponentTaskPlanResolver
	scriptPlans      ScriptExecutionPlanReader
}

type ComponentTaskPlanResolver interface {
	ResolveComponentExecutionPlan(context.Context, etcd.TaskRecord) (*agentpb.ExecutionPlan, error)
}

func (resolver *TaskPlanResolver) EnableComponentPlans(componentPlans ComponentTaskPlanResolver) error {
	if resolver == nil || componentPlans == nil {
		return errs.New(errs.KindInternal, "Component Task plan resolver is required")
	}
	resolver.componentPlans = componentPlans
	return nil
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

func NewTaskPlanResolver(
	volumeRoot string,
	componentCatalog []EnvironmentComponentRegistration,
) (*TaskPlanResolver, error) {
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return nil, err
	}
	if err := ValidateEnvironmentComponentCatalog(componentCatalog); err != nil {
		return nil, err
	}
	return &TaskPlanResolver{
		volumeRoot:       volumeRoot,
		componentCatalog: CloneEnvironmentComponentCatalog(componentCatalog),
	}, nil
}

func NewTaskPlanResolverWithBlueprints(
	volumeRoot string,
	blueprints blueprintPlanStateReader,
	componentCatalog []EnvironmentComponentRegistration,
) (*TaskPlanResolver, error) {
	resolver, err := NewTaskPlanResolver(volumeRoot, componentCatalog)
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
	if task.Type == etcd.TaskScript {
		if resolver.scriptPlans == nil {
			return nil, errs.New(errs.KindInternal, "Script Task plan resolver is not configured")
		}
		return resolver.scriptPlans.GetScriptExecutionPlan(ctx, task)
	}
	if task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceComponent {
		if resolver.componentPlans == nil {
			return nil, errs.New(errs.KindInternal, "Component Task plan resolver is not configured")
		}
		return resolver.componentPlans.ResolveComponentExecutionPlan(ctx, task)
	}
	if task.Type == etcd.TaskBackup {
		return resolver.resolveBackupRunPlan(ctx, task)
	}
	if task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceHierarchyDeletion {
		return resolver.resolveHierarchyDeletionPlan(ctx, task)
	}
	if task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceVolume {
		return resolver.resolveVolumePlan(ctx, task)
	}
	if task.Type == etcd.TaskAttach || task.Type == etcd.TaskDetach {
		return resolver.resolveAttachPlan(ctx, task)
	}
	if task.Type == etcd.TaskCreate {
		if _, backingCreation := task.Params[etcd.TaskBackingServiceHealthParam]; backingCreation {
			return resolver.resolveEnvironmentBlueprintPlan(ctx, task)
		}
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
		if ids.Validate(ids.KindService, task.Target) == nil {
			return resolver.resolveServiceRemovalPlan(ctx, task)
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

func (resolver *TaskPlanResolver) resolveHierarchyDeletionPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver.blueprints == nil || task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskRemove ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || task.RenderGeneration != 1 ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 || len(task.Params) != 9 ||
		task.Params[etcd.TaskHierarchyDeletionProcedureParam] != "environment.cleanup" ||
		(len(task.Steps) != 1 && len(task.Steps) != 2) {
		return nil, errs.New(errs.KindInternal, "durable hierarchy Environment cleanup Task is invalid")
	}
	for _, step := range task.Steps {
		if ids.Validate(ids.KindStep, step.ID) != nil {
			return nil, errs.New(errs.KindInternal, "durable hierarchy Environment cleanup Task is invalid")
		}
	}
	environment, err := resolver.blueprints.GetEnvironment(ctx, task.Target)
	if err != nil {
		return nil, err
	}
	projection, found, err := resolver.blueprints.GetEnvironmentComposeProjection(ctx, task.Target)
	if err != nil {
		return nil, err
	}
	composeStepID := ""
	directoryStepID := task.Steps[0].ID
	var composeArtifact []byte
	if found {
		if len(task.Steps) != 2 {
			return nil, errs.New(errs.KindInternal, "durable hierarchy Environment cleanup lost its Compose step")
		}
		composeStepID = task.Steps[0].ID
		directoryStepID = task.Steps[1].ID
		composeArtifact = projection.Record.ComposeArtifact
	} else if len(task.Steps) != 1 {
		return nil, errs.New(errs.KindInternal, "artifact-free hierarchy Environment cleanup has a Compose step")
	}
	plan, err := hierarchyplan.EnvironmentCleanup(
		task.PlanID,
		uint64(task.RenderGeneration),
		task.Target,
		composeStepID,
		directoryStepID,
		uint32(task.TimeoutSeconds),
		environment.Record.VolumeDir,
		composeArtifact,
	)
	if err != nil {
		return nil, err
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, resolver.volumeRoot); err != nil {
		return nil, err
	}
	return plan, nil
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
	managedVolumeIDs, volumeIntentDigest, err := blueprintManagedVolumeProcedure(task.Params)
	if err != nil {
		return nil, err
	}
	expectedParams := 3
	if len(managedVolumeIDs) != 0 {
		expectedParams = 5
	}
	healthServiceID, hasHealthStep := task.Params[etcd.TaskBackingServiceHealthParam]
	backingVolumeDirectory := task.Params[etcd.TaskBackingServiceVolumeDirectoryParam]
	if hasHealthStep {
		expectedParams += 2
	}
	if resolver.blueprints == nil || task.Executor != etcd.TaskExecutorAgent ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || len(task.Params) != expectedParams ||
		hasHealthStep && (ids.Validate(ids.KindService, healthServiceID) != nil || backingVolumeDirectory == "") ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Task shape is invalid")
	}
	revisionID := task.Params[etcd.EnvironmentDesiredRevisionParam]
	artifactID := task.Params[EnvironmentBlueprintArtifactParam]
	if task.Params[etcd.TaskMaterializationEnvironmentParam] != task.Target ||
		ids.Validate(ids.KindTask, revisionID) != nil || ids.Validate(ids.KindConfig, artifactID) != nil {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Task parameters are invalid")
	}
	pinned, err := resolver.renderPinnedEnvironmentBlueprintArtifact(ctx, task, revisionID, artifactID)
	if err != nil {
		return nil, err
	}
	caddyApply, hasCaddyApply, err := ResolveEnvironmentCaddyApply(EnvironmentCaddyApplyInput{
		RevisionID: revisionID, RenderGeneration: uint64(task.RenderGeneration), Components: pinned.components,
		ComponentCatalog: resolver.componentCatalog,
		Materializations: task.Materializations, Artifact: pinned.artifact,
	})
	if err != nil {
		return nil, err
	}
	attachCandidates, attachStepCount, err := resolver.blueprintAttachPlanCandidates(ctx, task)
	if err != nil {
		return nil, err
	}
	expectedSteps := len(task.Materializations) + 1
	if len(managedVolumeIDs) != 0 {
		expectedSteps++
	}
	if hasHealthStep {
		expectedSteps += 2
	}
	if hasCaddyApply {
		expectedSteps++
	}
	expectedSteps += attachStepCount
	if len(task.Steps) != expectedSteps {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Task step count is invalid")
	}
	steps := make([]*agentpb.ExecutionStep, 0, expectedSteps)
	stepIndex := 0
	if hasHealthStep {
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId: task.Target, ExpectedVolumeDir: backingVolumeDirectory,
				},
			},
		})
		stepIndex++
	}
	appendVolumeStep := func() {
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
				ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
					ArtifactId: artifactID, VolumeIds: managedVolumeIDs, IntentSha256: volumeIntentDigest,
				},
			},
		})
		stepIndex++
	}
	volumePrecedesMaterialization := hasHealthStep && len(managedVolumeIDs) != 0
	if volumePrecedesMaterialization {
		appendVolumeStep()
	}
	materializations := make(map[string]etcd.TaskMaterializationRecord, len(task.Materializations))
	for _, reference := range task.Materializations {
		materializations[reference.StepID] = reference
	}
	materializationEnd := stepIndex + len(task.Materializations)
	for ; stepIndex < materializationEnd; stepIndex++ {
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
	if len(managedVolumeIDs) != 0 && !volumePrecedesMaterialization {
		appendVolumeStep()
	}
	attachSteps, nextStepIndex, err := resolver.blueprintAttachProcedureSteps(
		ctx, task, attachCandidates, stepIndex,
	)
	if err != nil {
		return nil, err
	}
	defer clearAdapterProcedurePasswords(attachSteps)
	steps = append(steps, attachSteps...)
	stepIndex = nextStepIndex
	steps = append(steps, &agentpb.ExecutionStep{
		StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifactID, FullReconcile: true,
		}},
	})
	stepIndex++
	if hasCaddyApply {
		caddyStep, stepErr := caddyApply.ExecutionStep(
			task.Steps[stepIndex].ID,
			uint32(task.TimeoutSeconds),
		)
		if stepErr != nil {
			return nil, stepErr
		}
		steps = append(steps, caddyStep)
		stepIndex++
	}
	if hasHealthStep {
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
				ArtifactId: artifactID, ServiceIds: []string{healthServiceID},
			}},
		})
		stepIndex++
	}
	if stepIndex != len(task.Steps) {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Task step order is invalid")
	}
	operation := agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	if hasHealthStep {
		operation = agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        operation, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{pinned.artifact}, Steps: steps,
	})
}

func blueprintManagedVolumeProcedure(params map[string]string) ([]string, []byte, error) {
	encodedIDs, hasIDs := params[EnvironmentBlueprintManagedVolumesParam]
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
			return nil, nil, errs.New(errs.KindInternal, "durable Blueprint managed Volume ids are invalid")
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
	pinned, err := resolver.renderPinnedEnvironmentBlueprintArtifact(ctx, task, revisionID, artifactID)
	if err != nil {
		return nil, err
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{pinned.artifact},
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

type pinnedEnvironmentBlueprintArtifact struct {
	artifact   *agentpb.ComposeArtifact
	components []etcd.ComponentRecord
}

func (resolver *TaskPlanResolver) renderPinnedEnvironmentBlueprintArtifact(
	ctx context.Context,
	task etcd.TaskRecord,
	revisionID string,
	artifactID string,
) (pinnedEnvironmentBlueprintArtifact, error) {
	environment, err := resolver.blueprints.GetEnvironment(ctx, task.Target)
	if err != nil {
		return pinnedEnvironmentBlueprintArtifact{}, err
	}
	if environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady {
		return pinnedEnvironmentBlueprintArtifact{}, errs.New(
			errs.KindInternal,
			"Blueprint Task Environment is not ready",
		)
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return pinnedEnvironmentBlueprintArtifact{}, err
	}
	if project.Record.Kind != etcd.ProjectKindTenant && project.Record.Kind != etcd.ProjectKindBacking {
		return pinnedEnvironmentBlueprintArtifact{}, errs.New(
			errs.KindInternal,
			"Blueprint Task Project kind is invalid",
		)
	}
	projection, found, err := resolver.blueprints.GetEnvironmentComposeProjectionRevision(
		ctx,
		task.Target,
		revisionID,
	)
	if err != nil {
		return pinnedEnvironmentBlueprintArtifact{}, err
	}
	if !found || projection.Record.RevisionID != revisionID ||
		projection.Record.RenderGeneration != uint64(task.RenderGeneration) {
		return pinnedEnvironmentBlueprintArtifact{}, errs.New(
			errs.KindInternal,
			"Blueprint Task Compose projection is stale",
		)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		projection.Record.ComposeArtifact,
		artifact,
	); err != nil || artifact.GetArtifactId() != artifactID || artifact.GetOwnerId() != task.Target {
		return pinnedEnvironmentBlueprintArtifact{}, errs.New(
			errs.KindInternal,
			"Blueprint Task normalized Compose artifact is corrupt",
		)
	}
	return pinnedEnvironmentBlueprintArtifact{
		artifact:   proto.Clone(artifact).(*agentpb.ComposeArtifact),
		components: projection.Record.Components,
	}, nil
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
	if projection.RevisionID != revisionID || projection.EnvironmentID != identity.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "pinned Environment normalized projection changed")
	}
	project, err := loadNormalizedEnvironmentProject(ctx, projection)
	if err != nil {
		return nil, err
	}
	if err := applyProjectedServiceDependencyPhase(project, projection.ServiceDependencyPlans, phase); err != nil {
		return nil, err
	}
	externalNetworks := []ComposeResourceIdentity(nil)
	if transform != nil {
		externalNetworks, err = transform(project, projection)
		if err != nil {
			return nil, err
		}
	} else {
		externalNetworks, err = managedAttachExternalNetworks(project)
		if err != nil {
			return nil, err
		}
	}
	return RenderCompose(ComposeRenderInput{
		Project: project, ArtifactID: artifactID,
		ProjectOwnerKind: ComposeProjectOwnerTenant,
		TenantID:         identity.TenantID, ProjectID: identity.ProjectID, EnvironmentID: identity.EnvironmentID,
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
