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
	VolumeRoot                   string
	PlanID                       string
	RenderGeneration             uint64
	Operation                    agentpb.PlanOperation
	TargetID                     string
	Artifacts                    []*agentpb.ComposeArtifact
	ScriptRunnerSnapshots        []*agentpb.ResolvedRunnerSnapshot
	ScriptRunnerProjections      []*agentpb.ScriptRunnerProjection
	ScriptBodyArtifacts          []*agentpb.ScriptBodyArtifactMetadata
	Steps                        []*agentpb.ExecutionStep
	ComponentLifecycleMode       agentpb.ComponentLifecycleMode
	ComponentRollbackObservation *agentpb.ComponentApply
	CandidateReleaseProcedure    *agentpb.CandidateReleaseProcedure
	ManagedComponentProcedure    *agentpb.ManagedComponentProcedure
	ServiceLifecycleProcedure    *agentpb.ServiceLifecycleProcedure
	EntryMutationProcedure       *agentpb.EntryMutationProcedure
}

// BuildPlan owns schema selection, defensive copying, deterministic hashing,
// and closed-shape validation for every Controller-produced execution plan.
func BuildPlan(input PlanBuildInput) (*ExecutionPlan, error) {
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: 1, PlanId: input.PlanID, RenderGeneration: input.RenderGeneration,
		Operation:                    input.Operation,
		TargetId:                     input.TargetID,
		Artifacts:                    input.Artifacts,
		ScriptRunnerSnapshots:        input.ScriptRunnerSnapshots,
		ScriptRunnerProjections:      input.ScriptRunnerProjections,
		ScriptBodyArtifacts:          input.ScriptBodyArtifacts,
		Steps:                        input.Steps,
		ComponentLifecycleMode:       input.ComponentLifecycleMode,
		ComponentRollbackObservation: input.ComponentRollbackObservation,
		CandidateReleaseProcedure:    input.CandidateReleaseProcedure,
		ManagedComponentProcedure:    input.ManagedComponentProcedure,
		ServiceLifecycleProcedure:    input.ServiceLifecycleProcedure,
		EntryMutationProcedure:       input.EntryMutationProcedure,
	})
	if err != nil {
		return nil, err
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, input.VolumeRoot); err != nil {
		return nil, err
	}
	return plan, nil
}

// TaskPlanResolver rebuilds plans from closed durable Task inputs and daemon-owned policy; plans are never stored.
type TaskPlanResolver struct {
	volumeRoot         string
	blueprints         blueprintPlanStateReader
	attaches           attachPlanRecordReader
	services           attachPlanServiceReader
	attachIdentities   attachPlanIdentityResolver
	componentCatalog   []EnvironmentComponentRegistration
	serviceProxyImage  *etcd.ReleaseProxyImage
	routeState         routeProviderStateReader
	releases           *etcd.ReleaseLedger
	backupRuns         backupRunPlanReader
	componentPlans     ComponentTaskPlanResolver
	scriptPlans        ScriptExecutionPlanReader
	volumeRemovalPlans volumeRemovalPlanReader
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

func (resolver *TaskPlanResolver) resolveExecutionPlan(
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
		return resolver.resolveUpdatePlan(ctx, task)
	}
	if task.Type == etcd.TaskCreate && ids.Validate(ids.KindRoute, task.Target) == nil {
		return resolver.resolveRouteMutationPlan(ctx, task)
	}
	if task.Type == etcd.TaskRemove {
		if ids.Validate(ids.KindEnvironment, task.Target) == nil {
			return resolver.resolveEnvironmentRemovalPlan(ctx, task)
		}
		if ids.Validate(ids.KindNetwork, task.Target) == nil {
			return resolver.resolveZoneRemovalPlan(ctx, task)
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

func (resolver *TaskPlanResolver) resolveEnvironmentBlueprintPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	procedure, validProcedure := taskcontract.ParseBlueprintComposeProcedure(
		task.Params[taskcontract.EnvironmentBlueprintProcedureParam],
	)
	if !validProcedure {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Compose procedure is invalid")
	}
	var blueprintReleases etcd.ReleaseTaskRenderInput
	hasBlueprintReleases := procedure == taskcontract.BlueprintComposeProcedureCandidateReleases
	hasReleasePublication := task.Params[etcd.TaskReleasePublicationParam] != ""
	if hasBlueprintReleases != hasReleasePublication {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Release procedure is inconsistent")
	}
	if hasBlueprintReleases {
		if resolver.releases == nil {
			return nil, errs.New(errs.KindInternal, "Blueprint Release plan resolver is not configured")
		}
		if resolver.scriptPlans != nil {
			sealed, found, err := resolver.scriptPlans.GetReleaseScriptExecutionPlan(ctx, task)
			if err != nil || found {
				return sealed, err
			}
		}
		var err error
		blueprintReleases, err = resolver.releases.GetBlueprintTaskRenderInput(ctx, task)
		if err != nil {
			return nil, err
		}
	}
	releaseHookCount, releaseProcedureStepCount, err := blueprintReleaseProcedureCounts(
		blueprintReleases.Members,
		hasBlueprintReleases,
	)
	if err != nil {
		return nil, err
	}
	managedVolumeIDs, volumeIntentDigest, err := blueprintManagedVolumeProcedure(task.Params)
	if err != nil {
		return nil, err
	}
	expectedParams := 4
	resourceStepIDs, resourceParams, err := blueprintReleaseResourceStepIDs(task)
	if err != nil {
		return nil, err
	}
	expectedParams += resourceParams
	_, hasRequirementGate := task.Params[etcd.TaskBlueprintRequirementGateSHA256Param]
	if hasRequirementGate {
		expectedParams++
	}
	if hasBlueprintReleases {
		expectedParams += 1 + releaseHookCount*2
	}
	if len(managedVolumeIDs) != 0 {
		expectedParams += 2
	}
	healthServiceID, hasHealthStep := task.Params[etcd.TaskBackingServiceHealthParam]
	backingVolumeDirectory := task.Params[etcd.TaskBackingServiceVolumeDirectoryParam]
	if hasHealthStep {
		expectedParams += 2
	}
	if resolver.blueprints == nil || task.Executor != etcd.TaskExecutorAgent ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || len(task.Params) != expectedParams ||
		hasHealthStep && procedure != taskcontract.BlueprintComposeProcedureFullReconcile ||
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
	if hasBlueprintReleases && len(resourceStepIDs) != len(pinned.artifact.Networks)+len(pinned.artifact.Volumes) ||
		!hasBlueprintReleases && len(resourceStepIDs) != 0 {
		return nil, errs.New(
			errs.KindInternal,
			"durable Blueprint resource preparation count differs from its owned resources",
		)
	}
	if hasRequirementGate != (len(pinned.requirements.Resolved) != 0) {
		return nil, errs.New(errs.KindInternal, "durable Blueprint requirement marker is inconsistent")
	}
	managedConfigApply, hasManagedConfigApply, err := ResolveEnvironmentManagedConfigApply(
		EnvironmentManagedConfigApplyInput{
			RevisionID: revisionID, RenderGeneration: uint64(task.RenderGeneration), Components: pinned.components,
			ComponentCatalog: resolver.componentCatalog,
			Materializations: task.Materializations, Artifact: pinned.artifact,
		},
	)
	if err != nil {
		return nil, err
	}
	attachCandidates, attachStepCount, err := resolver.blueprintAttachPlanCandidates(ctx, task)
	if err != nil {
		return nil, err
	}
	var managedServiceSteps []*agentpb.ExecutionStep
	managedTeardown := BlueprintManagedComponentTeardown{Artifacts: []*agentpb.ComposeArtifact{pinned.artifact}}
	if procedure != taskcontract.BlueprintComposeProcedureFullReconcile {
		managedTeardown, err = resolver.BlueprintManagedComponentTeardown(
			ctx,
			task,
			pinned.projection,
			pinned.artifact,
			"",
			hasBlueprintReleases,
		)
		if err != nil {
			return nil, err
		}
		managedServiceSteps, err = BlueprintManagedServiceSteps(task, pinned.artifact, "", hasBlueprintReleases)
		if err != nil {
			return nil, err
		}
	}
	expectedSteps := len(task.Materializations)
	expectedSteps += len(managedTeardown.Steps)
	expectedSteps += len(managedServiceSteps)
	expectedSteps += len(resourceStepIDs)
	if hasBlueprintReleases {
		expectedSteps += releaseProcedureStepCount
	} else if procedure == taskcontract.BlueprintComposeProcedureFullReconcile {
		expectedSteps++
	}
	if len(managedVolumeIDs) != 0 {
		expectedSteps++
	}
	if hasHealthStep {
		expectedSteps += 2
	}
	if hasManagedConfigApply {
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
	if hasBlueprintReleases {
		releaseInput, releaseEnd, stepErr := blueprintReleaseProcedureStepIDs(
			task,
			blueprintReleases,
			stepIndex,
		)
		if stepErr != nil {
			return nil, stepErr
		}
		componentSteps := []*agentpb.ExecutionStep(nil)
		if err := blueprintManagedStepsMatch(task, releaseEnd, managedServiceSteps); err != nil {
			return nil, err
		}
		releaseEnd += len(managedServiceSteps)
		if hasManagedConfigApply {
			managedConfigStep, stepErr := managedConfigApply.ExecutionStep(
				task.Steps[releaseEnd].ID,
				uint32(task.TimeoutSeconds),
			)
			if stepErr != nil {
				return nil, stepErr
			}
			componentSteps = append(componentSteps, managedConfigStep)
			releaseEnd++
		}
		if releaseEnd != len(task.Steps) {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Release step order is invalid")
		}
		releaseInput.PrefixSteps, releaseInput.ComponentSteps = steps, componentSteps
		_, plan, prepareErr := resolver.PrepareBlueprintReleaseTask(ctx, task, releaseInput)
		return plan, prepareErr
	}
	if procedure != taskcontract.BlueprintComposeProcedureFullReconcile {
		prerequisite := ""
		if len(steps) != 0 {
			prerequisite = steps[len(steps)-1].StepId
		}
		managedTeardown, err = resolver.BlueprintManagedComponentTeardown(
			ctx,
			task,
			pinned.projection,
			pinned.artifact,
			prerequisite,
			false,
		)
		if err != nil {
			return nil, err
		}
		if err := blueprintManagedStepsMatch(task, stepIndex, managedTeardown.Steps); err != nil {
			return nil, err
		}
		steps = append(steps, managedTeardown.Steps...)
		stepIndex += len(managedTeardown.Steps)
		if len(steps) != 0 {
			prerequisite = steps[len(steps)-1].StepId
		}
		managedServiceSteps, err = BlueprintManagedServiceSteps(task, pinned.artifact, prerequisite, false)
		if err != nil {
			return nil, err
		}
		if err := blueprintManagedStepsMatch(task, stepIndex, managedServiceSteps); err != nil {
			return nil, err
		}
		steps = append(steps, managedServiceSteps...)
		stepIndex += len(managedServiceSteps)
	}
	if procedure == taskcontract.BlueprintComposeProcedureFullReconcile {
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: artifactID, FullReconcile: true,
			}},
		})
		stepIndex++
	}
	if hasManagedConfigApply {
		managedConfigStep, stepErr := managedConfigApply.ExecutionStep(
			task.Steps[stepIndex].ID,
			uint32(task.TimeoutSeconds),
		)
		if stepErr != nil {
			return nil, stepErr
		}
		steps = append(steps, managedConfigStep)
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
	operation := agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	if procedure == taskcontract.BlueprintComposeProcedureFullReconcile {
		operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	}
	if hasHealthStep {
		operation = agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        operation, TargetID: task.Target,
		Artifacts: managedTeardown.Artifacts, Steps: steps,
		ManagedComponentProcedure: managedTeardown.Procedure,
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
	artifact     *agentpb.ComposeArtifact
	projection   etcd.EnvironmentComposeProjection
	components   []etcd.ComponentRecord
	requirements core.BlueprintRequirements
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
		artifact:     proto.Clone(artifact).(*agentpb.ComposeArtifact),
		projection:   projection.Record,
		components:   projection.Record.Components,
		requirements: projection.Record.BlueprintRequirements.Clone(),
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
	project, err := loadPinnedEnvironmentProject(ctx, projection, identity.AuthorizedVolumeDir, releases)
	if err != nil {
		return nil, err
	}
	if len(projection.Components) != 0 {
		managed, err := projectPinnedEnvironmentComponents(project, nil, identity, projection,
			nil, nil, projectedEnvironmentEntries(projection.Entries), resolver.componentCatalog)
		if err != nil {
			return nil, err
		}
		project = managed.Project
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
	identities, err := ComposeIdentitySnapshotFromProjection(projection)
	if err != nil {
		return nil, err
	}
	return RenderCompose(ComposeRenderInput{
		Project: project, ArtifactID: artifactID,
		ProjectOwnerKind: ComposeProjectOwnerTenant,
		TenantID:         identity.TenantID, ProjectID: identity.ProjectID, EnvironmentID: identity.EnvironmentID,
		PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		AuthorizedVolumeDir: identity.AuthorizedVolumeDir,
		Identities:          identities,
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

func ComposeIdentitySnapshotFromProjection(
	projection etcd.EnvironmentComposeProjection,
) (ComposeIdentitySnapshot, error) {
	componentOwners := make(map[string]string)
	for _, component := range projection.Components {
		for _, serviceID := range component.Runtime.GeneratedServices {
			if owner, exists := componentOwners[serviceID]; exists && owner != component.Desired.ID {
				return ComposeIdentitySnapshot{}, errs.New(
					errs.KindValidationFailed, "generated Service has multiple Component owners",
				)
			}
			componentOwners[serviceID] = component.Desired.ID
		}
	}
	services := make([]ComposeResourceIdentity, 0, len(projection.DesiredServices))
	usedServiceIDs := make(map[string]struct{}, cap(services))
	usedServiceNames := make(map[string]struct{}, cap(services))
	desiredServiceNames := make(map[string]string, len(projection.DesiredServices))
	desiredNames := make(map[string]struct{}, len(projection.DesiredServices))
	for _, desired := range projection.DesiredServices {
		service := desired.Desired
		if ids.Validate(ids.KindService, service.ID) != nil || service.Name == "" {
			return ComposeIdentitySnapshot{}, errs.New(errs.KindInternal, "desired Service render identity is invalid")
		}
		if _, duplicate := desiredServiceNames[service.ID]; duplicate {
			return ComposeIdentitySnapshot{}, errs.New(
				errs.KindInternal,
				"desired Service render identity is duplicated",
			)
		}
		if _, duplicate := desiredNames[service.Name]; duplicate {
			return ComposeIdentitySnapshot{}, errs.New(errs.KindInternal, "desired Service render name is duplicated")
		}
		desiredServiceNames[service.ID] = service.Name
		desiredNames[service.Name] = struct{}{}
		if _, generated := componentOwners[service.ID]; generated {
			continue
		}
		usedServiceIDs[service.ID] = struct{}{}
		usedServiceNames[service.Name] = struct{}{}
		services = append(services, ComposeResourceIdentity{ID: service.ID, Name: service.Name})
	}
	if len(componentOwners) != 0 {
		artifact := &agentpb.ComposeArtifact{}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(projection.ComposeArtifact, artifact); err != nil {
			return ComposeIdentitySnapshot{}, errs.New(
				errs.KindInternal,
				"Component generated Service render metadata is corrupt",
			)
		}
		for _, service := range artifact.GetServices() {
			componentID, generated := componentOwners[service.GetServiceId()]
			if !generated {
				continue
			}
			// Generated Services are owned by the Component and its pinned
			// artifact, not the native desired-Service collection.
			if ids.Validate(ids.KindService, service.GetServiceId()) != nil || service.GetComposeName() == "" {
				return ComposeIdentitySnapshot{}, errs.New(
					errs.KindInternal,
					"Component generated Service render name is missing",
				)
			}
			if desiredName, present := desiredServiceNames[service.GetServiceId()]; present &&
				desiredName != service.GetComposeName() {
				return ComposeIdentitySnapshot{}, errs.New(
					errs.KindInternal,
					"Component generated Service render name diverges",
				)
			}
			_, duplicateID := usedServiceIDs[service.GetServiceId()]
			_, duplicateName := usedServiceNames[service.GetComposeName()]
			if duplicateID || duplicateName {
				return ComposeIdentitySnapshot{}, errs.New(
					errs.KindInternal,
					"Component generated Service render identity or name is duplicated",
				)
			}
			identity, err := pinnedComponentServiceIdentity(service, componentID)
			if err != nil {
				return ComposeIdentitySnapshot{}, err
			}
			usedServiceIDs[identity.ID], usedServiceNames[identity.Name] = struct{}{}, struct{}{}
			services = append(services, identity)
		}
		for serviceID := range componentOwners {
			if _, found := usedServiceIDs[serviceID]; !found {
				return ComposeIdentitySnapshot{}, errs.New(
					errs.KindInternal,
					"Component generated Service render metadata is missing",
				)
			}
		}
	}
	sort.Slice(services, func(left, right int) bool { return services[left].Name < services[right].Name })
	return ComposeIdentitySnapshot{
		Services: services,
		Networks: desiredZoneResourceIdentities(projection.DesiredZones),
		Volumes:  composeVolumeResourceIdentities(projection.Volumes),
	}, nil
}
