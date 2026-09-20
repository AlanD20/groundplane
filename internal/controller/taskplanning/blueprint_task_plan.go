package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"math"
	"strings"
)

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
	backingCreation, err := resolveBackingServiceCreationPlan(task, procedure)
	if err != nil {
		return nil, err
	}
	expectedParams += backingCreation.parameterCount()
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
		expectedSteps += releaseProcedureStepCount + 2*len(task.Materializations)
	} else if procedure == taskcontract.BlueprintComposeProcedureFullReconcile {
		expectedSteps++
	}
	if len(managedVolumeIDs) != 0 {
		expectedSteps++
	}
	expectedSteps += backingCreation.stepCount()
	if hasManagedConfigApply {
		expectedSteps++
	}
	expectedSteps += attachStepCount
	if len(task.Steps) != expectedSteps {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Task step count is invalid")
	}
	steps := make([]*agentpb.ExecutionStep, 0, expectedSteps)
	stepIndex := 0
	if backingCreation.enabled {
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: task.Steps[stepIndex].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId: task.Target, ExpectedVolumeDir: backingCreation.volumeDirectory,
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
	volumePrecedesMaterialization := backingCreation.enabled && len(managedVolumeIDs) != 0
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
		step, stepErr := taskmaterialization.BuildTaskMaterializationStep(reference, artifactID, uint32(task.TimeoutSeconds))
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
		if err := resolver.configurationRecoverySuffix(ctx, task, releaseEnd); err != nil {
			return nil, err
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
	steps, stepIndex, err = resolver.appendBackingServiceCreationFinalSteps(
		ctx, task, backingCreation, pinned.projection, artifactID, steps, stepIndex,
	)
	if err != nil {
		return nil, err
	}
	if backingCreation.hasAfterStart {
		defer taskplan.ClearBackingHookProcedure(steps[len(steps)-1].GetBackingHookProcedure())
	}
	if stepIndex != len(task.Steps) {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Task step order is invalid")
	}
	operation := agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	if procedure == taskcontract.BlueprintComposeProcedureFullReconcile {
		operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	}
	if backingCreation.enabled {
		operation = agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE
	}
	return taskplan.Build(taskplan.BuildInput{
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
