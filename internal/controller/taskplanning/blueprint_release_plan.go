package taskplanning

import (
	"context"
	"encoding/hex"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BlueprintReleasePlanInput struct {
	NativePredecessors        []etcd.BlueprintNativePredecessor
	Members                   []etcd.ReleaseTaskRenderMember
	PrefixSteps               []*agentpb.ExecutionStep
	ComponentSteps            []*agentpb.ExecutionStep
	ApplyStepIDs              []string
	HealthStepIDs             []string
	RecoveryProbeStepIDs      []string
	RecoveryCompensateStepIDs []string
	PostStepIDs               [][]string
	PreStepIDs                [][]string
}

type blueprintReleaseForwardStage uint8

const (
	blueprintReleaseForwardPrefix blueprintReleaseForwardStage = iota + 1
	blueprintReleaseForwardComponent
)

func (resolver *TaskPlanResolver) PrepareBlueprintReleaseTask(
	ctx context.Context,
	task etcd.TaskRecord,
	input BlueprintReleasePlanInput,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	if resolver == nil || ctx == nil || len(input.Members) == 0 || task.Type != taskjournal.TaskUpdate ||
		task.Params[etcd.TaskReleasePublicationParam] == "" || len(input.ApplyStepIDs) != len(input.Members) ||
		len(input.HealthStepIDs) != len(input.Members) || len(input.RecoveryProbeStepIDs) != len(input.Members) ||
		len(input.RecoveryCompensateStepIDs) != len(input.Members) || len(input.PostStepIDs) != len(input.Members) ||
		len(input.PreStepIDs) != 0 && len(input.PreStepIDs) != len(input.Members) {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindValidationFailed,
			"Blueprint Release Task preparation is invalid",
		)
	}
	first := input.Members[0].Render
	images := make(map[string]domain.WorkloadSeal, len(input.Members))
	labels := make(map[string]composerender.ComposeReleaseIdentity, len(input.Members))
	serviceIDs := make([]string, len(input.Members))
	for index, member := range input.Members {
		if member.Render.PlanID != task.PlanID || member.Render.ArtifactID != first.ArtifactID ||
			member.Render.Projection.RevisionID != first.Projection.RevisionID ||
			member.Intent.OperationID != task.OperationID {
			return etcd.TaskRecord{}, nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint candidate Release render inputs diverge",
			)
		}
		images[member.Render.ServiceName] = member.Render.CandidateWorkload
		labels[member.Render.ServiceID] = composerender.ComposeReleaseIdentity{
			ProxyImage: member.Render.ProxyImage,
			ReleaseID:  member.Intent.ID, Target: member.Render.CandidateTarget,
			Image: member.Render.CandidateWorkload.LocalImageID, ServingReleaseID: member.Intent.ID,
			ServingTarget: member.Render.CandidateTarget, ServingProxyGeneration: member.Render.ProxyGeneration,
			Strategy: member.Render.Strategy,
		}
		serviceIDs[index] = member.Render.ServiceID
	}
	artifact, err := renderBlueprintCandidateArtifact(ctx, task, first, images, labels)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	artifact, err = retainBlueprintUnselectedRuntime(artifact, first.Projection.ComposeArtifact, serviceIDs)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	if err := bindBlueprintCandidateServiceImages(artifact, input.Members); err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	steps, err := cloneBlueprintReleaseForwardSteps(input.PrefixSteps, blueprintReleaseForwardPrefix)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	prerequisite := ""
	if len(steps) != 0 {
		prerequisite = steps[len(steps)-1].StepId
	}
	task, networkSteps, err := prepareBlueprintReleaseNetworks(task, artifact, prerequisite)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	steps = append(steps, networkSteps...)
	if len(steps) != 0 {
		prerequisite = steps[len(steps)-1].StepId
	}
	task, volumeSteps, err := prepareBlueprintReleaseVolumes(task, artifact, prerequisite)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	steps = append(steps, volumeSteps...)
	candidateServices := make([]executionplan.CandidateServiceIdentity, len(input.Members))
	procedureMembers := make([]executionplan.CandidateReleaseMemberInput, 0, len(input.Members))
	for index, member := range input.Members {
		candidateServices[index] = executionplan.CandidateServiceIdentity{
			ServiceID: member.Render.ServiceID,
			ReleaseID: member.Intent.ID,
		}
	}
	snapshots := []*agentpb.ResolvedRunnerSnapshot{}
	projections := []*agentpb.ScriptRunnerProjection{}
	bodies := []*agentpb.ScriptBodyArtifactMetadata{}
	var preSteps, applySteps, postSteps, healthSteps, recoverySteps []*agentpb.ExecutionStep
	for index, member := range input.Members {
		apply := &agentpb.ExecutionStep{
			StepId: input.ApplyStepIDs[index], TimeoutSeconds: uint32(task.TimeoutSeconds),
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: first.ArtifactID, ServiceIds: []string{member.Render.ServiceID},
				ForceRecreate: true, NoDependencies: true,
			}},
		}
		applySteps = append(applySteps, apply)
		var preIDs []string
		if len(input.PreStepIDs) != 0 {
			preIDs = input.PreStepIDs[index]
		}
		hooks, err := BuildReleaseHookPlan(ReleaseHookPlanInput{
			Operation: domain.OperationDeploy, CandidateReleaseID: member.Intent.ID,
			PostHookAnchorStepID: apply.StepId, PreStepIDs: preIDs, PostStepIDs: input.PostStepIDs[index], Hooks: member.Render.Hooks,
		})
		if err != nil {
			return etcd.TaskRecord{}, nil, err
		}

		preSteps = append(preSteps, hooks.PreSteps...)
		postSteps = append(postSteps, hooks.PostSteps...)
		snapshots = append(snapshots, hooks.Snapshots...)
		projections = append(projections, hooks.Projections...)
		bodies = append(bodies, hooks.Bodies...)
		prerequisite := apply.StepId
		if len(hooks.PostSteps) != 0 {
			prerequisite = hooks.PostSteps[len(hooks.PostSteps)-1].StepId
		}
		health := &agentpb.ExecutionStep{
			StepId: input.HealthStepIDs[index], PrerequisiteStepId: prerequisite,
			TimeoutSeconds: uint32(task.TimeoutSeconds),
			Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
				ArtifactId: first.ArtifactID, ServiceIds: []string{member.Render.ServiceID},
			}},
		}
		probe := &agentpb.ExecutionStep{
			StepId: input.RecoveryProbeStepIDs[index], TimeoutSeconds: uint32(task.TimeoutSeconds),
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
			Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
				CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
					CandidateArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, CandidateReleaseId: member.Intent.ID,
				},
			},
		}
		compensate := &agentpb.ExecutionStep{
			StepId: input.RecoveryCompensateStepIDs[index], TimeoutSeconds: uint32(task.TimeoutSeconds),
			Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
			PrerequisiteStepId: apply.StepId,
			Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
				CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
					CandidateArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, CandidateReleaseId: member.Intent.ID,
				},
			},
		}
		healthSteps = append(healthSteps, health)
		recoverySteps = append(recoverySteps, probe, compensate)
		forwardStepIDs := []string{apply.GetStepId(), health.GetStepId()}
		procedureMembers = append(procedureMembers, executionplan.CandidateReleaseMemberInput{
			ServiceID: member.Render.ServiceID, CandidateReleaseID: member.Intent.ID, CandidateArtifactID: first.ArtifactID,
			ForwardStepIDs: forwardStepIDs,
			ServingPredecessor: &executionplan.ServingPredecessorInput{
				ProbeStepID: probe.GetStepId(), CompensateStepID: compensate.GetStepId(),
			},
			CandidateAbsence: &executionplan.CandidateAbsenceInput{
				ComposeProjectName: artifact.GetProjectName(), Services: candidateServices,
				ProbeStepID: probe.GetStepId(), CompensateStepID: compensate.GetStepId(),
			},
		})
	}
	// The executor traverses forward steps in order. Explicit predecessor links
	// also seal the cleanup barrier across Services and between all phases.
	for _, phase := range [][]*agentpb.ExecutionStep{preSteps, applySteps, postSteps, healthSteps} {
		for _, step := range phase {
			if len(steps) != 0 {
				step.PrerequisiteStepId = steps[len(steps)-1].StepId
			}
			steps = append(steps, step)
		}
	}
	managedPrerequisite := ""
	if len(steps) != 0 {
		managedPrerequisite = steps[len(steps)-1].StepId
	}
	teardown, err := resolver.BlueprintManagedComponentTeardown(
		ctx,
		task,
		first.Projection,
		artifact,
		managedPrerequisite,
		true,
	)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	if len(teardown.Steps) != 0 {
		managedPrerequisite = teardown.Steps[len(teardown.Steps)-1].GetStepId()
	}
	managedSteps, err := BlueprintManagedServiceSteps(task, artifact, managedPrerequisite, true)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	steps = append(steps, recoverySteps...)
	steps = append(steps, teardown.Steps...)
	steps = append(steps, managedSteps...)
	componentSteps, err := cloneBlueprintReleaseForwardSteps(input.ComponentSteps, blueprintReleaseForwardComponent)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	steps = append(steps, componentSteps...)
	procedure, err := executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, Members: procedureMembers,
	})
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	for _, captured := range input.NativePredecessors {
		for _, value := range [][]byte{captured.CurrentArtifact, captured.RetainedPriorArtifact} {
			if len(value) == 0 {
				continue
			}
			prior := &agentpb.ComposeArtifact{}
			if proto.Unmarshal(value, prior) != nil {
				return etcd.TaskRecord{}, nil, errs.New(
					errs.KindStateConflict,
					"Blueprint native predecessor artifact is invalid",
				)
			}
			teardown.Artifacts = append(teardown.Artifacts, prior)
		}
		for _, member := range procedure.Members {
			if member.ServiceId != captured.ServiceID || captured.Serving == nil {
				continue
			}
			artifact := &agentpb.ComposeArtifact{}
			if proto.Unmarshal(captured.CurrentArtifact, artifact) != nil {
				return etcd.TaskRecord{}, nil, errs.New(
					errs.KindStateConflict,
					"Blueprint native predecessor artifact is invalid",
				)
			}
			prior := member.ServingPredecessor
			prior.PriorArtifactId, prior.PriorReleaseId, prior.PriorTarget = artifact.ArtifactId, captured.Serving.ServingReleaseID, string(
				captured.Serving.Target,
			)
			if len(captured.RetainedPriorArtifact) != 0 {
				retained := &agentpb.ComposeArtifact{}
				if proto.Unmarshal(captured.RetainedPriorArtifact, retained) != nil {
					return etcd.TaskRecord{}, nil, errs.New(
						errs.KindStateConflict,
						"Blueprint inactive predecessor artifact is invalid",
					)
				}
				prior.RetainedPriorArtifactId = retained.ArtifactId
			}
		}
	}
	configuration, fileSteps, err := resolver.configurationRecoverySteps(ctx, task, first.ArtifactID)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	procedure.ConfigurationRestoration = configuration
	steps = append(steps, fileSteps...)
	plan, err := taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		TargetID:         task.Target, Artifacts: teardown.Artifacts,
		ScriptRunnerSnapshots: snapshots, ScriptRunnerProjections: projections,
		ScriptBodyArtifacts: bodies, Steps: steps, CandidateReleaseProcedure: procedure,
		ManagedComponentProcedure: teardown.Procedure,
	})
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	task.ComponentActionStepIDs, err = executionplan.ComponentActionStepIDs(plan)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	task.Steps, err = blueprintTaskStepRecords(plan.Steps, input.Members)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	return task, plan, nil
}

func cloneBlueprintReleaseForwardSteps(
	steps []*agentpb.ExecutionStep,
	stage blueprintReleaseForwardStage,
) ([]*agentpb.ExecutionStep, error) {
	owned := make([]*agentpb.ExecutionStep, len(steps))
	for index, step := range steps {
		if step == nil || step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_UNSPECIFIED ||
			!validBlueprintReleaseForwardPayload(step, stage) {
			return nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint Release caller step is not valid for its forward stage",
			)
		}
		owned[index] = proto.Clone(step).(*agentpb.ExecutionStep)
		owned[index].Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
	}
	return owned, nil
}

func validBlueprintReleaseForwardPayload(step *agentpb.ExecutionStep, stage blueprintReleaseForwardStage) bool {
	switch stage {
	case blueprintReleaseForwardPrefix:
		return step.GetEnvironmentDirectoryCreate() != nil ||
			step.GetManagedVolumeDirectoriesEnsure() != nil ||
			step.GetMaterializeFile() != nil ||
			step.GetAdapterProcedure() != nil
	case blueprintReleaseForwardComponent:
		return step.GetComponentApply() != nil
	default:
		return false
	}
}

func bindBlueprintCandidateServiceImages(
	artifact *agentpb.ComposeArtifact,
	members []etcd.ReleaseTaskRenderMember,
) error {
	const op = "bind blueprint candidate service images"
	if artifact == nil {
		return errs.New(errs.KindInternal, op+": compose artifact is required")
	}
	byService := make(map[string]etcd.ReleaseTaskRenderMember, len(members))
	candidateRoles := make(map[string]agentpb.ComposeServiceRole, len(members))
	for _, member := range members {
		if member.Render.ServiceID == "" || member.Intent.ID == "" ||
			member.Render.CandidateWorkload.LocalImageID == "" {
			return errs.New(errs.KindInternal, op+": release member is incomplete")
		}
		if _, exists := byService[member.Render.ServiceID]; exists {
			return errs.New(errs.KindInternal, op+": release service is duplicated")
		}
		byService[member.Render.ServiceID] = member
		candidateRole := agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
		if member.Render.Strategy == domain.StrategyRecreate {
			candidateRole = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
		}
		candidateRoles[member.Render.ServiceID] = candidateRole
	}
	seen := make(map[string]struct{}, len(byService))
	for _, service := range artifact.GetServices() {
		if service == nil {
			return errs.New(errs.KindInternal, op+": compose service is required")
		}
		member, selected := byService[service.GetServiceId()]
		if !selected {
			continue
		}
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if service.GetRole() != candidateRoles[service.GetServiceId()] {
			return errs.New(errs.KindInternal, op+": compose service release ownership does not match")
		}
		releaseID := ""
		for _, label := range service.GetExpectedLabels() {
			if label.GetKey() == composerender.ComposeLabelReleaseID {
				releaseID = label.GetValue()
				break
			}
		}
		if releaseID == "" {
			continue
		}
		if releaseID != member.Intent.ID {
			return errs.New(errs.KindInternal, op+": compose service release ownership does not match")
		}
		if _, duplicate := seen[service.GetServiceId()]; duplicate {
			return errs.New(errs.KindInternal, op+": candidate compose service is duplicated")
		}
		service.ImageReference = member.Render.CandidateWorkload.LocalImageID
		seen[service.GetServiceId()] = struct{}{}
	}
	if len(seen) != len(byService) {
		return errs.New(errs.KindInternal, op+": selected candidate compose service is missing")
	}
	return nil
}
