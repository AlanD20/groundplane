package taskplanning

import (
	"context"
	"encoding/hex"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
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
	Members                   []releaserender.ReleaseTaskRenderMember
	PrefixSteps               []*agentpb.ExecutionStep
	ComponentSteps            []*agentpb.ExecutionStep
	ApplyStepIDs              []string
	HealthStepIDs             []string
	SwitchStepIDs             []string
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
	serviceUnit := taskjournal.IsBlueprintChild(task.Params) ||
		task.Params[taskcontract.EnvironmentBlueprintSelectedServiceParam] != ""
	if resolver == nil || ctx == nil || len(input.Members) == 0 || task.Type != taskjournal.TaskUpdate ||
		task.Params[releaserender.TaskReleasePublicationParam] == "" || len(input.ApplyStepIDs) != len(input.Members) ||
		len(input.HealthStepIDs) != len(input.Members) || len(input.RecoveryProbeStepIDs) != len(input.Members) ||
		len(input.SwitchStepIDs) != len(input.Members) ||
		len(input.RecoveryCompensateStepIDs) != len(input.Members) || len(input.PostStepIDs) != len(input.Members) ||
		len(input.PreStepIDs) != 0 && len(input.PreStepIDs) != len(input.Members) ||
		serviceUnit && (len(input.Members) != 1 || len(input.ComponentSteps) != 0 ||
			len(task.ManagedComponentTeardownSources) != 0) {
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
		identity := composerender.ComposeReleaseIdentity{
			ProxyAddresses: member.Render.ProxyAddresses,
			ProxyImage:     member.Render.ProxyImage,
			ReleaseID:      member.Intent.ID, Target: member.Render.CandidateTarget,
			Image: member.Render.CandidateWorkload.LocalImageID, ServingReleaseID: member.Intent.PriorServingReleaseID,
			ServingTarget: member.Render.PriorTarget, ServingProxyGeneration: member.Render.PriorProxyGeneration,
			Strategy: member.Render.Strategy,
		}
		if member.Render.Strategy == domain.StrategyRecreate {
			identity.ServingReleaseID = member.Intent.ID
			identity.ServingTarget = member.Render.CandidateTarget
			identity.ServingProxyGeneration = member.Render.ProxyGeneration
		}
		labels[member.Render.ServiceID] = identity
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
	if serviceUnit {
		for _, step := range steps {
			if step.GetMaterializeFile() == nil {
				return etcd.TaskRecord{}, nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Service unit prefix contains a non-Service effect",
				)
			}
		}
	}
	prerequisite := ""
	if len(steps) != 0 {
		prerequisite = steps[len(steps)-1].StepId
	}
	if !serviceUnit {
		var networkSteps []*agentpb.ExecutionStep
		task, networkSteps, err = prepareBlueprintReleaseNetworks(task, artifact, prerequisite)
		if err != nil {
			return etcd.TaskRecord{}, nil, err
		}
		steps = append(steps, networkSteps...)
		if len(steps) != 0 {
			prerequisite = steps[len(steps)-1].StepId
		}
		var volumeSteps []*agentpb.ExecutionStep
		task, volumeSteps, err = prepareBlueprintReleaseVolumes(task, artifact, prerequisite)
		if err != nil {
			return etcd.TaskRecord{}, nil, err
		}
		steps = append(steps, volumeSteps...)
	}
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
	var preSteps, forwardSteps, recoverySteps []*agentpb.ExecutionStep
	for index, member := range input.Members {
		memberTask := task
		memberTask.Steps = []taskjournal.TaskStepRecord{
			{ID: input.ApplyStepIDs[index]}, {ID: input.HealthStepIDs[index]},
			{ID: input.SwitchStepIDs[index]}, {ID: input.RecoveryProbeStepIDs[index]},
			{ID: input.RecoveryCompensateStepIDs[index]},
		}
		priorArtifacts := make(map[string]*agentpb.ComposeArtifact)
		if member.Render.PriorArtifactID != "" {
			if member.Render.PriorRuntime == nil {
				return etcd.TaskRecord{}, nil, errs.New(
					errs.KindStateConflict,
					"Blueprint Release predecessor runtime is missing",
				)
			}
			prior := &agentpb.ComposeArtifact{}
			if err := proto.Unmarshal(member.Render.PriorRuntime.CurrentArtifact, prior); err != nil {
				return etcd.TaskRecord{}, nil, errs.Wrap(errs.KindInternal, err)
			}
			priorArtifacts[member.Render.ServiceID] = prior
		}
		if err := validateReleaseProxyTopology(member.Render.Strategy, artifact, priorArtifacts[member.Render.ServiceID], member.Render.ServiceID); err != nil {
			return etcd.TaskRecord{}, nil, err
		}
		memberInput := etcd.ReleaseTaskRenderInput{
			Members:   []releaserender.ReleaseTaskRenderMember{member},
			Operation: releases.ReleaseOperationHead{FailurePolicy: member.Intent.OnFailure},
		}
		memberSteps, err := buildReleaseMemberSteps(memberTask, memberInput, priorArtifacts)
		if err != nil {
			return etcd.TaskRecord{}, nil, err
		}
		var preIDs []string
		if len(input.PreStepIDs) != 0 {
			preIDs = input.PreStepIDs[index]
		}
		anchor := releasePostHookAnchorStepID(memberTask, 0, member.Render.Strategy)
		hooks, err := BuildReleaseHookPlan(ReleaseHookPlanInput{
			Operation: domain.OperationDeploy, CandidateReleaseID: member.Intent.ID,
			PostHookAnchorStepID: anchor, PreStepIDs: preIDs,
			PostStepIDs: input.PostStepIDs[index], Hooks: member.Render.Hooks,
		})
		if err != nil {
			return etcd.TaskRecord{}, nil, err
		}
		preSteps = append(preSteps, hooks.PreSteps...)
		snapshots = append(snapshots, hooks.Snapshots...)
		projections = append(projections, hooks.Projections...)
		bodies = append(bodies, hooks.Bodies...)
		for _, step := range memberSteps[:3] {
			forwardSteps = append(forwardSteps, step)
			if step.StepId == anchor {
				forwardSteps = append(forwardSteps, hooks.PostSteps...)
			}
		}
		// Blueprint ownership is fenced at assignment time. Keep its generic
		// restoration pair; the forward rollout is shared with ordinary Deploy.
		recoverySteps = append(recoverySteps,
			&agentpb.ExecutionStep{
				StepId: input.RecoveryProbeStepIDs[index], TimeoutSeconds: uint32(task.TimeoutSeconds),
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
				Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
					CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
						CandidateArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, CandidateReleaseId: member.Intent.ID,
					},
				},
			},
			&agentpb.ExecutionStep{
				StepId: input.RecoveryCompensateStepIDs[index], TimeoutSeconds: uint32(task.TimeoutSeconds),
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
				PrerequisiteStepId: input.ApplyStepIDs[index],
				Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
					CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
						CandidateArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, CandidateReleaseId: member.Intent.ID,
					},
				},
			},
		)
		procedureMember := executionplan.CandidateReleaseMemberInput{
			ServiceID: member.Render.ServiceID, CandidateReleaseID: member.Intent.ID,
			CandidateArtifactID: first.ArtifactID,
			ForwardStepIDs: []string{
				input.ApplyStepIDs[index],
				input.HealthStepIDs[index],
				input.SwitchStepIDs[index],
			},
		}
		procedureMember.CandidateAbsence = &executionplan.CandidateAbsenceInput{
			ComposeProjectName: artifact.GetProjectName(), Services: candidateServices,
			ProbeStepID: input.RecoveryProbeStepIDs[index], CompensateStepID: input.RecoveryCompensateStepIDs[index],
		}
		procedureMember.ServingPredecessor = &executionplan.ServingPredecessorInput{
			ProbeStepID: input.RecoveryProbeStepIDs[index], CompensateStepID: input.RecoveryCompensateStepIDs[index],
		}
		procedureMembers = append(procedureMembers, procedureMember)
	}
	for _, phase := range [][]*agentpb.ExecutionStep{preSteps, forwardSteps} {
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
	steps = append(steps, recoverySteps...)
	teardown := BlueprintManagedComponentTeardown{Artifacts: []*agentpb.ComposeArtifact{artifact}}
	if !serviceUnit {
		teardown, err = resolver.BlueprintManagedComponentTeardown(
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
		managedSteps, buildErr := BlueprintManagedServiceSteps(task, artifact, managedPrerequisite, true)
		if buildErr != nil {
			return etcd.TaskRecord{}, nil, buildErr
		}
		steps = append(steps, teardown.Steps...)
		steps = append(steps, managedSteps...)
		componentSteps, buildErr := cloneBlueprintReleaseForwardSteps(
			input.ComponentSteps,
			blueprintReleaseForwardComponent,
		)
		if buildErr != nil {
			return etcd.TaskRecord{}, nil, buildErr
		}
		steps = append(steps, componentSteps...)
	}
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
	task.Steps = taskjournal.CaptureStepDescriptions(task.Steps, plan.Steps)
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
	members []releaserender.ReleaseTaskRenderMember,
) error {
	const op = "bind blueprint candidate service images"
	if artifact == nil {
		return errs.New(errs.KindInternal, op+": compose artifact is required")
	}
	byService := make(map[string]releaserender.ReleaseTaskRenderMember, len(members))
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
