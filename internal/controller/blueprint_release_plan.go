package controller

import (
	"context"
	"encoding/hex"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

type BlueprintReleasePlanInput struct {
	Members        []etcd.ReleaseTaskRenderMember
	PrefixSteps    []*agentpb.ExecutionStep
	ComponentSteps []*agentpb.ExecutionStep
	ApplyStepIDs   []string
	HealthStepIDs  []string
	PostStepIDs    [][]string
}

func (resolver *TaskPlanResolver) PrepareBlueprintReleaseTask(
	ctx context.Context,
	task etcd.TaskRecord,
	input BlueprintReleasePlanInput,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	if resolver == nil || ctx == nil || len(input.Members) == 0 || task.Type != etcd.TaskUpdate ||
		task.Params[etcd.TaskReleasePublicationParam] == "" || len(input.ApplyStepIDs) != len(input.Members) ||
		len(input.HealthStepIDs) != len(input.Members) || len(input.PostStepIDs) != len(input.Members) {
		return etcd.TaskRecord{}, nil, errs.New(errs.KindValidationFailed, "Blueprint Release Task preparation is invalid")
	}
	first := input.Members[0].Render
	images := make(map[string]string, len(input.Members))
	labels := make(map[string]ComposeReleaseIdentity, len(input.Members))
	serviceIDs := make([]string, len(input.Members))
	for index, member := range input.Members {
		if member.Render.PlanID != task.PlanID || member.Render.ArtifactID != first.ArtifactID ||
			member.Render.Projection.RevisionID != first.Projection.RevisionID ||
			member.Intent.OperationID != task.OperationID {
			return etcd.TaskRecord{}, nil, errs.New(errs.KindValidationFailed, "Blueprint candidate Release render inputs diverge")
		}
		images[member.Render.ServiceName] = member.Render.Image
		labels[member.Render.ServiceID] = ComposeReleaseIdentity{
			ReleaseID: member.Intent.ID, Target: member.Render.CandidateTarget,
			Image: member.Render.Image, ServingReleaseID: priorReleaseLabel(member.Intent.PriorServingReleaseID),
			ServingTarget: member.Render.PriorTarget, ServingProxyGeneration: member.Render.PriorProxyGeneration,
			Strategy: member.Render.Strategy,
		}
		serviceIDs[index] = member.Render.ServiceID
	}
	artifact, err := resolver.renderPinnedEnvironmentArtifactWithReleases(
		ctx, task,
		pinnedEnvironmentIdentity{
			TenantID: first.TenantID, TenantSlug: first.TenantSlug,
			ProjectID: first.ProjectID, ProjectSlug: first.ProjectSlug,
			EnvironmentID: first.EnvironmentID, EnvironmentName: first.EnvironmentName,
			AuthorizedVolumeDir: first.AuthorizedVolumeDir,
		},
		first.Projection.RevisionID, first.ArtifactID, first.Projection,
		func(project *composetypes.Project, projection etcd.EnvironmentComposeProjection) ([]ComposeResourceIdentity, error) {
			for name, image := range images {
				service, err := project.GetService(name)
				if err != nil {
					return nil, errs.New(errs.KindInternal, "Blueprint candidate Service is absent from its sealed projection")
				}
				service.Image = image
				if _, enabled := project.Services[name]; enabled {
					project.Services[name] = service
				} else {
					project.DisabledServices[name] = service
				}
			}
			return managedAttachExternalNetworks(project)
		},
		labels,
	)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	if err := bindBlueprintCandidateServiceImages(artifact, input.Members); err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	steps := append([]*agentpb.ExecutionStep(nil), input.PrefixSteps...)
	snapshots := []*agentpb.ResolvedRunnerSnapshot{}
	projections := []*agentpb.ScriptRunnerProjection{}
	bodies := []*agentpb.ScriptBodyArtifactMetadata{}
	for index, member := range input.Members {
		apply := &agentpb.ExecutionStep{
			StepId: input.ApplyStepIDs[index], TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: first.ArtifactID, ServiceIds: []string{member.Render.ServiceID},
				ForceRecreate: true, NoDependencies: true,
			}},
		}
		steps = append(steps, apply)
		hooks, err := BuildReleaseHookPlan(ReleaseHookPlanInput{
			Operation: domain.OperationDeploy, CandidateReleaseID: member.Intent.ID,
			PostHookAnchorStepID: apply.StepId, PostStepIDs: input.PostStepIDs[index], Hooks: member.Render.Hooks,
		})
		if err != nil {
			return etcd.TaskRecord{}, nil, err
		}
		if err := bindBlueprintProcedureImageAuthorities(hooks, apply.StepId, first.ArtifactID, member); err != nil {
			return etcd.TaskRecord{}, nil, err
		}
		steps = append(steps, hooks.PostSteps...)
		snapshots = append(snapshots, hooks.Snapshots...)
		projections = append(projections, hooks.Projections...)
		bodies = append(bodies, hooks.Bodies...)
		prerequisite := apply.StepId
		if len(hooks.PostSteps) != 0 {
			prerequisite = hooks.PostSteps[len(hooks.PostSteps)-1].StepId
		}
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: input.HealthStepIDs[index], PrerequisiteStepId: prerequisite,
			TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
				ArtifactId: first.ArtifactID, ServiceIds: []string{member.Render.ServiceID},
			}},
		})
	}
	steps = append(steps, input.ComponentSteps...)
	plan, err := BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		TargetID:         task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact},
		ScriptRunnerSnapshots: snapshots, ScriptRunnerProjections: projections,
		ScriptBodyArtifacts: bodies, Steps: steps,
	})
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	task.Steps, err = blueprintTaskStepRecords(plan.Steps, input.Members)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	return task, plan, nil
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
	for _, member := range members {
		if member.Render.ServiceID == "" || member.Intent.ID == "" || member.Render.Image == "" {
			return errs.New(errs.KindInternal, op+": release member is incomplete")
		}
		if _, exists := byService[member.Render.ServiceID]; exists {
			return errs.New(errs.KindInternal, op+": release service is duplicated")
		}
		byService[member.Render.ServiceID] = member
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
		releaseOwned := false
		for _, label := range service.GetExpectedLabels() {
			if label.GetKey() == composeLabelReleaseID && label.GetValue() == member.Intent.ID {
				releaseOwned = true
				break
			}
		}
		if !releaseOwned {
			return errs.New(errs.KindInternal, op+": compose service release ownership does not match")
		}
		service.ImageReference = member.Render.Image
		seen[service.GetServiceId()] = struct{}{}
	}
	if len(seen) != len(byService) {
		return errs.New(errs.KindInternal, op+": selected candidate compose service is missing")
	}
	return nil
}

func bindBlueprintProcedureImageAuthorities(
	hooks ReleaseHookPlan,
	applyStepID string,
	artifactID string,
	member etcd.ReleaseTaskRenderMember,
) error {
	const op = "bind blueprint procedure image authorities"
	for _, step := range hooks.PostSteps {
		run := step.GetRunScript()
		if run == nil {
			continue
		}
		var snapshot *agentpb.ResolvedRunnerSnapshot
		for _, candidate := range hooks.Snapshots {
			if candidate != nil && candidate.GetScriptExecutionId() == run.GetScriptExecutionId() {
				snapshot = candidate
				break
			}
		}
		if snapshot == nil {
			return errs.New(errs.KindInternal, op+": runner snapshot is missing")
		}
		authority := snapshot.GetProcedureServiceImage()
		if authority == nil {
			continue
		}
		if authority.GetComposeApplyStepId() != "" ||
			authority.GetArtifactId() != artifactID ||
			authority.GetServiceId() != member.Render.ServiceID ||
			authority.GetReleaseId() != member.Intent.ID ||
			authority.GetRequestedReference() != member.Render.Image {
			return errs.New(errs.KindInternal, op+": authority does not match selected candidate")
		}
		authority.ComposeApplyStepId = applyStepID
		digest, err := deterministicScriptProtoDigest(snapshot)
		if err != nil {
			return err
		}
		run.RunnerSnapshotSha256 = digest
	}
	return nil
}
