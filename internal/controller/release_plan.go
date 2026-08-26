package controller

import (
	"context"
	"encoding/hex"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

const releaseMemberStepTimeoutSeconds = uint32(5 * 60)

func (resolver *TaskPlanResolver) EnableReleasePlans(ledger *etcd.ReleaseLedger) error {
	if resolver == nil || ledger == nil {
		return errs.New(errs.KindInternal, "release plan dependencies are required")
	}
	resolver.releases = ledger
	return nil
}

func (resolver *TaskPlanResolver) PrepareReleaseTask(
	ctx context.Context,
	task etcd.TaskRecord,
	input etcd.ReleaseTaskRenderInput,
) (etcd.TaskRecord, error) {
	if resolver == nil || ctx == nil || len(input.Members) == 0 || len(input.Members) > 32 ||
		task.Executor != etcd.TaskExecutorAgent || task.OperationID != input.Operation.OperationID ||
		task.Params[etcd.TaskReleasePublicationParam] != input.PublicationID {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "release Task preparation is invalid")
	}
	prepared := task
	prepared.RenderGeneration = int32(input.Members[0].Render.Projection.RenderGeneration)
	if prepared.RenderGeneration <= 0 || len(prepared.Steps) != len(input.Members)*5 {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "release Task procedure is invalid")
	}
	plan, err := resolver.buildReleasePlan(ctx, prepared, input)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.PlanHash)
	return prepared, nil
}

func (resolver *TaskPlanResolver) resolveReleasePlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver == nil || resolver.releases == nil {
		return nil, errs.New(errs.KindInternal, "release plan resolver is not configured")
	}
	input, err := resolver.releases.GetTaskRenderInput(ctx, task)
	if err != nil {
		return nil, err
	}
	return resolver.buildReleasePlan(ctx, task, input)
}

func (resolver *TaskPlanResolver) buildReleasePlan(
	ctx context.Context,
	task etcd.TaskRecord,
	input etcd.ReleaseTaskRenderInput,
) (*agentpb.ExecutionPlan, error) {
	if task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 || len(input.Members) == 0 ||
		len(task.Steps) != len(input.Members)*5 {
		return nil, errs.New(errs.KindInternal, "durable release Task shape is invalid")
	}
	first := input.Members[0].Render
	expectedPhase := core.ServiceLifecycleDeploy
	if task.Type == etcd.TaskRollback {
		expectedPhase = core.ServiceLifecycleRollback
	}
	serviceNames := make([]string, len(first.Projection.Services))
	for index, service := range first.Projection.Services {
		serviceNames[index] = service.Name
	}
	if first.ServiceDependencyPlans.Validate(serviceNames) != nil {
		return nil, errs.New(errs.KindInternal, "frozen release dependency plan is invalid")
	}
	dependencyPlan := first.DeployDependencyPlan
	if expectedPhase == core.ServiceLifecycleRollback {
		dependencyPlan = first.RollbackDependencyPlan
	}
	if dependencyPlan.Phase != "" && dependencyPlan.Phase != expectedPhase {
		return nil, errs.New(errs.KindInternal, "frozen release dependency phase is invalid")
	}
	images := make(map[string]string, len(input.Members))
	labels := make(map[string]ComposeReleaseIdentity, len(input.Members))
	for _, member := range input.Members {
		if member.Render.PlanID != task.PlanID || member.Render.ArtifactID != first.ArtifactID ||
			member.Render.EnvironmentID != first.EnvironmentID ||
			member.Render.Projection.RevisionID != first.Projection.RevisionID ||
			member.Render.Projection.RenderGeneration != first.Projection.RenderGeneration ||
			!member.Render.ServiceDependencyPlans.Equal(first.ServiceDependencyPlans) {
			return nil, errs.New(errs.KindInternal, "release render inputs do not share one frozen projection")
		}
		images[member.Render.ServiceName] = member.Render.Image
		labels[member.Render.ServiceID] = ComposeReleaseIdentity{
			ReleaseID: member.Render.ReleaseID, Target: member.Render.CandidateTarget, Image: member.Render.Image,
			ServingReleaseID:       priorReleaseLabel(member.Intent.PriorServingReleaseID),
			ServingTarget:          member.Render.PriorTarget,
			ServingProxyGeneration: member.Render.PriorProxyGeneration, Strategy: member.Render.Strategy,
		}
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
		func(project *composetypes.Project, _ etcd.EnvironmentComposeProjection) ([]ComposeResourceIdentity, error) {
			selected := make(map[string]struct{}, len(images))
			for name := range images {
				selected[name] = struct{}{}
			}
			for name, image := range images {
				service, exists := project.Services[name]
				if !exists {
					service, exists = project.DisabledServices[name]
				}
				if !exists {
					return nil, errs.New(errs.KindInternal, "release service is missing from frozen Blueprint")
				}
				service.Image = image
				if _, enabled := project.Services[name]; enabled {
					project.Services[name] = service
				} else {
					project.DisabledServices[name] = service
				}
			}
			if err := applyExternalReleaseDependencies(project, selected, dependencyPlan); err != nil {
				return nil, err
			}
			return nil, nil
		},
		labels,
	)
	if err != nil {
		return nil, err
	}
	artifacts := []*agentpb.ComposeArtifact{artifact}
	priorArtifacts := make(map[string]*agentpb.ComposeArtifact, len(input.Members))
	priorImages := make(map[string]string)
	priorLabels := make(map[string]ComposeReleaseIdentity)
	priorArtifactID := ""
	for _, member := range input.Members {
		if member.Render.PriorArtifactID == "" {
			continue
		}
		if priorArtifactID == "" {
			priorArtifactID = member.Render.PriorArtifactID
		} else if priorArtifactID != member.Render.PriorArtifactID {
			return nil, errs.New(errs.KindInternal, "recreate members do not share one sealed prior artifact")
		}
		priorReleaseID := member.Intent.PriorServingReleaseID
		if priorReleaseID == "" {
			priorReleaseID = "baseline"
		}
		labelReleaseID := priorReleaseID
		if labelReleaseID == "baseline" {
			labelReleaseID = ""
		}
		priorImages[member.Render.ServiceName] = member.Render.PriorImage
		priorLabels[member.Render.ServiceID] = ComposeReleaseIdentity{
			ReleaseID: labelReleaseID, Target: member.Render.PriorTarget, Image: member.Render.PriorImage,
			ServingReleaseID: priorReleaseID, ServingTarget: member.Render.PriorTarget,
			ServingProxyGeneration: member.Render.PriorProxyGeneration, Strategy: member.Render.PriorStrategy,
		}
	}
	if priorArtifactID != "" {
		priorArtifact, renderErr := resolver.renderPinnedEnvironmentArtifactWithReleases(
			ctx, task,
			pinnedEnvironmentIdentity{
				TenantID: first.TenantID, TenantSlug: first.TenantSlug,
				ProjectID: first.ProjectID, ProjectSlug: first.ProjectSlug,
				EnvironmentID: first.EnvironmentID, EnvironmentName: first.EnvironmentName,
				AuthorizedVolumeDir: first.AuthorizedVolumeDir,
			},
			first.Projection.RevisionID, priorArtifactID, first.Projection,
			func(project *composetypes.Project, _ etcd.EnvironmentComposeProjection) ([]ComposeResourceIdentity, error) {
				for name, image := range priorImages {
					service, active := project.Services[name]
					if !active {
						var disabled bool
						service, disabled = project.DisabledServices[name]
						if !disabled {
							return nil, errs.New(errs.KindInternal, "recreate prior service is missing from frozen Blueprint")
						}
					}
					service.Image = image
					if active {
						project.Services[name] = service
					} else {
						project.DisabledServices[name] = service
					}
				}
				return nil, nil
			},
			priorLabels,
		)
		if renderErr != nil {
			return nil, renderErr
		}
		artifacts = append(artifacts, priorArtifact)
		for _, member := range input.Members {
			if member.Render.PriorArtifactID != "" {
				priorArtifacts[member.Render.ServiceID] = priorArtifact
			}
		}
	}
	operation := agentpb.PlanOperation_PLAN_OPERATION_DEPLOY
	if task.Type == etcd.TaskRollback {
		operation = agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(input.Members)*5)
	for index, member := range input.Members {
		base := index * 5
		applyID, healthID, switchID := task.Steps[base].ID, task.Steps[base+1].ID, task.Steps[base+2].ID
		probeID, compensateID := task.Steps[base+3].ID, task.Steps[base+4].ID
		if ids.Validate(ids.KindStep, applyID) != nil || ids.Validate(ids.KindStep, healthID) != nil ||
			ids.Validate(ids.KindStep, switchID) != nil || ids.Validate(ids.KindStep, probeID) != nil ||
			ids.Validate(ids.KindStep, compensateID) != nil {
			return nil, errs.New(errs.KindInternal, "release Task step identity is invalid")
		}
		priorReleaseID := member.Intent.PriorServingReleaseID
		if priorReleaseID == "" {
			priorReleaseID = "baseline"
		}
		if member.Render.Strategy == domain.StrategyRecreate {
			if priorArtifacts[member.Render.ServiceID] == nil {
				return nil, errs.New(errs.KindInternal, "recreate prior artifact is missing")
			}
			steps = append(steps,
				&agentpb.ExecutionStep{StepId: applyID, TimeoutSeconds: releaseMemberStepTimeoutSeconds, Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
					Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{ArtifactId: member.Render.PriorArtifactID, ServiceIds: []string{member.Render.ServiceID}}}},
				&agentpb.ExecutionStep{StepId: healthID, TimeoutSeconds: releaseMemberStepTimeoutSeconds, Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD, PrerequisiteStepId: applyID,
					Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{ArtifactId: first.ArtifactID, ServiceIds: []string{member.Render.ServiceID}, ForceRecreate: true, NoDependencies: true}}},
				&agentpb.ExecutionStep{StepId: switchID, TimeoutSeconds: releaseMemberStepTimeoutSeconds, Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD, PrerequisiteStepId: healthID,
					Payload: &agentpb.ExecutionStep_ServiceRecreateAcknowledge{ServiceRecreateAcknowledge: &agentpb.ServiceRecreateAcknowledge{ArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, ReleaseId: member.Intent.ID}}},
				&agentpb.ExecutionStep{StepId: probeID, TimeoutSeconds: releaseMemberStepTimeoutSeconds, Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
					Payload: &agentpb.ExecutionStep_ServiceRecreateProbe{ServiceRecreateProbe: &agentpb.ServiceRecreateProbe{CandidateArtifactId: first.ArtifactID, PriorArtifactId: member.Render.PriorArtifactID, ServiceId: member.Render.ServiceID, CandidateReleaseId: member.Intent.ID, PriorReleaseId: priorReleaseID}}},
				&agentpb.ExecutionStep{StepId: compensateID, TimeoutSeconds: releaseMemberStepTimeoutSeconds, Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE, PrerequisiteStepId: healthID,
					Payload: &agentpb.ExecutionStep_ServiceRecreateCompensate{ServiceRecreateCompensate: &agentpb.ServiceRecreateCompensate{ArtifactId: member.Render.PriorArtifactID, CandidateArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, CandidateReleaseId: member.Intent.ID, PriorReleaseId: priorReleaseID, PriorTarget: string(member.Render.PriorTarget), Enabled: input.Operation.FailurePolicy == domain.OnFailureSwitchBack}}},
			)
			if index > 0 {
				steps[len(steps)-5].PrerequisiteStepId = task.Steps[base-3].ID
			}
			continue
		}
		candidateConfig, err := domain.RenderProxyConfig(member.Render.ServiceName, member.Intent.ID, member.Render.CandidateTarget, member.Render.ProxyGeneration, member.Render.ProxyPorts)
		if err != nil {
			return nil, err
		}
		priorConfig, err := domain.RenderProxyConfig(member.Render.ServiceName, priorReleaseID, member.Render.PriorTarget, member.Render.PriorProxyGeneration, member.Render.ProxyPorts)
		if err != nil {
			return nil, err
		}
		steps = append(steps,
			&agentpb.ExecutionStep{
				StepId: applyID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ComposeWorkloadApply{ComposeWorkloadApply: &agentpb.ComposeWorkloadApply{
					ArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, Target: string(member.Render.CandidateTarget),
					EnsureProxy: member.Render.PriorStrategy == domain.StrategyRecreate,
				}},
			},
			&agentpb.ExecutionStep{
				StepId: healthID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_WaitWorkloadHealthy{WaitWorkloadHealthy: &agentpb.WaitWorkloadHealthy{
					ArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, Target: string(member.Render.CandidateTarget),
				}},
			},
			&agentpb.ExecutionStep{
				StepId: switchID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ServiceProxySwitch{ServiceProxySwitch: &agentpb.ServiceProxySwitch{
					CandidateArtifactId: first.ArtifactID, PriorArtifactId: member.Render.PriorArtifactID,
					ServiceId:  member.Render.ServiceID,
					FromTarget: string(member.Render.PriorTarget), ToTarget: string(member.Render.CandidateTarget),
					ProxyGeneration: member.Render.ProxyGeneration, ConfigJson: candidateConfig.JSON,
					ConfigSha256: candidateConfig.SHA256[:], ReleaseId: member.Intent.ID,
				}},
			},
			&agentpb.ExecutionStep{
				StepId: probeID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
				Payload: &agentpb.ExecutionStep_ServiceProxyProbe{ServiceProxyProbe: &agentpb.ServiceProxyProbe{
					CandidateArtifactId: first.ArtifactID, PriorArtifactId: member.Render.PriorArtifactID,
					ServiceId:      member.Render.ServiceID,
					ExpectedTarget: string(member.Render.PriorTarget), ProxyGeneration: member.Render.PriorProxyGeneration,
					ConfigJson: priorConfig.JSON, ConfigSha256: priorConfig.SHA256[:], ReleaseId: priorReleaseID,
					AlternateTarget: string(member.Render.CandidateTarget), AlternateProxyGeneration: member.Render.ProxyGeneration,
					AlternateConfigJson: candidateConfig.JSON, AlternateConfigSha256: candidateConfig.SHA256[:],
					AlternateReleaseId: member.Intent.ID,
				}},
			},
			&agentpb.ExecutionStep{
				StepId: compensateID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
				PrerequisiteStepId: applyID,
				Payload: &agentpb.ExecutionStep_ServiceProxyCompensate{ServiceProxyCompensate: &agentpb.ServiceProxyCompensate{
					CandidateArtifactId: first.ArtifactID, PriorArtifactId: member.Render.PriorArtifactID,
					ServiceId:       member.Render.ServiceID,
					CandidateTarget: string(member.Render.CandidateTarget), PriorTarget: string(member.Render.PriorTarget),
					ProxyGeneration: member.Render.PriorProxyGeneration, ConfigJson: priorConfig.JSON,
					ConfigSha256: priorConfig.SHA256[:], PriorReleaseId: priorReleaseID,
					Enabled: input.Operation.FailurePolicy == domain.OnFailureSwitchBack,
				}},
			},
		)
		if index > 0 {
			steps[len(steps)-5].PrerequisiteStepId = task.Steps[base-3].ID
		}
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration), Operation: operation,
		TargetID: task.Target, Artifacts: artifacts, Steps: steps,
	})
}

func priorReleaseLabel(value string) string {
	if value == "" {
		return "baseline"
	}
	return value
}

func applyExternalReleaseDependencies(
	project *composetypes.Project,
	selected map[string]struct{},
	plan core.ServiceDependencyPhasePlan,
) error {
	for _, edge := range plan.Edges {
		if _, consumerSelected := selected[edge.Service]; !consumerSelected {
			continue
		}
		if _, dependencySelected := selected[edge.Dependency]; dependencySelected {
			continue
		}
		service, active := project.Services[edge.Service]
		if !active {
			var disabled bool
			service, disabled = project.DisabledServices[edge.Service]
			if !disabled {
				return errs.New(errs.KindInternal, "release dependency consumer is missing from frozen Blueprint")
			}
		}
		if service.DependsOn == nil {
			service.DependsOn = make(composetypes.DependsOnConfig)
		}
		service.DependsOn[edge.Dependency] = composetypes.ServiceDependency{
			Condition: edge.Condition.String(), Required: true,
		}
		if active {
			project.Services[edge.Service] = service
		} else {
			project.DisabledServices[edge.Service] = service
		}
	}
	return nil
}
