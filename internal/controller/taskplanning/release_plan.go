package taskplanning

import (
	"context"
	"encoding/hex"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"math"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

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
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	if resolver == nil || ctx == nil || len(input.Members) == 0 || len(input.Members) > 32 ||
		task.Executor != taskjournal.TaskExecutorAgent || task.OperationID != input.Operation.OperationID ||
		task.Params[releaserender.TaskReleasePublicationParam] != input.PublicationID {
		return etcd.TaskRecord{}, nil, errs.New(errs.KindValidationFailed, "release Task preparation is invalid")
	}
	prepared := task
	prepared.RenderGeneration = int32(input.Members[0].Render.Projection.RenderGeneration)
	if prepared.RenderGeneration <= 0 || len(prepared.Steps) < len(input.Members)*5 {
		return etcd.TaskRecord{}, nil, errs.New(errs.KindValidationFailed, "release Task procedure is invalid")
	}
	plan, err := resolver.buildReleasePlan(ctx, prepared, input)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.PlanHash)
	return prepared, plan, nil
}

func (resolver *TaskPlanResolver) resolveReleasePlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver == nil || resolver.releases == nil {
		return nil, errs.New(errs.KindInternal, "release plan resolver is not configured")
	}
	if resolver.scriptPlans != nil {
		plan, found, err := resolver.scriptPlans.GetReleaseScriptExecutionPlan(ctx, task)
		if err != nil {
			return nil, err
		}
		if found {
			return plan, nil
		}
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
		len(task.Steps) < len(input.Members)*5 {
		return nil, errs.New(errs.KindInternal, "durable release Task shape is invalid")
	}
	first := input.Members[0].Render
	expectedPhase := core.ServiceLifecycleDeploy
	if task.Type == taskjournal.TaskRollback {
		expectedPhase = core.ServiceLifecycleRollback
	}
	serviceNames := make([]string, len(first.Projection.DesiredServices))
	for index, service := range first.Projection.DesiredServices {
		serviceNames[index] = service.Desired.Name
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
	images := make(map[string]domain.WorkloadSeal, len(input.Members))
	labels := make(map[string]composerender.ComposeReleaseIdentity, len(input.Members))
	for _, member := range input.Members {
		if member.Render.PlanID != task.PlanID || member.Render.ArtifactID != first.ArtifactID ||
			member.Render.EnvironmentID != first.EnvironmentID ||
			member.Render.Projection.RevisionID != first.Projection.RevisionID ||
			member.Render.Projection.RenderGeneration != first.Projection.RenderGeneration ||
			!member.Render.ServiceDependencyPlans.Equal(first.ServiceDependencyPlans) {
			return nil, errs.New(errs.KindInternal, "release render inputs do not share one frozen projection")
		}
		if (member.Intent.PriorServingReleaseID == "") != (member.Render.PriorArtifactID == "") {
			return nil, errs.New(errs.KindInternal, "ordinary release predecessor authority is partial")
		}
		images[member.Render.ServiceName] = member.Render.CandidateWorkload
		labels[member.Render.ServiceID] = composerender.ComposeReleaseIdentity{
			ProxyImage: member.Render.ProxyImage,
			ReleaseID:  member.Render.ReleaseID, Target: member.Render.CandidateTarget, Image: member.Render.CandidateWorkload.LocalImageID,
			ServingReleaseID:       member.Intent.PriorServingReleaseID,
			ServingTarget:          member.Render.PriorTarget,
			ServingProxyGeneration: member.Render.PriorProxyGeneration, Strategy: member.Render.Strategy,
		}
		if member.Render.Strategy == domain.StrategyRecreate {
			// Recreate applies the proxy with the candidate workload; unlike
			// blue-green, no later proxy-switch step replaces this config.
			identity := labels[member.Render.ServiceID]
			identity.ServingReleaseID = member.Intent.ID
			identity.ServingTarget = member.Render.CandidateTarget
			identity.ServingProxyGeneration = member.Render.ProxyGeneration
			labels[member.Render.ServiceID] = identity
		}
	}
	releaseProjection := releaseWorkloadProjection(first.Projection)
	artifact, err := resolver.renderPinnedEnvironmentArtifactWithReleases(
		ctx,
		task,
		pinnedEnvironmentIdentity{
			TenantID: first.TenantID, TenantSlug: first.TenantSlug,
			ProjectID: first.ProjectID, ProjectSlug: first.ProjectSlug,
			EnvironmentID: first.EnvironmentID, EnvironmentName: first.EnvironmentName,
			AuthorizedVolumeDir: first.AuthorizedVolumeDir,
		},
		first.Projection.RevisionID,
		first.ArtifactID,
		releaseProjection,
		func(project *composetypes.Project, _ projectionrecord.EnvironmentComposeProjection) ([]composeidentity.Resource, error) {
			if err := projectReleaseWorkloadServices(project, releaseProjection); err != nil {
				return nil, err
			}
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
				if err := composerender.ApplySealedWorkload(&service, image); err != nil {
					return nil, err
				}
				// Explicit Deploy selects this Service, not its whole profile.
				project.Services[name] = service
				delete(project.DisabledServices, name)
			}
			if err := applyExternalReleaseDependencies(project, selected, dependencyPlan); err != nil {
				return nil, err
			}
			return managedAttachExternalNetworks(project)
		},
		labels,
	)
	if err != nil {
		return nil, err
	}
	artifacts := []*agentpb.ComposeArtifact{artifact}
	priorArtifacts := make(map[string]*agentpb.ComposeArtifact, len(input.Members))
	for _, member := range input.Members {
		if member.Render.PriorArtifactID == "" {
			if member.Render.PriorRuntime != nil {
				return nil, errs.New(errs.KindInternal, "first Release carries predecessor runtime")
			}
			continue
		}
		witness := member.Render.PriorRuntime
		if member.Render.PriorWorkload == nil || witness == nil || witness.ServiceID != member.Render.ServiceID ||
			executionplan.ValidateNativePredecessorWitness(first.EnvironmentID, witness.ServiceID,
				witness.CurrentArtifact, witness.RetainedPriorArtifact) != nil {
			return nil, errs.New(errs.KindInternal, "release prior runtime authority is missing or invalid")
		}
		for index, encoded := range [][]byte{witness.CurrentArtifact, witness.RetainedPriorArtifact} {
			if len(encoded) == 0 {
				continue
			}
			prior := &agentpb.ComposeArtifact{}
			if err := proto.Unmarshal(encoded, prior); err != nil {
				return nil, errs.Wrap(errs.KindInternal, err)
			}
			if index == 0 {
				if prior.ArtifactId != member.Render.PriorArtifactID {
					return nil, errs.New(errs.KindInternal, "release prior runtime identity changed")
				}
				priorArtifacts[member.Render.ServiceID] = prior
			}
			artifacts = append(artifacts, prior)
		}
	}
	operation := agentpb.PlanOperation_PLAN_OPERATION_DEPLOY
	if task.Type == taskjournal.TaskRollback {
		operation = agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK
	}
	steps, err := buildReleaseMemberSteps(task, input, priorArtifacts)
	if err != nil {
		return nil, err
	}
	totalPre, totalPost, totalFailure := 0, 0, 0
	for _, member := range input.Members {
		for _, hook := range member.Render.Hooks {
			switch hook.When {
			case core.ScriptPreDeploy, core.ScriptPreRollback:
				totalPre++
			case core.ScriptPostDeploy, core.ScriptPostRollback:
				totalPost++
			case core.ScriptOnFailure:
				totalFailure++
			}
		}
	}
	baseCount := len(input.Members) * 5
	if len(task.Steps) != baseCount+totalPre+totalPost+totalFailure {
		return nil, errs.New(errs.KindInternal, "release hook Task steps do not match selection")
	}
	preCursor, postCursor, failureCursor := baseCount, baseCount+totalPre, baseCount+totalPre+totalPost
	preSteps, postSteps, failureSteps := []*agentpb.ExecutionStep{}, []*agentpb.ExecutionStep{}, []*agentpb.ExecutionStep{}
	snapshots := []*agentpb.ResolvedRunnerSnapshot{}
	projections := []*agentpb.ScriptRunnerProjection{}
	bodies := []*agentpb.ScriptBodyArtifactMetadata{}
	for memberIndex, member := range input.Members {
		preCount, postCount, failureCount := 0, 0, 0
		for _, hook := range member.Render.Hooks {
			switch hook.When {
			case core.ScriptPreDeploy, core.ScriptPreRollback:
				preCount++
			case core.ScriptPostDeploy, core.ScriptPostRollback:
				postCount++
			case core.ScriptOnFailure:
				failureCount++
			}
		}
		preIDs, postIDs, failureIDs := make([]string, preCount), make([]string, postCount), make([]string, failureCount)
		for index := range preIDs {
			preIDs[index] = task.Steps[preCursor+index].ID
		}
		for index := range postIDs {
			postIDs[index] = task.Steps[postCursor+index].ID
		}
		for index := range failureIDs {
			failureIDs[index] = task.Steps[failureCursor+index].ID
		}
		preCursor, postCursor, failureCursor = preCursor+preCount, postCursor+postCount, failureCursor+failureCount
		hookOperation := domain.OperationDeploy
		if task.Type == taskjournal.TaskRollback {
			hookOperation = domain.OperationRollback
		}
		hooks, err := BuildReleaseHookPlan(ReleaseHookPlanInput{
			Operation: hookOperation, CandidateReleaseID: member.Intent.ID,
			FailureReleaseID:     domain.FailureHookTargetReleaseID(member.Intent),
			PostHookAnchorStepID: releasePostHookAnchorStepID(task, memberIndex, member.Render.Strategy),
			CompensationStepID:   task.Steps[memberIndex*5+4].ID,
			PreStepIDs:           preIDs, PostStepIDs: postIDs, FailureStepIDs: failureIDs,
			Hooks: member.Render.Hooks,
		})
		if err != nil {
			return nil, err
		}
		preSteps, postSteps, failureSteps = append(
			preSteps,
			hooks.PreSteps...), append(
			postSteps,
			hooks.PostSteps...), append(
			failureSteps,
			hooks.FailureSteps...)
		snapshots, projections, bodies = append(
			snapshots,
			hooks.Snapshots...), append(
			projections,
			hooks.Projections...), append(
			bodies,
			hooks.Bodies...)
	}
	steps = append(steps, preSteps...)
	steps = append(steps, postSteps...)
	steps = append(steps, failureSteps...)
	procedureMembers := make([]executionplan.CandidateReleaseMemberInput, len(input.Members))
	candidateServices := make([]executionplan.CandidateServiceIdentity, len(input.Members))
	for index, member := range input.Members {
		candidateServices[index] = executionplan.CandidateServiceIdentity{
			ServiceID: member.Render.ServiceID,
			ReleaseID: member.Intent.ID,
		}
	}
	for index, member := range input.Members {
		base := index * 5
		procedureMember := executionplan.CandidateReleaseMemberInput{
			ServiceID: member.Render.ServiceID, CandidateReleaseID: member.Intent.ID,
			CandidateArtifactID: first.ArtifactID,
			ForwardStepIDs:      []string{task.Steps[base].ID, task.Steps[base+1].ID, task.Steps[base+2].ID},
		}
		if member.Intent.PriorServingReleaseID == "" {
			procedureMember.CandidateAbsence = &executionplan.CandidateAbsenceInput{
				ComposeProjectName: artifact.GetProjectName(), Services: candidateServices,
				ProbeStepID: task.Steps[base+3].ID, CompensateStepID: task.Steps[base+4].ID,
			}
		} else {
			procedureMember.ServingPredecessor = &executionplan.ServingPredecessorInput{
				ProbeStepID: task.Steps[base+3].ID, CompensateStepID: task.Steps[base+4].ID,
				PriorArtifactID: member.Render.PriorArtifactID,
				PriorReleaseID:  member.Intent.PriorServingReleaseID,
				PriorTarget:     string(member.Render.PriorTarget),
			}
			if encoded := member.Render.PriorRuntime.RetainedPriorArtifact; len(encoded) != 0 {
				retained := &agentpb.ComposeArtifact{}
				if err := proto.Unmarshal(encoded, retained); err != nil {
					return nil, errs.Wrap(errs.KindInternal, err)
				}
				procedureMember.ServingPredecessor.RetainedPriorArtifactID = retained.ArtifactId
			}
		}
		procedureMembers[index] = procedureMember
	}
	procedure, err := executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
		Operation: operation, Members: procedureMembers,
	})
	if err != nil {
		return nil, err
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration), Operation: operation,
		TargetID: task.Target, Artifacts: artifacts,
		ScriptRunnerSnapshots: snapshots, ScriptRunnerProjections: projections,
		ScriptBodyArtifacts: bodies, Steps: steps, CandidateReleaseProcedure: procedure,
	})
}

func releasePostHookAnchorStepID(task etcd.TaskRecord, memberIndex int, strategy domain.Strategy) string {
	offset := 0
	if strategy == domain.StrategyRecreate {
		offset = 1
	}
	return task.Steps[memberIndex*5+offset].ID
}

// releaseWorkloadProjection narrows a Service release to durable workload
// Services. Registered Component output is validated when the Environment
// artifact is produced and is not part of a workload-targeted release.
func releaseWorkloadProjection(
	projection projectionrecord.EnvironmentComposeProjection,
) projectionrecord.EnvironmentComposeProjection {
	projection.Components = nil
	return projection
}

func projectReleaseWorkloadServices(
	project *composetypes.Project,
	projection projectionrecord.EnvironmentComposeProjection,
) error {
	if project == nil {
		return errs.New(errs.KindInternal, "release workload Compose project is missing")
	}
	desired := make(map[string]struct{}, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		desired[service.Desired.Name] = struct{}{}
	}
	found := make(map[string]struct{}, len(desired))
	for name := range project.Services {
		if _, workload := desired[name]; workload {
			found[name] = struct{}{}
			continue
		}
		delete(project.Services, name)
	}
	for name := range project.DisabledServices {
		if _, workload := desired[name]; workload {
			found[name] = struct{}{}
			continue
		}
		delete(project.DisabledServices, name)
	}
	if len(found) != len(desired) {
		return errs.New(errs.KindInternal, "release workload Service is missing from frozen Blueprint")
	}
	return nil
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
