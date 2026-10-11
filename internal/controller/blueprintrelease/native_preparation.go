package blueprintrelease

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/rollingpredecessor"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// preparedSelectedRuntime owns staged native rollout inputs, selected hooks and
// their sources. It does not publish a Task, hold a Release fence or establish
// applied runtime. Final file binding and publication remain separate decisions.
type preparedSelectedRuntime struct {
	task         etcd.TaskRecord
	runtime      taskplanning.BlueprintReleaseRuntime
	members      []releaserender.ReleaseTaskRenderMember
	hooks        preparedHooks
	manifest     releases.VersionedReleaseManifest
	predecessors []etcd.BlueprintNativePredecessorCapture
}

func (service *Service) stageSelectedRuntime(
	ctx context.Context,
	input prepareInput,
	candidates []blueprints.EnvironmentBlueprintServiceChange,
	task etcd.TaskRecord,
	addresses networkreservations.ProxyAddresses,
) (preparedSelectedRuntime, error) {
	var err error
	task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(
		taskcontract.BlueprintComposeProcedureCandidateReleases,
	)
	publicationID := strings.TrimPrefix(
		input.AllocateNamed(ids.KindDeployment, "blueprint-release-publication"),
		string(ids.KindDeployment)+"_",
	)
	if len(publicationID) != 26 {
		return preparedSelectedRuntime{}, errs.New(
			errs.KindInternal,
			"Blueprint Release publication allocator is invalid",
		)
	}
	task.Params[releaserender.TaskReleasePublicationParam] = publicationID
	artifactID := task.Params[taskcontract.EnvironmentBlueprintArtifactParam]
	stage := releases.ReleaseStage{
		PublicationID: publicationID,
		OperationID:   task.OperationID,
		CreatedAt:     input.CreatedAt,
		Members:       make([]releases.ReleaseStageMember, len(candidates)),
	}
	members := make([]releaserender.ReleaseTaskRenderMember, len(candidates))
	for index, candidate := range candidates {
		strategy, strategyErr := domain.DeploymentStrategy("", string(candidate.Record.Desired.Strategy))
		if strategyErr != nil {
			return preparedSelectedRuntime{}, strategyErr
		}
		releaseID := input.AllocateNamed(ids.KindDeployment, "blueprint-release/"+candidate.Record.Desired.ID)
		_, tag, _, imageErr := releaseImage(candidate.Record.Desired.Image)
		if imageErr != nil {
			return preparedSelectedRuntime{}, imageErr
		}
		workload, sealErr := sealedCandidate(input.Workloads.candidates, candidate.Record.Desired)
		if sealErr != nil {
			return preparedSelectedRuntime{}, sealErr
		}
		render := releaserender.ReleaseRenderInput{
			ProxyAddresses: addresses.ForService(candidate.Record.Desired.ID),
			ReleaseID:      releaseID, PlanID: task.PlanID, ArtifactID: artifactID,
			ServiceID: candidate.Record.Desired.ID, ServiceName: candidate.Record.Desired.Name,
			CandidateWorkload: workload, Strategy: strategy,
			PriorStrategy: domain.StrategyRecreate, CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
			ServiceDependencyPlans: input.Projection.ServiceDependencyPlans.Clone(),
			TenantID:               input.Tenant.Record.ID, TenantSlug: input.Tenant.Record.Slug,
			ProjectID: input.Project.Record.ID, ProjectSlug: input.Project.Record.Slug,
			EnvironmentID: input.Environment.Record.ID, EnvironmentName: input.Environment.Record.Name,
			AuthorizedVolumeDir: input.Environment.Record.VolumeDir, Projection: input.Projection,
		}
		intent := domain.Intent{
			ID: releaseID, EnvironmentID: input.Environment.Record.ID, ServiceID: render.ServiceID,
			OperationID: task.OperationID, OperationKind: domain.OperationBlueprintApply,
			CandidateWorkload: workload, Tag: tag, Strategy: strategy,
			OnFailure:     domain.OnFailure(candidate.Record.Desired.OnFailure.WithDefault()),
			RenderInputID: artifactID, CreatedAt: input.CreatedAt,
			Actor: "operator", OriginatingTaskID: input.DesiredRevisionID,
			Workspace: domain.Workspace{Kind: domain.WorkspaceTenant, TenantID: input.Tenant.Record.ID,
				ProjectID: input.Project.Record.ID, EnvironmentID: input.Environment.Record.ID},
		}
		if input.Tenant.Record.ID == "" {
			intent.Workspace.Kind = domain.WorkspacePlatform
		}
		if err := service.preparePredecessor(ctx, input, candidate, &render, &intent); err != nil {
			return preparedSelectedRuntime{}, err
		}
		render.Slot = domain.InactiveSlot(strategy, render.PriorSlot)
		render.CandidateTarget, err = domain.TargetFor(strategy, render.Slot)
		if err != nil {
			return preparedSelectedRuntime{}, err
		}
		intent.Slot = render.Slot
		if candidate.Record.Runtime.PostgresToolsImage != "" && intent.PriorServingReleaseID == "" &&
			intent.OnFailure != domain.OnFailureLeaveActive {
			return preparedSelectedRuntime{}, errs.New(errs.KindValidationFailed,
				"first PostgreSQL image Deploy requires leave_active; no prior Release exists")
		}
		expose := candidate.Record.Desired.ProxyExposures()
		if err := rollingpredecessor.Capture(ctx, service.agents, service.observer, &render, candidate.Record.Desired.RollingReplicaTimeoutSeconds); err != nil {
			return preparedSelectedRuntime{}, err
		}
		if err := configureBlueprintProxy(&render, expose); err != nil {
			return preparedSelectedRuntime{}, err
		}
		var priorRender *releaserender.ReleaseRenderInput
		if captured, found := input.Workloads.predecessors[render.ServiceName]; found && captured.serving != nil {
			priorRender = &captured.historical.Record
		}
		if frozen, found := input.Workloads.proxyImages[render.ServiceName]; found {
			render.ProxyImage = &frozen
		}
		if err := service.plans.PrepareReleaseProxyImage(&render, priorRender); err != nil {
			return preparedSelectedRuntime{}, err
		}
		raw, encodeErr := releaserender.EncodeReleaseRenderInput(render)
		if encodeErr != nil {
			return preparedSelectedRuntime{}, encodeErr
		}
		intent.RenderInputDigest, _ = domain.Digest(json.RawMessage(raw))
		checkpoint := domain.Checkpoint{ReleaseID: releaseID, State: domain.StatePending, UpdatedAt: input.CreatedAt}
		stage.Members[index] = releases.ReleaseStageMember{Intent: intent, RenderInput: raw, Checkpoint: checkpoint}
		members[index] = releaserender.ReleaseTaskRenderMember{Intent: intent, Render: render}
	}
	manifest, err := service.ledger.Stage(ctx, stage)
	if err != nil {
		return preparedSelectedRuntime{}, err
	}
	hooks, err := service.prepareDeployHooks(ctx, input, manifest, task, members)
	if err != nil {
		return preparedSelectedRuntime{}, err
	}
	task, members = hooks.task, hooks.members
	memberSteps := make([]taskplanning.ReleaseMemberStepIDs, len(members))
	for index, member := range members {
		memberSteps[index], err = taskplanning.AllocateReleaseMemberSteps(member.Render, func(name string) string {
			return input.AllocateNamed(ids.KindStep, "blueprint-"+name)
		})
		if err != nil {
			return preparedSelectedRuntime{}, err
		}
	}
	task, runtime, err := service.plans.PrepareBlueprintReleaseRuntime(
		ctx,
		task,
		taskplanning.BlueprintReleasePlanInput{
			NativePredecessors: nativePredecessors(input, members),
			Members:            members, PrefixSteps: input.PrefixSteps,
			MemberSteps: memberSteps,
			PreStepIDs:  hooks.preStepIDs, PostStepIDs: hooks.postStepIDs,
		},
	)
	if err != nil {
		return preparedSelectedRuntime{}, err
	}

	return preparedSelectedRuntime{
		task: task, runtime: runtime, members: members, hooks: hooks, manifest: manifest,
		predecessors: nativePredecessorCaptures(input, members),
	}, nil
}
