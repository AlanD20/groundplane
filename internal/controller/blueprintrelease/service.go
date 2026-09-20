package blueprintrelease

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"strings"
	"time"
)

type Service struct {
	agents      *etcd.LocalAgentRepository
	images      workloadseal.Resolver
	ledger      *etcd.ReleaseLedger
	scripts     *etcd.ScriptRepository
	plans       *taskplanning.TaskPlanResolver
	preparation *taskplanning.ScriptRunnerPreparationService
	sources     *etcd.ScriptSourceReferenceAuthority
}

func NewService(
	ledger *etcd.ReleaseLedger,
	scripts *etcd.ScriptRepository,
	plans *taskplanning.TaskPlanResolver,
	artifacts *taskplanning.ScriptArtifactService,
	sources *etcd.ScriptSourceReferenceAuthority,
	agents *etcd.LocalAgentRepository,
	images workloadseal.Resolver,
) (*Service, error) {
	if ledger == nil || scripts == nil || plans == nil || artifacts == nil || sources == nil || agents == nil ||
		images == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Release dependencies are not configured")
	}
	preparation, err := taskplanning.NewScriptRunnerPreparationService(artifacts, agents, images)
	if err != nil {
		return nil, err
	}
	return &Service{
		ledger:      ledger,
		scripts:     scripts,
		plans:       plans,
		preparation: preparation,
		sources:     sources,
		agents:      agents,
		images:      images,
	}, nil
}

type PrepareInput struct {
	IntendedAttaches []etcd.Versioned[attachrecord.Record]
	Workloads        WorkloadPreparation
	VolumeRoot       string
	Tenant           etcd.Versioned[hierarchyrecord.TenantRecord]
	Project          etcd.Versioned[hierarchyrecord.ProjectRecord]
	Environment      etcd.Versioned[hierarchyrecord.EnvironmentRecord]
	Projection       etcd.EnvironmentComposeProjection
	ServiceChanges   []etcd.EnvironmentBlueprintServiceChange
	Memberships      NormalizedServiceMemberships
	Scripts          []scriptrecord.Record
	ReleaseGroups    map[string]core.ReleaseGroupSpec
	Task             etcd.TaskRecord
	PrefixSteps      []*agentpb.ExecutionStep
	ComponentSteps   []*agentpb.ExecutionStep
	Artifact         *agentpb.ComposeArtifact
	AllocateNamed    func(ids.Kind, string) string
	CreatedAt        time.Time
}

type Prepared struct {
	Task        etcd.TaskRecord
	Plan        *agentpb.ExecutionPlan
	Publication etcd.BlueprintReleasePublication
}

func (prepared Prepared) Abandon(ctx context.Context) error { return prepared.Publication.Abandon(ctx) }

func (service *Service) Prepare(ctx context.Context, input PrepareInput) (Prepared, error) {
	if ctx == nil || service == nil || service.ledger == nil || service.plans == nil || input.AllocateNamed == nil ||
		input.Task.ID == "" || input.Task.OperationID == "" || input.Projection.RevisionID != input.Task.ID ||
		!input.CreatedAt.Equal(input.CreatedAt.UTC()) {
		return Prepared{}, errs.New(errs.KindValidationFailed, "Blueprint Release preparation is invalid")
	}
	candidates, err := selectCandidates(input.Projection, input.ServiceChanges, input.ReleaseGroups, input.Memberships)
	if err != nil {
		return Prepared{}, err
	}
	task, err := service.prepareTaskConfiguration(ctx, input, len(candidates) != 0)
	if err != nil {
		return Prepared{}, err
	}
	if task.Params == nil {
		task.Params = make(map[string]string)
	}
	procedure, validProcedure := taskcontract.ParseBlueprintComposeProcedure(
		task.Params[taskcontract.EnvironmentBlueprintProcedureParam],
	)
	if !validProcedure || procedure != taskcontract.BlueprintComposeProcedureNone {
		return Prepared{}, errs.New(errs.KindValidationFailed, "Blueprint Release Task procedure is invalid")
	}
	if len(candidates) == 0 {
		steps := append([]*agentpb.ExecutionStep(nil), input.PrefixSteps...)
		prerequisite := ""
		if len(steps) != 0 {
			prerequisite = steps[len(steps)-1].StepId
		}
		teardown, buildErr := service.plans.BlueprintManagedComponentTeardown(
			ctx,
			task,
			input.Projection,
			input.Artifact,
			prerequisite,
			false,
		)
		if buildErr != nil {
			return Prepared{}, buildErr
		}
		steps = append(steps, teardown.Steps...)
		if len(steps) != 0 {
			prerequisite = steps[len(steps)-1].StepId
		}
		managedSteps, buildErr := taskplanning.BlueprintManagedServiceSteps(task, input.Artifact, prerequisite, false)
		if buildErr != nil {
			return Prepared{}, buildErr
		}
		steps = append(steps, managedSteps...)
		steps = append(steps, input.ComponentSteps...)
		plan, buildErr := taskplan.Build(taskplan.BuildInput{
			VolumeRoot: input.VolumeRoot, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
			Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, TargetID: task.Target,
			Artifacts:                 teardown.Artifacts,
			Steps:                     steps,
			ManagedComponentProcedure: teardown.Procedure,
		})
		if buildErr != nil {
			return Prepared{}, buildErr
		}
		task.PlanHash = bytesToHex(plan.PlanHash)
		task.ComponentActionStepIDs, buildErr = executionplan.ComponentActionStepIDs(plan)
		if buildErr != nil {
			return Prepared{}, buildErr
		}
		task.Steps = taskStepRecords(plan.Steps)
		publication, buildErr := service.prepareRetainedPublication(
			ctx,
			input,
			task,
			etcd.BlueprintReleasePublication{},
		)
		if buildErr != nil {
			return Prepared{}, buildErr
		}
		return Prepared{Task: task, Plan: plan, Publication: publication}, nil
	}
	task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(
		taskcontract.BlueprintComposeProcedureCandidateReleases,
	)
	publicationID := strings.TrimPrefix(
		input.AllocateNamed(ids.KindDeployment, "blueprint-release-publication"),
		string(ids.KindDeployment)+"_",
	)
	if len(publicationID) != 26 {
		return Prepared{}, errs.New(errs.KindInternal, "Blueprint Release publication allocator is invalid")
	}
	task.Params[etcd.TaskReleasePublicationParam] = publicationID
	artifactID := task.Params[taskplanning.EnvironmentBlueprintArtifactParam]
	stage := etcd.ReleaseStage{PublicationID: publicationID, OperationID: task.OperationID, CreatedAt: input.CreatedAt,
		Members: make([]etcd.ReleaseStageMember, len(candidates))}
	members := make([]etcd.ReleaseTaskRenderMember, len(candidates))
	for index, candidate := range candidates {
		releaseID := input.AllocateNamed(ids.KindDeployment, "blueprint-release/"+candidate.Record.Desired.ID)
		_, tag, _, imageErr := releaseImage(candidate.Record.Desired.Image)
		if imageErr != nil {
			return Prepared{}, imageErr
		}
		workload, sealErr := sealedCandidate(input.Workloads.candidates, candidate.Record.Desired)
		if sealErr != nil {
			return Prepared{}, sealErr
		}
		render := etcd.ReleaseRenderInput{
			ReleaseID: releaseID, PlanID: task.PlanID, ArtifactID: artifactID,
			ServiceID: candidate.Record.Desired.ID, ServiceName: candidate.Record.Desired.Name,
			CandidateWorkload: workload, Strategy: domain.StrategyRecreate,
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
			CandidateWorkload: workload, Tag: tag, Strategy: domain.StrategyRecreate,
			OnFailure:     domain.OnFailure(candidate.Record.Desired.OnFailure.WithDefault()),
			RenderInputID: artifactID, CreatedAt: input.CreatedAt,
			Actor: "operator", OriginatingTaskID: task.ID,
			Workspace: domain.Workspace{Kind: domain.WorkspaceTenant, TenantID: input.Tenant.Record.ID,
				ProjectID: input.Project.Record.ID, EnvironmentID: input.Environment.Record.ID},
		}
		if err := service.preparePredecessor(ctx, input, candidate, &render, &intent); err != nil {
			return Prepared{}, err
		}
		if err := configureBlueprintProxy(&render, candidate.Record.Desired.Expose); err != nil {
			return Prepared{}, err
		}
		if err := service.plans.PrepareReleaseProxyImage(&render, nil); err != nil {
			return Prepared{}, err
		}
		raw, encodeErr := etcd.EncodeReleaseRenderInput(render)
		if encodeErr != nil {
			return Prepared{}, encodeErr
		}
		intent.RenderInputDigest, _ = domain.Digest(json.RawMessage(raw))
		checkpoint := domain.Checkpoint{ReleaseID: releaseID, State: domain.StatePending, UpdatedAt: input.CreatedAt}
		stage.Members[index] = etcd.ReleaseStageMember{Intent: intent, RenderInput: raw, Checkpoint: checkpoint}
		members[index] = etcd.ReleaseTaskRenderMember{Intent: intent, Render: render}
	}
	manifest, err := service.ledger.Stage(ctx, stage)
	if err != nil {
		return Prepared{}, err
	}
	hooks, err := service.prepareDeployHooks(ctx, input, manifest, task, members)
	if err != nil {
		return Prepared{}, err
	}
	task, members = hooks.task, hooks.members
	applyStepIDs, healthStepIDs := make([]string, len(members)), make([]string, len(members))
	recoveryProbeStepIDs, recoveryCompensateStepIDs := make([]string, len(members)), make([]string, len(members))
	for index, member := range members {
		applyStepIDs[index] = input.AllocateNamed(ids.KindStep, "blueprint-candidate-apply/"+member.Render.ServiceID)
		healthStepIDs[index] = input.AllocateNamed(ids.KindStep, "blueprint-candidate-health/"+member.Render.ServiceID)
		recoveryProbeStepIDs[index] = input.AllocateNamed(
			ids.KindStep,
			"blueprint-candidate-recovery-probe/"+member.Render.ServiceID,
		)
		recoveryCompensateStepIDs[index] = input.AllocateNamed(
			ids.KindStep,
			"blueprint-candidate-recovery-compensate/"+member.Render.ServiceID,
		)
	}
	task, plan, err := service.plans.PrepareBlueprintReleaseTask(ctx, task, taskplanning.BlueprintReleasePlanInput{
		NativePredecessors: nativePredecessors(input, members),
		Members:            members, PrefixSteps: input.PrefixSteps, ComponentSteps: input.ComponentSteps,
		ApplyStepIDs: applyStepIDs, HealthStepIDs: healthStepIDs,
		RecoveryProbeStepIDs: recoveryProbeStepIDs, RecoveryCompensateStepIDs: recoveryCompensateStepIDs,
		PreStepIDs: hooks.preStepIDs, PostStepIDs: hooks.postStepIDs,
	})
	if err != nil {
		return Prepared{}, err
	}
	candidateDescriptor, err := executionplan.DescribeCandidateRelease(plan)
	if err != nil {
		return Prepared{}, errs.Wrap(errs.KindInternal, err)
	}
	var executions []etcd.ScriptExecutionRecord
	if len(plan.GetScriptRunnerSnapshots()) != 0 {
		executions, err = etcd.NewScriptExecutionRecords(task, plan, input.CreatedAt)
		if err != nil {
			return Prepared{}, err
		}
	}
	if len(executions) != hooks.executions {
		return Prepared{}, errs.New(errs.KindInternal, "Blueprint post-deploy execution authority is incomplete")
	}
	hookPublications := make([]etcd.ReleaseHookExecutionPublication, len(executions))
	for index, execution := range executions {
		sources, exists := hooks.sources[execution.ID]
		if !exists {
			return Prepared{}, errs.New(errs.KindInternal, "Blueprint post-deploy execution source is missing")
		}
		hookPublications[index] = etcd.ReleaseHookExecutionPublication{Sources: sources, Execution: execution}
	}
	hookPrepared, err := service.ledger.PrepareBlueprintReleaseHooks(ctx, task, hookPublications)
	if err != nil {
		return Prepared{}, err
	}
	for index := range hookPublications {
		hookPublications[index].SnapshotRevision = hookPrepared.SnapshotRevision(
			hookPublications[index].Execution.SnapshotID,
		)
	}
	sourceMembers, err := service.ledger.BlueprintReleaseSourceMembers(ctx, manifest, hookPublications)
	if err != nil {
		return Prepared{}, err
	}
	var sourcePrepared etcd.PreparedSourceSet
	if len(sourceMembers) != 0 {
		sourcePrepared, err = service.sources.Prepare(ctx, task.OperationID, sourceMembers)
		if err != nil {
			if blueprintPublicationOutcomeUnknown(err) {
				return Prepared{}, err
			}
			if cleanupErr := service.sources.Abandon(ctx, task.OperationID, sourceMembers); cleanupErr != nil {
				return Prepared{}, errors.Join(err, cleanupErr)
			}
			return Prepared{}, err
		}
	}
	publication, err := service.ledger.PrepareBlueprintReleasePublication(
		ctx,
		service.sources,
		etcd.BlueprintReleasePublicationEvidence{
			NativePredecessors: nativePredecessorCaptures(input, members),
			Manifest:           manifest, EnvironmentID: input.Environment.Record.ID, Task: task, PublishedAt: input.CreatedAt,
			CandidateReleaseDescriptor: candidateDescriptor,
			Plan:                       plan,
			Hooks:                      hookPublications, HookPrepared: hookPrepared, SourcePrepared: sourcePrepared, SourceMembers: sourceMembers,
		},
	)
	if err != nil {
		if blueprintPublicationOutcomeUnknown(err) {
			return Prepared{}, err
		}
		if len(sourceMembers) != 0 {
			if cleanupErr := service.sources.Abandon(ctx, task.OperationID, sourceMembers); cleanupErr != nil {
				return Prepared{}, errors.Join(err, cleanupErr)
			}
		}
		return Prepared{}, err
	}
	retainedPublication, err := service.prepareRetainedPublication(ctx, input, task, publication)
	if err != nil {
		if cleanupErr := publication.Abandon(ctx); cleanupErr != nil {
			return Prepared{}, errors.Join(err, cleanupErr)
		}
		return Prepared{}, err
	}
	return Prepared{Task: task, Plan: plan, Publication: retainedPublication}, nil
}

func blueprintPublicationOutcomeUnknown(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
