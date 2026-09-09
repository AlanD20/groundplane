package blueprintrelease

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controller "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/distribution/reference"
)

type Service struct {
	agents    *etcd.LocalAgentRepository
	images    workloadseal.Resolver
	ledger    *etcd.ReleaseLedger
	scripts   *etcd.ScriptRepository
	plans     *controller.TaskPlanResolver
	artifacts *controller.ScriptArtifactService
	sources   *etcd.ScriptSourceReferenceAuthority
}

func NewService(
	ledger *etcd.ReleaseLedger,
	scripts *etcd.ScriptRepository,
	plans *controller.TaskPlanResolver,
	artifacts *controller.ScriptArtifactService,
	sources *etcd.ScriptSourceReferenceAuthority,
	agents *etcd.LocalAgentRepository,
	images workloadseal.Resolver,
) (*Service, error) {
	if ledger == nil || scripts == nil || plans == nil || artifacts == nil || sources == nil || agents == nil ||
		images == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Release dependencies are not configured")
	}
	return &Service{
		ledger:    ledger,
		scripts:   scripts,
		plans:     plans,
		artifacts: artifacts,
		sources:   sources,
		agents:    agents,
		images:    images,
	}, nil
}

type PrepareInput struct {
	IntendedAttaches []etcd.Versioned[etcd.AttachRecord]
	Workloads        WorkloadPreparation
	VolumeRoot       string
	Tenant           etcd.Versioned[etcd.TenantRecord]
	Project          etcd.Versioned[etcd.ProjectRecord]
	Environment      etcd.Versioned[etcd.EnvironmentRecord]
	Projection       etcd.EnvironmentComposeProjection
	ServiceChanges   []etcd.EnvironmentBlueprintServiceChange
	Memberships      NormalizedServiceMemberships
	Scripts          []etcd.ScriptRecord
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

type preparedHooks struct {
	preStepIDs  [][]string
	task        etcd.TaskRecord
	members     []etcd.ReleaseTaskRenderMember
	postStepIDs [][]string
	sources     map[string]etcd.ScriptExecutionSources
	executions  int
}

func (prepared Prepared) Abandon(ctx context.Context) error { return prepared.Publication.Abandon(ctx) }

func (service *Service) Prepare(ctx context.Context, input PrepareInput) (Prepared, error) {
	if ctx == nil || service == nil || service.ledger == nil || service.plans == nil || input.AllocateNamed == nil ||
		input.Task.ID == "" || input.Task.OperationID == "" || input.Projection.RevisionID != input.Task.ID ||
		!input.CreatedAt.Equal(input.CreatedAt.UTC()) {
		return Prepared{}, errs.New(errs.KindValidationFailed, "Blueprint Release preparation is invalid")
	}
	groupMembers := releaseGroupMembers(input.ReleaseGroups, input.ServiceChanges)
	candidates, err := selectCandidates(input.Projection, input.ServiceChanges, groupMembers, input.Memberships)
	if err != nil {
		return Prepared{}, err
	}
	task := input.Task
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
		managedSteps, buildErr := controller.BlueprintManagedServiceSteps(task, input.Artifact, prerequisite, false)
		if buildErr != nil {
			return Prepared{}, buildErr
		}
		steps = append(steps, managedSteps...)
		steps = append(steps, input.ComponentSteps...)
		plan, buildErr := controller.BuildPlan(controller.PlanBuildInput{
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
	artifactID := task.Params[controller.EnvironmentBlueprintArtifactParam]
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
	task, plan, err := service.plans.PrepareBlueprintReleaseTask(ctx, task, controller.BlueprintReleasePlanInput{
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

func (service *Service) prepareDeployHooks(
	ctx context.Context,
	input PrepareInput,
	manifest etcd.VersionedReleaseManifest,
	task etcd.TaskRecord,
	members []etcd.ReleaseTaskRenderMember,
) (preparedHooks, error) {
	result := preparedHooks{
		task: task, members: members, preStepIDs: make([][]string, len(members)), postStepIDs: make([][]string, len(members)),
		sources: make(map[string]etcd.ScriptExecutionSources),
	}
	candidateByService := make(map[string]etcd.EnvironmentBlueprintServiceChange, len(input.ServiceChanges))
	for _, change := range input.ServiceChanges {
		candidateByService[change.Record.Desired.ID] = change
	}
	scriptsByService := deployScriptsByService(input.Scripts)
	projectionValue, err := etcd.EncodeEnvironmentComposeProjectionStorage(input.Projection)
	if err != nil {
		return preparedHooks{}, err
	}
	projectionDigest := sha256.Sum256(projectionValue)
	clear(projectionValue)
	var bodyBytes uint64
	for memberIndex := range result.members {
		member := &result.members[memberIndex]
		candidate, exists := candidateByService[member.Render.ServiceID]
		if !exists {
			return preparedHooks{}, errs.New(
				errs.KindInternal,
				"Blueprint candidate Service source is missing",
			)
		}
		serviceValue, err := etcd.EncodeServiceRuntimeRecordStorage(candidate.Record)
		if err != nil {
			return preparedHooks{}, err
		}
		serviceDigest := sha256.Sum256(serviceValue)
		clear(serviceValue)
		for _, script := range scriptsByService[member.Render.ServiceID] {
			sources, err := service.scripts.LoadBlueprintReleaseHookExecutionSources(
				ctx,
				manifest.Record.PublicationID,
				script,
				candidate.Record,
				*member,
				input.Tenant,
				input.Project,
				input.Environment,
				input.Projection,
				input.IntendedAttaches,
				manifest.ReadRevision,
			)
			if err != nil {
				return preparedHooks{}, err
			}
			bindings, err := service.artifacts.BuildScriptEntryBindings(ctx, sources)
			if err != nil {
				return preparedHooks{}, err
			}
			stepID := input.AllocateNamed(
				ids.KindStep,
				fmt.Sprintf(
					"blueprint-%s/%s/%s",
					script.Desired.When,
					member.Render.ServiceID,
					sources.Script.Record.Desired.Slug,
				),
			)
			executionID, idErr := allocatedRawULID(
				input.AllocateNamed,
				"blueprint-script-execution/"+member.Render.ServiceID+"/"+sources.Script.Record.Desired.ID,
			)
			if idErr != nil {
				return preparedHooks{}, idErr
			}
			snapshotID, idErr := allocatedRawULID(
				input.AllocateNamed,
				"blueprint-script-snapshot/"+member.Render.ServiceID+"/"+sources.Script.Record.Desired.ID,
			)
			if idErr != nil {
				return preparedHooks{}, idErr
			}
			hook, err := controller.BuildReleaseHookRenderInput(ctx, controller.ManualScriptPlanInput{
				TaskID: task.ID, OperationID: task.OperationID, PlanID: task.PlanID,
				StepID: stepID, ExecutionID: executionID, SnapshotID: snapshotID,
				Sources: sources, EntryBindings: bindings,
				Candidate: &controller.BlueprintScriptCandidateSources{
					EnvironmentID:     input.Environment.Record.ID,
					RevisionID:        input.Projection.RevisionID,
					RenderGeneration:  input.Projection.RenderGeneration,
					FixedReadRevision: manifest.ReadRevision,
					ServiceSHA256:     serviceDigest,
					ProjectionSHA256:  projectionDigest,
				},
			})
			if err != nil {
				return preparedHooks{}, err
			}
			member.Render.Hooks = append(member.Render.Hooks, hook)
			if script.Desired.When == core.ScriptPreDeploy {
				result.preStepIDs[memberIndex] = append(result.preStepIDs[memberIndex], stepID)
			} else {
				result.postStepIDs[memberIndex] = append(result.postStepIDs[memberIndex], stepID)
			}
			result.sources[executionID] = sources
			result.task.Params[etcd.ReleaseHookStepMemberParam(stepID)] = strconv.Itoa(memberIndex + 1)
			result.task.Params[etcd.ReleaseHookStepExecutionParam(stepID)] = executionID
			result.executions++
			bodyBytes += uint64(hook.BodySize)
			if err := validateDeployHookBounds(result.executions, bodyBytes); err != nil {
				return preparedHooks{}, err
			}
		}
	}
	return result, nil
}

func blueprintPublicationOutcomeUnknown(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

func allocatedRawULID(allocate func(ids.Kind, string) string, name string) (string, error) {
	value := strings.TrimPrefix(allocate(ids.KindTask, name), string(ids.KindTask)+"_")
	if len(value) != 26 {
		return "", errs.New(errs.KindInternal, "Blueprint Script identity allocator is invalid")
	}
	return value, nil
}

func deployScriptsByService(
	scripts []etcd.ScriptRecord,
) map[string][]etcd.ScriptRecord {
	result := make(map[string][]etcd.ScriptRecord)
	for _, script := range scripts {
		if script.Desired.When != core.ScriptPostDeploy && script.Desired.When != core.ScriptPreDeploy {
			continue
		}
		result[script.ServiceID] = append(result[script.ServiceID], script)
	}
	for serviceID := range result {
		sort.Slice(result[serviceID], func(left, right int) bool {
			return result[serviceID][left].Desired.Slug <
				result[serviceID][right].Desired.Slug
		})
	}
	return result
}

func validateDeployHookBounds(executions int, bodyBytes uint64) error {
	if executions > taskcontract.MaximumBlueprintPostDeployHooks || bodyBytes > 1<<20 {
		return errs.New(
			errs.KindValidationFailed,
			"Blueprint deploy Script selection exceeds its operation bounds",
		)
	}
	return nil
}

func selectCandidates(
	projection etcd.EnvironmentComposeProjection,
	changes []etcd.EnvironmentBlueprintServiceChange,
	groupMembers map[string]struct{},
	memberships NormalizedServiceMemberships,
) ([]etcd.EnvironmentBlueprintServiceChange, error) {
	if !memberships.initialized {
		return nil, errs.New(errs.KindInternal, "Blueprint normalized Service memberships are absent")
	}
	selected := make(map[string]etcd.EnvironmentBlueprintServiceChange)
	for _, change := range changes {
		service := change.Record.Desired
		candidateMembership, candidateExists := memberships.candidate[service.Name]
		if !candidateExists ||
			(candidateMembership != blueprintServiceActive && candidateMembership != blueprintServiceProfileDisabled) {
			return nil, errs.New(errs.KindInternal, "Blueprint candidate Service is absent from its sealed projection")
		}
		previousMembership, previousExists := memberships.previous[service.Name]
		if change.Current == nil && previousExists {
			return nil, errs.New(errs.KindInternal, "Blueprint new Service exists in its predecessor projection")
		}
		if change.Current != nil && (!previousExists ||
			(previousMembership != blueprintServiceActive && previousMembership != blueprintServiceProfileDisabled)) {
			return nil, errs.New(
				errs.KindInternal,
				"Blueprint existing Service is absent from its predecessor projection",
			)
		}
		if candidateMembership == blueprintServiceProfileDisabled {
			continue
		}
		if _, grouped := groupMembers[service.ID]; grouped {
			continue
		}
		if change.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
			continue
		}
		material := change.Current == nil
		if change.Current != nil {
			before, beforeErr := json.Marshal(change.Current.Record.Desired)
			after, afterErr := json.Marshal(change.Record.Desired)
			if beforeErr != nil || afterErr != nil {
				return nil, errs.New(errs.KindInternal, "Blueprint Service material comparison failed")
			}
			material = previousMembership != candidateMembership || !bytes.Equal(before, after) ||
				memberships.previousNative[service.Name] != memberships.candidateNative[service.Name]
		}
		if material {
			selected[service.Name] = change
		}
	}
	ordered := make([]etcd.EnvironmentBlueprintServiceChange, 0, len(selected))
	seen := make(map[string]struct{}, len(selected))
	for _, name := range projection.DeployDependencyPlan.OrderedServices {
		if change, exists := selected[name]; exists {
			ordered = append(ordered, change)
			seen[name] = struct{}{}
		}
	}
	remaining := make([]string, 0, len(selected)-len(seen))
	for name := range selected {
		if _, exists := seen[name]; !exists {
			remaining = append(remaining, name)
		}
	}
	sort.Strings(remaining)
	for _, name := range remaining {
		ordered = append(ordered, selected[name])
	}
	return ordered, nil
}

func releaseGroupMembers(
	groups map[string]core.ReleaseGroupSpec,
	changes []etcd.EnvironmentBlueprintServiceChange,
) map[string]struct{} {
	byName := make(map[string]string, len(changes))
	for _, change := range changes {
		byName[change.Record.Desired.Name] = change.Record.Desired.ID
	}
	result := make(map[string]struct{})
	for _, group := range groups {
		for _, name := range group.Services {
			if serviceID := byName[name]; serviceID != "" {
				result[serviceID] = struct{}{}
			}
		}
	}
	return result
}

func releaseImage(value string) (string, string, string, error) {
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return "", "", "", errs.New(errs.KindValidationFailed, "Blueprint candidate image reference is invalid")
	}
	tag := "blueprint"
	if tagged, ok := named.(reference.NamedTagged); ok {
		tag = tagged.Tag()
	}
	digest := ""
	if digested, ok := named.(reference.Digested); ok {
		digest = digested.Digest().Encoded()
	}
	return named.String(), tag, digest, nil
}

func taskStepRecords(steps []*agentpb.ExecutionStep) []etcd.TaskStepRecord {
	result := make([]etcd.TaskStepRecord, len(steps))
	for index, step := range steps {
		result[index] = etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: step.StepId}
	}
	return result
}

func bytesToHex(value []byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, len(value)*2)
	for index, item := range value {
		result[index*2], result[index*2+1] = digits[item>>4], digits[item&15]
	}
	return string(result)
}
