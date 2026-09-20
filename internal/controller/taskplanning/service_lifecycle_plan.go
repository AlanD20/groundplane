package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"gopkg.in/yaml.v3"
)

const ServiceStopGraceSeconds = uint32(30)

type serviceLifecyclePlanReader interface {
	GetServiceLifecycleRenderInput(
		context.Context,
		string,
	) (etcdstore.Versioned[etcd.ServiceLifecycleRenderInput], bool, error)
}

type serviceLifecycleHookInputResolver interface {
	ResolveLifecycleHookInput(
		context.Context,
		etcd.TaskRecord,
		string,
		backinghook.Event,
		BackingHookInputConsumer,
	) error
	ResolveDraftLifecycleHookInput(
		context.Context,
		etcd.TaskRecord,
		*etcd.BackingHookEncryptedInputs,
		string,
		backinghook.Event,
		BackingHookInputConsumer,
	) error
}

func (resolver *TaskPlanResolver) PrepareServiceLifecycleTask(
	ctx context.Context,
	task etcd.TaskRecord,
	input etcd.ServiceLifecycleRenderInput,
	stepIDs []string,
) (etcd.TaskRecord, error) {
	return resolver.prepareServiceLifecycleTask(ctx, task, input, nil, stepIDs)
}

func (resolver *TaskPlanResolver) PrepareServiceLifecycleHookTask(
	ctx context.Context,
	task etcd.TaskRecord,
	input etcd.ServiceLifecycleRenderInput,
	hookInputs *etcd.BackingHookEncryptedInputs,
	stepIDs []string,
) (etcd.TaskRecord, error) {
	return resolver.prepareServiceLifecycleTask(ctx, task, input, hookInputs, stepIDs)
}

func (resolver *TaskPlanResolver) prepareServiceLifecycleTask(
	ctx context.Context,
	task etcd.TaskRecord,
	input etcd.ServiceLifecycleRenderInput,
	hookInputs *etcd.BackingHookEncryptedInputs,
	stepIDs []string,
) (etcd.TaskRecord, error) {
	wantSteps := 1
	if input.Release.RetainedPrior != nil {
		wantSteps++
	}
	if _, definition := serviceLifecycleHook(input, task.Type); definition != nil {
		wantSteps++
	}
	if resolver == nil || ctx == nil || len(stepIDs) != wantSteps ||
		task.Executor != etcd.TaskExecutorAgent || task.PlanID != input.PlanID || task.Target != input.ServiceID {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Service lifecycle Task preparation is invalid")
	}
	for _, stepID := range stepIDs {
		if ids.Validate(ids.KindStep, stepID) != nil {
			return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Service lifecycle step identity is invalid")
		}
	}
	if input.Projection.RenderGeneration == 0 || input.Projection.RenderGeneration > uint64(^uint32(0)>>1) {
		return etcd.TaskRecord{}, errs.New(errs.KindStateConflict, "Service render generation exceeds Task limits")
	}
	prepared := task
	prepared.RenderGeneration = int32(input.Projection.RenderGeneration)
	prepared.Params = map[string]string{
		etcd.TaskServiceEnvironmentParam: input.EnvironmentID,
		etcd.TaskComposeArtifactParam:    input.ArtifactID,
	}
	prepared.Steps = make([]etcd.TaskStepRecord, len(stepIDs))
	for index, stepID := range stepIDs {
		prepared.Steps[index] = etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: stepID}
	}
	plan, err := resolver.buildServiceLifecyclePlanWithHookInputs(ctx, prepared, input, hookInputs)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.PlanHash)
	return prepared, nil
}

func (resolver *TaskPlanResolver) resolveServiceLifecyclePlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.services.(serviceLifecyclePlanReader)
	if !ok || reader == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle render input reader is not configured")
	}
	input, found, err := reader.GetServiceLifecycleRenderInput(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if !found || input.Record.PlanID != task.PlanID || input.Record.ServiceID != task.Target ||
		input.Record.Projection.RenderGeneration != uint64(task.RenderGeneration) {
		return nil, errs.New(errs.KindInternal, "Service lifecycle render input does not match Task")
	}
	return resolver.buildServiceLifecyclePlan(ctx, task, input.Record)
}

func (resolver *TaskPlanResolver) buildServiceLifecyclePlan(
	ctx context.Context,
	task etcd.TaskRecord,
	input etcd.ServiceLifecycleRenderInput,
) (*agentpb.ExecutionPlan, error) {
	return resolver.buildServiceLifecyclePlanWithHookInputs(ctx, task, input, nil)
}

func (resolver *TaskPlanResolver) buildServiceLifecyclePlanWithHookInputs(
	ctx context.Context,
	task etcd.TaskRecord,
	input etcd.ServiceLifecycleRenderInput,
	hookInputs *etcd.BackingHookEncryptedInputs,
) (*agentpb.ExecutionPlan, error) {
	event, definition := serviceLifecycleHook(input, task.Type)
	wantSteps := 1
	if input.Release.RetainedPrior != nil {
		wantSteps++
	}
	if definition != nil {
		wantSteps++
	}
	if len(task.Steps) != wantSteps ||
		task.Params[etcd.TaskServiceEnvironmentParam] != input.EnvironmentID ||
		task.Params[etcd.TaskComposeArtifactParam] != input.ArtifactID {
		return nil, errs.New(errs.KindInternal, "Service lifecycle Task procedure changed")
	}
	phase := core.ServiceLifecyclePhase("")
	if task.Type == etcd.TaskStart {
		phase = core.ServiceLifecycleStart
	}
	artifacts, err := resolver.renderServiceLifecycleArtifacts(ctx, input, phase)
	if err != nil {
		return nil, err
	}
	composeTask := task
	composeTask.Steps = task.Steps
	if definition != nil {
		if task.Type == etcd.TaskStart {
			composeTask.Steps = task.Steps[:len(task.Steps)-1]
		} else {
			composeTask.Steps = task.Steps[1:]
		}
	}
	operation, composeSteps, err := serviceLifecycleProcedure(composeTask, artifacts)
	if err != nil {
		return nil, err
	}
	steps := composeSteps
	if definition != nil {
		inputs, ok := resolver.attachIdentities.(serviceLifecycleHookInputResolver)
		if !ok || inputs == nil {
			return nil, errs.New(errs.KindInternal, "Service lifecycle hook input resolver is not configured")
		}
		hookRecord := task.Steps[0]
		if task.Type == etcd.TaskStart {
			hookRecord = task.Steps[len(task.Steps)-1]
		}
		consume := func(resolved backinghook.Input) error {
			hookStep, buildErr := backingHookStep(hookRecord, *definition, resolved, nil)
			if buildErr != nil {
				return buildErr
			}
			if task.Type == etcd.TaskStart {
				hookStep.PrerequisiteStepId = composeSteps[len(composeSteps)-1].StepId
				steps = append(composeSteps, hookStep)
			} else {
				composeSteps[0].PrerequisiteStepId = hookStep.StepId
				steps = append([]*agentpb.ExecutionStep{hookStep}, composeSteps...)
			}
			return nil
		}
		if hookInputs != nil {
			err = inputs.ResolveDraftLifecycleHookInput(ctx, task, hookInputs, task.Target, event, consume)
		} else {
			err = inputs.ResolveLifecycleHookInput(ctx, task, task.Target, event, consume)
		}
		if err != nil {
			return nil, err
		}
		defer func() {
			for _, step := range steps {
				taskplan.ClearBackingHookProcedure(step.GetBackingHookProcedure())
			}
		}()
	}
	sources := make([]*agentpb.ServiceLifecycleSource, len(artifacts))
	renders := []etcd.ReleaseRenderInput{input.Release.Current}
	if input.Release.RetainedPrior != nil {
		renders = append(renders, *input.Release.RetainedPrior)
	}
	for index, artifact := range artifacts {
		names := make([]string, len(artifact.Services))
		for serviceIndex, service := range artifact.Services {
			names[serviceIndex] = service.ComposeName
		}
		sort.Strings(names)
		sources[index] = &agentpb.ServiceLifecycleSource{
			ArtifactId: artifact.ArtifactId, SourcePlanId: renders[index].PlanID,
			SourceRenderGeneration: renders[index].Projection.RenderGeneration,
			ServiceId:              input.ServiceID, ComposeNames: names, StepId: steps[index].StepId,
		}
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration), Operation: operation,
		TargetID: task.Target, Artifacts: artifacts, Steps: steps,
		ServiceLifecycleProcedure: &agentpb.ServiceLifecycleProcedure{Sources: sources},
	})
}

func serviceLifecycleHook(
	input etcd.ServiceLifecycleRenderInput,
	taskType etcd.TaskType,
) (backinghook.Event, *backinghook.Definition) {
	if input.HookConfiguration == nil {
		return "", nil
	}
	switch taskType {
	case etcd.TaskStart:
		return backinghook.AfterStart, input.HookConfiguration.AfterStart
	case etcd.TaskStop, etcd.TaskDestroy:
		return backinghook.BeforeStop, input.HookConfiguration.BeforeStop
	default:
		return "", nil
	}
}

func (resolver *TaskPlanResolver) renderServiceLifecycleArtifacts(
	ctx context.Context,
	input etcd.ServiceLifecycleRenderInput,
	phase core.ServiceLifecyclePhase,
) ([]*agentpb.ComposeArtifact, error) {
	current, err := resolver.renderServiceLifecycleArtifact(ctx, input.Release.Current, phase, true)
	if err != nil {
		return nil, err
	}
	artifacts := []*agentpb.ComposeArtifact{current}
	if input.Release.RetainedPrior != nil {
		prior, renderErr := resolver.renderServiceLifecycleArtifact(ctx, *input.Release.RetainedPrior, phase, false)
		if renderErr != nil {
			return nil, renderErr
		}
		artifacts = append(artifacts, prior)
	}
	return artifacts, nil
}

func (resolver *TaskPlanResolver) renderServiceLifecycleArtifact(
	ctx context.Context,
	source etcd.ReleaseRenderInput,
	phase core.ServiceLifecyclePhase,
	includeProxy bool,
) (*agentpb.ComposeArtifact, error) {
	projection := releaseWorkloadProjection(source.Projection)
	selected := make([]etcd.EnvironmentServiceProjection, 0, 1)
	for _, service := range projection.DesiredServices {
		if service.Desired.ID == source.ServiceID {
			selected = append(selected, service)
		}
	}
	projection.DesiredServices = selected
	if len(selected) != 1 {
		return nil, errs.New(errs.KindInternal, "lifecycle source projection does not contain selected Service")
	}
	sourceTask := etcd.TaskRecord{PlanID: source.PlanID, RenderGeneration: int32(source.Projection.RenderGeneration)}
	identity := ComposeReleaseIdentity{
		ProxyImage: source.ProxyImage, ReleaseID: source.ReleaseID, Target: source.CandidateTarget,
		Image: source.CandidateWorkload.LocalImageID, ServingReleaseID: source.ReleaseID,
		ServingTarget: source.CandidateTarget, ServingProxyGeneration: source.ProxyGeneration,
		Strategy: source.Strategy,
	}
	artifact, err := resolver.renderPinnedEnvironmentArtifactForPhaseWithReleases(
		ctx, sourceTask,
		pinnedEnvironmentIdentity{
			TenantID: source.TenantID, TenantSlug: source.TenantSlug,
			ProjectID: source.ProjectID, ProjectSlug: source.ProjectSlug,
			EnvironmentID: source.EnvironmentID, EnvironmentName: source.EnvironmentName,
			AuthorizedVolumeDir: source.AuthorizedVolumeDir,
		},
		projection.RevisionID, source.ArtifactID, projection, phase,
		func(project *composetypes.Project, _ etcd.EnvironmentComposeProjection) ([]composeidentity.Resource, error) {
			if err := projectReleaseWorkloadServices(project, projection); err != nil {
				return nil, err
			}
			service, active := project.Services[source.ServiceName]
			if !active {
				service = project.DisabledServices[source.ServiceName]
			}
			service.DependsOn = nil
			if err := applySealedWorkload(&service, source.CandidateWorkload); err != nil {
				return nil, err
			}
			if active {
				project.Services[source.ServiceName] = service
			} else {
				project.DisabledServices[source.ServiceName] = service
			}
			return managedAttachExternalNetworks(project)
		},
		map[string]ComposeReleaseIdentity{source.ServiceID: identity},
	)
	if err != nil {
		return nil, err
	}
	allowed := map[string]bool{}
	workloadName, err := domain.WorkloadComposeName(source.ServiceName, source.CandidateTarget)
	if err != nil {
		return nil, err
	}
	if len(source.ProxyPorts) == 0 {
		workloadName = source.ServiceName
	}
	allowed[workloadName] = true
	if includeProxy && len(source.ProxyPorts) != 0 {
		allowed[source.ServiceName] = true
	}
	return pruneServiceLifecycleArtifact(artifact, allowed)
}

func pruneServiceLifecycleArtifact(
	artifact *agentpb.ComposeArtifact,
	allowed map[string]bool,
) (*agentpb.ComposeArtifact, error) {
	if artifact == nil || len(allowed) == 0 {
		return nil, errs.New(errs.KindInternal, "lifecycle artifact selection is empty")
	}
	var document yaml.Node
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "lifecycle Compose document is invalid")
	}
	root := document.Content[0]
	for index := 0; index+1 < len(root.Content); index += 2 {
		if root.Content[index].Value != "services" {
			continue
		}
		services := root.Content[index+1]
		kept := make([]*yaml.Node, 0, len(services.Content))
		for item := 0; item+1 < len(services.Content); item += 2 {
			if allowed[services.Content[item].Value] {
				kept = append(kept, services.Content[item], services.Content[item+1])
			}
		}
		services.Content = kept
	}
	encoded, err := yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	services := make([]*agentpb.ComposeService, 0, len(allowed))
	for _, service := range artifact.Services {
		if allowed[service.ComposeName] {
			services = append(services, service)
			delete(allowed, service.ComposeName)
		}
	}
	if len(allowed) != 0 {
		return nil, errs.New(errs.KindInternal, "lifecycle artifact selection is incomplete")
	}
	sort.Slice(services, func(i, j int) bool { return services[i].ComposeName < services[j].ComposeName })
	artifact.Services = services
	artifact.CanonicalYaml = encoded
	digest := sha256.Sum256(encoded)
	artifact.YamlSha256 = digest[:]
	return artifact, nil
}

func serviceLifecycleProcedure(
	task etcd.TaskRecord,
	artifacts []*agentpb.ComposeArtifact,
) (agentpb.PlanOperation, []*agentpb.ExecutionStep, error) {
	if len(artifacts) != len(task.Steps) || len(artifacts) == 0 {
		return agentpb.PlanOperation_PLAN_OPERATION_UNSPECIFIED, nil, errs.New(
			errs.KindInternal,
			"Service lifecycle artifact procedure is incomplete",
		)
	}
	steps := make([]*agentpb.ExecutionStep, len(artifacts))
	for index, artifact := range artifacts {
		step := &agentpb.ExecutionStep{StepId: task.Steps[index].ID, TimeoutSeconds: uint32(task.TimeoutSeconds)}
		if index > 0 {
			step.PrerequisiteStepId = steps[index-1].StepId
		}
		switch task.Type {
		case etcd.TaskStart:
			step.Payload = &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: artifact.ArtifactId, ServiceIds: []string{task.Target}, FullReconcile: false,
			}}
		case etcd.TaskStop:
			step.Payload = &agentpb.ExecutionStep_ComposeStop{ComposeStop: &agentpb.ComposeStop{
				ArtifactId: artifact.ArtifactId, ServiceIds: []string{task.Target}, GraceSeconds: ServiceStopGraceSeconds,
			}}
		case etcd.TaskDestroy:
			step.Payload = &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: artifact.ArtifactId, ServiceIds: []string{task.Target}, WholeProject: false,
			}}
		default:
			return agentpb.PlanOperation_PLAN_OPERATION_UNSPECIFIED, nil,
				errs.New(errs.KindInternal, "Service lifecycle Task type is invalid")
		}
		steps[index] = step
	}
	switch task.Type {
	case etcd.TaskStart:
		return agentpb.PlanOperation_PLAN_OPERATION_START, steps, nil
	case etcd.TaskStop:
		return agentpb.PlanOperation_PLAN_OPERATION_STOP, steps, nil
	case etcd.TaskDestroy:
		return agentpb.PlanOperation_PLAN_OPERATION_DESTROY, steps, nil
	default:
		return agentpb.PlanOperation_PLAN_OPERATION_UNSPECIFIED, nil,
			errs.New(errs.KindInternal, "Service lifecycle Task type is invalid")
	}
}
