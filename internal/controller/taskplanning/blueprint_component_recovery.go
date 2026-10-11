package taskplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareBlueprintComponentRecovery closes one authored Component child over
// its already-rendered candidate, exact predecessor artifact, and captured
// configuration sources. It never reads desired or applied state.
func (resolver *TaskPlanResolver) PrepareBlueprintComponentRecovery(
	ctx context.Context,
	task etcd.TaskRecord,
	candidate *agentpb.ComposeArtifact,
	artifacts []*agentpb.ComposeArtifact,
	managed *agentpb.ManagedComponentProcedure,
	forward []*agentpb.ExecutionStep,
) ([]*agentpb.ExecutionStep, *agentpb.ComponentRecoveryProcedure, error) {
	componentID := task.Params[taskjournal.TaskBlueprintComponentUnitParam]
	var prior *agentpb.ManagedComponentService
	if managed != nil {
		if len(managed.GetServices()) != 1 {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Component predecessor is ambiguous")
		}
		prior = managed.GetServices()[0]
	}
	return resolver.prepareComponentRecovery(ctx, task, componentID, candidate, artifacts, prior, forward)
}

func (resolver *TaskPlanResolver) prepareComponentRecovery(
	ctx context.Context, task etcd.TaskRecord, componentID string, candidate *agentpb.ComposeArtifact,
	artifacts []*agentpb.ComposeArtifact, prior *agentpb.ManagedComponentService, forward []*agentpb.ExecutionStep,
) ([]*agentpb.ExecutionStep, *agentpb.ComponentRecoveryProcedure, error) {
	if resolver == nil || resolver.configurationRecovery == nil || ctx == nil ||
		ids.Validate(ids.KindComponent, componentID) != nil || candidate == nil ||
		candidate.GetOwnerId() != task.Owner.EnvironmentID || candidate.GetArtifactId() == "" ||
		task.TimeoutSeconds <= 0 {
		return nil, nil, errs.New(errs.KindInternal, "Blueprint Component recovery input is invalid")
	}
	artifactByID := make(map[string]*agentpb.ComposeArtifact, len(artifacts))
	for _, artifact := range artifacts {
		if artifact == nil || artifactByID[artifact.GetArtifactId()] != nil {
			return nil, nil, errs.New(errs.KindInternal, "Blueprint Component recovery artifact set is invalid")
		}
		artifactByID[artifact.GetArtifactId()] = artifact
	}
	if artifactByID[candidate.GetArtifactId()] == nil {
		return nil, nil, errs.New(errs.KindInternal, "Blueprint Component recovery candidate is absent")
	}
	procedure := &agentpb.ComponentRecoveryProcedure{
		ComponentId: componentID, CandidateArtifactId: candidate.GetArtifactId(),
	}
	steps := append([]*agentpb.ExecutionStep(nil), forward...)
	for _, step := range steps {
		if step == nil {
			return nil, nil, errs.New(errs.KindInternal, "Blueprint Component forward step is absent")
		}
		if action := step.GetComponentApply(); action != nil {
			if err := resolver.bindContainerConfigTransition(ctx, task, action); err != nil {
				return nil, nil, err
			}
			if transition := action.GetContainerConfigTransition(); transition != nil &&
				len(transition.GetPreviousSource()) != 0 {
				if procedure.GetConfigurationActionStepId() != "" {
					return nil, nil, errs.New(
						errs.KindStateConflict,
						"Component recovery has multiple discovery configurations",
					)
				}
				procedure.ConfigurationSource = &agentpb.ComponentRecoveryProcedure_ConfigurationActionStepId{
					ConfigurationActionStepId: step.GetStepId(),
				}
			}
		}
		step.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
		if componentRecoveryForwardMutation(step) {
			procedure.ForwardStepIds = append(procedure.ForwardStepIds, step.GetStepId())
		}
	}
	configuration, fileSteps, err := resolver.configurationRecoverySteps(ctx, task, candidate.GetArtifactId())
	if err != nil {
		return nil, nil, err
	}
	procedure.ConfigurationRestoration = configuration
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return nil, nil, err
	}
	probeID := ids.DeriveAt(ids.KindStep, planTime, task.PlanID, "component-runtime-probe:"+componentID)
	compensateID := ids.DeriveAt(ids.KindStep, planTime, task.PlanID, "component-runtime-restore:"+componentID)
	if prior != nil {
		if prior.GetComponentId() != componentID || artifactByID[prior.GetSourceArtifactId()] == nil {
			return nil, nil, errs.New(
				errs.KindStateConflict,
				"Component predecessor differs from its captured runtime",
			)
		}
		procedure.RuntimeTarget = &agentpb.ComponentRecoveryProcedure_Predecessor{
			Predecessor: &agentpb.ComponentPredecessorRestoration{
				PriorArtifactId: prior.GetSourceArtifactId(), ServiceId: prior.GetServiceId(),
				ProbeStepId: probeID, CompensateStepId: compensateID,
			},
		}
		if procedure.GetConfigurationActionStepId() == "" {
			if err := resolver.bindRetainedComponentConfiguration(ctx, task, procedure, prior); err != nil {
				return nil, nil, err
			}
		}
	} else {
		serviceID, projectName, err := componentRecoveryCandidateIdentity(candidate, componentID)
		if err != nil {
			return nil, nil, err
		}
		procedure.RuntimeTarget = &agentpb.ComponentRecoveryProcedure_CandidateAbsence{
			CandidateAbsence: &agentpb.ComponentCandidateAbsenceRestoration{
				ComposeProjectName: projectName, ServiceId: serviceID,
				ProbeStepId: probeID, CompensateStepId: compensateID,
			},
		}
	}
	runtimeSteps := []*agentpb.ExecutionStep{
		{
			StepId: probeID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
			Payload: &agentpb.ExecutionStep_ComponentRestorationProbe{
				ComponentRestorationProbe: &agentpb.ComponentRestorationProbe{
					ComponentId: componentID, CandidateArtifactId: candidate.GetArtifactId(),
				},
			},
		},
		{
			StepId: compensateID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
			Payload: &agentpb.ExecutionStep_ComponentRestorationCompensate{
				ComponentRestorationCompensate: &agentpb.ComponentRestorationCompensate{
					ComponentId: componentID, CandidateArtifactId: candidate.GetArtifactId(),
				},
			},
		},
	}
	steps = append(steps, fileSteps...)
	steps = append(steps, runtimeSteps...)
	return steps, procedure, nil
}

func componentRecoveryCandidateIdentity(
	artifact *agentpb.ComposeArtifact,
	componentID string,
) (string, string, error) {
	serviceID := ""
	for _, service := range artifact.GetServices() {
		if service.GetOwnerComponentId() != componentID {
			continue
		}
		if serviceID != "" || ids.Validate(ids.KindService, service.GetServiceId()) != nil {
			return "", "", errs.New(errs.KindStateConflict, "Blueprint Component candidate runtime is ambiguous")
		}
		serviceID = service.GetServiceId()
	}
	if serviceID == "" || artifact.GetProjectName() == "" {
		return "", "", errs.New(errs.KindStateConflict, "Blueprint Component candidate runtime is absent")
	}
	return serviceID, artifact.GetProjectName(), nil
}

func componentRecoveryForwardMutation(step *agentpb.ExecutionStep) bool {
	return step.GetMaterializeFile() != nil || step.GetComposeApply() != nil ||
		step.GetComposeRemove() != nil || step.GetComponentApply() != nil
}
