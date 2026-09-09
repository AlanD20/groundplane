package agent

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	composeHelperSchema       = 1
	composeObserveTimeout     = 30 * time.Second
	composeHealthPollInterval = 250 * time.Millisecond
)

type ComposeHelper interface {
	Execute(context.Context, *agentpb.ComposeHelperRequest) (*agentpb.ComposeHelperResponse, error)
}

type ComposeObserver interface {
	Observe(context.Context, *agentpb.ExecutionPlan, string) (*agentpb.ObservedProject, error)
	ObserveRestoration(context.Context, *executionplan.RestorationObservation) (*agentpb.ObservedProject, error)
}

type ComposeRuntime struct {
	helper   ComposeHelper
	observer ComposeObserver
}

type composeStepResult struct {
	Observed                 *agentpb.ObservedProject
	ExitCode                 int32
	Diagnostic               agentpb.ComposeHelperDiagnostic
	MutationAttempted        bool
	ReconciliationRequired   bool
	RestorationRequired      bool
	ProxyEvidence            *agentpb.ServiceProxyEvidence
	RecreateEvidence         *agentpb.ServiceRecreateEvidence
	CandidateAbsenceEvidence *agentpb.CandidateAbsenceEvidence
}

func NewComposeRuntime(helper ComposeHelper, observer ComposeObserver) (*ComposeRuntime, error) {
	if helper == nil || observer == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: Compose helper and observer are required")
	}
	return &ComposeRuntime{helper: helper, observer: observer}, nil
}

func (runtime *ComposeRuntime) executeStep(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
) (composeStepResult, error) {
	if runtime == nil || runtime.helper == nil || runtime.observer == nil {
		return composeStepResult{}, errs.New(errs.KindInternal, "agent: Compose runtime is not configured")
	}
	if err := ctx.Err(); err != nil {
		return composeStepResult{}, err
	}

	switch payload := step.GetPayload().(type) {
	case *agentpb.ExecutionStep_ComposeApply:
		if assignment.Plan.GetServiceLifecycleProcedure() != nil {
			return runtime.executeServiceLifecycleMutation(ctx, assignment, step, payload.ComposeApply.GetArtifactId())
		}
		return runtime.mutate(ctx, assignment, step, payload.ComposeApply.GetArtifactId(), nil)
	case *agentpb.ExecutionStep_ComposeStop:
		if assignment.Plan.GetServiceLifecycleProcedure() != nil {
			return runtime.executeServiceLifecycleMutation(ctx, assignment, step, payload.ComposeStop.GetArtifactId())
		}
		return runtime.mutate(
			ctx,
			assignment,
			step,
			payload.ComposeStop.GetArtifactId(),
			func(observed *agentpb.ObservedProject) error {
				return stoppedServices(observed, payload.ComposeStop.GetServiceIds())
			},
		)
	case *agentpb.ExecutionStep_ComposeRemove:
		if assignment.Plan.GetServiceLifecycleProcedure() != nil {
			return runtime.executeServiceLifecycleMutation(ctx, assignment, step, payload.ComposeRemove.GetArtifactId())
		}
		postcondition := func(observed *agentpb.ObservedProject) error {
			return removedServices(observed, payload.ComposeRemove.GetServiceIds())
		}
		if source := managedComponentRemovalSource(assignment.Plan, step.GetStepId()); source != nil {
			postcondition = func(observed *agentpb.ObservedProject) error {
				if err := removedServices(observed, payload.ComposeRemove.GetServiceIds()); err != nil {
					return err
				}
				return managedComponentRemovalPostcondition(observed, source)
			}
		}
		return runtime.mutate(
			ctx,
			assignment,
			step,
			payload.ComposeRemove.GetArtifactId(),
			postcondition,
		)
	case *agentpb.ExecutionStep_ManagedNetworkRemove:
		return runtime.removeManagedNetwork(ctx, assignment, step)
	case *agentpb.ExecutionStep_ManagedNetworkEnsure, *agentpb.ExecutionStep_ManagedVolumeEnsure:
		return runtime.ensureManagedComposeResource(ctx, assignment, step)
	case *agentpb.ExecutionStep_ManagedVolumeRemove:
		return runtime.removeManagedVolume(ctx, assignment, step)
	case *agentpb.ExecutionStep_WaitHealthy:
		return runtime.waitHealthy(ctx, assignment.Plan, payload.WaitHealthy)
	case *agentpb.ExecutionStep_ComposeWorkloadApply:
		return runtime.mutate(ctx, assignment, step, payload.ComposeWorkloadApply.GetArtifactId(), nil)
	case *agentpb.ExecutionStep_WaitWorkloadHealthy:
		return runtime.waitWorkloadHealthy(ctx, assignment.Plan, payload.WaitWorkloadHealthy)
	case *agentpb.ExecutionStep_ServiceProxySwitch, *agentpb.ExecutionStep_ServiceProxyProbe, *agentpb.ExecutionStep_ServiceProxyCompensate:
		return runtime.proxyProcedure(ctx, assignment, step)
	case *agentpb.ExecutionStep_ServiceRecreateAcknowledge:
		value := payload.ServiceRecreateAcknowledge
		return runtime.observeRecreateSet(ctx, assignment.Plan, value.ArtifactId, "", value.ServiceId, value.ReleaseId, "")
	case *agentpb.ExecutionStep_ServiceRecreateProbe:
		return runtime.observeRecreateRecovery(ctx, assignment, step)
	case *agentpb.ExecutionStep_ServiceRecreateCompensate:
		result, err := runtime.mutate(ctx, assignment, step, payload.ServiceRecreateCompensate.ArtifactId, nil)
		return runtime.verifyReleaseRestorationPostcondition(ctx, assignment, step, result, err)
	case *agentpb.ExecutionStep_CandidateRestorationProbe, *agentpb.ExecutionStep_CandidateRestorationCompensate:
		return runtime.candidateRestoration(ctx, assignment, step)
	default:
		return composeStepResult{}, errs.New(errs.KindInternal, "agent: Controller sent an unknown step payload")
	}
}

func (runtime *ComposeRuntime) waitWorkloadHealthy(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	wait *agentpb.WaitWorkloadHealthy,
) (composeStepResult, error) {
	result := composeStepResult{}
	artifact := composeArtifact(plan, wait.ArtifactId)
	role, slot := agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT, wait.Target
	if wait.Target == "singleton" {
		role, slot = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON, ""
	}
	name := releaseRuntimeComposeName(artifact, wait.ServiceId, slot, role)
	if name == "" {
		return result, errs.New(errs.KindInternal, "agent: release workload is missing from the plan")
	}
	ticker := time.NewTicker(composeHealthPollInterval)
	defer ticker.Stop()
	for {
		observed, err := runtime.observer.Observe(ctx, plan, wait.ArtifactId)
		if err != nil {
			result.ReconciliationRequired = true
			return result, err
		}
		result.Observed = observed
		convergence, err := evaluateReleaseWorkloadConvergence(artifact, observed, []string{name})
		if err != nil {
			result.ReconciliationRequired = true
			return result, err
		}
		if convergence.Ready {
			return result, nil
		}
		select {
		case <-ctx.Done():
			result.ReconciliationRequired = true
			return result, ctx.Err()
		case <-ticker.C:
		}
	}
}

func releaseRuntimeComposeName(
	artifact *agentpb.ComposeArtifact,
	serviceID, slot string,
	role agentpb.ComposeServiceRole,
) string {
	if artifact == nil {
		return ""
	}
	for _, service := range artifact.Services {
		if service.ServiceId == serviceID && service.Slot == slot && service.Role == role {
			return service.ComposeName
		}
	}
	return ""
}

func (runtime *ComposeRuntime) removeManagedVolume(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
) (composeStepResult, error) {
	result := composeStepResult{MutationAttempted: true}
	response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
		Schema: composeHelperSchema, AssignmentId: assignment.AssignmentID,
		TaskId: assignment.TaskID, OperationId: assignment.OperationID,
		Plan: assignment.Plan, StepId: step.GetStepId(),
		TimeoutSeconds: remainingSeconds(ctx, step.GetTimeoutSeconds()),
	})
	if err != nil {
		result.ReconciliationRequired = true
		return result, err
	}
	if response == nil || response.GetSchema() != composeHelperSchema {
		result.ReconciliationRequired = true
		return result, errs.New(errs.KindInternal, "agent: Compose helper returned an invalid response")
	}
	result.ExitCode = response.GetExitCode()
	result.Diagnostic = response.GetDiagnostic()
	if response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		response.GetExitCode() != 0 ||
		response.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		result.ReconciliationRequired = true
		return result, errs.New(errs.KindRequestFailed, "agent: managed Volume removal failed")
	}
	return result, nil
}
func (runtime *ComposeRuntime) removeManagedNetwork(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
) (composeStepResult, error) {
	result := composeStepResult{MutationAttempted: true}
	response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
		Schema: composeHelperSchema, AssignmentId: assignment.AssignmentID,
		TaskId: assignment.TaskID, OperationId: assignment.OperationID,
		Plan: assignment.Plan, StepId: step.GetStepId(),
		TimeoutSeconds: remainingSeconds(ctx, step.GetTimeoutSeconds()),
	})
	if err != nil {
		result.ReconciliationRequired = true
		return result, err
	}
	if response == nil || response.GetSchema() != composeHelperSchema {
		result.ReconciliationRequired = true
		return result, errs.New(errs.KindInternal, "agent: Compose helper returned an invalid response")
	}
	result.ExitCode = response.GetExitCode()
	result.Diagnostic = response.GetDiagnostic()
	if response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		result.ReconciliationRequired = true
		return result, errs.Newf(
			errs.KindRequestFailed,
			"agent: Compose helper failed with diagnostic %s",
			response.GetDiagnostic().String(),
		)
	}
	if response.GetExitCode() != 0 ||
		response.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		result.ReconciliationRequired = true
		return result, errs.New(errs.KindInternal, "agent: Compose helper returned an inconsistent success response")
	}
	if response.RecreateEvidence != nil {
		result.RecreateEvidence = proto.Clone(response.RecreateEvidence).(*agentpb.ServiceRecreateEvidence)
	}
	return result, nil
}

func (runtime *ComposeRuntime) mutate(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
	artifactID string,
	postcondition func(*agentpb.ObservedProject) error,
) (composeStepResult, error) {
	result := composeStepResult{MutationAttempted: true}
	response, helperErr := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
		Schema:         composeHelperSchema,
		AssignmentId:   assignment.AssignmentID,
		TaskId:         assignment.TaskID,
		OperationId:    assignment.OperationID,
		Plan:           assignment.Plan,
		StepId:         step.GetStepId(),
		TimeoutSeconds: remainingSeconds(ctx, step.GetTimeoutSeconds()),
	})

	observed, observeErr := runtime.observeAfterMutation(ctx, assignment.Plan, artifactID)
	result.Observed = observed
	if observeErr == nil && postcondition != nil {
		observeErr = postcondition(observed)
	}
	if helperErr != nil {
		result.ReconciliationRequired = true
		if observeErr != nil {
			return result, errs.Wrap(errs.KindInternal, errors.Join(helperErr, observeErr))
		}
		return result, helperErr
	}
	if response == nil || response.GetSchema() != composeHelperSchema {
		result.ReconciliationRequired = true
		return result, errs.New(errs.KindInternal, "agent: Compose helper returned an invalid response")
	}
	result.ExitCode = response.GetExitCode()
	result.Diagnostic = response.GetDiagnostic()
	if observeErr != nil {
		result.ReconciliationRequired = true
		return result, observeErr
	}
	if response.RecreateEvidence != nil {
		result.RecreateEvidence = proto.Clone(response.RecreateEvidence).(*agentpb.ServiceRecreateEvidence)
	}
	if response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		result.ReconciliationRequired = true
		return result, errs.Newf(
			errs.KindRequestFailed,
			"agent: Compose helper failed with diagnostic %s",
			response.GetDiagnostic().String(),
		)
	}
	if response.GetExitCode() != 0 ||
		response.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		result.ReconciliationRequired = true
		return result, errs.New(errs.KindInternal, "agent: Compose helper returned an inconsistent success response")
	}
	return result, nil
}

func (runtime *ComposeRuntime) observeAfterMutation(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	artifactID string,
) (*agentpb.ObservedProject, error) {
	observeCtx := ctx
	cancel := func() {}
	if ctx.Err() != nil {
		observeCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), composeObserveTimeout)
	}
	defer cancel()
	return runtime.observer.Observe(observeCtx, plan, artifactID)
}

func (runtime *ComposeRuntime) waitHealthy(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	wait *agentpb.WaitHealthy,
) (composeStepResult, error) {
	result := composeStepResult{}
	artifact := composeArtifact(plan, wait.GetArtifactId())
	if artifact == nil {
		return result, errs.New(errs.KindInternal, "agent: WaitHealthy artifact is missing from the plan")
	}
	selected, err := composeNamesForServiceIDs(artifact, wait.GetServiceIds())
	if err != nil {
		return result, err
	}

	ticker := time.NewTicker(composeHealthPollInterval)
	defer ticker.Stop()
	for {
		observed, observeErr := runtime.observer.Observe(ctx, plan, wait.GetArtifactId())
		if observeErr != nil {
			result.ReconciliationRequired = true
			return result, observeErr
		}
		result.Observed = observed
		var convergence composeConvergence
		var convergeErr error
		if len(wait.GetServiceIds()) > 0 &&
			(plan.GetServiceLifecycleProcedure() != nil || plan.GetManagedComponentProcedure() != nil ||
				plan.GetCandidateReleaseProcedure() != nil || blueprintManagedHealthSelection(plan, artifact, selected)) {
			convergence, convergeErr = evaluateLifecycleComposeConvergence(artifact, observed, selected, true)
		} else {
			convergence, convergeErr = evaluateComposeConvergence(artifact, observed, selected)
		}
		if convergeErr != nil {
			result.ReconciliationRequired = true
			return result, convergeErr
		}
		if convergence.Ready {
			return result, nil
		}
		select {
		case <-ctx.Done():
			result.ReconciliationRequired = true
			return result, ctx.Err()
		case <-ticker.C:
		}
	}
}

func composeArtifact(plan *agentpb.ExecutionPlan, artifactID string) *agentpb.ComposeArtifact {
	if plan == nil {
		return nil
	}
	for _, artifact := range plan.GetArtifacts() {
		if artifact != nil && artifact.GetArtifactId() == artifactID {
			return artifact
		}
	}
	return nil
}

func composeNamesForServiceIDs(
	artifact *agentpb.ComposeArtifact,
	serviceIDs []string,
) ([]string, error) {
	if len(serviceIDs) == 0 {
		return nil, nil
	}
	selected := make(map[string]struct{}, len(serviceIDs))
	for _, serviceID := range serviceIDs {
		selected[serviceID] = struct{}{}
	}
	names := make([]string, 0, len(serviceIDs)*2)
	for _, service := range artifact.GetServices() {
		if service != nil && service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			if _, ok := selected[service.GetServiceId()]; ok {
				names = append(names, service.GetComposeName())
			}
		}
	}
	if len(names) == 0 {
		return nil, errs.New(errs.KindInternal, "agent: selected services are missing from the artifact")
	}
	sort.Strings(names)
	return names, nil
}

func stoppedServices(observed *agentpb.ObservedProject, serviceIDs []string) error {
	selected := selectedServiceIDs(serviceIDs)
	for _, container := range observed.GetContainers() {
		if container == nil || !selectedIncludes(selected, container.GetServiceId()) {
			continue
		}
		if container.GetState() == agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING {
			return errs.Newf(
				errs.KindRequestFailed,
				"agent: service %s remained running after Compose stop",
				container.GetServiceId(),
			)
		}
	}
	return nil
}

func removedServices(observed *agentpb.ObservedProject, serviceIDs []string) error {
	selected := selectedServiceIDs(serviceIDs)
	for _, container := range observed.GetContainers() {
		if container != nil && selectedIncludes(selected, container.GetServiceId()) {
			return errs.Newf(
				errs.KindRequestFailed,
				"agent: service %s remained after Compose remove",
				container.GetServiceId(),
			)
		}
	}
	return nil
}

func selectedServiceIDs(serviceIDs []string) map[string]struct{} {
	if len(serviceIDs) == 0 {
		return nil
	}
	selected := make(map[string]struct{}, len(serviceIDs))
	for _, serviceID := range serviceIDs {
		selected[serviceID] = struct{}{}
	}
	return selected
}

func selectedIncludes(selected map[string]struct{}, serviceID string) bool {
	if len(selected) == 0 {
		return true
	}
	_, ok := selected[serviceID]
	return ok
}

func remainingSeconds(ctx context.Context, maximum uint32) uint32 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return maximum
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 1
	}
	seconds := uint32((remaining + time.Second - 1) / time.Second)
	if seconds > maximum {
		return maximum
	}
	return seconds
}
