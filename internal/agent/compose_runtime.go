package agent

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
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
}

type ComposeRuntime struct {
	helper   ComposeHelper
	observer ComposeObserver
}

type composeStepResult struct {
	Observed               *agentpb.ObservedProject
	ExitCode               int32
	Diagnostic             agentpb.ComposeHelperDiagnostic
	MutationAttempted      bool
	ReconciliationRequired bool
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
		return runtime.mutate(ctx, assignment, step, payload.ComposeApply.GetArtifactId(), nil)
	case *agentpb.ExecutionStep_ComposeStop:
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
		return runtime.mutate(
			ctx,
			assignment,
			step,
			payload.ComposeRemove.GetArtifactId(),
			func(observed *agentpb.ObservedProject) error {
				return removedServices(observed, payload.ComposeRemove.GetServiceIds())
			},
		)
	case *agentpb.ExecutionStep_ManagedNetworkRemove:
		return runtime.removeManagedNetwork(ctx, assignment, step)
	case *agentpb.ExecutionStep_WaitHealthy:
		return runtime.waitHealthy(ctx, assignment.Plan, payload.WaitHealthy)
	default:
		return composeStepResult{}, errs.New(errs.KindInternal, "agent: Controller sent an unknown step payload")
	}
}

func (runtime *ComposeRuntime) removeManagedNetwork(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
) (composeStepResult, error) {
	result := composeStepResult{MutationAttempted: true}
	response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
		Schema: composeHelperSchema, TaskId: assignment.TaskID, OperationId: assignment.OperationID,
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
		convergence, convergeErr := evaluateComposeConvergence(artifact, observed, selected)
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
	byID := make(map[string]string, len(artifact.GetServices()))
	for _, service := range artifact.GetServices() {
		if service != nil {
			byID[service.GetServiceId()] = service.GetComposeName()
		}
	}
	names := make([]string, 0, len(serviceIDs))
	for _, serviceID := range serviceIDs {
		name := byID[serviceID]
		if name == "" {
			return nil, errs.Newf(errs.KindInternal, "agent: service id %s is missing from the artifact", serviceID)
		}
		names = append(names, name)
	}
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
