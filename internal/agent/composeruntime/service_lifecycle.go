package composeruntime

import (
	"context"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (runtime *Runtime) executeServiceLifecycleMutation(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	artifactID string,
) (StepResult, error) {
	artifact, source, err := serviceLifecycleStepSource(assignment.Plan, artifactID, step.GetStepId())
	if err != nil {
		return StepResult{}, err
	}
	if step.GetComposeStop() != nil || step.GetComposeRemove() != nil {
		observed, observeErr := runtime.observer.Observe(ctx, assignment.Plan, artifactID)
		if observeErr != nil {
			return StepResult{Observed: observed, ReconciliationRequired: true}, observeErr
		}
		if footprintErr := verifyServiceLifecycleFootprint(artifact, source, observed); footprintErr != nil {
			return StepResult{Observed: observed, ReconciliationRequired: true}, footprintErr
		}
	}
	postcondition := func(observed *agentpb.ObservedProject) error {
		switch {
		case step.GetComposeApply() != nil:
			return rejectSelectedServiceLifecycleCollisions(source, observed)
		case step.GetComposeStop() != nil:
			return serviceLifecycleStopped(artifact, source, observed)
		case step.GetComposeRemove() != nil:
			return serviceLifecycleRemoved(artifact, source, observed)
		default:
			return errs.New(errs.KindInternal, "agent: Service lifecycle step is unsupported")
		}
	}
	result, err := runtime.mutate(ctx, assignment, step, artifactID, postcondition)
	if err != nil || step.GetComposeApply() == nil {
		return result, err
	}
	if serviceLifecycleNeedsHealthPoll(artifact) {
		waitResult, waitErr := runtime.waitHealthy(ctx, assignment.Plan, &agentpb.WaitHealthy{
			ArtifactId: artifactID, ServiceIds: []string{source.GetServiceId()},
		})
		result.Observed = waitResult.Observed
		result.ReconciliationRequired = result.ReconciliationRequired || waitResult.ReconciliationRequired
		if waitErr != nil {
			return result, waitErr
		}
	}
	if result.Observed == nil {
		result.ReconciliationRequired = true
		return result, errs.New(errs.KindInternal, "agent: Service lifecycle start observation is missing")
	}
	if startErr := serviceLifecycleStarted(artifact, source, result.Observed); startErr != nil {
		result.ReconciliationRequired = true
		return result, startErr
	}
	return result, nil
}

func serviceLifecycleNeedsHealthPoll(artifact *agentpb.ComposeArtifact) bool {
	for _, service := range artifact.GetServices() {
		if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY &&
			service.GetHasHealthcheck() {
			return true
		}
	}
	return false
}

func serviceLifecycleStepSource(
	plan *agentpb.ExecutionPlan,
	artifactID string,
	stepID string,
) (*agentpb.ComposeArtifact, *agentpb.ServiceLifecycleSource, error) {
	if plan == nil || plan.GetServiceLifecycleProcedure() == nil {
		return nil, nil, errs.New(errs.KindInternal, "agent: Service lifecycle procedure is missing")
	}
	artifact := taskassignment.ComposeArtifact(plan, artifactID)
	for _, source := range plan.GetServiceLifecycleProcedure().GetSources() {
		if source.GetArtifactId() == artifactID && source.GetStepId() == stepID && artifact != nil {
			return artifact, source, nil
		}
	}
	return nil, nil, errs.New(errs.KindInternal, "agent: Service lifecycle source is missing")
}

func verifyServiceLifecycleFootprint(
	artifact *agentpb.ComposeArtifact,
	source *agentpb.ServiceLifecycleSource,
	observed *agentpb.ObservedProject,
) error {
	if err := rejectSelectedServiceLifecycleCollisions(source, observed); err != nil {
		return err
	}
	total := 0
	for _, service := range artifact.GetServices() {
		containers := containersForService(observed.GetContainers(), service)
		total += len(containers)
		if len(containers) != 0 && len(containers) != int(service.GetExpectedReplicas()) {
			return errs.New(errs.KindStateConflict, "agent: Service lifecycle runtime footprint is partial")
		}
	}
	if total == 0 {
		return nil
	}
	for _, service := range artifact.GetServices() {
		if len(containersForService(observed.GetContainers(), service)) != int(service.GetExpectedReplicas()) {
			return errs.New(errs.KindStateConflict, "agent: Service lifecycle runtime footprint is incomplete")
		}
	}
	return nil
}

func serviceLifecycleStarted(
	artifact *agentpb.ComposeArtifact,
	source *agentpb.ServiceLifecycleSource,
	observed *agentpb.ObservedProject,
) error {
	if err := rejectSelectedServiceLifecycleCollisions(source, observed); err != nil {
		return err
	}
	convergence, err := evaluateLifecycleComposeConvergence(artifact, observed, source.GetComposeNames(), false)
	if err != nil {
		return err
	}
	if !convergence.Ready {
		return errs.Newf(
			errs.KindRequestFailed,
			"agent: Service lifecycle start did not converge: %s",
			convergence.Summary,
		)
	}
	return nil
}

func serviceLifecycleStopped(
	artifact *agentpb.ComposeArtifact,
	source *agentpb.ServiceLifecycleSource,
	observed *agentpb.ObservedProject,
) error {
	if err := verifyServiceLifecycleFootprint(artifact, source, observed); err != nil {
		return err
	}
	for _, service := range artifact.GetServices() {
		for _, container := range containersForService(observed.GetContainers(), service) {
			if container.GetState() == agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING {
				return errs.New(errs.KindRequestFailed, "agent: Service lifecycle container remained running")
			}
		}
	}
	return nil
}

func serviceLifecycleRemoved(
	artifact *agentpb.ComposeArtifact,
	source *agentpb.ServiceLifecycleSource,
	observed *agentpb.ObservedProject,
) error {
	if err := rejectSelectedServiceLifecycleCollisions(source, observed); err != nil {
		return err
	}
	for _, service := range artifact.GetServices() {
		if len(containersForService(observed.GetContainers(), service)) != 0 {
			return errs.New(errs.KindRequestFailed, "agent: Service lifecycle container remained after removal")
		}
	}
	return nil
}

func rejectSelectedServiceLifecycleCollisions(
	source *agentpb.ServiceLifecycleSource,
	observed *agentpb.ObservedProject,
) error {
	names := make(map[string]bool, len(source.GetComposeNames()))
	for _, name := range source.GetComposeNames() {
		names[name] = true
	}
	for _, collision := range observed.GetCollisions() {
		if collision.GetKind() != agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER {
			continue
		}
		if names[collision.GetComposeServiceName()] ||
			collision.GetServiceId() != "" && collision.GetServiceId() == source.GetServiceId() {
			return errs.New(errs.KindStateConflict, "agent: Service lifecycle target has an ownership collision")
		}
	}
	return nil
}
