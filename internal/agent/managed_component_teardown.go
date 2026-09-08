package agent

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// preflightManagedComponentTeardown observes every selected physical Compose
// name before the first forward mutation. Volatile plan labels may differ, but
// the existing Component and Service ownership labels must match exactly.
func (runtime *ComposeRuntime) preflightManagedComponentTeardown(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
) (string, error) {
	procedure := plan.GetManagedComponentProcedure()
	if procedure == nil {
		return "", nil
	}
	observations := make(map[string]*agentpb.ObservedProject)
	for _, source := range procedure.GetServices() {
		observed := observations[source.GetSourceArtifactId()]
		if observed == nil {
			var err error
			observed, err = runtime.observer.Observe(ctx, plan, source.GetSourceArtifactId())
			if err != nil {
				return source.GetRemoveStepId(), err
			}
			observations[source.GetSourceArtifactId()] = observed
		}
		if err := managedComponentRemovalOwnership(observed, source); err != nil {
			return source.GetRemoveStepId(), err
		}
	}
	return "", nil
}

func managedComponentRemovalOwnership(
	observed *agentpb.ObservedProject,
	source *agentpb.ManagedComponentService,
) error {
	if observed == nil || source == nil {
		return errs.New(errs.KindInternal, "agent: managed Component removal observation is incomplete")
	}
	for _, collision := range observed.GetCollisions() {
		if collision.GetKind() != agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER ||
			collision.GetComposeServiceName() != source.GetComposeServiceName() {
			continue
		}
		if collision.GetComponentId() != source.GetComponentId() ||
			collision.GetServiceId() != source.GetServiceId() {
			return errs.New(
				errs.KindStateConflict,
				"agent: foreign container occupies a managed Component Compose service name",
			)
		}
	}
	return nil
}

func managedComponentRemovalPostcondition(
	observed *agentpb.ObservedProject,
	source *agentpb.ManagedComponentService,
) error {
	if observed == nil || source == nil {
		return errs.New(errs.KindInternal, "agent: managed Component removal postcondition is incomplete")
	}
	for _, container := range observed.GetContainers() {
		if container.GetServiceId() == source.GetServiceId() {
			return errs.New(
				errs.KindRequestFailed,
				"agent: managed Component Compose service remained after removal",
			)
		}
	}
	for _, collision := range observed.GetCollisions() {
		if collision.GetKind() == agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER &&
			collision.GetComposeServiceName() == source.GetComposeServiceName() {
			return errs.New(
				errs.KindRequestFailed,
				"agent: managed Component Compose service name remained after removal",
			)
		}
	}
	return nil
}

func managedComponentRemovalSource(
	plan *agentpb.ExecutionPlan,
	stepID string,
) *agentpb.ManagedComponentService {
	for _, source := range plan.GetManagedComponentProcedure().GetServices() {
		if source.GetRemoveStepId() == stepID {
			return source
		}
	}
	return nil
}
