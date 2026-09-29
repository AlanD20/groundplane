package composeruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (runtime *Runtime) removeServicePhysicalMember(
	ctx context.Context, assignment taskassignment.Assignment, step *agentpb.ExecutionStep,
	artifact *agentpb.ComposeArtifact, source *agentpb.ServiceLifecycleSource,
) (StepResult, error) {
	// A different captured slot may still exist until its later removal step.
	// Only that exact future name is exempt from this artifact's collision list.
	futureNames := make(map[string]bool)
	ownedNames := make(map[string]bool)
	after := false
	for _, member := range assignment.Plan.GetServiceLifecycleProcedure().GetSources() {
		for _, name := range member.GetComposeNames() {
			ownedNames[name] = true
		}
		if after {
			for _, name := range member.GetComposeNames() {
				futureNames[name] = true
			}
		}
		if member.StepId == source.StepId {
			after = true
		}
	}
	check := func(observed *agentpb.ObservedProject) error {
		if observed == nil {
			return errs.New(errs.KindStateConflict, "Service removal observation is missing")
		}
		for _, collision := range observed.GetCollisions() {
			if collision.GetKind() == agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER &&
				(collision.GetServiceId() == source.ServiceId || ownedNames[collision.GetComposeServiceName()]) &&
				!futureNames[collision.GetComposeServiceName()] {
				return errs.New(errs.KindStateConflict, "Service removal has unexpected or remaining runtime")
			}
		}
		return nil
	}
	observed, err := runtime.observer.Observe(ctx, assignment.Plan, artifact.ArtifactId)
	if err == nil {
		err = check(observed)
	}
	if err != nil {
		return StepResult{Observed: observed, ReconciliationRequired: true}, err
	}
	return runtime.mutate(ctx, assignment, step, artifact.ArtifactId, func(observed *agentpb.ObservedProject) error {
		if err := check(observed); err != nil {
			return err
		}
		return removedServices(observed, []string{source.ServiceId})
	})
}
