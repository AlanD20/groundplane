package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (observer *fakeComposeObserver) ObserveReleaseRestoration(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	_ string,
	artifactID string,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, plan, artifactID)
}

func (observer releaseExecutionObserver) ObserveReleaseRestoration(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	_ string,
	artifactID string,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, plan, artifactID)
}

func (observer negativeProbeObserver) ObserveReleaseRestoration(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	_ string,
	artifactID string,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, plan, artifactID)
}

func (observer blueprintPhaseObserver) ObserveReleaseRestoration(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	_ string,
	artifactID string,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, plan, artifactID)
}

func (observer *recreateArtifactObserver) ObserveReleaseRestoration(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	_ string,
	artifactID string,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, plan, artifactID)
}

func (observer *fakeComposeObserver) ObserveRestoration(
	ctx context.Context,
	_ *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, nil, "")
}

func (observer releaseExecutionObserver) ObserveRestoration(
	ctx context.Context,
	_ *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, nil, "")
}

func (observer negativeProbeObserver) ObserveRestoration(
	ctx context.Context,
	_ *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, nil, "")
}

func (observer blueprintPhaseObserver) ObserveRestoration(
	ctx context.Context,
	observation *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, nil, observation.Artifact().GetArtifactId())
}

func (observer *recreateArtifactObserver) ObserveRestoration(
	_ context.Context,
	observation *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	observer.restorationArtifact = observation.Artifact()
	return observer.restorationProject, nil
}
