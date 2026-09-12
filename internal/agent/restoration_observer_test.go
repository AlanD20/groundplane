package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

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
