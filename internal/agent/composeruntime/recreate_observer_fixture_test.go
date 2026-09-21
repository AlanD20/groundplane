package composeruntime

import (
	context "context"

	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

func (observer *recreateArtifactObserver) ObserveReleaseRestoration(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	_ string,
	artifactID string,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, plan, artifactID)
}

func (observer *recreateArtifactObserver) ObserveRestoration(
	_ context.Context,
	observation *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	observer.restorationArtifact = observation.Artifact()
	return observer.restorationProject, nil
}
