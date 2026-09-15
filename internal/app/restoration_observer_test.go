package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (observer *fakeOwnedComposeObserver) ObserveReleaseRestoration(
	ctx context.Context, plan *agentpb.ExecutionPlan, _ string, artifactID string,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, plan, artifactID)
}

func (observer *fakeOwnedComposeObserver) ObserveRestoration(
	ctx context.Context,
	observation *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	artifact := observation.Artifact()
	return observer.Observe(
		ctx,
		&agentpb.ExecutionPlan{Artifacts: []*agentpb.ComposeArtifact{artifact}},
		artifact.GetArtifactId(),
	)
}
