package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

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
