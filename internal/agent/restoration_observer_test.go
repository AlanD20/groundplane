package agent

import (
	context "context"

	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

func (observer *fakeComposeObserver) ObserveReleaseRestoration(
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

func (observer *fakeComposeObserver) ObserveRestoration(
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
