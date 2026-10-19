package composeobserver

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ObserveRelease uses captured candidate and predecessor ownership directly.
// It does not manufacture an ExecutionPlan or reread desired configuration.
func (observer *Observer) ObserveRelease(
	ctx context.Context,
	input releaseexecution.Input,
) (*agentpb.ObservedProject, error) {
	if observer == nil || observer.engine == nil || observer.now == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Release observer is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(input.Attempt, input.Release)
	if err != nil {
		return nil, err
	}
	var companions []*agentpb.ComposeService
	if artifacts.Prior != nil {
		companions = append(companions, artifacts.Prior.Artifact.Services...)
		if artifacts.Prior.Retained != nil {
			companions = append(companions, artifacts.Prior.Retained.Services...)
		}
	}
	return observer.observeArtifact(ctx, artifacts.Candidate, companions, false)
}
