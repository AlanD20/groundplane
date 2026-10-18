package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RenderServiceReleaseArtifact materializes one captured Release without constructing
// a Task or generic execution plan. Unselected workload ownership remains intact.
func RenderServiceReleaseArtifact(
	ctx context.Context,
	member releaserender.ReleaseTaskRenderMember,
) (*agentpb.ComposeArtifact, error) {
	render, intent := member.Render, member.Intent
	if err := releaserender.ValidateReleaseRenderInput(render); err != nil {
		return nil, err
	}
	if err := domain.ValidateIntent(intent); err != nil {
		return nil, err
	}
	if render.ReleaseID != intent.ID || render.ServiceID != intent.ServiceID ||
		render.EnvironmentID != intent.EnvironmentID || render.ArtifactID != intent.RenderInputID ||
		render.CandidateWorkload != intent.CandidateWorkload || render.Strategy != intent.Strategy ||
		render.Slot != intent.Slot {
		return nil, errs.New(errs.KindValidationFailed, "Service candidate differs from its captured Release")
	}
	initialProxy, err := releaserender.InitialProxyConfiguration(member)
	if err != nil {
		return nil, err
	}
	identity := composerender.ComposeReleaseIdentity{
		ProxyAddresses: render.ProxyAddresses, ProxyImage: render.ProxyImage,
		ProxyImplementation: render.ProxyImplementation,
		ReleaseID:           intent.ID, Target: render.CandidateTarget,
		Image: render.CandidateWorkload.LocalImageID, ServingReleaseID: intent.PriorServingReleaseID,
		ServingTarget: render.PriorTarget, ServingProxyGeneration: render.PriorProxyGeneration,
		Strategy: render.Strategy, Rolling: render.Rolling, ServingProxyConfig: initialProxy,
	}
	if render.Strategy == domain.StrategyRecreate {
		identity.ServingReleaseID = intent.ID
		identity.ServingTarget = render.CandidateTarget
		identity.ServingProxyGeneration = render.ProxyGeneration
	}
	artifact, err := renderBlueprintCandidateArtifact(ctx, render,
		map[string]domain.WorkloadSeal{render.ServiceName: render.CandidateWorkload},
		map[string]composerender.ComposeReleaseIdentity{render.ServiceID: identity})
	if err != nil {
		return nil, err
	}
	artifact, err = retainBlueprintUnselectedRuntime(
		artifact,
		render.Projection.ComposeArtifact,
		[]string{render.ServiceID},
	)
	if err != nil {
		return nil, err
	}
	if err := bindBlueprintCandidateServiceImages(artifact, []releaserender.ReleaseTaskRenderMember{member}); err != nil {
		return nil, err
	}
	if render.Strategy != domain.StrategyRecreate && render.PriorRuntime != nil {
		prior, err := executionplan.OpenNativeServingRuntime(render.EnvironmentID, render.ServiceID,
			render.PriorRuntime.CurrentArtifact, render.PriorRuntime.RetainedPriorArtifact)
		if err != nil {
			return nil, err
		}
		if err := executionplan.ValidateLiveProxyTopology(artifact, prior.Artifact, render.ServiceID); err != nil {
			return nil, err
		}
	}
	return artifact, nil
}
