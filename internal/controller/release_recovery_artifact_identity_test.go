package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: ordinary publication allocates a new prior-topology ID. The real
// compiler must use that identity for recovery, while the selected predecessor
// workload remains the same as the separately captured applied artifact.
func TestOrdinaryRecreateCompilerKeepsPlanLocalPriorArtifactIdentity(t *testing.T) {
	resolver, task, input := redeployRestorationInput(t, domain.StrategyRecreate)
	_, previous, err := resolver.PrepareReleaseTask(t.Context(), task, input)
	if err != nil {
		t.Fatal(err)
	}
	captured := proto.CloneOf(previous.Artifacts[1])
	capturedBytes, err := proto.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	const priorTopologyID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC1"
	member := &input.Members[0]
	member.Render.PriorArtifactID = priorTopologyID
	member.Render.Projection.ComposeArtifact = capturedBytes
	member.Render.Projection.RenderGeneration = 5
	prepared, plan, err := resolver.PrepareReleaseTask(t.Context(), task, input)
	if err != nil {
		t.Fatal(err)
	}
	prior := plan.Artifacts[1]
	if prior.ArtifactId != priorTopologyID || prior.ArtifactId == captured.ArtifactId ||
		plan.Steps[3].GetServiceRecreateProbe().GetPriorArtifactId() != priorTopologyID ||
		plan.Steps[4].GetServiceRecreateCompensate().GetArtifactId() != priorTopologyID {
		t.Fatal("ordinary compiler confused captured artifact and plan-local prior topology")
	}
	var capturedWorkload, priorWorkload *agentpb.ComposeService
	for _, artifact := range []*agentpb.ComposeArtifact{captured, prior} {
		for _, service := range artifact.Services {
			if service.ServiceId != member.Render.ServiceID ||
				service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
				continue
			}
			if artifact == captured {
				capturedWorkload = service
			} else {
				priorWorkload = service
			}
		}
	}
	// Render-generation labels legitimately change with the frozen desired
	// revision; the predecessor workload seal, not the entire protobuf, is stable.
	if capturedWorkload == nil || priorWorkload == nil ||
		capturedWorkload.ImageReference != priorWorkload.ImageReference ||
		capturedWorkload.ExpectedReplicas != priorWorkload.ExpectedReplicas ||
		capturedWorkload.Role != priorWorkload.Role || capturedWorkload.Slot != priorWorkload.Slot ||
		!priorWorkload.HasHealthcheck ||
		labelPairMap(capturedWorkload.ExpectedLabels)[composeLabelReleaseID] != member.Intent.PriorServingReleaseID ||
		labelPairMap(priorWorkload.ExpectedLabels)[composeLabelReleaseID] != member.Intent.PriorServingReleaseID {
		t.Fatal("distinct prior topology changed the sealed predecessor workload")
	}
	descriptor, err := executionplan.DescribeCandidateRelease(plan)
	if err != nil || executionplan.CandidateReleaseDescriptorMatchesPlan(descriptor, plan) != nil {
		t.Fatalf("compiled prior identity is not plan-bound: %v", err)
	}
	replayed, err := resolver.buildReleasePlan(t.Context(), prepared, input)
	if err != nil || !proto.Equal(plan, replayed) {
		t.Fatalf("immutable ordinary recovery replay diverged: %v", err)
	}
}
