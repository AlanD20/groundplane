package releaseoperation

import (
	"context"
	"strings"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/hostplacement"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestRemoteImageSealChecksAttestedProxyOnTheSelectedProcess(t *testing.T) {
	selected := hostplacement.Snapshot{
		Identity: workexecution.Identity{AgentID: "remote-agent"}, ProcessGeneration: [16]byte{42},
	}
	proxy := domain.ProxyImage{Repository: "example.test/edge/proxy", Platform: componentsdk.OCIPlatform{
		ChildDigest: strings.Repeat("a", 64), ConfigDigest: strings.Repeat("b", 64),
	}}
	workload := &workloadseal.Requested{Reference: "example.test/app:1", Replicas: 1}
	resolver := remoteImageResolver{
		t: t, selected: selected, wanted: []string{workload.Reference, proxy.Reference()},
		ids: []string{"sha256:" + strings.Repeat("c", 64), "sha256:" + strings.Repeat("d", 64)},
	}
	service := Service{images: &resolver}
	candidates := []releaseCandidateInput{
		{remoteProxyImage: &proxy, selection: workloadseal.Selection{Requested: workload}},
	}
	err := service.sealCandidatesOnHost(t.Context(), candidates, &selected)
	if kind, _ := errs.KindOf(err); kind != errs.KindStateConflict {
		t.Fatalf("wrong proxy config returned %v", err)
	}
	if candidates[0].workload.LocalImageID != "" {
		t.Fatal("wrong proxy configuration published the workload seal")
	}
	resolver.ids[1] = "sha256:" + proxy.Platform.ConfigDigest
	if err := service.sealCandidatesOnHost(t.Context(), candidates, &selected); err != nil {
		t.Fatal(err)
	}
	if candidates[0].workload.LocalImageID != resolver.ids[0] {
		t.Fatal("Deploy did not retain the resolved application image")
	}
}

type remoteImageResolver struct {
	t        *testing.T
	selected hostplacement.Snapshot
	wanted   []string
	ids      []string
}

func (resolver *remoteImageResolver) ResolveWorkloadImages(
	context.Context, string, []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	resolver.t.Fatal("Remote Deploy used an unfenced Agent query")
	return nil, nil
}

func (resolver *remoteImageResolver) ResolveWorkloadImagesForProcess(
	_ context.Context, agentID string, process [16]byte, selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	if agentID != resolver.selected.Identity.AgentID || process != resolver.selected.ProcessGeneration ||
		len(selectors) != len(resolver.wanted) {
		resolver.t.Fatal("image query did not select the captured process and both images")
	}
	resolutions := make([]*agentpb.WorkloadImageResolution, len(selectors))
	for index, selector := range selectors {
		if selector.GetRequestedReference() != resolver.wanted[index] {
			resolver.t.Fatalf("unexpected image selector %v", selector)
		}
		resolutions[index] = &agentpb.WorkloadImageResolution{Selector: selector, LocalImageId: resolver.ids[index]}
	}
	return &agentpb.WorkloadImageResolutionResult{RequestId: strings.Repeat("1", 32),
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{
			Success: &agentpb.WorkloadImageResolutions{Resolutions: resolutions},
		}}, nil
}
