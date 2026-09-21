package blueprintrelease

import (
	"encoding/hex"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
)

// Rationale: first addressable Blueprint candidates need generation-one config
// authority; successors use current ports without rewriting historical config.
func TestBlueprintProxyConfigSealsFirstAndSuccessor(t *testing.T) {
	render := testreleaserender.ReleaseRenderInput{
		ServiceName:     "api",
		ReleaseID:       ids.New(ids.KindDeployment),
		CandidateTarget: domain.WorkloadSingleton,
	}
	if err := configureBlueprintProxy(&render, []string{"9090", "8080/tcp"}); err != nil {
		t.Fatal(err)
	}
	if render.ProxyGeneration != 1 || render.PriorProxyGeneration != 0 || render.PriorProxyDigest != "" ||
		!slices.Equal(render.ProxyPorts, []uint16{8080, 9090}) {
		t.Fatalf("first proxy authority = %+v", render)
	}
	first, err := domain.RenderProxyConfig("api", render.ReleaseID, domain.WorkloadSingleton, 1, render.ProxyPorts)
	if err != nil || render.ProxyConfigDigest != hex.EncodeToString(first.SHA256[:]) {
		t.Fatalf("first config digest = %q, %v", render.ProxyConfigDigest, err)
	}
	priorDigest := render.ProxyConfigDigest
	render.ReleaseID = ids.New(ids.KindDeployment)
	render.PriorWorkload = &domain.WorkloadSeal{}
	render.PriorProxyGeneration, render.PriorProxyDigest = 1, priorDigest
	if err := configureBlueprintProxy(&render, []string{"8081"}); err != nil {
		t.Fatal(err)
	}
	if render.ProxyGeneration != 2 || render.PriorProxyGeneration != 1 || render.PriorProxyDigest != priorDigest ||
		!slices.Equal(render.ProxyPorts, []uint16{8081}) ||
		render.ProxyConfigDigest == priorDigest {
		t.Fatalf("successor rewrote historical config = %+v", render)
	}
}

// Rationale: unsupported addressability transitions must not invent a prior
// proxy image/config or discard a still-serving proxy's authority.
func TestBlueprintProxyConfigRejectsUnsealedAddressabilityTransitions(t *testing.T) {
	for _, previouslyAddressable := range []bool{false, true} {
		render := testreleaserender.ReleaseRenderInput{
			ServiceName:     "api",
			ReleaseID:       ids.New(ids.KindDeployment),
			CandidateTarget: domain.WorkloadSingleton,
			PriorWorkload:   &domain.WorkloadSeal{},
		}
		exposures := []string{"8080"}
		if previouslyAddressable {
			render.PriorProxyGeneration = 1
			exposures = nil
		}
		if configureBlueprintProxy(&render, exposures) == nil {
			t.Fatal("unsupported addressability transition accepted")
		}
	}
}
