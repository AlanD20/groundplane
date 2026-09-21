package releaseoperation

import (
	"encoding/hex"
	"strings"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
)

// Rationale: first addressable recreate has no workload to restore. Publishing
// invented prior proxy authority contradicts the sealed absent predecessor.
func TestConfigureReleaseProxyFirstRecreateHasNoPriorAuthority(t *testing.T) {
	render := testreleaserender.ReleaseRenderInput{
		ReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceName: "api",
		Strategy: domain.StrategyRecreate, PriorStrategy: domain.StrategyRecreate,
		CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
	}
	if err := configureReleaseProxy(&render, []string{"8080"}, nil, 0); err != nil {
		t.Fatal(err)
	}
	if render.PriorProxyGeneration != 0 || render.PriorProxyDigest != "" {
		t.Fatalf(
			"first recreate invented predecessor proxy authority: generation=%d digest=%q",
			render.PriorProxyGeneration,
			render.PriorProxyDigest,
		)
	}
	want, err := domain.RenderProxyConfig("api", render.ReleaseID, domain.WorkloadSingleton, 2, []uint16{8080})
	if err != nil {
		t.Fatal(err)
	}
	if render.ProxyGeneration != 2 || render.ProxyConfigDigest != hex.EncodeToString(want.SHA256[:]) {
		t.Fatal("candidate proxy does not route the sealed first Release")
	}
}

func TestConfigureReleaseProxyFirstBlueGreenHasNoPriorAuthority(t *testing.T) {
	render := testreleaserender.ReleaseRenderInput{
		ReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceName: "api",
		Strategy: domain.StrategyBlueGreen, PriorStrategy: domain.StrategyRecreate,
		Slot: domain.SlotBlue, CandidateTarget: domain.WorkloadBlue, PriorTarget: domain.WorkloadSingleton,
		CandidateWorkload: domain.WorkloadSeal{
			RequestedReference: "api:first",
			LocalImageID:       "sha256:" + strings.Repeat("a", 64),
			ReplicaCount:       1,
		},
	}
	if err := configureReleaseProxy(&render, []string{"8080"}, nil, 0); err != nil {
		t.Fatal(err)
	}
	if render.PriorProxyGeneration != 0 || render.PriorProxyDigest != "" {
		t.Fatal("first blue-green invented predecessor proxy authority")
	}
	want, err := domain.RenderProxyConfig("api", render.ReleaseID, domain.WorkloadBlue, 2, []uint16{8080})
	if err != nil {
		t.Fatal(err)
	}
	if render.ProxyGeneration != 2 || render.ProxyConfigDigest != hex.EncodeToString(want.SHA256[:]) {
		t.Fatal("first blue-green proxy does not route the sealed candidate slot")
	}
}

func TestConfigureReleaseProxyPreservesServingPredecessor(t *testing.T) {
	const priorID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	render := testreleaserender.ReleaseRenderInput{
		ReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceName: "api",
		Strategy: domain.StrategyRecreate, PriorStrategy: domain.StrategyRecreate,
		CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
		PriorWorkload: &domain.WorkloadSeal{
			RequestedReference: "api:old",
			LocalImageID:       "sha256:" + strings.Repeat("a", 64),
			ReplicaCount:       2,
		},
	}
	want, err := domain.RenderProxyConfig("api", priorID, domain.WorkloadSingleton, 7, []uint16{8080})
	if err != nil {
		t.Fatal(err)
	}
	prior := &testreleaserender.ReleaseRenderInput{ReleaseID: priorID, ProxyGeneration: 7,
		ProxyConfigDigest: hex.EncodeToString(want.SHA256[:])}
	// A ledger revision is not the serving proxy generation. Exposure edits
	// also must not reconstruct historical proxy bytes from current decisions.
	if err := configureReleaseProxy(&render, []string{"9090"}, prior, 1); err != nil {
		t.Fatal(err)
	}
	if render.PriorProxyGeneration != 7 || render.PriorProxyDigest != hex.EncodeToString(want.SHA256[:]) {
		t.Fatal("serving predecessor proxy authority was discarded or changed")
	}
	if render.ProxyGeneration != 8 {
		t.Fatal("candidate proxy generation did not advance from its predecessor")
	}
}
