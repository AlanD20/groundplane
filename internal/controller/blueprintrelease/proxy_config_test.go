package blueprintrelease

import (
	"encoding/hex"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/core/serviceports"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
)

// Rationale: first addressable Blueprint candidates need generation-one config
// authority; successors use current ports without rewriting historical config.
func TestBlueprintProxyConfigSealsFirstAndSuccessor(t *testing.T) {
	render := testreleaserender.ReleaseRenderInput{
		ServiceName:     "api",
		ReleaseID:       ids.New(ids.KindDeployment),
		CandidateTarget: domain.WorkloadSingleton,
		Projection:      blueprintProxyTestProjection(t),
	}
	if err := configureBlueprintProxy(&render, []string{"9090", "8080/tcp"}); err != nil {
		t.Fatal(err)
	}
	if render.ProxyGeneration != 1 || render.PriorProxyGeneration != 0 || render.PriorProxyDigest != "" ||
		!slices.Equal(
			render.ProxyPorts,
			[]domain.ProxyPort{{Port: 8080, Protocol: serviceports.TCP}, {Port: 9090, Protocol: serviceports.TCP}},
		) {
		t.Fatalf("first proxy authority = %+v", render)
	}
	first, err := domain.RenderProxyConfig(
		serviceproxy.Caddy,
		"api",
		render.ReleaseID,
		domain.WorkloadSingleton,
		1,
		render.ProxyPorts,
	)
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
		!slices.Equal(render.ProxyPorts, []domain.ProxyPort{{Port: 8081, Protocol: serviceports.TCP}}) ||
		render.ProxyConfigDigest == priorDigest {
		t.Fatalf("successor rewrote historical config = %+v", render)
	}
}

func blueprintProxyTestProjection(t *testing.T) projectionrecord.EnvironmentComposeProjection {
	t.Helper()
	projection := projectionrecord.EnvironmentComposeProjection{EnvironmentID: ids.New(ids.KindEnvironment)}
	for _, kind := range []core.ComponentKind{core.ComponentKindIngressCaddy, core.ComponentKindEdgeCloudflare} {
		record, err := componentrecord.NewRecord(core.Component{ID: ids.New(ids.KindComponent),
			Owner: core.ComponentOwnerEnvironment, OwnerID: projection.EnvironmentID, Kind: kind})
		if err != nil {
			t.Fatal(err)
		}
		projection.Components = append(projection.Components, record)
	}
	return projection
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
