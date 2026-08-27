package coredns

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

func TestBuildRenderInputUsesAppliedCaddyIPv4ForPublicRoutes(t *testing.T) {
	t.Parallel()
	environment := core.Environment{
		ID: "env_one",
		Components: []core.Component{{
			ID: "cmp_caddy", Owner: core.ComponentOwnerEnvironment, OwnerID: "env_one",
			Kind: core.ComponentKindIngressCaddy, Enabled: true, PinnedIPv4: "10.200.30.4",
		}},
		Routes: []core.Route{
			{ID: "rte_public", Host: "app.example.com", Path: "/", TargetServiceID: "svc_app", TargetPort: 8080, Exposure: "public"},
			{ID: "rte_internal", Host: "admin.example.com", Path: "/", TargetServiceID: "svc_admin", TargetPort: 8080, Exposure: "internal"},
		},
	}
	input, err := BuildRenderInput([]core.Environment{environment}, Config{UpstreamAuto: true}, []ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}})
	if err != nil {
		t.Fatalf("BuildRenderInput() error = %v", err)
	}
	corefile, err := RenderCorefile(input)
	if err != nil {
		t.Fatalf("RenderCorefile() error = %v", err)
	}
	if !strings.Contains(string(corefile), "10.200.30.4 app.example.com") || strings.Contains(string(corefile), "admin.example.com") {
		t.Fatalf("Corefile has wrong route projection: %q", corefile)
	}
}

func TestBuildRenderInputRejectsEnabledCaddyWithoutPinnedAddress(t *testing.T) {
	t.Parallel()
	_, err := BuildRenderInput([]core.Environment{{
		ID: "env_one",
		Components: []core.Component{{
			ID: "cmp_caddy", Owner: core.ComponentOwnerEnvironment, OwnerID: "env_one",
			Kind: core.ComponentKindIngressCaddy, Enabled: true,
		}},
	}}, Config{UpstreamAuto: true}, []ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}})
	if err == nil {
		t.Fatal("BuildRenderInput() accepted an enabled Caddy without pinned IPv4")
	}
}
