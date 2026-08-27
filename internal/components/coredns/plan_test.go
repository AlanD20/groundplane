package coredns

import (
	"net/netip"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

func TestBuildTaskPlanIsDeterministic(t *testing.T) {
	t.Parallel()
	component := core.Component{
		ID: "cmp_coredns", Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKindCoreDNS,
		Enabled: true, GeneratedServices: []string{"svc_coredns"}, Config: map[string]any{
			"upstream_auto": true, "upstream_resolvers": []any{}, "forwarders": []any{}, "tailnet_delegation": true,
		},
	}
	environments := []core.Environment{{
		ID: "env_one",
		Components: []core.Component{{
			ID: "cmp_caddy", Owner: core.ComponentOwnerEnvironment, OwnerID: "env_one",
			Kind: core.ComponentKindIngressCaddy, Enabled: true, PinnedIPv4: "10.200.30.4",
		}},
		Routes: []core.Route{{Host: "app.example.com", Path: "/", TargetServiceID: "svc_app", TargetPort: 8080, Exposure: "public"}},
	}}
	first, err := BuildTaskPlan(component, environments, []ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}})
	if err != nil {
		t.Fatalf("BuildTaskPlan() error = %v", err)
	}
	second, err := BuildTaskPlan(component, environments, []ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}})
	if err != nil {
		t.Fatalf("BuildTaskPlan() second error = %v", err)
	}
	if string(first.Corefile) != string(second.Corefile) || first.InputSHA256 != second.InputSHA256 || first.CorefileSHA256 != second.CorefileSHA256 {
		t.Fatal("BuildTaskPlan() was not deterministic")
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("TaskPlan.Validate() error = %v", err)
	}
}
