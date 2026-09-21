package taskplanning

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestReconcileBlueprintComponentsPreservesOmissionAndExplicitlyDisables(t *testing.T) {
	// Rationale: Blueprint omission must never become an implicit ingress
	// teardown, while enabled:false must produce the complete removal candidate.
	t.Parallel()
	now := time.Date(2026, 8, 22, 21, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	caddy := core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 2), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{ids.NewAt(ids.KindNetwork, now, 3)},
		}},
		GeneratedServices: []string{ids.NewAt(ids.KindService, now, 4)},
		PinnedIPv4:        "10.40.10.2", Healthy: true,
	}
	tunnel := core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 5), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare,
	}
	omitted, err := ReconcileBlueprintComponents(nil, []core.Component{tunnel, caddy}, ids.New)
	if err != nil || len(omitted.Candidates) != 0 || !omitted.Effective[0].Enabled ||
		omitted.Effective[0].PinnedIPv4 != caddy.PinnedIPv4 {
		t.Fatalf("ReconcileBlueprintComponents(omitted) = %#v, %v", omitted, err)
	}
	disabled, err := ReconcileBlueprintComponents(
		map[string]core.ComponentSpec{
			"http-router": {Implementation: core.ComponentKindIngressCaddy, Enabled: false},
		},
		[]core.Component{caddy, tunnel},
		ids.New,
	)
	if err != nil || len(disabled.Candidates) != 1 || disabled.Effective[0].Enabled ||
		len(disabled.Effective[0].GeneratedServices) != 0 || disabled.Effective[0].PinnedIPv4 != "" {
		t.Fatalf("ReconcileBlueprintComponents(disabled) = %#v, %v", disabled, err)
	}
}

func TestReconcileBlueprintComponentsPlansUnhealthyEnabledRepair(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 29, 2, 35, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	caddy := core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 2), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{ids.NewAt(ids.KindNetwork, now, 3)},
		}},
		GeneratedServices: []string{ids.NewAt(ids.KindService, now, 4)}, PinnedIPv4: "10.40.10.2",
	}
	tunnel := core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 5), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare,
	}

	result, err := ReconcileBlueprintComponents(nil, []core.Component{caddy, tunnel}, ids.New)
	if err != nil {
		t.Fatalf("ReconcileBlueprintComponents() error = %v", err)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].Current.ID != caddy.ID ||
		result.Candidates[0].Candidate.PinnedIPv4 != "" || result.Candidates[0].Candidate.Healthy {
		t.Fatalf("unhealthy Component repair = %#v", result)
	}
}

func TestReconcileBlueprintComponentsAllocatesIndependentEnableIdentities(t *testing.T) {
	// Rationale: each independently enabled singleton needs one stable generated
	// Service id; Tunnel lifecycle must not depend on an HTTP router.
	t.Parallel()
	now := time.Date(2026, 8, 22, 21, 10, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	caddy := core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 2), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy,
	}
	tunnel := core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 3), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare,
	}
	allocated := ids.NewAt(ids.KindService, now, 4)
	result, err := ReconcileBlueprintComponents(
		map[string]core.ComponentSpec{
			"http-router": {
				Implementation: core.ComponentKindIngressCaddy, Enabled: true,
				Settings: core.ComponentCapabilitySettings{ZoneIDs: []string{ids.NewAt(ids.KindNetwork, now, 5)}},
			},
		},
		[]core.Component{caddy, tunnel},
		func(kind ids.Kind) string {
			if kind != ids.KindService {
				t.Fatalf("allocator kind = %q", kind)
			}
			return allocated
		},
	)
	if err != nil || len(result.Candidates) != 1 ||
		len(result.Candidates[0].Candidate.GeneratedServices) != 1 ||
		result.Candidates[0].Candidate.GeneratedServices[0] != allocated {
		t.Fatalf("ReconcileBlueprintComponents(enable) = %#v, %v", result, err)
	}
	tunnelResult, err := ReconcileBlueprintComponents(
		map[string]core.ComponentSpec{
			"edge-tunnel": {
				Implementation: core.ComponentKindEdgeCloudflare, Enabled: true,
				Settings: core.ComponentCapabilitySettings{
					ZoneIDs:  []string{ids.NewAt(ids.KindNetwork, now, 7)},
					SecretID: ids.NewAt(ids.KindSecret, now, 6),
				},
			},
		},
		[]core.Component{caddy, tunnel},
		ids.New,
	)
	if err != nil || len(tunnelResult.Candidates) != 1 ||
		tunnelResult.Candidates[0].Candidate.Kind != core.ComponentKindEdgeCloudflare ||
		len(tunnelResult.Candidates[0].Candidate.GeneratedServices) != 1 {
		t.Fatalf("ReconcileBlueprintComponents(independent Tunnel) = %#v, %v", tunnelResult, err)
	}
}
