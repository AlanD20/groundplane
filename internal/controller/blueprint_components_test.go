package controller

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
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
		Config:            map[string]any{"zone_id": ids.NewAt(ids.KindNetwork, now, 3)},
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
			"caddy": {Kind: core.ComponentKindIngressCaddy, Enabled: false},
		},
		[]core.Component{caddy, tunnel},
		ids.New,
	)
	if err != nil || len(disabled.Candidates) != 1 || disabled.Effective[0].Enabled ||
		len(disabled.Effective[0].GeneratedServices) != 0 || disabled.Effective[0].PinnedIPv4 != "" {
		t.Fatalf("ReconcileBlueprintComponents(disabled) = %#v, %v", disabled, err)
	}
}

func TestReconcileBlueprintComponentsAllocatesEnableIdentityAndRequiresCaddy(t *testing.T) {
	// Rationale: enabling a previously disabled singleton needs one stable
	// generated Service id, and Tunnel may never form a graph without Caddy.
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
			"caddy": {
				Kind: core.ComponentKindIngressCaddy, Enabled: true,
				Config: map[string]any{"zone_id": ids.NewAt(ids.KindNetwork, now, 5)},
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
	_, err = ReconcileBlueprintComponents(
		map[string]core.ComponentSpec{
			"cloudflare-tunnel": {
				Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
				Config: map[string]any{"token_entry_id": ids.NewAt(ids.KindEnvEntry, now, 6)},
			},
		},
		[]core.Component{caddy, tunnel},
		ids.New,
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("ReconcileBlueprintComponents(Tunnel without Caddy) error = %v", err)
	}
}
