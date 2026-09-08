package core

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: Component placement is durable desired state, so malformed or
// duplicate Zone ids must fail even while a Component is disabled.
func TestComponentValidateRejectsInvalidSelectedZones(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.September, 6, 7, 0, 0, 0, time.UTC)
	zoneID := ids.NewAt(ids.KindNetwork, at, 1)
	base := Component{
		ID: ids.NewAt(ids.KindComponent, at, 2), Kind: ComponentKindIngressCaddy,
		Owner: ComponentOwnerEnvironment, OwnerID: ids.NewAt(ids.KindEnvironment, at, 3),
		Config: ComponentConfig{Caddy: &CaddyComponentConfig{ZoneIDs: []string{zoneID}}},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	for name, zoneIDs := range map[string][]string{
		"invalid":   {"net_invalid"},
		"duplicate": {zoneID, zoneID},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			candidate.Config = CloneComponentConfig(base.Config)
			candidate.Config.Caddy.ZoneIDs = zoneIDs
			if candidate.Validate() == nil {
				t.Fatalf("Validate() accepted Zone ids %#v", zoneIDs)
			}
		})
	}
	base.Enabled = true
	base.Config.Caddy.ZoneIDs = nil
	if base.Validate() == nil {
		t.Fatal("Validate() accepted enabled Caddy without Zones")
	}
}

// Rationale: callers must not be able to mutate ordered placement through a
// cloned desired Component config used by fixed-revision replay.
func TestCloneComponentConfigDetachesSelectedZones(t *testing.T) {
	t.Parallel()
	config := ComponentConfig{
		Caddy:            &CaddyComponentConfig{ZoneIDs: []string{"net_caddy"}},
		CloudflareTunnel: &CloudflareTunnelComponentConfig{ZoneIDs: []string{"net_tunnel"}, SecretID: "sec_token"},
	}
	// Exercise each branch independently because the union itself deliberately
	// rejects multiple variants only during Component validation.
	caddy := CloneComponentConfig(ComponentConfig{Caddy: config.Caddy})
	tunnel := CloneComponentConfig(ComponentConfig{CloudflareTunnel: config.CloudflareTunnel})
	caddy.Caddy.ZoneIDs[0] = "changed"
	tunnel.CloudflareTunnel.ZoneIDs[0] = "changed"
	if config.Caddy.ZoneIDs[0] != "net_caddy" || config.CloudflareTunnel.ZoneIDs[0] != "net_tunnel" {
		t.Fatalf("CloneComponentConfig() aliased source: %#v", config)
	}
}
