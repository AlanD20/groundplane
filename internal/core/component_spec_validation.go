package core

import (
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// EnvironmentCapability identifies the stable ownership slot, independently
// of which registered implementation currently serves it.
func (kind ComponentKind) EnvironmentCapability() (ComponentCapability, bool) {
	if kind.IsEnvironmentProxy() {
		return ComponentCapabilityHTTPRouter, true
	}
	if kind == ComponentKindEdgeCloudflare {
		return ComponentCapabilityEdgeTunnel, true
	}
	return "", false
}

// SharesEnvironmentCapability permits implementation replacement only within
// the same closed Environment ownership slot. It does not grant mutation authority.
func (kind ComponentKind) SharesEnvironmentCapability(other ComponentKind) bool {
	capability, supported := kind.EnvironmentCapability()
	nextCapability, nextSupported := other.EnvironmentCapability()
	return supported && nextSupported && capability == nextCapability
}

// Validate checks authored decisions before identity binding or runtime
// preparation. Runtime addresses and generated Services are not Blueprint inputs.
func (spec ComponentSpec) Validate(capability ComponentCapability) error {
	provided, supported := spec.Implementation.EnvironmentCapability()
	if !supported || provided != capability {
		return fmt.Errorf(
			"%s: implementation %q does not provide this Environment capability",
			capability,
			spec.Implementation,
		)
	}
	if err := validateComponentZoneIDs(spec.Settings.ZoneIDs); err != nil {
		return fmt.Errorf("%s: %w", capability, err)
	}
	if spec.Enabled && len(spec.Settings.ZoneIDs) == 0 {
		return fmt.Errorf("%s: zone_ids is required while enabled", capability)
	}
	switch capability {
	case ComponentCapabilityHTTPRouter:
		if spec.Settings.SecretID != "" {
			return fmt.Errorf("http-router: secret_id is not supported")
		}
		if !validHTTPRouterAlias(spec.Settings.Alias) {
			return fmt.Errorf("http-router: alias must be a DNS label without dots")
		}
		if spec.Implementation == ComponentKindIngressEnvoy && spec.ImplementationConfig.CaddyfileTemplate != "" {
			return fmt.Errorf("envoy: caddyfile_template is not supported")
		}
	case ComponentCapabilityEdgeTunnel:
		if spec.Settings.Alias != "" || spec.ImplementationConfig.CaddyfileTemplate != "" {
			return fmt.Errorf("edge-tunnel: alias and caddyfile_template are not supported")
		}
		if spec.Enabled && spec.Settings.SecretID == "" {
			return fmt.Errorf("edge-tunnel: secret_id is required while enabled")
		}
		if spec.Settings.SecretID != "" && ids.Validate(ids.KindSecret, spec.Settings.SecretID) != nil {
			return fmt.Errorf("edge-tunnel: secret_id must be a Secret id")
		}
	}
	return nil
}
