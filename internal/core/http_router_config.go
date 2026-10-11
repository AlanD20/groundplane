package core

import (
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/dnsname"
)

type EnvoyComponentConfig struct {
	Alias   string   `yaml:"alias,omitempty" json:"alias,omitempty"`
	ZoneIDs []string `yaml:"zone_ids" json:"zone_ids"`
}

func (kind ComponentKind) IsEnvironmentProxy() bool {
	return kind == ComponentKindIngressCaddy || kind == ComponentKindIngressEnvoy
}

// ValidateEnvironmentProxySelection admits at most one enabled implementation
// within this Environment. It never selects an implicit provider.
func ValidateEnvironmentProxySelection(environmentID string, components []Component) error {
	var selected ComponentKind
	for _, component := range components {
		if !component.Kind.IsEnvironmentProxy() {
			continue
		}
		if component.Owner != ComponentOwnerEnvironment || component.OwnerID != environmentID ||
			component.Validate() != nil {
			return fmt.Errorf("environment proxy ownership or configuration is invalid")
		}
		if !component.Enabled {
			continue
		}
		if selected != "" {
			return fmt.Errorf(
				"environment can enable only one proxy; %s and %s are both enabled",
				selected,
				component.Kind,
			)
		}
		selected = component.Kind
	}
	return nil
}

// HTTPRouterZoneIDs preserves the operator's ordering; the first Zone owns
// the router's reserved address. Callers validate the Component before use.
func (component Component) HTTPRouterZoneIDs() []string {
	switch component.Kind {
	case ComponentKindIngressCaddy:
		if component.Config.Caddy != nil {
			return component.Config.Caddy.ZoneIDs
		}
	case ComponentKindIngressEnvoy:
		if component.Config.Envoy != nil {
			return component.Config.Envoy.ZoneIDs
		}
	}
	return nil
}

func (component Component) validateHTTPRouterConfig() error {
	present := false
	alias := ""
	switch component.Kind {
	case ComponentKindIngressCaddy:
		if component.Config.Envoy != nil {
			return fmt.Errorf("component %s: Caddy cannot use Envoy configuration", component.ID)
		}
		if config := component.Config.Caddy; config != nil {
			present, alias = true, config.Alias
		}
	case ComponentKindIngressEnvoy:
		if component.Config.Caddy != nil {
			return fmt.Errorf("component %s: Envoy cannot use Caddy configuration", component.ID)
		}
		if config := component.Config.Envoy; config != nil {
			present, alias = true, config.Alias
		}
	}
	zoneIDs := component.HTTPRouterZoneIDs()
	if component.Owner != ComponentOwnerEnvironment || component.Config.CloudflareTunnel != nil ||
		component.Config.CoreDNS != nil || component.Enabled && (!present || len(zoneIDs) == 0) ||
		present && (validateComponentZoneIDs(zoneIDs) != nil || !validHTTPRouterAlias(alias)) {
		return fmt.Errorf("component %s: %s ownership or config is invalid", component.ID, component.Kind)
	}
	return nil
}

func validHTTPRouterAlias(alias string) bool {
	return alias == "" || dnsname.ValidWithin(alias, 63) && !strings.Contains(alias, ".")
}
