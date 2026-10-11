package cli

import (
	"fmt"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

// One existing router singleton changes implementation through its ordinary
// config API. Preserve common placement/alias settings, not engine-specific
// template configuration.
func componentProxySelection(cmd *cobra.Command, component apiTypes.Component) (apiTypes.Component, error) {
	if !cmd.Flags().Changed("implementation") {
		return component, nil
	}
	kind, err := cmd.Flags().GetString("implementation")
	if err != nil {
		return apiTypes.Component{}, err
	}
	if component.EnvironmentID == "" || component.Kind != "caddy" && component.Kind != "envoy" ||
		kind != "caddy" && kind != "envoy" {
		return apiTypes.Component{}, fmt.Errorf(
			"--implementation selects caddy or envoy for an Environment router only",
		)
	}
	if component.Kind == kind {
		return component, nil
	}
	var zones []string
	var alias string
	if component.Config != nil {
		if component.Config.Caddy != nil {
			zones, alias = component.Config.Caddy.ZoneIDs, component.Config.Caddy.Alias
		} else if component.Config.Envoy != nil {
			zones, alias = component.Config.Envoy.ZoneIDs, component.Config.Envoy.Alias
		}
	}
	component.Kind = kind
	component.Config = &apiTypes.ComponentConfig{}
	if kind == "caddy" {
		component.Config.Caddy = &apiTypes.CaddyComponentConfig{ZoneIDs: append([]string(nil), zones...), Alias: alias}
	} else {
		component.Config.Envoy = &apiTypes.EnvoyComponentConfig{ZoneIDs: append([]string(nil), zones...), Alias: alias}
	}
	return component, nil
}
