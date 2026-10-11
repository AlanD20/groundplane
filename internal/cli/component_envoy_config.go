package cli

import (
	"encoding/json"
	"fmt"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

// componentEnvoyConfig prepares the same complete typed input for Enable and
// Config Set. Explicit placement/alias flags replace captured current settings.
func componentEnvoyConfig(
	cmd *cobra.Command, component apiTypes.Component, configFile string, zoneIDs []string,
) (*apiTypes.ComponentConfigMutationInput, error) {
	envoy := apiTypes.EnvoyComponentConfigMutationInput{}
	if component.Config != nil && component.Config.Envoy != nil {
		envoy.ZoneIDs = append([]string(nil), component.Config.Envoy.ZoneIDs...)
		envoy.Alias = component.Config.Envoy.Alias
	}
	if configFile != "" {
		value, err := readValueFile(configFile, cmd.InOrStdin(), 64<<10, "Envoy config")
		if err != nil {
			return nil, err
		}
		var input apiTypes.ComponentConfigMutationInput
		if err := json.Unmarshal([]byte(value), &input); err != nil {
			return nil, err
		}
		if input.Envoy == nil {
			return nil, fmt.Errorf("envoy config requires implementation: envoy")
		}
		envoy = *input.Envoy
	}
	if zoneIDs != nil {
		envoy.ZoneIDs = append([]string(nil), zoneIDs...)
	}
	if cmd.Flags().Changed("alias") {
		alias, err := cmd.Flags().GetString("alias")
		if err != nil {
			return nil, err
		}
		envoy.Alias = alias
	}
	return &apiTypes.ComponentConfigMutationInput{Envoy: &envoy}, nil
}
