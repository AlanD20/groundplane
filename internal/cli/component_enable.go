package cli

import (
	"encoding/json"
	"fmt"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

func newComponentEnableCmd() *cobra.Command {
	var configFile string
	var alias string
	var templateFile string
	var zones []string
	var ordinaryZones []string
	var internalZones []string
	cmd := &cobra.Command{
		Use:   "enable <id>",
		Short: "Enable a component",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			id := target(app, args[0])
			configChanged := cmd.Flags().Changed("alias") || cmd.Flags().Changed("file") || cmd.Flags().Changed("template-file") ||
				cmd.Flags().Changed("zone") || cmd.Flags().Changed("create-zone") ||
				cmd.Flags().Changed("create-internal-zone")
			if !configChanged {
				accepted, err := app.Client.EnableComponent(
					cmd.Context(), id, apiTypes.ComponentEnableRequest{},
				)
				if err != nil {
					return err
				}
				return renderDispatchedTask(cmd, accepted)
			}

			component, err := app.Client.ShowComponent(cmd.Context(), id)
			if err != nil {
				return err
			}
			if component.Owner == "platform" || component.Kind == "coredns" {
				return fmt.Errorf(
					"platform Component enable does not accept config; use component config set, then run a bodyless enable",
				)
			}
			zoneOperation := cmd.Flags().Changed("zone") || cmd.Flags().Changed("create-zone") ||
				cmd.Flags().Changed("create-internal-zone")
			if zoneOperation && component.EnvironmentID == "" {
				return fmt.Errorf("Zone placement requires an environment-owned Component")
			}
			creations, err := parseComponentZoneCreations(ordinaryZones, internalZones)
			if err != nil {
				return err
			}
			if len(creations) > 0 && component.Kind != "caddy" && component.Kind != "cloudflare-tunnel" {
				return fmt.Errorf("Zone creation during enable is valid only for Caddy and Cloudflare Tunnel")
			}
			zoneIDs, err := resolveComponentZoneIDs(cmd, component.EnvironmentID, zones)
			if err != nil {
				return err
			}
			config, err := componentEnableConfig(
				cmd,
				component,
				configFile,
				templateFile,
				zoneIDs,
				len(creations) > 0,
			)
			if err != nil {
				return err
			}
			created, createErr := createComponentZones(cmd, component.EnvironmentID, creations)
			if createErr != nil {
				return returnWithRetainedComponentZones(cmd, created, createErr)
			}
			createdIDs := make([]string, len(created))
			for index := range created {
				createdIDs[index] = created[index].ID
			}
			appendComponentZoneIDs(config, createdIDs)
			if validationErr := config.Validate(); validationErr != nil {
				return returnWithRetainedComponentZones(cmd, created, validationErr)
			}
			accepted, enableErr := app.Client.EnableComponent(
				cmd.Context(), id, apiTypes.ComponentEnableRequest{Config: config},
			)
			if enableErr != nil {
				return returnWithRetainedComponentZones(cmd, created, enableErr)
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	cmd.Flags().
		StringVar(&configFile, "file", "", "read a Caddyfile template (Caddy) or complete JSON config (other kinds) from PATH, or -")
	cmd.Flags().StringVar(&alias, "alias", "", "optional Caddy primary-Zone alias; empty clears it")
	cmd.Flags().StringVar(&templateFile, "template-file", "", "read a CoreDNS Corefile template from PATH, or -")
	cmd.Flags().StringArrayVar(&zones, "zone", nil, "repeatable Zone slug; with global --id, repeatable stable Zone id")
	cmd.Flags().
		StringArrayVar(&ordinaryZones, "create-zone", nil, "repeatable NAME=CIDR Zone to create and select before enabling")
	cmd.Flags().
		StringArrayVar(&internalZones, "create-internal-zone", nil, "repeatable internal NAME=CIDR Zone to create and select before enabling")
	return cmd
}

func componentEnableConfig(
	cmd *cobra.Command,
	component apiTypes.Component,
	configFile string,
	templateFile string,
	zoneIDs []string,
	willCreateZones bool,
) (*apiTypes.ComponentConfigMutationInput, error) {
	if cmd.Flags().Changed("alias") && component.Kind != "caddy" {
		return nil, fmt.Errorf("--alias is valid only for Caddy")
	}
	if templateFile != "" && component.Kind != "coredns" {
		return nil, fmt.Errorf("--template-file is valid only for CoreDNS")
	}
	switch component.Kind {
	case "caddy":
		if templateFile != "" {
			return nil, fmt.Errorf("Caddy config accepts --file, --zone, and Zone creation flags only")
		}
		caddy := apiTypes.CaddyComponentConfigMutationInput{}
		if component.Config != nil && component.Config.Caddy != nil {
			caddy.ZoneIDs = append([]string(nil), component.Config.Caddy.ZoneIDs...)
			caddy.CaddyfileTemplate = component.Config.Caddy.CaddyfileTemplate
			caddy.Alias = component.Config.Caddy.Alias
		}
		if zoneIDs != nil {
			caddy.ZoneIDs = append([]string(nil), zoneIDs...)
		}
		if configFile != "" {
			value, err := readValueFile(configFile, cmd.InOrStdin(), maxCaddyfileTemplateBytes, "Caddyfile template")
			if err != nil {
				return nil, err
			}
			caddy.CaddyfileTemplate = value
		}
		if cmd.Flags().Changed("alias") {
			alias, err := cmd.Flags().GetString("alias")
			if err != nil {
				return nil, err
			}
			caddy.Alias = alias
		}
		return &apiTypes.ComponentConfigMutationInput{Caddy: &caddy}, nil
	case "cloudflare-tunnel":
		input := apiTypes.ComponentConfigMutationInput{}
		if configFile == "" && !component.Enabled {
			return nil, fmt.Errorf("Cloudflare Tunnel Zone placement requires --file while disabled or unconfigured")
		}
		if configFile != "" {
			value, err := readValueFile(configFile, cmd.InOrStdin(), 64<<10, "component config")
			if err != nil {
				return nil, err
			}
			input, err = decodeCloudflareTunnelConfigFile(
				value,
				len(zoneIDs) > 0 || willCreateZones,
			)
			if err != nil {
				return nil, err
			}
		} else {
			if component.Config == nil || component.Config.CloudflareTunnel == nil {
				return nil, fmt.Errorf("Cloudflare Tunnel Zone placement requires --file while disabled or unconfigured")
			}
			current := component.Config.CloudflareTunnel
			input.CloudflareTunnel = &apiTypes.CloudflareTunnelComponentConfigMutationInput{
				ZoneIDs: append([]string(nil), current.ZoneIDs...),
				Credential: apiTypes.CloudflareTunnelCredentialInput{
					Mode: "existing", SecretID: current.SecretID,
				},
			}
		}
		if zoneIDs != nil {
			input.CloudflareTunnel.ZoneIDs = append([]string(nil), zoneIDs...)
		}
		return &input, nil
	case "coredns":
		if len(zoneIDs) > 0 {
			return nil, fmt.Errorf("CoreDNS does not accept Zone placement")
		}
		if configFile != "" {
			value, err := readValueFile(configFile, cmd.InOrStdin(), 64<<10, "component config")
			if err != nil {
				return nil, err
			}
			input := apiTypes.ComponentConfigMutationInput{}
			if err := json.Unmarshal([]byte(value), &input); err != nil {
				return nil, fmt.Errorf("component config file must contain one JSON object: %w", err)
			}
			return &input, nil
		}
		if templateFile == "" {
			return nil, fmt.Errorf("CoreDNS enable config requires --file or --template-file")
		}
		value, err := readValueFile(
			templateFile, cmd.InOrStdin(), maxCorefileTemplateBytes, "Corefile template",
		)
		if err != nil {
			return nil, err
		}
		upstreamAuto := false
		upstreamResolvers := []string{}
		forwarders := []apiTypes.ComponentDNSForwarder{}
		tailnetDelegation := false
		if component.Config != nil && component.Config.CoreDNS != nil {
			current := component.Config.CoreDNS
			upstreamAuto = current.UpstreamAuto
			upstreamResolvers = append([]string(nil), current.UpstreamResolvers...)
			forwarders = append([]apiTypes.ComponentDNSForwarder(nil), current.Forwarders...)
			tailnetDelegation = current.TailnetDelegation
		}
		return &apiTypes.ComponentConfigMutationInput{CoreDNS: &apiTypes.CoreDNSComponentConfigMutationInput{
			CorefileTemplate:  &value,
			UpstreamAuto:      &upstreamAuto,
			UpstreamResolvers: &upstreamResolvers,
			Forwarders:        &forwarders,
			TailnetDelegation: &tailnetDelegation,
		}}, nil
	default:
		return nil, fmt.Errorf("Component kind %q does not support inline enable config", component.Kind)
	}
}

func appendComponentZoneIDs(input *apiTypes.ComponentConfigMutationInput, zoneIDs []string) {
	if input.Caddy != nil {
		input.Caddy.ZoneIDs = append(input.Caddy.ZoneIDs, zoneIDs...)
	}
	if input.CloudflareTunnel != nil {
		input.CloudflareTunnel.ZoneIDs = append(input.CloudflareTunnel.ZoneIDs, zoneIDs...)
	}
}
