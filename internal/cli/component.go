package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

const (
	maxCaddyfileTemplateBytes = 32 << 10
	maxCorefileTemplateBytes  = 32 << 10
)

// component: list | show | enable | disable | config show | config set |
// update. One noun spans environment and platform owners; each kind's
// registration declares which owner is valid.
func newComponentCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "component", Short: "Manage environment- and platform-owned components"}
	var platform bool
	cmd.PersistentFlags().BoolVar(&platform, "platform", false, "target platform-owned components")
	cmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List components",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				app := fromContext(cmd)
				environmentID := ""
				if !platform {
					var err error
					environmentID, err = resolveEnvironmentTarget(cmd, app.Scope.Environment)
					if err != nil {
						return err
					}
				}
				page, err := app.Client.ListComponents(cmd.Context(), environmentID, platform, "", 0, "")
				if err != nil {
					return err
				}
				items := make([]map[string]any, len(page.Items))
				for index, component := range page.Items {
					items[index] = componentFields(component)
				}
				headers, rows := tabulateVia(app, items)
				return app.Out.Render(headers, rows, page)
			},
		},
	)
	cmd.AddCommand(
		&cobra.Command{
			Use:   "show <id>",
			Short: "Show a component (status, generated services, health)",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				app := fromContext(cmd)
				component, err := app.Client.ShowComponent(cmd.Context(), target(app, args[0]))
				if err != nil {
					return err
				}
				return renderComponent(cmd, component)
			},
		},
	)
	cmd.AddCommand(newComponentEnableCmd())
	cmd.AddCommand(
		&cobra.Command{
			Use:   "disable <id>",
			Short: "Disable a component",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				app := fromContext(cmd)
				accepted, err := app.Client.DisableComponent(cmd.Context(), target(app, args[0]))
				if err != nil {
					return err
				}
				return renderDispatchedTask(cmd, accepted)
			},
		},
	)
	config := &cobra.Command{Use: "config", Short: "A component's kind-specific config"}
	config.AddCommand(
		&cobra.Command{
			Use:   "show <id>",
			Short: "Show a component's config",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				app := fromContext(cmd)
				response, err := app.Client.ShowComponentConfig(cmd.Context(), target(app, args[0]))
				if err != nil {
					return err
				}
				headers, rows := tabulateVia(app, []map[string]any{{
					"config": response.Config, "managed_files": response.ManagedFiles,
				}})
				return app.Out.Render(headers, rows, response)
			},
		},
	)
	var upstreamAuto bool
	var upstreamResolvers []string
	var forwarders []string
	var tailnetDelegation bool
	var configFile string
	var alias string
	var templateFile string
	var zones []string
	set := &cobra.Command{
		Use:   "set <id>",
		Short: "Set a component's config",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body := apiTypes.ComponentConfigMutationInput{}
			flagConfig := cmd.Flags().Changed("upstream-auto") || cmd.Flags().Changed("upstream") ||
				cmd.Flags().Changed("forward") || cmd.Flags().Changed("tailnet-delegation") ||
				cmd.Flags().Changed("template-file")
			app := fromContext(cmd)
			componentFile := cmd.Flags().Changed("file")
			componentTemplate := cmd.Flags().Changed("template-file")
			componentZone := cmd.Flags().Changed("zone")
			componentAlias := cmd.Flags().Changed("alias")
			component, err := app.Client.ShowComponent(cmd.Context(), target(app, args[0]))
			if err != nil {
				return err
			}
			if componentAlias && component.Kind != "caddy" {
				return fmt.Errorf("--alias is valid only for Caddy")
			}
			if componentZone && component.EnvironmentID == "" {
				return fmt.Errorf("zone placement requires an environment-owned Component")
			}
			zoneIDs, err := resolveComponentZoneIDs(cmd, component.EnvironmentID, zones)
			if err != nil {
				return err
			}
			if component.Kind == "coredns" {
				if !componentTemplate {
					return fmt.Errorf("CoreDNS config requires --template-file PATH or --template-file -")
				}
				if componentFile || componentZone {
					return fmt.Errorf("CoreDNS config accepts --template-file and resolver flags only")
				}
				current, currentErr := app.Client.ShowComponentConfig(cmd.Context(), target(app, args[0]))
				if currentErr != nil {
					return currentErr
				}
				coreDNS := apiTypes.CoreDNSComponentConfigMutationInput{}
				if current.Config != nil && current.Config.CoreDNS != nil {
					upstreamAutoValue := current.Config.CoreDNS.UpstreamAuto
					upstreamResolverValues := append([]string(nil), current.Config.CoreDNS.UpstreamResolvers...)
					forwarderValues := append(
						[]apiTypes.ComponentDNSForwarder(nil),
						current.Config.CoreDNS.Forwarders...)
					tailnetValue := current.Config.CoreDNS.TailnetDelegation
					coreDNS.UpstreamAuto = &upstreamAutoValue
					coreDNS.UpstreamResolvers = &upstreamResolverValues
					coreDNS.Forwarders = &forwarderValues
					coreDNS.TailnetDelegation = &tailnetValue
				}
				value, readErr := readValueFile(
					templateFile, cmd.InOrStdin(), maxCorefileTemplateBytes, "Corefile template",
				)
				if readErr != nil {
					return readErr
				}
				coreDNS.CorefileTemplate = &value
				body.CoreDNS = &coreDNS
			} else if componentFile || componentZone || componentAlias {
				if component.Kind == "caddy" {
					if flagConfig {
						return fmt.Errorf("config for Caddy accepts only --file and --zone")
					}
					caddy := apiTypes.CaddyComponentConfigMutationInput{}
					if component.Config != nil && component.Config.Caddy != nil {
						caddy.ZoneIDs = append([]string(nil), component.Config.Caddy.ZoneIDs...)
						caddy.CaddyfileTemplate = component.Config.Caddy.CaddyfileTemplate
						caddy.Alias = component.Config.Caddy.Alias
					}
					if componentZone {
						caddy.ZoneIDs = append([]string(nil), zoneIDs...)
					}
					if componentFile {
						value, readErr := readValueFile(configFile, cmd.InOrStdin(), maxCaddyfileTemplateBytes, "Caddyfile template")
						if readErr != nil {
							return readErr
						}
						caddy.CaddyfileTemplate = value
					}
					if componentAlias {
						caddy.Alias = alias
					}
					if len(caddy.ZoneIDs) == 0 {
						return fmt.Errorf("config for Caddy requires --zone when no current Zone is configured")
					}
					body.Caddy = &caddy
				} else if component.Kind == "cloudflare-tunnel" {
					if flagConfig {
						return fmt.Errorf("config for Cloudflare Tunnel accepts only --file and --zone")
					}
					if !componentFile && !component.Enabled {
						return fmt.Errorf("zone placement for Cloudflare Tunnel requires --file while disabled or unconfigured")
					}
					if componentFile {
						value, readErr := readValueFile(configFile, cmd.InOrStdin(), 64<<10, "component config")
						if readErr != nil {
							return readErr
						}
						body, err = decodeCloudflareTunnelConfigFile(value, componentZone)
						if err != nil {
							return err
						}
					} else {
						current, currentErr := app.Client.ShowComponentConfig(cmd.Context(), target(app, args[0]))
						if currentErr != nil {
							return currentErr
						}
						if current.Config == nil || current.Config.CloudflareTunnel == nil {
							return fmt.Errorf("zone placement for Cloudflare Tunnel requires --file while disabled or unconfigured")
						}
						body.CloudflareTunnel = &apiTypes.CloudflareTunnelComponentConfigMutationInput{
							ZoneIDs: append([]string(nil), current.Config.CloudflareTunnel.ZoneIDs...),
							Credential: apiTypes.CloudflareTunnelCredentialInput{
								Mode: "existing", SecretID: current.Config.CloudflareTunnel.SecretID,
							},
						}
					}
					if componentZone {
						body.CloudflareTunnel.ZoneIDs = append([]string(nil), zoneIDs...)
					}
				} else {
					if componentZone {
						return fmt.Errorf("--zone is valid only for Caddy and Cloudflare Tunnel")
					}
					if flagConfig {
						return fmt.Errorf("--file cannot be combined with kind-specific config flags")
					}
					value, readErr := readValueFile(configFile, cmd.InOrStdin(), 64<<10, "component config")
					if readErr != nil {
						return readErr
					}
					if err := json.Unmarshal([]byte(value), &body); err != nil {
						return fmt.Errorf("component config file must contain one JSON object: %w", err)
					}
				}
			} else if componentTemplate {
				return fmt.Errorf("--template-file is valid only for CoreDNS")
			}
			if body.CoreDNS == nil && flagConfig {
				body.CoreDNS = &apiTypes.CoreDNSComponentConfigMutationInput{}
			}
			if cmd.Flags().Changed("upstream-auto") {
				body.CoreDNS.UpstreamAuto = &upstreamAuto
			}
			if cmd.Flags().Changed("upstream") {
				values := append([]string(nil), upstreamResolvers...)
				body.CoreDNS.UpstreamResolvers = &values
			}
			if cmd.Flags().Changed("forward") {
				parsed, err := parseCoreDNSForwardFlags(forwarders)
				if err != nil {
					return err
				}
				body.CoreDNS.Forwarders = &parsed
			}
			if cmd.Flags().Changed("tailnet-delegation") {
				body.CoreDNS.TailnetDelegation = &tailnetDelegation
			}
			result, err := app.Client.SetComponentConfig(
				cmd.Context(), target(app, args[0]), body,
			)
			if err != nil {
				return err
			}
			fields := map[string]any{"config": result.Resource, "reconcile_task_id": result.ReconcileTaskID}
			headers, rows := tabulateVia(app, []map[string]any{fields})
			return app.Out.Render(headers, rows, result)
		},
	}
	set.Flags().
		BoolVar(&upstreamAuto, "upstream-auto", false, "derive upstream resolvers from the authoritative host baseline")
	set.Flags().StringArrayVar(&upstreamResolvers, "upstream", nil, "repeatable upstream resolver endpoint")
	set.Flags().StringArrayVar(&forwarders, "forward", nil, "repeatable DOMAIN=RESOLVER[,RESOLVER...]")
	set.Flags().BoolVar(&tailnetDelegation, "tailnet-delegation", false, "delegate ts.net to the tailnet resolver")
	set.Flags().
		StringVar(&configFile, "file", "", "read a Caddyfile template (Caddy) or complete JSON config (other kinds) from PATH, or -")
	set.Flags().
		StringVar(&templateFile, "template-file", "", "read the required CoreDNS Corefile template from PATH, or -")
	set.Flags().StringVar(&alias, "alias", "", "optional Caddy primary-Zone alias; empty clears it")
	set.Flags().StringArrayVar(&zones, "zone", nil, "repeatable Zone slug; with global --id, repeatable stable Zone id")
	config.AddCommand(set)
	cmd.AddCommand(config)
	cmd.AddCommand(
		&cobra.Command{
			Use:   "update <id>",
			Short: "Update a component",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				app := fromContext(cmd)
				accepted, err := app.Client.UpdateComponent(cmd.Context(), target(app, args[0]))
				if err != nil {
					return err
				}
				return renderDispatchedTask(cmd, accepted)
			},
		},
	)
	return cmd
}

func renderComponent(cmd *cobra.Command, component apiTypes.Component) error {
	fields := componentFields(component)
	headers, values := fieldsOfVia(fields)
	return fromContext(cmd).Out.RenderOne(headers, values, component)
}

func componentFields(component apiTypes.Component) map[string]any {
	return map[string]any{
		"id": component.ID, "owner": component.Owner, "owner_id": component.OwnerID,
		"environment_id": component.EnvironmentID, "kind": component.Kind, "enabled": component.Enabled,
		"config": component.Config, "generated_services": component.GeneratedServices,
		"pinned_ipv4": component.PinnedIPv4, "healthy": component.Healthy, "status": component.Status,
	}
}

func parseCoreDNSForwardFlags(values []string) ([]apiTypes.ComponentDNSForwarder, error) {
	result := make([]apiTypes.ComponentDNSForwarder, 0, len(values))
	for _, value := range values {
		domain, resolvers, ok := strings.Cut(value, "=")
		if !ok || domain == "" || resolvers == "" || strings.IndexFunc(value, unicode.IsSpace) >= 0 {
			return nil, fmt.Errorf("--forward must use DOMAIN=RESOLVER[,RESOLVER...]")
		}
		parts := strings.Split(resolvers, ",")
		for index := range parts {
			if parts[index] == "" {
				return nil, fmt.Errorf("--forward contains an empty resolver")
			}
		}
		result = append(result, apiTypes.ComponentDNSForwarder{
			Domain: domain, Resolvers: parts,
		})
	}
	return result, nil
}
