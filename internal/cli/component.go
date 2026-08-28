package cli

import (
	"encoding/json"
	"fmt"
	"strings"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

// component: list | show | enable | disable | config show | config set |
// update. One noun spans environment and platform owners; each kind's
// registration declares which owner is valid.
func newComponentCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "component", Short: "Manage environment- and platform-owned components"}
	var platform bool
	cmd.PersistentFlags().BoolVar(&platform, "platform", false, "target platform-owned components")
	cmd.AddCommand(&cobra.Command{Use: "list", Short: "List components", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
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
	}})
	cmd.AddCommand(&cobra.Command{Use: "show <id>", Short: "Show a component (status, generated services, health)", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		app := fromContext(cmd)
		component, err := app.Client.ShowComponent(cmd.Context(), target(app, args[0]))
		if err != nil {
			return err
		}
		return renderComponent(cmd, component)
	}})
	cmd.AddCommand(&cobra.Command{Use: "enable <id>", Short: "Enable a component", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		app := fromContext(cmd)
		accepted, err := app.Client.EnableComponent(cmd.Context(), target(app, args[0]))
		if err != nil {
			return err
		}
		return renderDispatchedTask(cmd, accepted)
	}})
	cmd.AddCommand(&cobra.Command{Use: "disable <id>", Short: "Disable a component", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		app := fromContext(cmd)
		accepted, err := app.Client.DisableComponent(cmd.Context(), target(app, args[0]))
		if err != nil {
			return err
		}
		return renderDispatchedTask(cmd, accepted)
	}})
	config := &cobra.Command{Use: "config", Short: "A component's kind-specific config"}
	config.AddCommand(&cobra.Command{Use: "show <id>", Short: "Show a component's config", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		app := fromContext(cmd)
		config, err := app.Client.ShowComponentConfig(cmd.Context(), target(app, args[0]))
		if err != nil {
			return err
		}
		headers, rows := tabulateVia(app, []map[string]any{{"config": config.Config}})
		return app.Out.Render(headers, rows, config)
	}})
	var upstreamAuto bool
	var upstreamResolvers []string
	var forwarders []string
	var tailnetDelegation bool
	var configFile string
	set := &cobra.Command{Use: "set <id>", Short: "Set a component's config", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		body := make(map[string]any)
		flagConfig := cmd.Flags().Changed("upstream-auto") || cmd.Flags().Changed("upstream") ||
			cmd.Flags().Changed("forward") || cmd.Flags().Changed("tailnet-delegation")
		if cmd.Flags().Changed("file") {
			if flagConfig {
				return fmt.Errorf("--file cannot be combined with kind-specific config flags")
			}
			value, err := readValueFile(configFile, cmd.InOrStdin(), 64<<10, "component config")
			if err != nil {
				return err
			}
			if err := json.Unmarshal([]byte(value), &body); err != nil {
				return fmt.Errorf("component config file must contain one JSON object: %w", err)
			}
		}
		if cmd.Flags().Changed("upstream-auto") {
			body["upstream_auto"] = upstreamAuto
		}
		if cmd.Flags().Changed("upstream") {
			body["upstream_resolvers"] = append([]string(nil), upstreamResolvers...)
		}
		if cmd.Flags().Changed("forward") {
			parsed, err := parseCoreDNSForwardFlags(forwarders)
			if err != nil {
				return err
			}
			body["forwarders"] = parsed
		}
		if cmd.Flags().Changed("tailnet-delegation") {
			body["tailnet_delegation"] = tailnetDelegation
		}
		app := fromContext(cmd)
		result, err := app.Client.SetComponentConfig(
			cmd.Context(), target(app, args[0]), apiTypes.ComponentConfig{Config: body},
		)
		if err != nil {
			return err
		}
		fields := map[string]any{"config": result.Resource.Config, "reconcile_task_id": result.ReconcileTaskID}
		headers, rows := tabulateVia(app, []map[string]any{fields})
		return app.Out.Render(headers, rows, result)
	}}
	set.Flags().BoolVar(&upstreamAuto, "upstream-auto", false, "derive upstream resolvers from the authoritative host baseline")
	set.Flags().StringArrayVar(&upstreamResolvers, "upstream", nil, "repeatable upstream resolver endpoint")
	set.Flags().StringArrayVar(&forwarders, "forward", nil, "repeatable DOMAIN=RESOLVER[,RESOLVER...]")
	set.Flags().BoolVar(&tailnetDelegation, "tailnet-delegation", false, "delegate ts.net to the tailnet resolver")
	set.Flags().StringVar(&configFile, "file", "", "read the complete kind-specific JSON config object from PATH, or -")
	config.AddCommand(set)
	cmd.AddCommand(config)
	cmd.AddCommand(&cobra.Command{Use: "update <id>", Short: "Update a component", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		app := fromContext(cmd)
		accepted, err := app.Client.UpdateComponent(cmd.Context(), target(app, args[0]))
		if err != nil {
			return err
		}
		return renderDispatchedTask(cmd, accepted)
	}})
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

func parseCoreDNSForwardFlags(values []string) ([]map[string]any, error) {
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		domain, resolvers, ok := strings.Cut(value, "=")
		if !ok || strings.TrimSpace(domain) == "" || strings.TrimSpace(resolvers) == "" {
			return nil, fmt.Errorf("--forward must use DOMAIN=RESOLVER[,RESOLVER...]")
		}
		parts := strings.Split(resolvers, ",")
		for index := range parts {
			parts[index] = strings.TrimSpace(parts[index])
			if parts[index] == "" {
				return nil, fmt.Errorf("--forward contains an empty resolver")
			}
		}
		result = append(result, map[string]any{"domain": strings.TrimSpace(domain), "resolvers": parts})
	}
	return result, nil
}
