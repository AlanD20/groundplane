package cli

import (
	"fmt"
	"strings"

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
		query := scopeQuery(fromContext(cmd), "environment")
		if platform {
			delete(query, "environment")
			query["platform"] = "true"
		}
		return runList(cmd, "/api/v1/components", query)
	}})
	cmd.AddCommand(&cobra.Command{Use: "show <id>", Short: "Show a component (status, generated services, health)", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runShow(cmd, "/api/v1/components/"+target(fromContext(cmd), args[0]))
	}})
	cmd.AddCommand(&cobra.Command{Use: "enable <id>", Short: "Enable a component", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runAction(cmd, "/api/v1/components/"+target(fromContext(cmd), args[0])+"/enable", nil)
	}})
	cmd.AddCommand(&cobra.Command{Use: "disable <id>", Short: "Disable a component", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runAction(cmd, "/api/v1/components/"+target(fromContext(cmd), args[0])+"/disable", nil)
	}})
	config := &cobra.Command{Use: "config", Short: "A component's kind-specific config"}
	config.AddCommand(&cobra.Command{Use: "show <id>", Short: "Show a component's config", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runShow(cmd, "/api/v1/components/"+target(fromContext(cmd), args[0])+"/config")
	}})
	var upstreamAuto bool
	var upstreamResolvers []string
	var forwarders []string
	var tailnetDelegation bool
	set := &cobra.Command{Use: "set <id>", Short: "Set a component's config", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		body := make(map[string]any)
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
		path := "/api/v1/components/" + target(fromContext(cmd), args[0]) + "/config"
		if len(body) == 0 {
			return runReplaceSingleton(cmd, path, nil)
		}
		return runReplaceSingleton(cmd, path, body)
	}}
	set.Flags().BoolVar(&upstreamAuto, "upstream-auto", false, "derive upstream resolvers from the authoritative host baseline")
	set.Flags().StringArrayVar(&upstreamResolvers, "upstream", nil, "repeatable upstream resolver endpoint")
	set.Flags().StringArrayVar(&forwarders, "forward", nil, "repeatable DOMAIN=RESOLVER[,RESOLVER...]")
	set.Flags().BoolVar(&tailnetDelegation, "tailnet-delegation", false, "delegate ts.net to the tailnet resolver")
	config.AddCommand(set)
	cmd.AddCommand(config)
	cmd.AddCommand(&cobra.Command{Use: "update <id>", Short: "Update a component", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runAction(cmd, "/api/v1/components/"+target(fromContext(cmd), args[0])+"/update", nil)
	}})
	return cmd
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
