package cli

import "github.com/spf13/cobra"

// component: list | show | enable | disable | config. Caddy and Cloudflare
// Tunnel are component KINDS ("ingress.caddy", "edge.cloudflare-tunnel"),
// not bespoke resources — replaces the old `router caddy|tunnel on|off`
// pair. The Router itself is now a read-only projection (see
// `environment show` / the Console); there is no `router` CLI noun. See
// blueprint.md, "x-gp-components", and api-cli.md's command tree.
func newComponentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "component",
		Short: "Manage environment- and platform-owned components",
	}

	var platform bool
	cmd.PersistentFlags().BoolVar(&platform, "platform", false, "target platform-owned components")

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List components",
		RunE: func(cmd *cobra.Command, args []string) error {
			query := scopeQuery(fromContext(cmd), "environment")
			if platform {
				query.Del("environment")
				query.Set("platform", "true")
			}
			return runList(cmd, "/api/v1/components", query)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show an component (status, generated services, health)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/components/"+target(fromContext(cmd), args[0]))
		},
	})

	var kind string
	enable := &cobra.Command{
		Use:   "enable",
		Short: "Enable (creating if needed) an component by kind",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/components", map[string]interface{}{
				"kind":        kind,
				"enabled":     true,
				"environment": app.Scope.Environment,
				"platform":    platform,
			})
		},
	}
	enable.Flags().StringVar(&kind, "kind", "", "ingress.caddy | edge.cloudflare-tunnel")
	_ = enable.MarkFlagRequired("kind")
	cmd.AddCommand(enable)

	cmd.AddCommand(&cobra.Command{
		Use:   "disable <id>",
		Short: "Disable an component",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/components/"+target(fromContext(cmd), args[0])+"/disable", nil)
		},
	})

	config := &cobra.Command{
		Use:   "config <id>",
		Short: "Set an component's kind-specific config (e.g. the Caddyfile template, tunnel hostnames)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: kind-specific flags once the component registry (mirroring
			// internal/adapters' pattern) defines each kind's config shape.
			return runEdit(cmd, "/api/v1/components/"+target(fromContext(cmd), args[0])+"/config", nil)
		},
	}
	cmd.AddCommand(config)

	cmd.AddCommand(&cobra.Command{
		Use:   "update <id>",
		Short: "Update a component",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/components/"+target(fromContext(cmd), args[0])+"/update", nil)
		},
	})

	return cmd
}
