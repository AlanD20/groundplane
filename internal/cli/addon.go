package cli

import "github.com/spf13/cobra"

// addon: list | show | enable | disable | config. Caddy and Cloudflare
// Tunnel are addon KINDS ("ingress.caddy", "edge.cloudflare-tunnel"),
// not bespoke resources — replaces the old `router caddy|tunnel on|off`
// pair. The Router itself is now a read-only projection (see
// `environment show` / the Console); there is no `router` CLI noun. See
// blueprint.md, "x-gp-addons", and api-cli.md's command tree.
func newAddonCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "addon", Short: "Environment addons — ingress (Caddy) and edge (Cloudflare Tunnel), and future kinds"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List addons",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/addons", scopeQuery(fromContext(cmd), "environment"))
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show an addon (status, generated services, health)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/addons/"+target(fromContext(cmd), args[0]))
		},
	})

	var kind string
	enable := &cobra.Command{
		Use:   "enable",
		Short: "Enable (creating if needed) an addon by kind",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/addons", map[string]interface{}{
				"kind": kind, "enabled": true, "environment": app.Scope.Environment,
			})
		},
	}
	enable.Flags().StringVar(&kind, "kind", "", "ingress.caddy | edge.cloudflare-tunnel")
	_ = enable.MarkFlagRequired("kind")
	cmd.AddCommand(enable)

	cmd.AddCommand(&cobra.Command{
		Use:   "disable <id>",
		Short: "Disable an addon",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/addons/"+target(fromContext(cmd), args[0])+"/disable", nil)
		},
	})

	config := &cobra.Command{
		Use:   "config <id>",
		Short: "Set an addon's kind-specific config (e.g. the Caddyfile template, tunnel hostnames)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: kind-specific flags once the addon registry (mirroring
			// internal/adapters' pattern) defines each kind's config shape.
			return runEdit(cmd, "/api/v1/addons/"+target(fromContext(cmd), args[0]), nil)
		},
	}
	cmd.AddCommand(config)

	return cmd
}
