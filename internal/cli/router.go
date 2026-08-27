package cli

import "github.com/spf13/cobra"

// router exposes the environment's read-only ingress projection.
func newRouterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "router",
		Short: "Inspect the environment router projection",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show the environment router projection",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := fromContext(cmd)
			environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
			if err != nil {
				return err
			}
			router, err := app.Client.ShowRouter(cmd.Context(), environmentID)
			if err != nil {
				return err
			}
			fields := map[string]any{"caddy": router.Caddy, "tunnel": router.Tunnel}
			headers, values := fieldsOfVia(fields)
			return app.Out.RenderOne(headers, values, router)
		},
	})

	return cmd
}
