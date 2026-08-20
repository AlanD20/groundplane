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
			return runShow(cmd, "/api/v1/environments/"+target(app, app.Scope.Environment)+"/router")
		},
	})

	return cmd
}
