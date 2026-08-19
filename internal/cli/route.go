package cli

import "github.com/spf13/cobra"

// route: list | add | edit | remove. Public routes need an enabled
// ingress component to be served. See mvp.md, "Route", and blueprint.md,
// "x-gp-route".
func newRouteCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "route", Short: "Routes — a domain or path sending traffic to a service"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List routes",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/routes", scopeQuery(fromContext(cmd), "environment"))
		},
	})

	var host, path, service, exposure string
	add := &cobra.Command{
		Use:   "add",
		Short: "Add a route",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/routes", map[string]string{
				"host": host, "path": path, "service": service, "exposure": exposure, "environment": app.Scope.Environment,
			})
		},
	}
	add.Flags().StringVar(&host, "host", "", "hostname")
	add.Flags().StringVar(&path, "path", "/", "path prefix")
	add.Flags().StringVar(&service, "service", "", "target service name")
	add.Flags().StringVar(&exposure, "exposure", "internal", "public | internal (public needs an enabled ingress component)")
	_ = add.MarkFlagRequired("service")
	cmd.AddCommand(add)

	var editExposure string
	edit := &cobra.Command{
		Use:   "edit <id>",
		Short: "Edit a route",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPatch(cmd, "/api/v1/routes/"+target(fromContext(cmd), args[0]), map[string]string{"exposure": editExposure})
		},
	}
	edit.Flags().StringVar(&editExposure, "exposure", "", "public | internal")
	cmd.AddCommand(edit)

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <id>",
		Aliases: []string{"delete"},
		Short:   "Remove a route (dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/routes/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
