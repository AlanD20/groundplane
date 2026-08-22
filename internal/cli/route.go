package cli

import "github.com/spf13/cobra"

// route: list | show | add | edit | remove. Public routes need an enabled
// ingress component to be served. See mvp.md, "Route", and blueprint.md,
// "x-gp-routes".
func newRouteCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "route", Short: "Routes — a domain or path sending traffic to a service"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List routes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/routes", scopeQuery(fromContext(cmd), "environment"))
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show a route",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/routes/"+target(fromContext(cmd), args[0]))
		},
	})

	var host, path, service, exposure string
	var targetPort uint16
	add := &cobra.Command{
		Use:   "add",
		Short: "Add a route",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
			if err != nil {
				return err
			}
			serviceID, err := resolveServiceTarget(cmd, service)
			if err != nil {
				return err
			}
			return runCreate(cmd, "/api/v1/routes", map[string]any{
				"host": host, "path": path, "target_service_id": serviceID,
				"target_port": targetPort, "exposure": exposure, "environment_id": environmentID,
			})
		},
	}
	add.Flags().StringVar(&host, "host", "", "hostname")
	add.Flags().StringVar(&path, "path", "/", "path prefix")
	add.Flags().StringVar(&service, "service", "", "target service name")
	add.Flags().Uint16Var(&targetPort, "target-port", 0, "required internal target port")
	add.Flags().
		StringVar(&exposure, "exposure", "internal", "public | internal (public needs an enabled ingress component)")
	_ = add.MarkFlagRequired("service")
	_ = add.MarkFlagRequired("target-port")
	cmd.AddCommand(add)

	var editExposure string
	edit := &cobra.Command{
		Use:   "edit <id>",
		Short: "Edit a route",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPatch(
				cmd,
				"/api/v1/routes/"+target(fromContext(cmd), args[0]),
				changedStringFields(cmd, map[string]string{"exposure": editExposure}),
			)
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
