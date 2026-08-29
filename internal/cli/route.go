package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

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
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListRoutes(cmd.Context(), environmentID, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, route := range page.Items {
				items[index] = routeFields(route)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show a route",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			route, err := fromContext(cmd).Client.GetRoute(cmd.Context(), target(fromContext(cmd), args[0]))
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(routeFields(route))
			return fromContext(cmd).Out.RenderOne(fields, values, route)
		},
	})

	var hostname, path, service, exposure string
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
			route, err := app.Client.CreateRoute(cmd.Context(), apiTypes.RouteCreate{
				EnvironmentID: environmentID, Host: hostname, Path: path,
				TargetServiceID: serviceID, TargetPort: targetPort, Exposure: exposure,
			})
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(routeFields(route))
			return app.Out.RenderOne(fields, values, route)
		},
	}
	add.Flags().StringVar(&hostname, "hostname", "", "route hostname")
	add.Flags().StringVar(&path, "path", "/", "absolute path with optional terminal *")
	add.Flags().StringVar(&service, "service", "", "target service name")
	add.Flags().Uint16Var(&targetPort, "target-port", 0, "required internal target port")
	add.Flags().StringVar(
		&exposure,
		"exposure",
		"internal",
		"public | internal (public needs an enabled ingress component)",
	)
	_ = add.MarkFlagRequired("service")
	_ = add.MarkFlagRequired("target-port")
	cmd.AddCommand(add)

	var editExposure string
	edit := &cobra.Command{
		Use:   "edit <id>",
		Short: "Edit a route's exposure",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			route, err := app.Client.EditRoute(
				cmd.Context(), target(app, args[0]), apiTypes.RouteEdit{Exposure: editExposure},
			)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(routeFields(route))
			return app.Out.RenderOne(fields, values, route)
		},
	}
	edit.Flags().StringVar(&editExposure, "exposure", "", "public | internal")
	_ = edit.MarkFlagRequired("exposure")
	cmd.AddCommand(edit)

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <id>",
		Aliases: []string{"delete"},
		Short:   "Remove a route (dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			accepted, err := fromContext(cmd).Client.RemoveRoute(
				cmd.Context(), target(fromContext(cmd), args[0]),
			)
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, accepted)
		},
	})

	return cmd
}

func routeFields(route apiTypes.Route) map[string]any {
	return map[string]any{
		"id": route.ID, "environment_id": route.EnvironmentID, "host": route.Host,
		"path": route.Path, "exposure": route.Exposure,
		"target_service_id": route.TargetServiceID, "target_port": route.TargetPort,
	}
}
