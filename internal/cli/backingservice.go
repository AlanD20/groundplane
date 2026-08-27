package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// backing-service (bs): list | show | create | start | stop | destroy.
// Created explicitly — never lazily. The creation form is the SAME full
// service form plus adapter + facts-prefix fields. See mvp.md, "Backing
// services are created explicitly."
func newBackingServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "backing-service",
		Aliases: []string{"bs"},
		Short:   "Backing services — shared datastores/caches/queues",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List backing services",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			page, err := fromContext(cmd).Client.ListBackingServices(cmd.Context(), 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, backing := range page.Items {
				items[index] = backingServiceFields(backing)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <slug>",
		Short: "Show a backing service (consumers, connection info)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			projectID, err := resolveBackingProjectTarget(cmd, args[0])
			if err != nil {
				return err
			}
			backing, err := fromContext(cmd).Client.ShowBackingService(cmd.Context(), projectID)
			if err != nil {
				return err
			}
			return renderBackingService(cmd, backing)
		},
	})

	var adapter, image, name, prefix string
	create := &cobra.Command{
		Use:   "create <slug>",
		Short: "Create a backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, "/api/v1/backing-services", map[string]string{
				"slug": args[0], "adapter": adapter, "image": image, "name": name, "facts_prefix": prefix,
			})
		},
	}
	create.Flags().
		StringVar(&adapter, "adapter", "", "adapter key, e.g. postgres:16 (see `groundplane backing-service create --help` for the registry)")
	create.Flags().StringVar(&image, "image", "", "container image (defaults to the adapter's default image)")
	create.Flags().StringVar(&name, "name", "", "unique DNS-resolvable service name, e.g. 'postgres'")
	create.Flags().
		StringVar(&prefix, "facts-prefix", "", "facts key prefix, e.g. 'pg16_' (defaults to the adapter's default)")
	_ = create.MarkFlagRequired("adapter")
	_ = create.MarkFlagRequired("name")
	cmd.AddCommand(create)

	cmd.AddCommand(&cobra.Command{
		Use:   "start <slug>",
		Short: "Start a backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBackingServiceAction(cmd, args[0], "start")
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "stop <slug>",
		Short: "Stop a backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBackingServiceAction(cmd, args[0], "stop")
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "destroy <slug>",
		Short: "Remove backing-service runtime while retaining durable data",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBackingServiceAction(cmd, args[0], "destroy")
		},
	})

	return cmd
}

func resolveBackingProjectTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	cursor := ""
	for {
		page, err := app.Client.ListProjects(cmd.Context(), "", "backing", 200, cursor)
		if err != nil {
			return "", err
		}
		for _, project := range page.Items {
			if project.Slug == argument {
				return target(app, project.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindBackingServiceNotFound, "backing-service slug %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func resolveBackingAdapterServiceTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	projectID, err := resolveBackingProjectTarget(cmd, argument)
	if err != nil {
		return "", err
	}
	backing, err := app.Client.ShowBackingService(cmd.Context(), projectID)
	if err != nil {
		return "", err
	}
	return target(app, backing.ServiceID), nil
}

func runBackingServiceAction(cmd *cobra.Command, argument string, action string) error {
	projectID, err := resolveBackingProjectTarget(cmd, argument)
	if err != nil {
		return err
	}
	return runAction(cmd, "/api/v1/backing-services/"+projectID+"/"+action, nil)
}

func renderBackingService(cmd *cobra.Command, backing apiTypes.BackingService) error {
	fields := backingServiceFields(backing)
	headers, values := fieldsOfVia(fields)
	return fromContext(cmd).Out.RenderOne(headers, values, backing)
}

func backingServiceFields(backing apiTypes.BackingService) map[string]any {
	return map[string]any{
		"project_id": backing.ProjectID, "environment_id": backing.EnvironmentID, "service_id": backing.ServiceID,
	}
}
