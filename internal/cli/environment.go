package cli

import (
	"fmt"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// environment (env): list | show | create | rename | apply | delete | logs
// [--follow]. Environment ids are static; the name is only a human label
// convenience — rename never breaks references. See mvp.md,
// "Environment", and blueprint.md, "Identity and rename rules".
func newEnvironmentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "environment",
		Aliases: []string{"env"},
		Short:   "Environments — one deployable instance of a project",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List environments",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			projectID, err := resolveProjectTarget(cmd, app.Scope.Project)
			if err != nil {
				return err
			}
			page, err := app.Client.ListEnvironments(cmd.Context(), projectID, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, environment := range page.Items {
				items[index] = environmentFields(environment)
			}
			headers, rows := tabulateVia(app, items)
			return app.Out.Render(headers, rows, page)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <name>",
		Short: "Show an environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveEnvironmentTarget(cmd, args[0])
			if err != nil {
				return err
			}
			environment, err := fromContext(cmd).Client.ShowEnvironment(cmd.Context(), id)
			if err != nil {
				return err
			}
			return renderEnvironment(cmd, environment)
		},
	})

	var networkPool string
	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create an environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			projectID, err := resolveProjectTarget(cmd, app.Scope.Project)
			if err != nil {
				return err
			}
			accepted, err := app.Client.CreateEnvironment(cmd.Context(), apiTypes.EnvironmentCreate{
				Name: args[0], ProjectID: projectID, NetworkPool: networkPool,
			})
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, accepted)
		},
	}
	create.Flags().StringVar(&networkPool, "network-pool", "", "reserved IPv4 CIDR for the environment")
	_ = create.MarkFlagRequired("network-pool")
	cmd.AddCommand(create)

	var newName string
	rename := &cobra.Command{
		Use:   "rename <name>",
		Short: "Rename an environment (label only — its id, and everything scoped to it, is unchanged)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveEnvironmentTarget(cmd, args[0])
			if err != nil {
				return err
			}
			environment, err := fromContext(cmd).Client.RenameEnvironment(cmd.Context(), id, newName)
			if err != nil {
				return err
			}
			return renderEnvironment(cmd, environment)
		},
	}
	rename.Flags().StringVar(&newName, "name", "", "new display name")
	_ = rename.MarkFlagRequired("name")
	cmd.AddCommand(rename)

	var bundleDirectory string
	var rootPath string
	var composeSources []string
	var interpolation []string
	apply := &cobra.Command{
		Use:   "apply <name>",
		Short: "Replace an environment's complete Blueprint desired state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveEnvironmentTarget(cmd, args[0])
			if err != nil {
				return err
			}
			body, contentType, err := buildBlueprintMultipart(
				bundleDirectory, rootPath, composeSources, interpolation,
			)
			if err != nil {
				return err
			}
			return runBlueprintApply(cmd, id, body, contentType)
		},
	}
	apply.Flags().StringVar(&bundleDirectory, "bundle-dir", "", "directory containing the closed Blueprint file bundle")
	apply.Flags().StringVar(&rootPath, "root", "", "relative path to the root Blueprint document")
	apply.Flags().StringArrayVar(&composeSources, "compose-file", nil, "additional Compose source in layer order")
	apply.Flags().StringArrayVar(&interpolation, "var", nil, "non-secret Compose interpolation KEY=VALUE")
	_ = apply.MarkFlagRequired("bundle-dir")
	_ = apply.MarkFlagRequired("root")
	cmd.AddCommand(apply)

	var follow bool
	logs := &cobra.Command{
		Use:   "logs",
		Short: "Tail every service's logs across the environment",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			id, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
			if err != nil {
				return err
			}
			path := "/api/v1/environments/" + id + "/logs"
			q := map[string]string{}
			if follow {
				q["follow"] = "true"
			}
			return app.Client.Stream(cmd.Context(), path, q, func(line string) error {
				_, err := fmt.Fprintln(cmd.OutOrStdout(), line)
				return err
			})
		},
	}
	logs.Flags().BoolVarP(&follow, "follow", "f", false, "stream new log lines as they arrive")
	cmd.AddCommand(logs)

	cmd.AddCommand(&cobra.Command{
		Use:     "delete <name>",
		Aliases: []string{"remove"},
		Short:   "Delete an environment (typed confirmation in the Console; removes everything scoped to it — dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveEnvironmentTarget(cmd, args[0])
			if err != nil {
				return err
			}
			return runDestroy(cmd, "/api/v1/environments/"+id)
		},
	})

	return cmd
}

func resolveEnvironmentTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	projectID, err := resolveProjectTarget(cmd, app.Scope.Project)
	if err != nil {
		return "", err
	}
	cursor := ""
	for {
		page, err := app.Client.ListEnvironments(cmd.Context(), projectID, 200, cursor)
		if err != nil {
			return "", err
		}
		for _, environment := range page.Items {
			if environment.Name == argument {
				return target(app, environment.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindEnvironmentNotFound, "environment name %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func renderEnvironment(cmd *cobra.Command, environment apiTypes.Environment) error {
	item := environmentFields(environment)
	fields, values := fieldsOfVia(item)
	return fromContext(cmd).Out.RenderOne(fields, values, environment)
}

func environmentFields(environment apiTypes.Environment) map[string]any {
	return map[string]any{
		"id": environment.ID, "project_id": environment.ProjectID, "name": environment.Name,
		"network_pool": environment.NetworkPool, "volume_dir": environment.VolumeDir,
		"provisioning_state": environment.ProvisioningState,
		"create_task_id":     environment.CreateTaskID,
	}
}

func renderTaskAccepted(cmd *cobra.Command, accepted apiTypes.TaskAccepted) error {
	item := map[string]any{"task_id": accepted.TaskID}
	fields, values := fieldsOfVia(item)
	return fromContext(cmd).Out.RenderOne(fields, values, accepted)
}
