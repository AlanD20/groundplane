package cli

import (
	"fmt"
	"net/http"

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
			return runList(cmd, "/api/v1/environments", map[string]string{"project": projectID})
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
			return runShow(cmd, "/api/v1/environments/"+id)
		},
	})

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
			return runCreate(cmd, "/api/v1/environments", map[string]string{
				"name": args[0], "project_id": projectID,
			})
		},
	}
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
			path := "/api/v1/environments/" + id + "/rename"
			return runPostUpdate(cmd, path, map[string]string{"name": newName})
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
		var page apiTypes.Page[apiTypes.Environment]
		request := app.Client.NewRequest(
			http.MethodGet,
			"/api/v1/environments",
			map[string]string{"project": projectID, "limit": "200", "cursor": cursor},
			nil,
			http.StatusOK,
		)
		if err := app.Client.Do(cmd.Context(), request, &page); err != nil {
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
