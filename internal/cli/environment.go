package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// environment (env): list | show | create | rename | delete | logs
// [--follow]. Environment ids are static; the label is only a display
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
			return runList(cmd, "/api/v1/environments", scopeQuery(fromContext(cmd), "project"))
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <slug>",
		Short: "Show an environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/environments/"+target(fromContext(cmd), args[0]))
		},
	})

	create := &cobra.Command{
		Use:   "create <slug>",
		Short: "Create an environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/environments", map[string]string{
				"slug": args[0], "project": app.Scope.Project,
			})
		},
	}
	cmd.AddCommand(create)

	var newName string
	rename := &cobra.Command{
		Use:   "rename <slug>",
		Short: "Rename an environment (label only — its id, and everything scoped to it, is unchanged)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := "/api/v1/environments/" + target(fromContext(cmd), args[0]) + "/rename"
			return runAction(cmd, path, map[string]string{"name": newName})
		},
	}
	rename.Flags().StringVar(&newName, "name", "", "new display name")
	_ = rename.MarkFlagRequired("name")
	cmd.AddCommand(rename)

	var follow bool
	logs := &cobra.Command{
		Use:   "logs",
		Short: "Tail every service's logs across the environment",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			path := "/api/v1/environments/" + target(app, app.Scope.Environment) + "/logs"
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
		Use:     "delete <slug>",
		Aliases: []string{"remove"},
		Short:   "Delete an environment (typed confirmation in the Console; removes everything scoped to it — dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/environments/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
