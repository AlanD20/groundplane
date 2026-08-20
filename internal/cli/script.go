package cli

import "github.com/spf13/cobra"

// script: list | add | edit | run <name> | remove. Scripts double as
// deploy/rollback hooks via --when. See mvp.md, "Scripts".
func newScriptCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "script", Short: "Per-environment scripts, optionally hooked into deploy/rollback"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List scripts",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/scripts", scopeQuery(fromContext(cmd), "environment"))
		},
	})

	var service, body, when string
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a script",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/scripts", map[string]string{
				"name": args[0], "service": service, "script": body, "when": when, "environment": app.Scope.Environment,
			})
		},
	}
	add.Flags().StringVar(&service, "service", "", "service the script runs against")
	add.Flags().StringVar(&body, "script", "", "one-line or multi-line script body")
	add.Flags().
		StringVar(&when, "when", "manual", "manual | pre-deploy | post-deploy | pre-rollback | post-rollback | on-failure")
	_ = add.MarkFlagRequired("service")
	_ = add.MarkFlagRequired("script")
	cmd.AddCommand(add)

	var editBody, editWhen string
	edit := &cobra.Command{
		Use:   "edit <name>",
		Short: "Edit a script",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body := changedStringFields(cmd, map[string]string{"script": editBody, "when": editWhen})
			return runPatch(cmd, "/api/v1/scripts/"+target(fromContext(cmd), args[0]), body)
		},
	}
	edit.Flags().StringVar(&editBody, "script", "", "new script body")
	edit.Flags().
		StringVar(&editWhen, "when", "", "new hook: manual | pre-deploy | post-deploy | pre-rollback | post-rollback | on-failure")
	cmd.AddCommand(edit)

	cmd.AddCommand(&cobra.Command{
		Use:   "run <name>",
		Short: "Run a script now",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/scripts/"+target(fromContext(cmd), args[0])+"/run", nil)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <name>",
		Aliases: []string{"delete"},
		Short:   "Remove a script (dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/scripts/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
