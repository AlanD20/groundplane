package cli

import "github.com/spf13/cobra"

// secret: list | add | show | remove. Scope = project (plus
// the platform default). See mvp.md, "Secrets" and "Secret kinds".
func newSecretCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "secret", Short: "Project-scoped secrets (env variable or file secret)"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List secrets",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/secrets", scopeQuery(fromContext(cmd), "project"))
		},
	})

	var kind, ref string
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a secret",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/secrets", map[string]string{
				"name": args[0], "kind": kind, "ref": ref, "project": app.Scope.Project,
			})
		},
	}
	add.Flags().StringVar(&kind, "kind", "env_var", "env_var | file")
	add.Flags().StringVar(&ref, "ref", "", "env file name or file path this secret is materialized to")
	cmd.AddCommand(add)

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show a secret's metadata (masked)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/secrets/"+target(fromContext(cmd), args[0]))
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <id>",
		Aliases: []string{"delete"},
		Short:   "Remove a secret",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/secrets/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
