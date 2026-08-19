package cli

import "github.com/spf13/cobra"

// runner: list | add | show | remove. Scope = tenant (org-scoped) OR
// project (repo-scoped) — see api-cli.md: "`?tenant=` or `?project=`".
// The GitHub registration token is operator-provided and short-lived —
// Groundplane never generates or auto-fetches it. See mvp.md, "Runner".
func newRunnerCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "runner", Short: "GitHub Actions self-hosted runners"}

	list := &cobra.Command{
		Use:   "list",
		Short: "List runners",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/runners", scopeQuery(fromContext(cmd), "tenant", "project"))
		},
	}
	cmd.AddCommand(list)

	var project, token string
	add := &cobra.Command{
		Use:   "add",
		Short: "Register a runner (repo-scoped with -p/--project, org-scoped without)",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			proj := project
			if proj == "" {
				proj = app.Scope.Project
			}
			return runCreate(cmd, "/api/v1/runners", map[string]string{
				"tenant": app.Scope.Tenant, "project": proj, "registration_token": token,
			})
		},
	}
	add.Flags().StringVar(&project, "project", "", "repo-scoped: the project slug (defaults to -p/--project; omit both for org-scoped)")
	add.Flags().StringVar(&token, "token", "", "short-lived GitHub registration token (obtained manually — see mvp.md)")
	_ = add.MarkFlagRequired("token")
	cmd.AddCommand(add)

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show a runner (online/offline status)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/runners/"+target(fromContext(cmd), args[0]))
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <id>",
		Aliases: []string{"delete"},
		Short:   "Remove a runner",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRemove(cmd, "/api/v1/runners/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
