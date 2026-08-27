package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

// runner: list | add | show | remove. Scope = tenant (org-scoped) OR
// project (repo-scoped) — see api-cli.md: "`?tenant=` or `?project=`".
// The GitHub registration token is operator-provided and short-lived —
// Groundplane never generates or auto-fetches it. See mvp.md, "Runner".
func newRunnerCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "runner", Short: "GitHub Actions self-hosted runners"}

	list := &cobra.Command{
		Use:   "list",
		Short: "List runners",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			tenantID := ""
			projectID := ""
			var err error
			if app.Scope.Project != "" {
				projectID, err = resolveProjectTarget(cmd, app.Scope.Project)
			} else {
				tenantID, err = resolveTenantTarget(cmd, app.Scope.Tenant)
			}
			if err != nil {
				return err
			}
			page, err := app.Client.ListRunners(cmd.Context(), tenantID, projectID, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, runner := range page.Items {
				items[index] = runnerFields(runner)
			}
			headers, rows := tabulateVia(app, items)
			return app.Out.Render(headers, rows, page)
		},
	}
	cmd.AddCommand(list)

	var project, token string
	add := &cobra.Command{
		Use:   "add",
		Short: "Register a runner (repo-scoped with -p/--project, org-scoped without)",
		Args:  cobra.NoArgs,
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
	add.Flags().
		StringVar(&project, "project", "", "repo-scoped: the project slug (defaults to -p/--project; omit both for org-scoped)")
	add.Flags().StringVar(&token, "token", "", "short-lived GitHub registration token (obtained manually — see mvp.md)")
	_ = add.MarkFlagRequired("token")
	cmd.AddCommand(add)

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show a runner (online/offline status)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			runner, err := app.Client.ShowRunner(cmd.Context(), target(app, args[0]))
			if err != nil {
				return err
			}
			headers, rows := tabulateVia(app, []map[string]any{runnerFields(runner)})
			return app.Out.Render(headers, rows, runner)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <id>",
		Aliases: []string{"delete"},
		Short:   "Remove the managed runner container and record (GitHub deregistration remains manual)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/runners/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}

func runnerFields(runner apiTypes.Runner) map[string]any {
	return map[string]any{
		"id":         runner.ID,
		"tenant_id":  runner.TenantID,
		"project_id": runner.ProjectID,
		"labels":     runner.Labels,
		"online":     runner.Online,
	}
}
