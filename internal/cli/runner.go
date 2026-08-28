package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// runner: list | add | show | edit | retry | remove. Scope = tenant (org-scoped) OR
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
		Use:   "show <slug>",
		Short: "Show a runner (online/offline status)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			runner, err := app.Client.ShowRunner(cmd.Context(), runnerID)
			if err != nil {
				return err
			}
			headers, rows := tabulateVia(app, []map[string]any{runnerFields(runner)})
			return app.Out.Render(headers, rows, runner)
		},
	})

	var nextSlug string
	edit := &cobra.Command{
		Use:   "edit <slug>",
		Short: "Replace a Runner slug without changing its GitHub registration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			runner, err := fromContext(cmd).Client.EditRunner(cmd.Context(), runnerID, nextSlug)
			if err != nil {
				return err
			}
			headers, rows := tabulateVia(fromContext(cmd), []map[string]any{runnerFields(runner)})
			return fromContext(cmd).Out.Render(headers, rows, runner)
		},
	}
	edit.Flags().StringVar(&nextSlug, "slug", "", "new Tenant-unique Runner slug")
	_ = edit.MarkFlagRequired("slug")
	cmd.AddCommand(edit)

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <slug>",
		Aliases: []string{"delete"},
		Short:   "Remove the managed runner container and record (GitHub deregistration remains manual)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := app.Client.RemoveRunner(cmd.Context(), runnerID)
			if err != nil {
				return err
			}
			headers, rows := tabulateVia(app, []map[string]any{{"task_id": accepted.TaskID}})
			return app.Out.Render(headers, rows, accepted)
		},
	})

	return cmd
}

func resolveRunnerTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	tenantID := ""
	projectID := ""
	var err error
	if app.Scope.Project != "" {
		projectID, err = resolveProjectTarget(cmd, app.Scope.Project)
	} else {
		tenantID, err = resolveTenantTarget(cmd, app.Scope.Tenant)
	}
	if err != nil {
		return "", err
	}
	for cursor := ""; ; {
		page, err := app.Client.ListRunners(cmd.Context(), tenantID, projectID, 200, cursor)
		if err != nil {
			return "", err
		}
		for _, runner := range page.Items {
			if runner.Slug == argument {
				return target(app, runner.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindRunnerNotFound, "Runner slug %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func runnerFields(runner apiTypes.Runner) map[string]any {
	return map[string]any{
		"id":             runner.ID,
		"slug":           runner.Slug,
		"tenant_id":      runner.TenantID,
		"project_id":     runner.ProjectID,
		"github_url":     runner.GitHubURL,
		"name":           runner.Name,
		"labels":         runner.Labels,
		"lifecycle":      runner.Lifecycle,
		"create_task_id": runner.CreateTaskID,
		"remove_task_id": runner.RemoveTaskID,
		"online":         runner.Online,
		"observed_at":    runner.ObservedAt,
		"created_at":     runner.CreatedAt,
	}
}
