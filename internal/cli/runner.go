package cli

import (
	"io"
	"strings"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

const maximumGitHubRegistrationTokenBytes = 4096

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

	var project, tokenFile, githubURL string
	var labels []string
	add := &cobra.Command{
		Use:   "add <slug>",
		Short: "Register a runner (repo-scoped with -p/--project, org-scoped without)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			proj := project
			if proj == "" {
				proj = app.Scope.Project
			}
			request := apiTypes.RunnerCreateRequest{Slug: args[0], GitHubURL: githubURL, Labels: labels}
			var err error
			if proj != "" {
				request.ProjectID, err = resolveProjectTarget(cmd, proj)
			} else {
				request.TenantID, err = resolveTenantTarget(cmd, app.Scope.Tenant)
			}
			if err != nil {
				return err
			}
			request.RegistrationToken, err = readRegistrationToken(tokenFile, cmd.InOrStdin())
			if err != nil {
				return err
			}
			accepted, err := app.Client.CreateRunner(cmd.Context(), request)
			request.RegistrationToken = ""
			if err != nil {
				return err
			}
			headers, rows := tabulateVia(app, []map[string]any{{"task_id": accepted.TaskID}})
			return app.Out.Render(headers, rows, accepted)
		},
	}
	add.Flags().
		StringVar(&project, "project", "", "repo-scoped: the project slug (defaults to -p/--project; omit both for org-scoped)")
	add.Flags().
		StringVar(&tokenFile, "registration-token-file", "", "read the short-lived GitHub registration token from PATH, or - for stdin")
	add.Flags().StringVar(&githubURL, "github-url", "", "GitHub organization or repository URL")
	add.Flags().StringSliceVar(&labels, "label", nil, "additional GitHub Runner label (repeatable)")
	_ = add.MarkFlagRequired("registration-token-file")
	_ = add.MarkFlagRequired("github-url")
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

	var retryTokenFile string
	retry := &cobra.Command{
		Use:   "retry <slug>",
		Short: "Retry failed Runner creation with a fresh registration token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			runnerID, err := resolveRunnerTarget(cmd, args[0])
			if err != nil {
				return err
			}
			retryToken, err := readRegistrationToken(retryTokenFile, cmd.InOrStdin())
			if err != nil {
				return err
			}
			accepted, err := app.Client.RetryRunner(cmd.Context(), runnerID, retryToken)
			retryToken = ""
			if err != nil {
				return err
			}
			headers, rows := tabulateVia(app, []map[string]any{{"task_id": accepted.TaskID}})
			return app.Out.Render(headers, rows, accepted)
		},
	}
	retry.Flags().
		StringVar(&retryTokenFile, "registration-token-file", "", "read the fresh short-lived GitHub registration token from PATH, or - for stdin")
	_ = retry.MarkFlagRequired("registration-token-file")
	cmd.AddCommand(retry)

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

func readRegistrationToken(path string, stdin io.Reader) (string, error) {
	token, err := readValueFile(path, stdin, maximumGitHubRegistrationTokenBytes, "GitHub registration token")
	if err != nil {
		return "", err
	}
	token = strings.TrimRight(token, "\r\n")
	if token == "" {
		return "", errs.New(errs.KindValidationFailed, "GitHub registration token is required")
	}
	return token, nil
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
