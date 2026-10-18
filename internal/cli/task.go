package cli

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/internal/cli/apiclient"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// task: list [--env NAME | --workspace platform|<tenant>] [--limit N] [--cursor VALUE] | show <id> |
// events <id> | retry <id> | abort <id>. The activity journal IS this record set. See mvp.md,
// "Baked-in actions become Tasks" and "Activity IS tasks (locked)".
func newTaskCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "task", Short: "Tasks — the one record set behind every action and the activity journal"}
	cmd.AddCommand(newTaskJournalListCmd("List tasks", false))

	var workLimit int64
	var after string
	var revision int64
	show := &cobra.Command{
		Use:   "show <id>",
		Short: "Show a Task, resource work, recorded failures and cleanup state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			task, err := app.Client.ShowTask(
				cmd.Context(),
				args[0],
				apiclient.TaskDetailOptions{Limit: workLimit, After: after, Revision: revision},
			)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(taskFields(task))
			return app.Out.RenderOne(fields, values, task)
		},
	}
	show.Flags().Int64Var(&workLimit, "limit", 25, "work items per page (1–100)")
	show.Flags().StringVar(&after, "after", "", "last work item from the previous page")
	show.Flags().Int64Var(&revision, "revision", 0, "snapshot revision from the previous page")
	cmd.AddCommand(newTaskAttemptsCmd(), newTaskImageCmd(), newTaskStagesCmd(), newTaskAttemptEventsCmd())
	cmd.AddCommand(show)

	cmd.AddCommand(&cobra.Command{
		Use:   "events <id>",
		Short: "Stream a task's events",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return app.Client.StreamTaskEvents(
				cmd.Context(),
				target(app, args[0]),
				func(event apiTypes.TaskEvent) error {
					encoded, err := json.Marshal(event)
					if err != nil {
						return err
					}
					_, err = fmt.Fprintln(cmd.OutOrStdout(), string(encoded))
					return err
				},
			)
		},
	})

	var retryWork string
	var retryAttempt uint64
	retry := &cobra.Command{
		Use:   "retry <id> --work <work-id> --attempt <number>",
		Short: "Authorize Retry of the selected captured work attempt",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			accepted, err := app.Client.RetryTask(cmd.Context(), target(app, args[0]), apiTypes.TaskRetryRequest{WorkItemID: retryWork, Attempt: retryAttempt})
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	retry.Flags().StringVar(&retryWork, "work", "", "work item shown in Task details")
	retry.Flags().Uint64Var(&retryAttempt, "attempt", 0, "recorded attempt count; 0 for work that failed before execution")
	cmd.AddCommand(retry)

	cmd.AddCommand(&cobra.Command{
		Use:   "abort <id>",
		Short: "Abort an in-flight task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			accepted, err := app.Client.AbortTask(cmd.Context(), target(app, args[0]))
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	})

	return cmd
}

func renderTaskPage(cmd *cobra.Command, page apiTypes.ExecutionPage[apiTypes.TaskSummary]) error {
	headers := []string{"ID", "ACTION", "TARGET", "STATUS", "CREATED", "UPDATED"}
	rows := make([][]string, len(page.Items))
	for index, task := range page.Items {
		rows[index] = []string{task.ID, string(task.Action), task.TargetName, task.Status, taskTimestamp(task.CreatedAt), taskTimestamp(task.UpdatedAt)}
	}
	return fromContext(cmd).Out.Render(headers, rows, page)
}
func taskFields(details apiTypes.TaskDetails) map[string]any {
	task := details.Task
	return map[string]any{"id": task.ID, "action": task.Action, "target": task.TargetName,
		"target_id": task.TargetID, "status": task.Status, "scope": task.Scope, "actor": task.Actor,
		"created_at": taskTimestamp(task.CreatedAt), "updated_at": taskTimestamp(task.UpdatedAt),
		"deadline": taskTimestamp(task.Deadline), "finished_at": nullableTaskTimestamp(task.FinishedAt),
		"counts": task.Counts, "cancel": task.Cancel, "history_expired": task.HistoryExpired,
		"preparation": details.Preparation, "work": details.Work}
}

func newTaskJournalListCmd(short string, activity bool) *cobra.Command {
	var workspace string
	var limit int64
	var after string
	var revision int64
	var status, action string
	var resourceKind string
	list := &cobra.Command{
		Use:   "list",
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			options, err := resolveTaskJournalListOptions(cmd, workspace, limit, after)
			if err != nil {
				return err
			}
			options.Status, options.Action, options.Revision = status, action, revision
			options.ResourceKind = resourceKind
			var page apiTypes.ExecutionPage[apiTypes.TaskSummary]
			if activity {
				page, err = fromContext(cmd).Client.ListActivity(cmd.Context(), options)
			} else {
				page, err = fromContext(cmd).Client.ListTasks(cmd.Context(), options)
			}
			if err != nil {
				return err
			}
			return renderTaskPage(cmd, page)
		},
	}
	list.Flags().StringVar(&workspace, "workspace", "", "platform | <tenant slug>")
	list.Flags().Int64Var(&limit, "limit", 25, "page size from 1 through 100")
	list.Flags().StringVar(&after, "after", "", "last Task inspected on the previous page")
	list.Flags().Int64Var(&revision, "revision", 0, "snapshot revision from the previous page")
	list.Flags().StringVar(&status, "status", "", "filter the whole journal by Task status")
	list.Flags().StringVar(&action, "action", "", "filter by action, such as deploy or fetch")
	list.Flags().StringVar(&resourceKind, "resource-kind", "", "filter by target resource kind, such as service or image")
	return list
}

func resolveTaskJournalListOptions(
	cmd *cobra.Command,
	workspace string,
	limit int64,
	after string,
) (apiclient.TaskListOptions, error) {
	app := fromContext(cmd)
	options := apiclient.TaskListOptions{Limit: limit, After: after, Scope: "all"}
	if workspace != "" && (app.Scope.Environment != "" || app.Scope.Project != "" || app.Scope.Tenant != "") {
		return apiclient.TaskListOptions{}, errs.New(
			errs.KindValidationFailed,
			"task journal accepts only one hierarchy scope or --workspace",
		)
	}
	if app.Scope.Environment != "" {
		environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
		if err != nil {
			return apiclient.TaskListOptions{}, err
		}
		options.Scope = environmentID
		return options, nil
	}
	if app.Scope.Project != "" {
		projectID, err := resolveProjectTarget(cmd, app.Scope.Project)
		if err != nil {
			return apiclient.TaskListOptions{}, err
		}
		options.Scope = projectID
		return options, nil
	}
	if app.Scope.Tenant != "" {
		tenantID, err := resolveTenantTarget(cmd, app.Scope.Tenant)
		if err != nil {
			return apiclient.TaskListOptions{}, err
		}
		options.Scope = tenantID
		return options, nil
	}
	if workspace == "" {
		return options, nil
	}
	if workspace == "platform" {
		options.Scope = workspace
		return options, nil
	}
	tenantID, err := resolveTenantTarget(cmd, workspace)
	if err != nil {
		return apiclient.TaskListOptions{}, err
	}
	options.Scope = tenantID
	return options, nil
}

func taskTimestamp(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func nullableTaskTimestamp(value *time.Time) string {
	if value == nil {
		return ""
	}
	return taskTimestamp(*value)
}
