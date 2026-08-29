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

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show a task (steps, current state)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			task, err := app.Client.ShowTask(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(taskFields(task))
			return app.Out.RenderOne(fields, values, task)
		},
	})

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

	cmd.AddCommand(&cobra.Command{
		Use:   "retry <id>",
		Short: "Retry a failed task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			accepted, err := app.Client.RetryTask(cmd.Context(), target(app, args[0]))
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	})

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

func renderTaskPage(cmd *cobra.Command, page apiTypes.Page[apiTypes.Task]) error {
	headers := []string{
		"ID", "TYPE", "TARGET", "STATUS", "WORKSPACE", "TENANT", "PROJECT", "ENVIRONMENT",
		"ACTOR", "CREATED", "UPDATED", "STARTED", "FINISHED",
	}
	rows := make([][]string, len(page.Items))
	for index, task := range page.Items {
		rows[index] = []string{
			task.ID, task.Type, task.Target, string(task.Status), string(task.WorkspaceType), task.TenantID,
			task.ProjectID, task.EnvironmentID, string(task.Actor), taskTimestamp(task.CreatedAt),
			taskTimestamp(task.UpdatedAt), nullableTaskTimestamp(task.StartedAt),
			nullableTaskTimestamp(task.FinishedAt),
		}
	}
	return fromContext(cmd).Out.Render(headers, rows, page)
}

func taskFields(task apiTypes.Task) map[string]any {
	return map[string]any{
		"id": task.ID, "operation_id": task.OperationID, "retry_of": task.RetryOf,
		"plan_hash": task.PlanHash, "type": task.Type, "target": task.Target,
		"status": task.Status, "workspace_type": task.WorkspaceType, "tenant_id": task.TenantID,
		"project_id": task.ProjectID, "environment_id": task.EnvironmentID, "actor": task.Actor,
		"created_at": taskTimestamp(task.CreatedAt), "updated_at": taskTimestamp(task.UpdatedAt),
		"started_at":  nullableTaskTimestamp(task.StartedAt),
		"finished_at": nullableTaskTimestamp(task.FinishedAt), "steps": task.Steps,
	}
}

func newTaskJournalListCmd(short string, activity bool) *cobra.Command {
	var workspace string
	var limit int
	var cursor string
	list := &cobra.Command{
		Use:   "list",
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			options, err := resolveTaskJournalListOptions(cmd, workspace, limit, cursor)
			if err != nil {
				return err
			}
			var page apiTypes.Page[apiTypes.Task]
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
	list.Flags().IntVar(&limit, "limit", 50, "page size from 1 through 200")
	list.Flags().StringVar(&cursor, "cursor", "", "opaque next-page cursor")
	return list
}

func resolveTaskJournalListOptions(
	cmd *cobra.Command,
	workspace string,
	limit int,
	cursor string,
) (apiclient.TaskListOptions, error) {
	app := fromContext(cmd)
	options := apiclient.TaskListOptions{Limit: limit, Cursor: cursor}
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
		options.Environment = environmentID
		return options, nil
	}
	if app.Scope.Project != "" {
		projectID, err := resolveProjectTarget(cmd, app.Scope.Project)
		if err != nil {
			return apiclient.TaskListOptions{}, err
		}
		options.Project = projectID
		return options, nil
	}
	if app.Scope.Tenant != "" {
		tenantID, err := resolveTenantTarget(cmd, app.Scope.Tenant)
		if err != nil {
			return apiclient.TaskListOptions{}, err
		}
		options.Workspace = tenantID
		return options, nil
	}
	if workspace == "" || workspace == "platform" {
		options.Workspace = workspace
		return options, nil
	}
	tenantID, err := resolveTenantTarget(cmd, workspace)
	if err != nil {
		return apiclient.TaskListOptions{}, err
	}
	options.Workspace = tenantID
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
