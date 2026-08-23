package cli

import (
	"encoding/json"
	"fmt"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

// task: list [--env NAME | --workspace platform|<tenant>] | show <id> |
// events <id> | retry <id> | abort <id>. The activity journal IS this record set. See mvp.md,
// "Baked-in actions become Tasks" and "Activity IS tasks (locked)".
func newTaskCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "task", Short: "Tasks — the one record set behind every action and the activity journal"}

	var workspace string
	list := &cobra.Command{
		Use:   "list",
		Short: "List tasks",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			q := scopeQuery(app, "environment")
			if workspace != "" {
				q["workspace"] = workspace
			}
			if len(q) != 0 {
				return runList(cmd, "/api/v1/tasks", q)
			}
			page, err := app.Client.ListTasks(cmd.Context(), 0, "")
			if err != nil {
				return err
			}
			return renderTaskPage(cmd, page)
		},
	}
	list.Flags().StringVar(&workspace, "workspace", "", "platform | <tenant slug>")
	cmd.AddCommand(list)

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
	items := make([]map[string]any, len(page.Items))
	for index, task := range page.Items {
		items[index] = taskFields(task)
	}
	headers, rows := tabulateVia(fromContext(cmd), items)
	return fromContext(cmd).Out.Render(headers, rows, page)
}

func taskFields(task apiTypes.Task) map[string]any {
	return map[string]any{
		"id": task.ID, "operation_id": task.OperationID, "retry_of": task.RetryOf,
		"plan_hash": task.PlanHash, "type": task.Type, "target": task.Target,
		"status": task.Status, "steps": task.Steps,
	}
}
