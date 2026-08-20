package cli

import "github.com/spf13/cobra"

// task: list [--env NAME | --workspace platform|<tenant>] | show <id> |
// retry <id> | abort <id>. The activity journal IS this record set. See mvp.md,
// "Baked-in actions become Tasks" and "Activity IS tasks (locked)".
func newTaskCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "task", Short: "Tasks — the one record set behind every action and the activity journal"}

	var workspace string
	list := &cobra.Command{
		Use:   "list",
		Short: "List tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			q := scopeQuery(app, "environment")
			if workspace != "" {
				q["workspace"] = workspace
			}
			return runList(cmd, "/api/v1/tasks", q)
		},
	}
	list.Flags().StringVar(&workspace, "workspace", "", "platform | <tenant slug>")
	cmd.AddCommand(list)

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show a task (steps, current state)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/tasks/"+target(fromContext(cmd), args[0]))
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "retry <id>",
		Short: "Retry a failed task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/tasks/"+target(fromContext(cmd), args[0])+"/retry", nil)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "abort <id>",
		Short: "Abort an in-flight task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/tasks/"+target(fromContext(cmd), args[0])+"/abort", nil)
		},
	})

	return cmd
}
