package cli

import "github.com/spf13/cobra"

// activity: list — a documented alias of `task list --workspace …`.
// See api-cli.md's resource map.
func newActivityCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "activity", Short: "The scoped activity journal (alias of `task list`)"}

	var workspace string
	list := &cobra.Command{
		Use:   "list",
		Short: "List activity",
		RunE: func(cmd *cobra.Command, args []string) error {
			q := map[string]string{}
			if workspace != "" {
				q["workspace"] = workspace
			}
			return runList(cmd, "/api/v1/activity", q)
		},
	}
	list.Flags().StringVar(&workspace, "workspace", "", "platform | <tenant slug>")
	cmd.AddCommand(list)

	return cmd
}
