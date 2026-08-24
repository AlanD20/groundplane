package cli

import "github.com/spf13/cobra"

// activity: list — a documented alias of `task list --workspace …`.
// See api-cli.md's resource map.
func newActivityCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "activity", Short: "The scoped activity journal (alias of `task list`)"}
	cmd.AddCommand(newTaskJournalListCmd("List activity", true))
	return cmd
}
