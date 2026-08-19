package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// agent-run: run — foreground, for debugging (mirrors cmd/agent/main.go
// but invoked from the CLI binary for a quick local run).
func newAgentRunCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "agent-run", Short: "Run an Agent in the foreground, for debugging"}
	cmd.AddCommand(&cobra.Command{
		Use:   "run",
		Short: "Run the Agent in the foreground",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), "agent-run run: run `go run ./cmd/agent` directly for now")
			return nil
		},
	})
	return cmd
}
