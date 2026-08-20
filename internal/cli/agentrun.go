package cli

import (
	"github.com/spf13/cobra"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// agent-run: run — foreground, for debugging (mirrors cmd/agent/main.go
// but invoked from the CLI binary for a quick local run).
func newAgentRunCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "agent-run", Short: "Run an Agent in the foreground, for debugging"}
	cmd.AddCommand(&cobra.Command{
		Use:   "run",
		Short: "Run the Agent in the foreground",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errs.New(errs.KindNotImplemented, "Agent foreground execution awaits the Controller-owned runtime cutover")
		},
	})
	return cmd
}
