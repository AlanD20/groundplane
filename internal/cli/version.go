package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Version is set via -ldflags "-X github.com/AlanD20/groundplane/internal/cli.Version=..." at build time.
var Version = "dev"

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the groundplane CLI version",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), "groundplane "+Version)
			return nil
		},
	}
}
