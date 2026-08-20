package cli

import (
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/spf13/cobra"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the groundplane CLI version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), "groundplane "+version.Value)
			return err
		},
	}
}
