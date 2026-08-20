package cli

import (
	"io"

	"github.com/spf13/cobra"
)

func newCompletionCmd(root *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "completion",
		Short: "Generate shell completion",
	}

	shells := []struct {
		name     string
		generate func(io.Writer) error
	}{
		{name: "bash", generate: func(out io.Writer) error {
			return root.GenBashCompletionV2(out, true)
		}},
		{name: "zsh", generate: root.GenZshCompletion},
		{name: "fish", generate: func(out io.Writer) error {
			return root.GenFishCompletion(out, true)
		}},
	}
	for _, shell := range shells {
		shell := shell
		cmd.AddCommand(&cobra.Command{
			Use:   shell.name,
			Short: "Generate " + shell.name + " completion",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				return shell.generate(cmd.OutOrStdout())
			},
		})
	}

	return cmd
}
