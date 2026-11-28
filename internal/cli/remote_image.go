package cli

import (
	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/spf13/cobra"
)

func newRemoteHostFetchImageCmd() *cobra.Command {
	var key string
	var prerequisite []string
	cmd := &cobra.Command{Use: "fetch-image <host> <reference>",
		Short: "Fetch an exact public image manifest into the selected remote Host", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			image, err := remotefetch.Reference(args[1])
			if err != nil {
				return err
			}
			after, err := validatedTaskAfter(prerequisite)
			if err != nil {
				return err
			}
			host, err := resolveRemoteHost(cmd, args[0])
			if err != nil {
				return err
			}
			task, err := fromContext(cmd).Client.FetchRemoteImage(cmd.Context(), host, image, key, after)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, task)
		},
	}
	remoteHostKeyFlag(cmd, &key)
	cmd.Flags().StringArrayVar(&prerequisite, "after", nil, "wait for an accepted prerequisite Task ID (repeatable)")
	return cmd
}
