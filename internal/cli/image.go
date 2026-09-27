package cli

import (
	"fmt"

	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/spf13/cobra"
)

func newImageCmd() *cobra.Command {
	command := &cobra.Command{Use: "image", Short: "Deliver private-registry images to the host"}
	var key string
	fetch := &cobra.Command{
		Use: "fetch <reference>", Short: "Fetch an explicit registry tag or digest without deploying", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := imagefetch.ParseRequested(args[0]); err != nil {
				return err
			}
			app := fromContext(cmd)
			accepted, err := app.Client.FetchImage(cmd.Context(), args[0], key)
			if err != nil {
				return err
			}
			if app.Out.Format == clicommon.FormatTable {
				_, err := fmt.Fprintf(
					cmd.OutOrStdout(),
					"task %s dispatched\nimage %s\nexpected image ID %s\nWait for `groundplane task show %s` to report completed before Deploy.\n",
					accepted.TaskID,
					accepted.Image,
					accepted.ImageID,
					accepted.TaskID,
				)
				return err
			}
			return app.Out.RenderOne(
				[]string{"task_id", "image", "image_id"},
				[]string{accepted.TaskID, accepted.Image, accepted.ImageID},
				accepted,
			)
		},
	}
	fetch.Flags().
		StringVar(&key, "idempotency-key", "", "reuse this key to resolve uncertain acceptance of the same request")
	command.AddCommand(fetch)
	return withExecutionClass(command, executionAPI)
}
