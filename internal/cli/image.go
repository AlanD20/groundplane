package cli

import (
	"fmt"
	"strconv"
	"strings"

	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/spf13/cobra"
)

func newImageCmd() *cobra.Command {
	command := &cobra.Command{
		Use:   "image",
		Short: "Manage local Agent host images from public and GP private registries",
	}
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
					"task %s dispatched\nimage %s\nconfiguration digest %s\nWait for `groundplane task show %s` to report completed before Deploy.\n",
					accepted.TaskID,
					accepted.Image,
					accepted.ConfigDigest,
					accepted.TaskID,
				)
				return err
			}
			return app.Out.RenderOne(
				[]string{"task_id", "image", "config_digest"},
				[]string{accepted.TaskID, accepted.Image, accepted.ConfigDigest},
				accepted,
			)
		},
	}
	fetch.Flags().
		StringVar(&key, "idempotency-key", "", "reuse this key to resolve uncertain acceptance of the same request")
	list := &cobra.Command{
		Use:   "ls",
		Short: "List live host images and their removal protection",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := fromContext(cmd)
			inventory, err := app.Client.ListImages(cmd.Context())
			if err != nil {
				return err
			}
			rows := make([][]string, 0, len(inventory.Images))
			for _, image := range inventory.Images {
				rows = append(
					rows,
					[]string{
						image.ID,
						strings.Join(image.Tags, ", "),
						strings.Join(image.Digests, ", "),
						strconv.FormatInt(image.SizeBytes, 10),
						strconv.Itoa(image.Containers),
						image.RemovalBlocked,
					},
				)
			}
			return app.Out.Render(
				[]string{"ID", "TAGS", "DIGESTS", "BYTES", "CONTAINERS", "REMOVAL BLOCKED"},
				rows,
				inventory,
			)
		},
	}
	var removeKey string
	remove := &cobra.Command{
		Use:   "remove <image-id>",
		Short: "Remove an unreferenced image without force or pruning",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := imagefetch.RemovalHash(args[0]); err != nil {
				return err
			}
			app := fromContext(cmd)
			accepted, err := app.Client.RemoveImage(cmd.Context(), args[0], removeKey)
			if err != nil {
				return err
			}
			return app.Out.RenderOne([]string{"task_id"}, []string{accepted.TaskID}, accepted)
		},
	}
	remove.Flags().
		StringVar(&removeKey, "idempotency-key", "", "reuse this key to resolve uncertain acceptance of the same request")
	command.AddCommand(fetch, list, remove)
	return withExecutionClass(command, executionAPI)
}
