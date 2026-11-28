package cli

import (
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newImageCmd() *cobra.Command {
	command := &cobra.Command{
		Use:   "image",
		Short: "Manage local Agent host images from public and GP private registries",
	}
	var key, fetchService string
	var fetchAfter []string
	fetch := &cobra.Command{
		Use: "fetch <reference>", Short: "Fetch an explicit registry tag or digest without deploying", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := imagefetch.ParseRequested(args[0]); err != nil {
				return err
			}
			app := fromContext(cmd)
			after, err := validatedTaskAfter(fetchAfter)
			if err != nil {
				return err
			}
			var accepted apiTypes.TaskAccepted
			switch {
			case app.Client.ScopedRunner():
				accepted, err = app.Client.FetchRunnerServiceImage(cmd.Context(), fetchService, args[0], key, after)
			case cmd.Flags().Changed("service"):
				return errs.New(errs.KindValidationFailed, "image fetch --service is available to scoped Runners")
			default:
				accepted, err = app.Client.FetchImage(cmd.Context(), args[0], key, after)
			}
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	fetch.Flags().
		StringVar(&key, "idempotency-key", "", "reuse this key to resolve uncertain acceptance of the same request")
	fetch.Flags().StringVar(&fetchService, "service", "", "stable Service ID required for a scoped Runner fetch")
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
				uses := make([]string, 0, len(image.ContainerUses))
				for _, use := range image.ContainerUses {
					owner := "not managed by GP"
					if use.Managed {
						owner = "GP-managed container"
					}
					if use.Owner != nil {
						owner = use.Owner.ProjectSlug + "/" + use.Owner.EnvironmentName
						if use.Owner.ServiceName != "" {
							owner += "/" + use.Owner.ServiceName
						}
					}
					uses = append(uses, use.Name+" ("+use.State+", "+owner+")")
				}
				fetches := make([]string, 0, len(image.Fetches))
				for _, fetch := range image.Fetches {
					fetches = append(
						fetches,
						fetch.Requested+" -> "+fetch.Image+" ("+string(fetch.Status)+", "+fetch.RequestedAt+")",
					)
				}
				rows = append(
					rows,
					[]string{
						image.ID,
						strings.Join(image.Tags, ", "),
						strings.Join(image.Digests, ", "),
						strconv.FormatInt(image.SizeBytes, 10),
						strconv.Itoa(image.Containers),
						strings.Join(uses, ", "), image.ProtectionReason,
						image.RemovalBlocked,
						strings.Join(fetches, ", "),
					},
				)
			}
			return app.Out.Render(
				[]string{
					"ID",
					"TAGS",
					"DIGESTS",
					"BYTES",
					"CONTAINERS",
					"CONTAINER USAGE",
					"OTHER PROTECTION",
					"REMOVAL BLOCKED",
					"FETCH HISTORY",
				},
				rows,
				inventory,
			)
		},
	}
	var removeKey string
	var removeAfter []string
	remove := &cobra.Command{
		Use:   "remove <image-id>",
		Short: "Remove an unreferenced image without force or pruning",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := imagefetch.RemovalHash(args[0]); err != nil {
				return err
			}
			app := fromContext(cmd)
			after, err := validatedTaskAfter(removeAfter)
			if err != nil {
				return err
			}
			accepted, err := app.Client.RemoveImage(cmd.Context(), args[0], removeKey, after)
			if err != nil {
				return err
			}
			return app.Out.RenderOne([]string{"task_id"}, []string{accepted.TaskID}, accepted)
		},
	}
	remove.Flags().
		StringVar(&removeKey, "idempotency-key", "", "reuse this key to resolve uncertain acceptance of the same request")
	addTaskAfterFlag(fetch, &fetchAfter)
	addTaskAfterFlag(remove, &removeAfter)
	command.AddCommand(fetch, list, remove)
	return withExecutionClass(command, executionAPI)
}
