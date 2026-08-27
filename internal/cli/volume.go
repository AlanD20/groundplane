package cli

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// volume: list | show | add | edit | remove. Bind mounts cannot traverse
// outside the environment's volume folder. See ADR 0049.
func volumeEnvironmentID(cmd *cobra.Command) (string, error) {
	argument := fromContext(cmd).Scope.Environment
	if argument == "" {
		return "", errs.New(errs.KindValidationFailed, "volume command requires --environment")
	}
	return resolveEnvironmentTarget(cmd, argument)
}

func resolveVolumeTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	environmentID, err := volumeEnvironmentID(cmd)
	if err != nil {
		return "", err
	}
	cursor := ""
	for {
		page, err := app.Client.ListVolumes(cmd.Context(), environmentID, 200, cursor)
		if err != nil {
			return "", err
		}
		for _, volume := range page.Items {
			if volume.Slug == argument {
				return target(app, volume.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindVolumeNotFound, "volume slug %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func newVolumeCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "volume", Short: "Volumes — persistent storage owned by an environment"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List volumes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			environmentID, err := volumeEnvironmentID(cmd)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListVolumes(cmd.Context(), environmentID, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, volume := range page.Items {
				items[index] = map[string]any{
					"id": volume.ID, "slug": volume.Slug, "key": volume.Key,
					"state": volume.State, "path": volume.Path,
				}
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <slug>",
		Short: "Show a volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveVolumeTarget(cmd, args[0])
			if err != nil {
				return err
			}
			volume, err := fromContext(cmd).Client.GetVolume(cmd.Context(), id)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(volumeFields(volume))
			return fromContext(cmd).Out.RenderOne(fields, values, volume)
		},
	})

	var volumeSlug string
	var volumeComposeKey string
	add := &cobra.Command{
		Use:   "add --slug <slug> [--key <compose-key>]",
		Short: "Add a volume",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			environmentID, err := volumeEnvironmentID(cmd)
			if err != nil {
				return err
			}
			if cmd.Flags().Changed("key") && volumeComposeKey == "" {
				return errs.New(errs.KindValidationFailed, "volume --key cannot be empty")
			}
			created, err := fromContext(cmd).Client.CreateVolume(cmd.Context(), apiTypes.VolumeCreate{
				EnvironmentID: environmentID, Slug: volumeSlug, Key: volumeComposeKey,
			})
			if err != nil {
				return err
			}
			fields := volumeFields(created.Volume)
			fields["task_id"] = created.TaskID
			headers, values := fieldsOfVia(fields)
			return fromContext(cmd).Out.RenderOne(headers, values, created)
		},
	}
	add.Flags().StringVar(&volumeSlug, "slug", "", "environment-scoped mutable slug")
	add.Flags().StringVar(&volumeComposeKey, "key", "", "immutable Compose volume key (defaults to slug)")
	_ = add.MarkFlagRequired("slug")
	cmd.AddCommand(add)

	var newSlug string
	edit := &cobra.Command{
		Use:   "edit <slug>",
		Short: "Edit a volume slug",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveVolumeTarget(cmd, args[0])
			if err != nil {
				return err
			}
			edited, err := fromContext(cmd).Client.EditVolume(
				cmd.Context(), id, apiTypes.VolumeEdit{Slug: newSlug},
			)
			if err != nil {
				return err
			}
			fields := volumeFields(edited.Volume)
			fields["task_id"] = edited.TaskID
			headers, values := fieldsOfVia(fields)
			return fromContext(cmd).Out.RenderOne(headers, values, edited)
		},
	}
	edit.Flags().StringVar(&newSlug, "slug", "", "new environment-scoped slug")
	_ = edit.MarkFlagRequired("slug")
	cmd.AddCommand(edit)

	var impactToken string
	var confirmKey string
	remove := &cobra.Command{
		Use:     "remove <slug>",
		Aliases: []string{"delete"},
		Short:   "Remove a volume after fixed-revision impact confirmation",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (impactToken == "") != (confirmKey == "") {
				return errs.New(errs.KindValidationFailed, "volume remove requires both --impact-token and --confirm-key")
			}
			id, err := resolveVolumeTarget(cmd, args[0])
			if err != nil {
				return err
			}
			if impactToken != "" {
				return removeVolumeWithConfirmation(cmd, id, impactToken, confirmKey)
			}
			pages, err := collectVolumeImpact(cmd, id)
			if err != nil {
				return err
			}
			if err := renderVolumeImpact(cmd, pages); err != nil {
				return err
			}
			volumeKey := pages[len(pages)-1].Key
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Type immutable volume key %q to confirm: ", volumeKey)
			answer, err := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
			if err != nil && err != io.EOF {
				return errs.Wrap(errs.KindValidationFailed, err)
			}
			if strings.TrimSpace(answer) != volumeKey {
				return errs.New(errs.KindValidationFailed, "volume key confirmation did not match")
			}
			final := pages[len(pages)-1]
			return removeVolumeWithConfirmation(cmd, id, final.ImpactToken, volumeKey)
		},
	}
	remove.Flags().StringVar(&impactToken, "impact-token", "", "final fixed-revision deletion-impact token")
	remove.Flags().StringVar(&confirmKey, "confirm-key", "", "immutable Compose volume key")
	cmd.AddCommand(remove)

	return cmd
}

func volumeFields(volume apiTypes.Volume) map[string]any {
	return map[string]any{
		"id": volume.ID, "environment_id": volume.EnvironmentID, "slug": volume.Slug,
		"key": volume.Key, "path": volume.Path, "state": volume.State,
		"create_task_id": volume.CreateTaskID, "origin_task_id": volume.OriginTaskID,
		"current_task_id": volume.CurrentTaskID,
	}
}

func collectVolumeImpact(cmd *cobra.Command, id string) ([]apiTypes.VolumeDeletionImpactPage, error) {
	pages := make([]apiTypes.VolumeDeletionImpactPage, 0, 1)
	seen := make(map[string]struct{})
	runningCount := 0
	cursor := ""
	for pageNumber := 0; pageNumber < 1024; pageNumber++ {
		page, err := fromContext(cmd).Client.GetVolumeDeletionImpact(cmd.Context(), id, cursor, 40)
		if err != nil {
			return nil, err
		}
		if page.VolumeID != id || page.Key == "" || page.Slug == "" || page.EnvironmentID == "" ||
			page.RollingDigest == "" || page.DataHandling != "recursive_destroy" {
			return nil, errs.New(errs.KindStateConflict, "volume deletion-impact page identity changed")
		}
		if len(pages) > 0 {
			first := pages[0]
			if page.VolumeID != first.VolumeID || page.Slug != first.Slug || page.Key != first.Key ||
				page.EnvironmentID != first.EnvironmentID || page.Revision != first.Revision ||
				page.EnvironmentHead != first.EnvironmentHead {
				return nil, errs.New(errs.KindStateConflict, "volume deletion-impact revision changed")
			}
		}
		runningCount += len(page.Items)
		if page.ItemCount != int64(runningCount) {
			return nil, errs.New(errs.KindStateConflict, "volume deletion-impact count does not join")
		}
		for _, item := range page.Items {
			if _, exists := seen[item.ID]; exists {
				return nil, errs.New(errs.KindStateConflict, "volume deletion-impact item repeated")
			}
			seen[item.ID] = struct{}{}
		}
		pages = append(pages, page)
		if page.Complete {
			if page.NextCursor != "" || page.ImpactToken == "" || page.ItemCount != int64(totalImpactItems(pages)) {
				return nil, errs.New(errs.KindStateConflict, "volume deletion-impact sequence is incomplete")
			}
			return pages, nil
		}
		if page.NextCursor == "" || page.ImpactToken != "" {
			return nil, errs.New(errs.KindStateConflict, "volume deletion-impact cursor is invalid")
		}
		cursor = page.NextCursor
	}
	return nil, errs.New(errs.KindStateConflict, "volume deletion-impact sequence exceeded its page bound")
}

func totalImpactItems(pages []apiTypes.VolumeDeletionImpactPage) int {
	total := 0
	for _, page := range pages {
		total += len(page.Items)
	}
	return total
}

func renderVolumeImpact(cmd *cobra.Command, pages []apiTypes.VolumeDeletionImpactPage) error {
	if len(pages) == 0 {
		return errs.New(errs.KindInternal, "volume deletion-impact sequence is empty")
	}
	if fromContext(cmd).Out.Format != clicommon.FormatTable {
		items := make([]apiTypes.VolumeDeletionImpactItem, 0, totalImpactItems(pages))
		for _, page := range pages {
			items = append(items, page.Items...)
		}
		return fromContext(cmd).Out.RenderOne(
			[]string{"volume_id", "slug", "key", "items", "data_handling"},
			[]string{pages[0].VolumeID, pages[0].Slug, pages[0].Key, fmt.Sprint(len(items)), pages[0].DataHandling},
			map[string]any{"volume_id": pages[0].VolumeID, "slug": pages[0].Slug, "key": pages[0].Key, "items": items, "data_handling": pages[0].DataHandling},
		)
	}
	for _, page := range pages {
		for _, item := range page.Items {
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s %s %s\n", item.Kind, item.ID, impactItemDetail(item)); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "data_handling: %s\n", pages[0].DataHandling); err != nil {
		return err
	}
	return nil
}

func impactItemDetail(item apiTypes.VolumeDeletionImpactItem) string {
	if item.Kind == apiTypes.VolumeImpactMount {
		return item.ServiceID + ":" + item.MountTarget
	}
	if item.SourceID != "" {
		return item.SourceID
	}
	return item.ServiceName
}

func removeVolumeWithConfirmation(cmd *cobra.Command, id, impactToken, confirmKey string) error {
	accepted, err := fromContext(cmd).Client.RemoveVolume(cmd.Context(), id, impactToken, confirmKey)
	if err != nil {
		return err
	}
	return renderDispatchedTask(cmd, accepted)
}
