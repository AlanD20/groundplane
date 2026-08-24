package cli

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// volume: list | add | edit | remove. Bind mounts cannot traverse
// outside the environment's volume folder. See mvp.md,
// "Environment-scoped volumes".
func volumeEnvironmentID(cmd *cobra.Command) (string, error) {
	argument := fromContext(cmd).Scope.Environment
	if argument == "" {
		return "", errs.New(errs.KindValidationFailed, "volume command requires --environment")
	}
	return resolveEnvironmentTarget(cmd, argument)
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
				items[index] = map[string]any{"id": volume.ID, "name": volume.Name}
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(
				cmd,
				"/api/v1/volumes",
				map[string]string{"name": args[0], "environment": app.Scope.Environment},
			)
		},
	}
	cmd.AddCommand(add)

	var newName string
	edit := &cobra.Command{
		Use:   "edit <name>",
		Short: "Edit a volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPatch(
				cmd,
				"/api/v1/volumes/"+target(fromContext(cmd), args[0]),
				changedStringFields(cmd, map[string]string{"name": newName}),
			)
		},
	}
	edit.Flags().StringVar(&newName, "name", "", "new name")
	cmd.AddCommand(edit)

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <name>",
		Aliases: []string{"delete"},
		Short:   "Remove a volume (dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/volumes/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
