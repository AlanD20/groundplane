package cli

import (
	"github.com/spf13/cobra"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func newReleaseCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "release", Short: "Immutable service release ledger"}
	var serviceName string
	var limit int
	var cursor string
	list := &cobra.Command{
		Use: "list", Short: "List releases in the selected environment", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			serviceID := ""
			if serviceName != "" {
				serviceID, err = resolveServiceTarget(cmd, serviceName)
				if err != nil {
					return err
				}
			}
			page, err := fromContext(cmd).Client.ListReleases(
				cmd.Context(), environmentID, serviceID, limit, cursor,
			)
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, release := range page.Items {
				items[index] = releaseFields(release)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	}
	list.Flags().StringVar(&serviceName, "service", "", "filter by Service name; --id accepts a stable id")
	list.Flags().IntVar(&limit, "limit", 50, "page size from 1 through 200")
	list.Flags().StringVar(&cursor, "cursor", "", "opaque next-page cursor")
	cmd.AddCommand(list)
	cmd.AddCommand(&cobra.Command{
		Use: "show <release-id>", Short: "Show one immutable release and its attempts", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			release, err := fromContext(cmd).Client.GetRelease(
				cmd.Context(), target(fromContext(cmd), args[0]),
			)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(releaseFields(release.ReleaseSummary))
			return fromContext(cmd).Out.RenderOne(fields, values, release)
		},
	})
	return cmd
}

func releaseFields(release apiTypes.ReleaseSummary) map[string]any {
	return map[string]any{
		"id":          release.ID,
		"service_id":  release.ServiceID,
		"image":       release.Image,
		"tag":         release.Tag,
		"state":       release.State,
		"serving":     release.Serving,
		"created_at":  release.CreatedAt,
		"completed_at": release.CompletedAt,
	}
}
