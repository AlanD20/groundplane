package cli

import (
	"strconv"

	"github.com/spf13/cobra"
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
			query := map[string]string{
				"environment_id": environmentID, "limit": strconv.Itoa(limit),
			}
			if serviceName != "" {
				serviceID, err := resolveServiceTarget(cmd, serviceName)
				if err != nil {
					return err
				}
				query["service_id"] = serviceID
			}
			if cursor != "" {
				query["cursor"] = cursor
			}
			return runList(cmd, "/api/v1/releases", query)
		},
	}
	list.Flags().StringVar(&serviceName, "service", "", "filter by Service name; --id accepts a stable id")
	list.Flags().IntVar(&limit, "limit", 50, "page size from 1 through 200")
	list.Flags().StringVar(&cursor, "cursor", "", "opaque next-page cursor")
	cmd.AddCommand(list)
	cmd.AddCommand(&cobra.Command{
		Use: "show <release-id>", Short: "Show one immutable release and its attempts", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/releases/"+target(fromContext(cmd), args[0]))
		},
	})
	return cmd
}
