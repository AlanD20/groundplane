package cli

import "github.com/spf13/cobra"

// zone: list | add | edit | remove. See mvp.md, "Network Zone" —
// Groundplane's domain term for a Compose network (blueprint.md).
func newZoneCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "zone", Short: "Network zones — named internal docker networks"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List zones",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/zones", scopeQuery(fromContext(cmd), "environment"))
		},
	})

	var subnet string
	var internal bool
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a zone",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/zones", map[string]interface{}{
				"name": args[0], "subnet": subnet, "internal": internal, "environment": app.Scope.Environment,
			})
		},
	}
	add.Flags().StringVar(&subnet, "subnet", "", "CIDR (auto-assigned if omitted)")
	add.Flags().BoolVar(&internal, "internal", false, "no route to the host's default gateway")
	cmd.AddCommand(add)

	var editSubnet string
	edit := &cobra.Command{
		Use:   "edit <name>",
		Short: "Edit a zone",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEdit(cmd, "/api/v1/zones/"+target(fromContext(cmd), args[0]), map[string]string{"subnet": editSubnet})
		},
	}
	edit.Flags().StringVar(&editSubnet, "subnet", "", "new CIDR")
	cmd.AddCommand(edit)

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <name>",
		Aliases: []string{"delete"},
		Short:   "Remove a zone (strips it from every service's membership — see mvp.md's zone-removal warning; dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/zones/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
