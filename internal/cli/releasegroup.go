package cli

import "github.com/spf13/cobra"

// release-group: list | show | create | edit | delete | deploy |
// rollback. Coordinates multiple services under one task lock and one
// failure policy — membership is always explicit, never inferred from
// shared image or service names. See blueprint.md, "x-gp-release-group".
func newReleaseGroupCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "release-group", Short: "Release groups — explicit multi-service deploy coordination"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List release groups",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/release-groups", scopeQuery(fromContext(cmd), "environment"))
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <name>",
		Short: "Show a release group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/release-groups/"+target(fromContext(cmd), args[0]))
		},
	})

	var services, order []string
	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a release group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runCreate(cmd, "/api/v1/release-groups", map[string]interface{}{
				"name": args[0], "services": services, "order": order, "environment": app.Scope.Environment,
			})
		},
	}
	create.Flags().StringSliceVar(&services, "service", nil, "member service name (repeatable); at least two required")
	create.Flags().StringSliceVar(&order, "order", nil, "deploy order within the group (defaults to --service order)")
	_ = create.MarkFlagRequired("service")
	cmd.AddCommand(create)

	var editServices, editOrder []string
	edit := &cobra.Command{
		Use:   "edit <name>",
		Short: "Edit a release group's membership or order",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body := map[string]interface{}{}
			if len(editServices) > 0 {
				body["services"] = editServices
			}
			if len(editOrder) > 0 {
				body["order"] = editOrder
			}
			return runEdit(cmd, "/api/v1/release-groups/"+target(fromContext(cmd), args[0]), body)
		},
	}
	edit.Flags().StringSliceVar(&editServices, "service", nil, "new member list (repeatable)")
	edit.Flags().StringSliceVar(&editOrder, "order", nil, "new deploy order")
	cmd.AddCommand(edit)

	cmd.AddCommand(&cobra.Command{
		Use:     "delete <name>",
		Aliases: []string{"remove"},
		Short:   "Delete a release group (dispatches a task; member services are untouched)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/release-groups/"+target(fromContext(cmd), args[0]))
		},
	})

	var deployTag string
	deploy := &cobra.Command{
		Use:   "deploy <name> --tag <tag>",
		Short: "Deploy every member service to the same tag under one task lock",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := "/api/v1/release-groups/" + target(fromContext(cmd), args[0]) + "/deploy"
			return runAction(cmd, path, map[string]string{"tag": deployTag})
		},
	}
	deploy.Flags().StringVar(&deployTag, "tag", "", "immutable image tag applied to every member")
	_ = deploy.MarkFlagRequired("tag")
	cmd.AddCommand(deploy)

	cmd.AddCommand(&cobra.Command{
		Use:   "rollback <name>",
		Short: "Roll back every member service under one task lock",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/release-groups/"+target(fromContext(cmd), args[0])+"/rollback", nil)
		},
	})

	return cmd
}
