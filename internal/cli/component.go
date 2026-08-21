package cli

import "github.com/spf13/cobra"

// component: list | show | enable | disable | config show | config set |
// update. One noun spans
// environment and platform owners; each kind's registration declares which
// owner is valid.
func newComponentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "component",
		Short: "Manage environment- and platform-owned components",
	}

	var platform bool
	cmd.PersistentFlags().BoolVar(&platform, "platform", false, "target platform-owned components")

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List components",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			query := scopeQuery(fromContext(cmd), "environment")
			if platform {
				delete(query, "environment")
				query["platform"] = "true"
			}
			return runList(cmd, "/api/v1/components", query)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show a component (status, generated services, health)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/components/"+target(fromContext(cmd), args[0]))
		},
	})

	enable := &cobra.Command{
		Use:   "enable <id>",
		Short: "Enable a component",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := "/api/v1/components/" + target(fromContext(cmd), args[0]) + "/enable"
			return runAction(cmd, path, nil)
		},
	}
	cmd.AddCommand(enable)

	cmd.AddCommand(&cobra.Command{
		Use:   "disable <id>",
		Short: "Disable a component",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(
				cmd,
				"/api/v1/components/"+target(fromContext(cmd), args[0])+"/disable",
				nil,
			)
		},
	})

	config := &cobra.Command{Use: "config", Short: "A component's kind-specific config"}
	config.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show a component's config",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := "/api/v1/components/" + target(fromContext(cmd), args[0]) + "/config"
			return runShow(cmd, path)
		},
	})
	config.AddCommand(&cobra.Command{
		Use:   "set <id>",
		Short: "Set a component's config",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Kind-specific flags remain closed until the component registry
			// defines each kind's exact config schema.
			path := "/api/v1/components/" + target(fromContext(cmd), args[0]) + "/config"
			return runReplaceSingleton(cmd, path, nil)
		},
	})
	cmd.AddCommand(config)

	cmd.AddCommand(&cobra.Command{
		Use:   "update <id>",
		Short: "Update a component",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(
				cmd,
				"/api/v1/components/"+target(fromContext(cmd), args[0])+"/update",
				nil,
			)
		},
	})

	return cmd
}
