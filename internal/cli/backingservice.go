package cli

import "github.com/spf13/cobra"

// backing-service (bs): list | show | create | start | stop | destroy.
// Created explicitly — never lazily. The creation form is the SAME full
// service form plus adapter + facts-prefix fields. See mvp.md, "Backing
// services are created explicitly."
func newBackingServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "backing-service",
		Aliases: []string{"bs"},
		Short:   "Backing services — shared datastores/caches/queues",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List backing services",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/backing-services", nil)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <slug>",
		Short: "Show a backing service (consumers, connection info)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/backing-services/"+target(fromContext(cmd), args[0]))
		},
	})

	var adapter, image, name, prefix string
	create := &cobra.Command{
		Use:   "create <slug>",
		Short: "Create a backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, "/api/v1/backing-services", map[string]string{
				"slug": args[0], "adapter": adapter, "image": image, "name": name, "facts_prefix": prefix,
			})
		},
	}
	create.Flags().
		StringVar(&adapter, "adapter", "", "adapter key, e.g. postgres:16 (see `groundplane backing-service create --help` for the registry)")
	create.Flags().StringVar(&image, "image", "", "container image (defaults to the adapter's default image)")
	create.Flags().StringVar(&name, "name", "", "unique DNS-resolvable service name, e.g. 'postgres'")
	create.Flags().
		StringVar(&prefix, "facts-prefix", "", "facts key prefix, e.g. 'pg16_' (defaults to the adapter's default)")
	_ = create.MarkFlagRequired("adapter")
	_ = create.MarkFlagRequired("name")
	cmd.AddCommand(create)

	cmd.AddCommand(&cobra.Command{
		Use:   "start <slug>",
		Short: "Start a backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/backing-services/"+target(fromContext(cmd), args[0])+"/start", nil)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "stop <slug>",
		Short: "Stop a backing service",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/backing-services/"+target(fromContext(cmd), args[0])+"/stop", nil)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "destroy <slug>",
		Short: "Destroy a backing service (typed confirmation in the Console; removes data)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/backing-services/"+target(fromContext(cmd), args[0])+"/destroy", nil)
		},
	})

	return cmd
}
