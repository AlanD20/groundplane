package cli

import "github.com/spf13/cobra"

// agent: list | show | join | config set | update | remove.
// The operational surface for pairing and per-instance config (distinct
// from `core component agent`, which is the read-only view). See
// api-cli.md, "Agent vs `core component agent` (locked split)".
func newAgentCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "agent", Short: "Agents — pairing, config, updates"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List agents",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/agents", nil)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show an agent",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runShow(cmd, "/api/v1/agents/"+target(fromContext(cmd), args[0]))
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "join",
		Short: "Create and start the local Agent",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAction(cmd, "/api/v1/agents", nil)
		},
	})

	config := &cobra.Command{Use: "config", Short: "Per-agent runtime config (Controller-owned, in etcd)"}
	var pullInterval, maxConcurrent int
	var labels []string
	set := &cobra.Command{
		Use:   "set <id>",
		Short: "Set an agent's runtime config",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := "/api/v1/agents/" + target(fromContext(cmd), args[0]) + "/config"
			return runEdit(cmd, path, map[string]interface{}{
				"pull_interval_seconds": pullInterval, "max_concurrent_tasks": maxConcurrent, "labels": labels,
			})
		},
	}
	set.Flags().IntVar(&pullInterval, "pull-interval", 0, "seconds between idle Ready heartbeats")
	set.Flags().IntVar(&maxConcurrent, "max-concurrent", 0, "worker pool size")
	set.Flags().StringSliceVar(&labels, "label", nil, "repeatable: key=value, for targeted dispatch")
	config.AddCommand(set)
	cmd.AddCommand(config)

	cmd.AddCommand(&cobra.Command{
		Use:   "update <id>",
		Short: "Update an agent through the Controller-owned container lifecycle",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/agents/"+target(fromContext(cmd), args[0])+"/update", nil)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "remove <id>",
		Short: "Remove an agent container and revoke its channel token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDeleteAction(cmd, "/api/v1/agents/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}
