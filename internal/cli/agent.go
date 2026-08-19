package cli

import "github.com/spf13/cobra"

// agent: list | show | join | approve <token> | config set | update.
// The operational surface for pairing and per-instance config (distinct
// from `core component agent`, which is the read-only view). See
// api-cli.md, "Agent vs `core component agent` (locked split)".
func newAgentCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "agent", Short: "Agents — pairing, config, updates"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List agents",
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
		Short: "Print a one-time join token for a new agent (see cmd/agent + groundplane-agent.service)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, "/api/v1/agents", nil)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "approve <token>",
		Short: "Approve a pending agent's join token (the operator-side half of pairing — POST /agents/pair)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, "/api/v1/agents/pair", map[string]string{"join_token": args[0]})
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
		Short: "Self-update an agent (ImageUpdate: acked before the agent recreates its own container)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(cmd, "/api/v1/agents/"+target(fromContext(cmd), args[0])+"/update", nil)
		},
	})

	return cmd
}
