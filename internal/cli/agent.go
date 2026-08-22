package cli

import (
	"strings"
	"time"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// agent: list | show | join | config show | config set | update | remove.
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
			app := fromContext(cmd)
			page, err := app.Client.ListAgents(cmd.Context(), 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, agent := range page.Items {
				items[index] = agentFields(agent)
			}
			headers, rows := tabulateVia(app, items)
			return app.Out.Render(headers, rows, page)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show an agent",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			agent, err := app.Client.ShowAgent(cmd.Context(), target(app, args[0]))
			if err != nil {
				return err
			}
			headers, rows := tabulateVia(app, []map[string]any{agentFields(agent)})
			return app.Out.Render(headers, rows, agent)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "join",
		Short: "Create and start the local Agent",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := fromContext(cmd)
			accepted, err := app.Client.JoinAgent(cmd.Context())
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	})

	config := &cobra.Command{
		Use:   "config",
		Short: "Per-agent runtime config (Controller-owned, in etcd)",
	}
	config.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show an agent's runtime config",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			config, err := app.Client.ShowAgentConfig(cmd.Context(), target(app, args[0]))
			if err != nil {
				return err
			}
			fields := map[string]any{
				"pull_interval_seconds": config.PullIntervalSeconds,
				"max_concurrent_tasks":  config.MaxConcurrentTasks,
				"labels":                config.Labels,
			}
			headers, rows := tabulateVia(app, []map[string]any{fields})
			return app.Out.Render(headers, rows, config)
		},
	})
	var pullInterval, maxConcurrent int
	var labels []string
	set := &cobra.Command{
		Use:   "set <id>",
		Short: "Set an agent's runtime config",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			parsedLabels, err := parseAgentLabels(labels)
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			updated, err := app.Client.SetAgentConfig(cmd.Context(), target(app, args[0]), apiTypes.AgentConfig{
				PullIntervalSeconds: pullInterval,
				MaxConcurrentTasks:  maxConcurrent,
				Labels:              parsedLabels,
			})
			if err != nil {
				return err
			}
			fields := map[string]any{
				"pull_interval_seconds": updated.PullIntervalSeconds,
				"max_concurrent_tasks":  updated.MaxConcurrentTasks,
				"labels":                updated.Labels,
			}
			headers, rows := tabulateVia(app, []map[string]any{fields})
			return app.Out.Render(headers, rows, updated)
		},
	}
	set.Flags().IntVar(&pullInterval, "pull-interval", 0, "seconds between idle Ready heartbeats")
	set.Flags().IntVar(&maxConcurrent, "max-concurrent", 0, "worker pool size")
	set.Flags().
		StringSliceVar(&labels, "label", nil, "repeatable: key=value, for targeted dispatch")
	config.AddCommand(set)
	cmd.AddCommand(config)

	cmd.AddCommand(&cobra.Command{
		Use:   "update <id>",
		Short: "Update an agent through the Controller-owned container lifecycle",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAction(
				cmd,
				"/api/v1/agents/"+target(fromContext(cmd), args[0])+"/update",
				nil,
			)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "remove <id>",
		Short: "Remove an agent container and revoke its channel token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			accepted, err := app.Client.RemoveAgent(cmd.Context(), target(app, args[0]))
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	})

	return cmd
}

func agentFields(agent apiTypes.Agent) map[string]any {
	return map[string]any{
		"id":                 agent.ID,
		"enrollment_task_id": agent.EnrollmentTaskID,
		"host":               agent.Host,
		"status":             agent.Status,
		"version":            optionalAgentString(agent.Version),
		"labels":             agent.Labels,
		"ready_at":           optionalAgentTime(agent.ReadyAt),
		"last_report_at":     optionalAgentTime(agent.LastReportAt),
		"in_flight":          agent.InFlight,
	}
}

func optionalAgentString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func optionalAgentTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.Format(time.RFC3339)
}

func parseAgentLabels(values []string) (map[string]string, error) {
	labels := make(map[string]string, len(values))
	for _, value := range values {
		key, labelValue, found := strings.Cut(value, "=")
		if !found || strings.TrimSpace(key) == "" {
			return nil, errs.New(errs.KindValidationFailed, "Agent labels must use key=value")
		}
		if _, exists := labels[key]; exists {
			return nil, errs.Newf(errs.KindValidationFailed, "Agent label key is duplicated: %s", key)
		}
		labels[key] = labelValue
	}
	return labels, nil
}
