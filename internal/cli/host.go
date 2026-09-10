package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

// host: show. Includes etcd status — etcd is host-level, not a Core
// component. See mvp.md, "etcd is host-level, not a Core component
// (locked)".
func newHostCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "host", Short: "The host this Controller runs on"}
	cmd.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show host status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			host, err := app.Client.ShowHost(cmd.Context())
			if err != nil {
				return err
			}
			headers, rows := tabulateVia(app, []map[string]any{hostFields(host)})
			return app.Out.Render(headers, rows, host)
		},
	})
	return cmd
}

func hostFields(host apiTypes.Host) map[string]any {
	fields := map[string]any{
		"hostname":                    host.Hostname,
		"arch":                        host.Arch,
		"os":                          host.OS,
		"uptime":                      host.Uptime,
		"cpu_model":                   host.CPU.Model,
		"cpu_cores":                   host.CPU.Cores,
		"cpu_load_pct":                host.CPU.Load,
		"memory_total":                host.Memory.Total,
		"memory_used":                 host.Memory.Used,
		"memory_used_pct":             host.Memory.UsedPct,
		"disk_total":                  host.Disk.Total,
		"disk_used":                   host.Disk.Used,
		"disk_used_pct":               host.Disk.UsedPct,
		"swap_total":                  host.Swap.Total,
		"swap_used":                   host.Swap.Used,
		"swap_used_pct":               host.Swap.UsedPct,
		"docker":                      host.Docker,
		"etcd_node":                   host.Etcd.Node,
		"etcd_status":                 host.Etcd.Status,
		"etcd_db_size":                host.Etcd.DBSize,
		"controller_service":          host.Controller.Service,
		"controller_status":           host.Controller.Status,
		"controller_version":          host.Controller.Version,
		"controller_running_sha256":   host.Controller.Update.RunningSHA256,
		"controller_update_available": host.Controller.Update.Available,
		"controller_update_error":     host.Controller.Update.Error,
		"agent_status":                host.Agent.Status,
		"agent_pull_interval":         host.Agent.PullInterval,
		"agent_max_concurrent":        host.Agent.MaxConcurrent,
		"agent_labels":                host.Agent.Labels,
	}
	if candidate := host.Controller.Update.Candidate; candidate != nil {
		fields["controller_candidate"] = candidate.Release
		fields["controller_candidate_version"] = candidate.ControllerVersion
		fields["controller_candidate_agent_image"] = candidate.AgentImage
	}
	if last := host.Controller.Update.LastUpdate; last != nil {
		fields["controller_update_task"] = last.TaskID
		fields["controller_update_status"] = last.Status
		fields["controller_update_phase"] = last.Phase
	}
	return fields
}
