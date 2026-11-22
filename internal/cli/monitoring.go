package cli

import (
	"fmt"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newMonitoringCmd() *cobra.Command {
	command := &cobra.Command{Use: "monitoring", Short: "Show live and historical resource monitoring"}
	var rangeName, cursor, consumerCursor string
	var points, limit int
	show := &cobra.Command{
		Use:   "show <scope> [target]",
		Short: "Show host, tenant, project, environment, service or backing monitoring",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			scope := args[0]
			id, err := resolveMonitoringTarget(cmd, scope, args[1:])
			if err != nil {
				return err
			}
			response, err := fromContext(cmd).Client.ShowMonitoring(
				cmd.Context(), scope, id, rangeName, points, limit, cursor, consumerCursor,
			)
			if err != nil {
				return err
			}
			fields, values := fieldsOfVia(monitoringFields(response))
			return fromContext(cmd).Out.RenderOne(fields, values, response)
		},
	}
	show.Flags().StringVar(&rangeName, "range", "1h", "history range: 1h, 6h, 24h or 7d")
	show.Flags().IntVar(&points, "points", 240, "history points (16–720)")
	show.Flags().IntVar(&limit, "limit", 25, "children, services or containers per page (1–200)")
	show.Flags().StringVar(&cursor, "cursor", "", "next_cursor for children, services or containers")
	show.Flags().StringVar(&consumerCursor, "consumer-cursor", "", "backing consumer next_cursor")
	command.AddCommand(show)
	return withExecutionClass(command, executionAPI)
}

func resolveMonitoringTarget(cmd *cobra.Command, scope string, targetArgs []string) (string, error) {
	if scope == "host" {
		if len(targetArgs) == 0 {
			return "local", nil
		}
		if targetArgs[0] == "local" {
			return "local", nil
		}
		return "", errs.New(errs.KindValidationFailed, "host monitoring target must be local or omitted")
	}
	if len(targetArgs) != 1 {
		return "", errs.Newf(errs.KindValidationFailed, "%s monitoring requires one target", scope)
	}
	switch scope {
	case "tenant":
		return resolveTenantTarget(cmd, targetArgs[0])
	case "project":
		return resolveProjectTarget(cmd, targetArgs[0])
	case "environment":
		return resolveEnvironmentTarget(cmd, targetArgs[0])
	case "service":
		return resolveServiceTarget(cmd, targetArgs[0])
	case "backing":
		return resolveBackingProjectTarget(cmd, targetArgs[0])
	default:
		return "", errs.New(
			errs.KindValidationFailed,
			"monitoring scope must be host, tenant, project, environment, service or backing",
		)
	}
}

func monitoringFields(response apiTypes.MonitoringResponse) map[string]any {
	fields := map[string]any{
		"scope": response.Scope.Kind, "id": response.Scope.ID, "name": response.Scope.Name,
		"history_status": response.HistoryStatus, "range": response.Window.Range,
		"window_start": response.Window.Start, "window_end": response.Window.End,
		"bucket_seconds": response.Window.BucketSeconds,
	}
	switch response.Scope.Kind {
	case "host":
		if response.Host != nil {
			monitoringLiveFields(fields, response.Host.Live)
			fields["history_points"] = len(response.Host.History)
			fields["interfaces"] = len(response.Host.Interfaces)
			fields["devices"] = len(response.Host.Devices)
		}
	case "tenant":
		monitoringAggregateFields(fields, response.Tenant)
	case "project":
		monitoringAggregateFields(fields, response.Project)
	case "environment":
		if response.Environment != nil {
			monitoringLiveFields(fields, response.Environment.Live)
			fields["history_points"] = len(response.Environment.History)
			fields["services"] = len(response.Environment.Services.Items)
			fields["next_cursor"] = response.Environment.Services.NextCursor
		}
	case "service":
		monitoringServiceFields(fields, response.Service)
	case "backing":
		if response.Backing != nil {
			monitoringServiceFields(fields, &response.Backing.Runtime)
			fields["consumers"] = len(response.Backing.Consumers.Items)
			fields["consumer_next_cursor"] = response.Backing.Consumers.NextCursor
		}
	}
	return fields
}

func monitoringAggregateFields(fields map[string]any, aggregate *apiTypes.AggregateMonitoring) {
	if aggregate == nil {
		return
	}
	monitoringLiveFields(fields, aggregate.Live)
	fields["history_points"] = len(aggregate.History)
	fields["children"] = len(aggregate.Children.Items)
	fields["next_cursor"] = aggregate.Children.NextCursor
}

func monitoringServiceFields(fields map[string]any, service *apiTypes.ServiceMonitoring) {
	if service == nil {
		return
	}
	monitoringLiveFields(fields, service.Live)
	fields["history_points"] = len(service.History)
	fields["containers_status"] = service.ContainersStatus
	fields["containers"] = len(service.Containers.Items)
	fields["volumes"] = len(service.Volumes)
	fields["next_cursor"] = service.Containers.NextCursor
}

func monitoringLiveFields(fields map[string]any, live apiTypes.MonitoringLive) {
	fields["live_status"] = live.Status
	fields["observed_at"] = live.ObservedAt
	fields["sampled_resources"] = live.Coverage.SampledResources
	fields["eligible_resources"] = live.Coverage.EligibleResources
	fields["sampled_volumes"] = live.Coverage.SampledVolumes
	fields["eligible_volumes"] = live.Coverage.EligibleVolumes
	metrics := []struct {
		name   string
		metric *apiTypes.MonitoringMetric
	}{
		{"cpu_cores", live.Values.CPUCores},
		{"cpu_utilization_ratio", live.Values.CPUUtilizationRatio},
		{"memory_working_set_bytes", live.Values.MemoryWorkingSetBytes},
		{"memory_used_bytes", live.Values.MemoryUsedBytes},
		{"network_receive_bytes_per_second", live.Values.NetworkReceiveBytesPerSecond},
		{"network_transmit_bytes_per_second", live.Values.NetworkTransmitBytesPerSecond},
		{"persistent_volume_used_bytes", live.Values.PersistentVolumeUsedBytes},
		{"disk_used_bytes", live.Values.DiskUsedBytes},
	}
	for _, item := range metrics {
		if item.metric != nil && item.metric.Average != nil {
			fields[item.name] = fmt.Sprintf("%g", *item.metric.Average)
		}
	}
}
