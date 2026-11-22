package apiclient

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) ShowMonitoring(
	ctx context.Context,
	scope string,
	id string,
	rangeName string,
	points int,
	limit int,
	cursor string,
	consumerCursor string,
) (apiTypes.MonitoringResponse, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.MonitoringResponse{}, err
	}
	pointsValue := int64(points)
	limitValue := int64(limit)
	rangeValue := generated.MonitoringShowParamsRange(rangeName)
	params := &generated.MonitoringShowParams{
		Range:  &rangeValue,
		Points: &pointsValue,
		Limit:  &limitValue,
	}
	if cursor != "" {
		params.Cursor = &cursor
	}
	if consumerCursor != "" {
		params.ConsumerCursor = &consumerCursor
	}
	path := "/api/v1/monitoring/" + url.PathEscape(scope) + "/" + url.PathEscape(id)
	response, err := client.MonitoringShowWithResponse(
		ctx,
		generated.MonitoringShowParamsScope(scope),
		id,
		params,
	)
	if err != nil {
		return apiTypes.MonitoringResponse{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.MonitoringResponse{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.MonitoringResponse{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.MonitoringResponse{}, err
		}
	}
	return monitoringResponseFromGenerated(*parsed)
}

func monitoringResponseFromGenerated(
	value generated.MonitoringResponse,
) (apiTypes.MonitoringResponse, error) {
	windowSeconds, err := monitoringUint32("window.bucket_seconds", value.Window.BucketSeconds)
	if err != nil {
		return apiTypes.MonitoringResponse{}, err
	}
	result := apiTypes.MonitoringResponse{
		Scope: monitoringScopeFromGenerated(value.Scope),
		Window: apiTypes.MonitoringWindow{
			Range:         string(value.Window.Range),
			Start:         value.Window.Start,
			End:           value.Window.End,
			BucketSeconds: windowSeconds,
		},
		HistoryStatus: string(value.HistoryStatus),
	}
	if value.Host != nil {
		converted, err := monitoringHostFromGenerated(*value.Host)
		if err != nil {
			return apiTypes.MonitoringResponse{}, err
		}
		result.Host = &converted
	}
	if value.Tenant != nil {
		converted, err := monitoringAggregateFromGenerated(*value.Tenant)
		if err != nil {
			return apiTypes.MonitoringResponse{}, err
		}
		result.Tenant = &converted
	}
	if value.Project != nil {
		converted, err := monitoringAggregateFromGenerated(*value.Project)
		if err != nil {
			return apiTypes.MonitoringResponse{}, err
		}
		result.Project = &converted
	}
	if value.Environment != nil {
		converted, err := monitoringEnvironmentFromGenerated(*value.Environment)
		if err != nil {
			return apiTypes.MonitoringResponse{}, err
		}
		result.Environment = &converted
	}
	if value.Service != nil {
		converted, err := monitoringServiceFromGenerated(*value.Service)
		if err != nil {
			return apiTypes.MonitoringResponse{}, err
		}
		result.Service = &converted
	}
	if value.Backing != nil {
		converted, err := monitoringBackingFromGenerated(*value.Backing)
		if err != nil {
			return apiTypes.MonitoringResponse{}, err
		}
		result.Backing = &converted
	}
	return result, nil
}

func monitoringHostFromGenerated(value generated.HostMonitoring) (apiTypes.HostMonitoring, error) {
	live, err := monitoringLiveFromGenerated(value.Live)
	if err != nil {
		return apiTypes.HostMonitoring{}, err
	}
	history, err := monitoringBucketsFromGenerated(value.History)
	if err != nil {
		return apiTypes.HostMonitoring{}, err
	}
	result := apiTypes.HostMonitoring{Live: live, History: history}
	if value.Interfaces != nil {
		result.Interfaces = make([]apiTypes.MonitoringInterface, 0, len(*value.Interfaces))
		for _, item := range *value.Interfaces {
			result.Interfaces = append(result.Interfaces, apiTypes.MonitoringInterface{
				Name: item.Name, Physical: item.Physical,
				ReceiveBytesPerSecond:  item.ReceiveBytesPerSecond,
				TransmitBytesPerSecond: item.TransmitBytesPerSecond,
			})
		}
	}
	if value.Devices != nil {
		result.Devices = make([]apiTypes.MonitoringDevice, 0, len(*value.Devices))
		for _, item := range *value.Devices {
			major, err := monitoringUint32("device.major", item.Major)
			if err != nil {
				return apiTypes.HostMonitoring{}, err
			}
			minor, err := monitoringUint32("device.minor", item.Minor)
			if err != nil {
				return apiTypes.HostMonitoring{}, err
			}
			result.Devices = append(result.Devices, apiTypes.MonitoringDevice{
				Name: item.Name, Major: major, Minor: minor, Partition: item.Partition,
				RootFilesystem: item.RootFilesystem, ReadBytesPerSecond: item.ReadBytesPerSecond,
				WriteBytesPerSecond: item.WriteBytesPerSecond,
			})
		}
	}
	return result, nil
}

func monitoringAggregateFromGenerated(
	value generated.AggregateMonitoring,
) (apiTypes.AggregateMonitoring, error) {
	live, err := monitoringLiveFromGenerated(value.Live)
	if err != nil {
		return apiTypes.AggregateMonitoring{}, err
	}
	history, err := monitoringBucketsFromGenerated(value.History)
	if err != nil {
		return apiTypes.AggregateMonitoring{}, err
	}
	children := apiTypes.Page[apiTypes.MonitoringChild]{
		NextCursor: monitoringString(value.Children.NextCursor),
		Revision:   monitoringInt64(value.Children.Revision),
	}
	if value.Children.Items != nil {
		children.Items = make([]apiTypes.MonitoringChild, 0, len(*value.Children.Items))
		for _, item := range *value.Children.Items {
			childLive, err := monitoringLiveFromGenerated(item.Live)
			if err != nil {
				return apiTypes.AggregateMonitoring{}, err
			}
			children.Items = append(children.Items, apiTypes.MonitoringChild{
				Scope: monitoringScopeFromGenerated(item.Scope), Live: childLive,
				RuntimeState: string(item.RuntimeState),
			})
		}
	}
	return apiTypes.AggregateMonitoring{Live: live, History: history, Children: children}, nil
}

func monitoringServiceFromGenerated(
	value generated.ServiceMonitoring,
) (apiTypes.ServiceMonitoring, error) {
	live, err := monitoringLiveFromGenerated(value.Live)
	if err != nil {
		return apiTypes.ServiceMonitoring{}, err
	}
	history, err := monitoringBucketsFromGenerated(value.History)
	if err != nil {
		return apiTypes.ServiceMonitoring{}, err
	}
	containers := apiTypes.Page[apiTypes.MonitoringContainer]{
		NextCursor: monitoringString(value.Containers.NextCursor),
		Revision:   monitoringInt64(value.Containers.Revision),
	}
	if value.Containers.Items != nil {
		containers.Items = make([]apiTypes.MonitoringContainer, 0, len(*value.Containers.Items))
		for _, item := range *value.Containers.Items {
			replica, err := monitoringUint32("container.replica", item.Replica)
			if err != nil {
				return apiTypes.ServiceMonitoring{}, err
			}
			restartCount, err := monitoringUint64("container.restart_count", item.RestartCount)
			if err != nil {
				return apiTypes.ServiceMonitoring{}, err
			}
			containerLive, err := monitoringLiveFromGenerated(item.Live)
			if err != nil {
				return apiTypes.ServiceMonitoring{}, err
			}
			containers.Items = append(containers.Items, apiTypes.MonitoringContainer{
				ID: item.Id, Replica: replica, RestartCount: restartCount, Live: containerLive,
			})
		}
	}
	result := apiTypes.ServiceMonitoring{
		ContainersStatus: string(value.ContainersStatus), Live: live, History: history,
		Containers: containers,
	}
	if value.Volumes != nil {
		result.Volumes = make([]apiTypes.MonitoringVolume, 0, len(*value.Volumes))
		for _, item := range *value.Volumes {
			usedBytes, err := monitoringOptionalUint64("volume.used_bytes", item.UsedBytes)
			if err != nil {
				return apiTypes.ServiceMonitoring{}, err
			}
			result.Volumes = append(result.Volumes, apiTypes.MonitoringVolume{
				ID: item.Id, ObservedAt: item.ObservedAt, UsedBytes: usedBytes,
			})
		}
	}
	return result, nil
}

func monitoringEnvironmentFromGenerated(
	value generated.EnvironmentMonitoring,
) (apiTypes.EnvironmentMonitoring, error) {
	services := apiTypes.Page[apiTypes.MonitoringServiceHealth]{
		NextCursor: monitoringString(value.Services.NextCursor),
		Revision:   monitoringInt64(value.Services.Revision),
	}
	if value.Services.Items != nil {
		services.Items = make([]apiTypes.MonitoringServiceHealth, 0, len(*value.Services.Items))
		for _, item := range *value.Services.Items {
			observation, err := monitoringObservationFromGenerated(item.Observation)
			if err != nil {
				return apiTypes.EnvironmentMonitoring{}, err
			}
			restartCount, err := monitoringOptionalUint64("service.restart_count", item.RestartCount)
			if err != nil {
				return apiTypes.EnvironmentMonitoring{}, err
			}
			services.Items = append(services.Items, apiTypes.MonitoringServiceHealth{
				ID: item.Id, Name: item.Name, RuntimeIntent: string(item.RuntimeIntent),
				Observation: observation, RestartCount: restartCount,
				RestartObservedAt: item.RestartObservedAt,
			})
		}
	}
	return apiTypes.EnvironmentMonitoring{Services: services}, nil
}

func monitoringBackingFromGenerated(
	value generated.BackingMonitoring,
) (apiTypes.BackingMonitoring, error) {
	runtime, err := monitoringServiceFromGenerated(value.Runtime)
	if err != nil {
		return apiTypes.BackingMonitoring{}, err
	}
	consumers := apiTypes.Page[apiTypes.MonitoringScope]{
		NextCursor: monitoringString(value.Consumers.NextCursor),
		Revision:   monitoringInt64(value.Consumers.Revision),
	}
	if value.Consumers.Items != nil {
		consumers.Items = make([]apiTypes.MonitoringScope, 0, len(*value.Consumers.Items))
		for _, item := range *value.Consumers.Items {
			consumers.Items = append(consumers.Items, monitoringScopeFromGenerated(item))
		}
	}
	return apiTypes.BackingMonitoring{Runtime: runtime, Consumers: consumers}, nil
}

func monitoringBucketsFromGenerated(
	items *[]generated.MonitoringBucket,
) ([]apiTypes.MonitoringBucket, error) {
	if items == nil {
		return nil, nil
	}
	result := make([]apiTypes.MonitoringBucket, 0, len(*items))
	for _, item := range *items {
		coverage, err := monitoringCoverageFromGenerated(item.Coverage)
		if err != nil {
			return nil, err
		}
		values, err := monitoringValuesFromGenerated(item.Values)
		if err != nil {
			return nil, err
		}
		result = append(result, apiTypes.MonitoringBucket{
			Start: item.Start, End: item.End, Values: values, Coverage: coverage,
		})
	}
	return result, nil
}

func monitoringLiveFromGenerated(value generated.MonitoringLive) (apiTypes.MonitoringLive, error) {
	coverage, err := monitoringCoverageFromGenerated(value.Coverage)
	if err != nil {
		return apiTypes.MonitoringLive{}, err
	}
	values, err := monitoringValuesFromGenerated(value.Values)
	if err != nil {
		return apiTypes.MonitoringLive{}, err
	}
	return apiTypes.MonitoringLive{
		Status: string(value.Status), ObservedAt: value.ObservedAt, Values: values, Coverage: coverage,
	}, nil
}

func monitoringCoverageFromGenerated(
	value generated.MonitoringCoverage,
) (apiTypes.MonitoringCoverage, error) {
	eligibleResources, err := monitoringUint32("coverage.eligible_resources", value.EligibleResources)
	if err != nil {
		return apiTypes.MonitoringCoverage{}, err
	}
	sampledResources, err := monitoringUint32("coverage.sampled_resources", value.SampledResources)
	if err != nil {
		return apiTypes.MonitoringCoverage{}, err
	}
	eligibleVolumes, err := monitoringUint32("coverage.eligible_volumes", value.EligibleVolumes)
	if err != nil {
		return apiTypes.MonitoringCoverage{}, err
	}
	sampledVolumes, err := monitoringUint32("coverage.sampled_volumes", value.SampledVolumes)
	if err != nil {
		return apiTypes.MonitoringCoverage{}, err
	}
	return apiTypes.MonitoringCoverage{
		EligibleResources: eligibleResources, SampledResources: sampledResources,
		EligibleVolumes: eligibleVolumes, SampledVolumes: sampledVolumes,
	}, nil
}

func monitoringValuesFromGenerated(
	value generated.MonitoringValues,
) (apiTypes.MonitoringValues, error) {
	result := apiTypes.MonitoringValues{}
	fields := []struct {
		name   string
		source *generated.MonitoringMetric
		target **apiTypes.MonitoringMetric
	}{
		{"cpu_cores", value.CpuCores, &result.CPUCores},
		{"cpu_utilization_ratio", value.CpuUtilizationRatio, &result.CPUUtilizationRatio},
		{"cpu_limit_cores", value.CpuLimitCores, &result.CPULimitCores},
		{"memory_working_set_bytes", value.MemoryWorkingSetBytes, &result.MemoryWorkingSetBytes},
		{"memory_used_bytes", value.MemoryUsedBytes, &result.MemoryUsedBytes},
		{"memory_total_bytes", value.MemoryTotalBytes, &result.MemoryTotalBytes},
		{"memory_limit_bytes", value.MemoryLimitBytes, &result.MemoryLimitBytes},
		{"network_receive_bytes_per_second", value.NetworkReceiveBytesPerSecond, &result.NetworkReceiveBytesPerSecond},
		{
			"network_transmit_bytes_per_second",
			value.NetworkTransmitBytesPerSecond,
			&result.NetworkTransmitBytesPerSecond,
		},
		{"writable_layer_bytes", value.WritableLayerBytes, &result.WritableLayerBytes},
		{"block_read_bytes_per_second", value.BlockReadBytesPerSecond, &result.BlockReadBytesPerSecond},
		{"block_write_bytes_per_second", value.BlockWriteBytesPerSecond, &result.BlockWriteBytesPerSecond},
		{"block_read_operations_per_second", value.BlockReadOperationsPerSecond, &result.BlockReadOperationsPerSecond},
		{
			"block_write_operations_per_second",
			value.BlockWriteOperationsPerSecond,
			&result.BlockWriteOperationsPerSecond,
		},
		{"persistent_volume_used_bytes", value.PersistentVolumeUsedBytes, &result.PersistentVolumeUsedBytes},
		{"disk_used_bytes", value.DiskUsedBytes, &result.DiskUsedBytes},
		{"disk_total_bytes", value.DiskTotalBytes, &result.DiskTotalBytes},
	}
	for _, field := range fields {
		if field.source == nil {
			continue
		}
		converted, err := monitoringMetricFromGenerated(field.name, *field.source)
		if err != nil {
			return apiTypes.MonitoringValues{}, err
		}
		*field.target = &converted
	}
	return result, nil
}

func monitoringMetricFromGenerated(
	name string,
	value generated.MonitoringMetric,
) (apiTypes.MonitoringMetric, error) {
	validSamples, err := monitoringUint32(name+".valid_samples", value.ValidSamples)
	if err != nil {
		return apiTypes.MonitoringMetric{}, err
	}
	expectedSamples, err := monitoringUint32(name+".expected_samples", value.ExpectedSamples)
	if err != nil {
		return apiTypes.MonitoringMetric{}, err
	}
	validContributions, err := monitoringUint32(name+".valid_contributions", value.ValidContributions)
	if err != nil {
		return apiTypes.MonitoringMetric{}, err
	}
	expectedContributions, err := monitoringUint32(
		name+".expected_contributions", value.ExpectedContributions,
	)
	if err != nil {
		return apiTypes.MonitoringMetric{}, err
	}
	return apiTypes.MonitoringMetric{
		Average: value.Average, Minimum: value.Minimum, Maximum: value.Maximum,
		ValidSamples: validSamples, ExpectedSamples: expectedSamples,
		ValidContributions: validContributions, ExpectedContributions: expectedContributions,
	}, nil
}

func monitoringObservationFromGenerated(
	value generated.ServiceObservation,
) (apiTypes.ServiceObservation, error) {
	result := apiTypes.ServiceObservation{
		State: apiTypes.ServiceObservationState(value.State), ObservedAt: value.ObservedAt,
		ExpiresAt: value.ExpiresAt, ServingReleaseID: value.ServingReleaseId,
	}
	var err error
	result.ExpectedReplicas, err = monitoringOptionalUint32("observation.expected_replicas", value.ExpectedReplicas)
	if err != nil {
		return apiTypes.ServiceObservation{}, err
	}
	if value.Replicas != nil {
		counts := apiTypes.ServiceReplicaCounts{}
		pairs := []struct {
			name   string
			source int32
			target *uint32
		}{
			{"running", value.Replicas.Running, &counts.Running},
			{"healthy", value.Replicas.Healthy, &counts.Healthy},
			{"starting", value.Replicas.Starting, &counts.Starting},
			{"unhealthy", value.Replicas.Unhealthy, &counts.Unhealthy},
			{"transitional", value.Replicas.Transitional, &counts.Transitional},
			{"stopped", value.Replicas.Stopped, &counts.Stopped},
			{"failed", value.Replicas.Failed, &counts.Failed},
		}
		for _, pair := range pairs {
			*pair.target, err = monitoringUint32("observation.replicas."+pair.name, pair.source)
			if err != nil {
				return apiTypes.ServiceObservation{}, err
			}
		}
		result.Replicas = &counts
	}
	if value.Containers != nil {
		result.Containers = make([]apiTypes.ServiceContainerObservation, 0, len(*value.Containers))
		for _, item := range *value.Containers {
			replica, err := monitoringUint32("observation.container.replica", item.Replica)
			if err != nil {
				return apiTypes.ServiceObservation{}, err
			}
			result.Containers = append(result.Containers, apiTypes.ServiceContainerObservation{
				ID: item.Id, Name: item.Name, Image: item.Image,
				State: string(item.State), Health: string(item.Health), Replica: replica,
			})
		}
	}
	return result, nil
}

func monitoringScopeFromGenerated(value generated.MonitoringScope) apiTypes.MonitoringScope {
	return apiTypes.MonitoringScope{Kind: string(value.Kind), ID: value.Id, Name: value.Name}
}

func monitoringString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func monitoringInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func monitoringUint32(field string, value int32) (uint32, error) {
	if value < 0 {
		return 0, errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("apiclient: monitoring response %s is negative", field),
		)
	}
	return uint32(value), nil
}

func monitoringUint64(field string, value int64) (uint64, error) {
	if value < 0 {
		return 0, errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("apiclient: monitoring response %s is negative", field),
		)
	}
	return uint64(value), nil
}

func monitoringOptionalUint32(field string, value *int32) (*uint32, error) {
	if value == nil {
		return nil, nil
	}
	converted, err := monitoringUint32(field, *value)
	if err != nil {
		return nil, err
	}
	return &converted, nil
}

func monitoringOptionalUint64(field string, value *int64) (*uint64, error) {
	if value == nil {
		return nil, nil
	}
	converted, err := monitoringUint64(field, *value)
	if err != nil {
		return nil, err
	}
	return &converted, nil
}
