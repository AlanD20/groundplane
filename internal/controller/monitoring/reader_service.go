package monitoring

import (
	"context"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/monitoringstore"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (reader *Reader) environment(
	ctx context.Context,
	query Query,
	window Window,
	response *api.MonitoringResponse,
) error {
	if ids.Validate(ids.KindEnvironment, query.ID) != nil {
		return invalidScope()
	}
	environment, err := reader.hierarchy.GetEnvironment(ctx, query.ID)
	if err != nil {
		return err
	}
	project, err := reader.hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant || project.Record.TenantID == "" {
		return invalidScope()
	}
	services, err := reader.services.ListServices(ctx, environment.Record.ID,
		etcdstore.PageRequest{Limit: query.Limit, Cursor: query.Cursor})
	if err != nil {
		return err
	}
	scope := monitoringstore.AggregateScope{
		TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID,
	}
	// A slow Agent must not consume the history query's deadline.
	var observations []api.ServiceObservation
	var reads sync.WaitGroup
	reads.Go(func() {
		observations = reader.observations.ObserveServices(ctx, services.Items)
	})
	history, historyErr := ReadAggregateHistory(ctx, reader.history, scope, window)
	reads.Wait()
	if len(observations) != len(services.Items) {
		return errs.New(errs.KindInternal, "monitoring health observation count is invalid")
	}
	value := api.EnvironmentMonitoring{
		Live:    unavailableLive(),
		History: history,
		Services: api.Page[api.MonitoringServiceHealth]{
			Items: make([]api.MonitoringServiceHealth, 0, len(services.Items)), NextCursor: services.NextCursor,
			Revision: services.Revision,
		},
	}
	for index, service := range services.Items {
		row := api.MonitoringServiceHealth{ID: service.Record.Desired.ID, Name: service.Record.Desired.Name,
			RuntimeIntent: string(service.Record.Runtime.RuntimeIntent), Observation: observations[index]}
		if sample, ok := reader.workloads.CurrentService(row.ID); ok && sample.Available &&
			sample.Ownership.EnvironmentID == environment.Record.ID {
			var count uint64
			for _, container := range sample.Containers {
				count += container.RestartCount
			}
			at := sample.Point.At.UTC()
			row.RestartCount, row.RestartObservedAt = &count, &at
		}
		value.Services.Items = append(value.Services.Items, row)
	}
	if snapshot, ok := reader.workloads.Snapshot(); ok {
		value.Live = aggregateLive(snapshot, scope, reader.now())
	}
	if historyErr != nil {
		response.HistoryStatus = "unavailable"
	}
	response.Scope = api.MonitoringScope{Kind: "environment", ID: environment.Record.ID, Name: environment.Record.Name}
	response.Environment = &value
	return nil
}

func (reader *Reader) service(ctx context.Context, query Query, window Window, response *api.MonitoringResponse) error {
	serviceID := query.ID
	if query.Kind == "backing" {
		if ids.Validate(ids.KindProject, query.ID) != nil {
			return invalidScope()
		}
		backing, err := reader.backing.GetBackingService(ctx, query.ID)
		if err != nil {
			return err
		}
		serviceID = backing.Record.ServiceID
	} else if ids.Validate(ids.KindService, serviceID) != nil {
		return invalidScope()
	}
	service, err := reader.services.GetService(ctx, serviceID)
	if err != nil {
		return err
	}
	owner, name, err := reader.serviceOwner(ctx, service)
	if err != nil {
		return err
	}
	if (query.Kind == "backing") != (owner.Class == metrics.OwnerBacking) ||
		query.Kind == "backing" && owner.ProjectID != query.ID {
		return invalidScope()
	}
	response.Scope = api.MonitoringScope{Kind: query.Kind, ID: query.ID, Name: name}
	value := api.ServiceMonitoring{
		ContainersStatus: "unavailable",
		Live:             unavailableLive(),
		Containers:       api.Page[api.MonitoringContainer]{Items: []api.MonitoringContainer{}},
		Volumes:          []api.MonitoringVolume{},
	}
	if sample, ok := reader.workloads.CurrentService(serviceID); ok && sample.Ownership == owner {
		value, err = reader.serviceLive(sample, query)
		if err != nil {
			return err
		}
	} else if query.Cursor != "" {
		return invalidContainerCursor()
	}
	value.History, err = ReadServiceHistory(ctx, reader.history, owner, window)
	if err != nil {
		response.HistoryStatus = "unavailable"
	}
	if query.Kind == "backing" {
		consumers, err := reader.consumers.ListAttachesByBackingProject(ctx, owner.ProjectID,
			etcdstore.PageRequest{Limit: query.Limit, Cursor: query.ConsumerCursor})
		if err != nil {
			return err
		}
		page := api.Page[api.MonitoringScope]{Items: []api.MonitoringScope{}, NextCursor: consumers.NextCursor}
		for _, consumer := range consumers.Items {
			if consumer.Record.BackingServiceID != serviceID {
				return invalidScope()
			}
			page.Items = append(
				page.Items,
				api.MonitoringScope{Kind: "service", ID: consumer.Record.ServiceID, Name: consumer.Record.Name},
			)
		}
		response.Backing = &api.BackingMonitoring{Runtime: value, Consumers: page}
	} else {
		response.Service = &value
	}
	return nil
}

func (reader *Reader) serviceOwner(
	ctx context.Context,
	service etcdstore.Versioned[servicerecord.ServiceRecord],
) (metrics.Ownership, string, error) {
	environment, err := reader.hierarchy.GetEnvironment(ctx, service.Record.EnvironmentID)
	if err != nil {
		return metrics.Ownership{}, "", err
	}
	project, err := reader.hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return metrics.Ownership{}, "", err
	}
	class, tenantID, err := inventoryOwnership(project.Record)
	if err != nil {
		return metrics.Ownership{}, "", err
	}
	return metrics.Ownership{TenantID: tenantID, ProjectID: project.Record.ID,
		EnvironmentID: environment.Record.ID, ServiceID: service.Record.Desired.ID, Class: class}, service.Record.Desired.Name, nil
}

func (reader *Reader) serviceLive(sample ServiceSample, query Query) (api.ServiceMonitoring, error) {
	value := api.ServiceMonitoring{ContainersStatus: "available", Live: livePoint(
		sample.Point,
		sample.Summaries,
		metrics.Coverage{
			Eligible: sample.ExpectedReplicas,
			Sampled:  sample.ObservedReplicas,
		},
		sample.Available,
		reader.now(),
	),
		Containers: api.Page[api.MonitoringContainer]{
			Items: []api.MonitoringContainer{},
		}, Volumes: []api.MonitoringVolume{}}
	if !sample.ContainersComplete || !sample.Available {
		if query.Cursor != "" {
			return value, invalidContainerCursor()
		}
		value.ContainersStatus = "unavailable"
	}
	last := ""
	if query.Cursor != "" {
		prefix := "v1." + sample.Ownership.ServiceID + "."
		if !strings.HasPrefix(query.Cursor, prefix) {
			return value, invalidContainerCursor()
		}
		last = strings.TrimPrefix(query.Cursor, prefix)
		decoded, err := hex.DecodeString(last)
		if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != last {
			return value, invalidContainerCursor()
		}
	}
	var containers []ContainerSample
	if sample.ContainersComplete && sample.Available {
		containers = append(containers, sample.Containers...)
	}
	sort.Slice(containers, func(i, j int) bool { return containers[i].ID < containers[j].ID })
	for _, container := range containers {
		if container.ID <= last {
			continue
		}
		if len(value.Containers.Items) == query.Limit {
			value.Containers.NextCursor = "v1." + sample.Ownership.ServiceID + "." + value.Containers.Items[len(value.Containers.Items)-1].ID
			break
		}
		point := container.Point
		point.At = container.ObservedAt
		value.Containers.Items = append(
			value.Containers.Items,
			api.MonitoringContainer{ID: container.ID, Replica: container.Replica, RestartCount: container.RestartCount,
				Live: livePoint(
					point,
					PointSummaries(point, 1, 1),
					metrics.Coverage{Eligible: 1, Sampled: 1},
					true,
					reader.now(),
				)},
		)
	}
	for _, volume := range sample.Volumes {
		var at *time.Time
		if !volume.ObservedAt.IsZero() {
			observed := volume.ObservedAt.UTC()
			at = &observed
		}
		value.Volumes = append(
			value.Volumes,
			api.MonitoringVolume{ID: volume.VolumeID, ObservedAt: at, UsedBytes: volume.UsedBytes},
		)
	}
	return value, nil
}

func invalidContainerCursor() error {
	return errs.New(errs.KindMalformedRequest, "monitoring container cursor is invalid for this Service")
}
