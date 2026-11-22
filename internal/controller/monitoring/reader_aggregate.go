package monitoring

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/monitoringstore"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (reader *Reader) aggregate(
	ctx context.Context,
	query Query,
	window Window,
	response *api.MonitoringResponse,
) error {
	scope := monitoringstore.AggregateScope{}
	page := etcdstore.PageRequest{Limit: query.Limit, Cursor: query.Cursor}
	children := api.Page[api.MonitoringChild]{Items: []api.MonitoringChild{}}
	switch query.Kind {
	case "tenant":
		if ids.Validate(ids.KindTenant, query.ID) != nil {
			return invalidScope()
		}
		tenant, err := reader.hierarchy.GetTenant(ctx, query.ID)
		if err != nil {
			return err
		}
		scope.TenantID = tenant.Record.ID
		response.Scope = api.MonitoringScope{Kind: query.Kind, ID: tenant.Record.ID, Name: tenant.Record.Name}
		projects, err := reader.hierarchy.ListProjects(ctx, hierarchyrecord.ProjectFilter{
			TenantID: scope.TenantID, Kind: hierarchyrecord.ProjectKindTenant,
		}, page)
		if err != nil {
			return err
		}
		children.NextCursor, children.Revision = projects.NextCursor, projects.Revision
		for _, project := range projects.Items {
			children.Items = append(children.Items, api.MonitoringChild{Scope: api.MonitoringScope{
				Kind: "project", ID: project.Record.ID, Name: project.Record.Name,
			}, Live: unavailableLive(), RuntimeState: "unavailable"})
		}
	case "project":
		if ids.Validate(ids.KindProject, query.ID) != nil {
			return invalidScope()
		}
		project, err := reader.hierarchy.GetProject(ctx, query.ID)
		if err != nil {
			return err
		}
		if project.Record.Kind != hierarchyrecord.ProjectKindTenant || project.Record.TenantID == "" {
			return invalidScope()
		}
		scope.TenantID, scope.ProjectID = project.Record.TenantID, project.Record.ID
		response.Scope = api.MonitoringScope{Kind: query.Kind, ID: project.Record.ID, Name: project.Record.Name}
		environments, err := reader.hierarchy.ListEnvironments(ctx, project.Record.ID, page)
		if err != nil {
			return err
		}
		children.NextCursor, children.Revision = environments.NextCursor, environments.Revision
		for _, environment := range environments.Items {
			children.Items = append(children.Items, api.MonitoringChild{Scope: api.MonitoringScope{
				Kind: "environment", ID: environment.Record.ID, Name: environment.Record.Name,
			}, Live: unavailableLive(), RuntimeState: "unavailable"})
		}
	}
	value := api.AggregateMonitoring{Live: unavailableLive(), Children: children}
	for index := range value.Children.Items {
		child := &value.Children.Items[index]
		selector := HealthScope{TenantID: scope.TenantID, ProjectID: scope.ProjectID}
		if child.Scope.Kind == "project" {
			selector.ProjectID = child.Scope.ID
		} else {
			selector.EnvironmentID = child.Scope.ID
		}
		child.RuntimeState = reader.health.State(selector)
	}
	if snapshot, ok := reader.workloads.Snapshot(); ok && snapshot.Complete {
		now := reader.now()
		value.Live = aggregateLive(snapshot, scope, now)
		for index := range value.Children.Items {
			child := &value.Children.Items[index]
			childScope := scope
			if child.Scope.Kind == "project" {
				childScope.ProjectID = child.Scope.ID
			} else {
				childScope.EnvironmentID = child.Scope.ID
			}
			child.Live = aggregateLive(snapshot, childScope, now)
		}
	}
	history, err := ReadAggregateHistory(ctx, reader.history, scope, window)
	if err != nil {
		response.HistoryStatus = "unavailable"
	}
	value.History = history
	if query.Kind == "tenant" {
		response.Tenant = &value
	} else {
		response.Project = &value
	}
	return nil
}

func invalidScope() error {
	return errs.New(errs.KindValidationFailed, "monitoring scope does not match the resource owner")
}

func aggregateLive(
	snapshot CollectionSnapshot,
	scope monitoringstore.AggregateScope,
	now time.Time,
) api.MonitoringLive {
	if !snapshot.Complete {
		return unavailableLive()
	}
	measurements := make([]metrics.ServiceMeasurement, 0, len(snapshot.Services))
	children := make([]metrics.MetricSummaries, 0, len(snapshot.Services))
	for _, sample := range snapshot.Services {
		owner := sample.Ownership
		if owner.Class != metrics.OwnerWorkload || owner.TenantID != scope.TenantID ||
			scope.ProjectID != "" && owner.ProjectID != scope.ProjectID ||
			scope.EnvironmentID != "" && owner.EnvironmentID != scope.EnvironmentID {
			continue
		}
		measurements = append(measurements, metrics.ServiceMeasurement{
			Ownership: owner, Point: sample.Point, Available: sample.Available, Volumes: sample.Volumes,
		})
		children = append(children, sample.Summaries)
	}
	if len(measurements) == 0 {
		return unavailableLive()
	}
	aggregated, err := metrics.AggregateWorkloads(measurements)
	if err != nil {
		return unavailableLive()
	}
	summaries := AggregateSummaries(aggregated.Point, children)
	return livePoint(aggregated.Point, summaries, aggregated.Coverage, aggregated.Coverage.Sampled > 0, now)
}
