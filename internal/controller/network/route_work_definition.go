package network

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

func (publisher *RouteWorkPublisher) publishDefinition(ctx context.Context, request routeWorkRequest,
	marker idempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	kinds, err := publisher.providers.InactiveKinds(request.Candidate)
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	record := request.Route
	task, item, err := taskplanning.NewRouteDefinitionWork(workexecution.Scope{
		TenantID: request.Project.Record.TenantID, ProjectID: request.Project.Record.ID,
		EnvironmentID: request.Environment.Record.ID,
	}, request.Claim, request.Action, record.Desired.ID, record.Desired.Host+record.Desired.Path)
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	return publisher.repository.PublishRouteWork(ctx, etcd.RouteWorkPublication{
		Project: request.Project, Environment: request.Environment, Target: request.Target, Current: request.Current,
		Claim: request.Claim, Projection: request.Candidate, EpochRevision: request.EpochRevision,
		Task: task, Item: item, Marker: marker, InactiveRouterKinds: kinds,
		Capture: componentwork.Capture{
			Route: &componentwork.RouteSettlement{Record: record, Remove: request.Action == workexecution.ActionRemove},
		},
	})
}
