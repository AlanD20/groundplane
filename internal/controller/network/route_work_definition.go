package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

func (publisher *RouteWorkPublisher) prepareDefinition(ctx context.Context, request routeWorkRequest,
	marker idempotency.IdempotencyMarker, finish func(context.Context, bool) error,
) (taskdependencies.Prepared, error) {
	kinds, err := publisher.providers.InactiveKinds(request.Candidate)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	record := request.Route
	task, item, err := taskplanning.NewRouteDefinitionWork(workexecution.Scope{
		TenantID: request.Project.Record.TenantID, ProjectID: request.Project.Record.ID,
		EnvironmentID: request.Environment.Record.ID,
	}, request.Claim, request.Action, record.Desired.ID, record.Desired.Host+record.Desired.Path)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	retained := request.Accepted.Task
	retained.Intent, retained.ExpectedItems, retained.UpdatedAt = *retained.AcceptedIntent, 1, request.At
	task = retained
	item.CreatedAt, item.UpdatedAt = request.At, request.At
	item.Deadline = acceptedaction.ExecutionDeadline(
		task,
		request.At,
		time.Duration(desiredrevision.TaskTimeoutSeconds)*time.Second,
	)
	result, err := publisher.repository.PrepareRouteWorkCapture(ctx, etcd.RouteWorkPublication{
		Project: request.Project, Environment: request.Environment, Target: request.Target, Current: request.Current,
		Claim: request.Claim, Projection: request.Candidate, EpochRevision: request.EpochRevision,
		Task: task, Item: item, Marker: marker, InactiveRouterKinds: kinds,
		Capture: componentwork.Capture{
			Route: &componentwork.RouteSettlement{Record: record, Remove: request.Action == workexecution.ActionRemove},
		},
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	evidence, extra, err := componentwork.PrepareRouteDefinitionCapture(task, item, request.At)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	// The resource capture already owns the identical result write.
	keyvalue.ClearMutationValues(extra.Mutations)
	return taskdependencies.Prepared{
		Task:      task,
		Graph:     []workexecution.Item{result.Item},
		Authority: result.Change,
		Decision:  &evidence,
		Finalize:  finish,
	}, nil
}
