package dnsresolver

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareSystemHostWork selects the enrolled target once. Assignment consumes this
// captured identity and platform; it never consults Controller-local GOARCH.
func (planner *PlatformRenderPlanner) PrepareSystemHostWork(ctx context.Context, tasks *etcd.TaskRepository,
	current keyvalue.Versioned[componentrecord.Record], desired core.Component,
	action workexecution.Action, ensureRuntime bool, createdAt time.Time, timeout time.Duration,
) (PreparedWork, error) {
	return planner.prepareHostWork(ctx, tasks, current, desired, WorkRequest{
		TaskID: ids.New(ids.KindTask), WorkID: ids.New(ids.KindWorkItem), PlanID: ids.New(ids.KindPlan),
		OperationID: ids.New(ids.KindOperation), Actor: workexecution.SystemActor, Action: action,
		CreatedAt: createdAt, Deadline: createdAt.Add(timeout), EnsureRuntime: ensureRuntime})
}

func (planner *PlatformRenderPlanner) prepareHostWork(ctx context.Context, tasks *etcd.TaskRepository,
	current keyvalue.Versioned[componentrecord.Record], desired core.Component, request WorkRequest,
) (PreparedWork, error) {
	host, err := tasks.SelectResolverHost(ctx)
	if err != nil {
		return PreparedWork{}, err
	}
	records, err := tasks.ResolverWorkRepository()
	if err != nil {
		return PreparedWork{}, err
	}
	current.ReadRevision = max(current.ReadRevision, host.ReadRevision)
	request.HostID, request.ImageOS, request.ImageArchitecture = host.Record.HostID, host.Record.OS, host.Record.Architecture
	prepared, err := planner.PrepareWork(ctx, records, current, desired, request)
	if err != nil {
		return PreparedWork{}, err
	}
	prepared.Capture.References, err = mergeResolverConditions(prepared.Capture.References,
		[]keyvalue.Condition{{Key: hostbindings.PlatformKey(host.Record.HostID), ModRevision: host.Revision}})
	if err != nil {
		return PreparedWork{}, err
	}
	return prepared, prepared.Capture.Validate(prepared.Input)
}

func (service *PlatformMutationService) prepareAcceptedMutation(
	ctx context.Context, current keyvalue.Versioned[componentrecord.Record], desired core.Component,
	task workexecution.Task, ensure bool, at time.Time,
) (PreparedWork, error) {
	return service.planner.prepareHostWork(ctx, service.tasks, current, desired, WorkRequest{
		TaskID: task.ID, WorkID: ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "resolver-work"),
		PlanID:      ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "resolver-input"),
		OperationID: ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "resolver-operation"),
		Actor:       workexecution.OperatorActor, Action: task.Action, CreatedAt: at,
		Deadline: acceptedaction.ExecutionDeadline(
			task,
			at,
			time.Duration(platformComponentTaskTimeoutSeconds)*time.Second,
		), EnsureRuntime: ensure})
}
