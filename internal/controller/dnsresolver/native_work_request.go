package dnsresolver

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareHostWork selects the enrolled target once. Assignment consumes this
// captured identity and platform; it never consults Controller-local GOARCH.
func (planner *PlatformRenderPlanner) PrepareHostWork(ctx context.Context, tasks *etcd.TaskRepository,
	current keyvalue.Versioned[componentrecord.Record], desired core.Component, actor workexecution.Actor,
	action workexecution.Action, ensureRuntime bool, createdAt time.Time, timeout time.Duration,
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
	prepared, err := planner.PrepareWork(ctx, records, current, desired, WorkRequest{
		TaskID: ids.New(ids.KindTask), WorkID: ids.New(ids.KindWorkItem), PlanID: ids.New(ids.KindPlan),
		OperationID: ids.New(ids.KindOperation), HostID: host.Record.HostID,
		ImageOS: host.Record.OS, ImageArchitecture: host.Record.Architecture,
		Actor: actor, Action: action, CreatedAt: createdAt, Deadline: createdAt.Add(timeout), EnsureRuntime: ensureRuntime})
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

func (repository *PlatformMutationService) prepareMutation(ctx context.Context, current keyvalue.Versioned[componentrecord.Record],
	desired core.Component, action workexecution.Action, ensure bool,
) (PreparedWork, error) {
	return repository.planner.PrepareHostWork(ctx, repository.tasks, current, desired, workexecution.OperatorActor,
		action, ensure, repository.now().UTC(), time.Duration(platformComponentTaskTimeoutSeconds)*time.Second)
}
