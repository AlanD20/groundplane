package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type resolverWork struct {
	owner      *dnsresolver.Work
	admission  []workdispatch.Registration
	retry      []workdispatch.RetryRegistration
	retention  []operationretention.WorkRegistration
	completion []workdispatch.CompletionRegistration
	starts     []workdispatch.AgentStartRegistration
}

func (platform *Platform) newResolverWork(queue *workqueue.Repository, diagnostics *executiondiagnostics.Repository) (resolverWork, error) {
	records, err := resolverwork.NewRepository(platform.dependencies.Store)
	if err != nil {
		return resolverWork{}, err
	}
	owner, err := dnsresolver.NewWork(records, queue)
	if err != nil {
		return resolverWork{}, err
	}
	reports, err := dnsresolver.NewWorkReports(queue, records, diagnostics)
	if err != nil {
		return resolverWork{}, err
	}
	if err := platform.dependencies.Channel.ConfigureResolverWork(reports); err != nil {
		return resolverWork{}, err
	}
	return resolverWork{owner: owner,
		admission: []workdispatch.Registration{{Operation: workexecution.Resolver, Executor: workexecution.Agent, Owner: owner}},
		retry:     []workdispatch.RetryRegistration{{Operation: workexecution.Resolver, Owner: owner}},
		retention: []operationretention.WorkRegistration{{Operation: workexecution.Resolver, Owner: owner}},
		completion: []workdispatch.CompletionRegistration{
			{Action: workexecution.ActionStart, Operation: workexecution.Resolver, Owner: owner},
			{Action: workexecution.ActionStop, Operation: workexecution.Resolver, Owner: owner},
			{Action: workexecution.ActionUpdate, Operation: workexecution.Resolver, Owner: owner}},
		starts: []workdispatch.AgentStartRegistration{{Operation: workexecution.Resolver, Owner: owner}},
	}, nil
}
