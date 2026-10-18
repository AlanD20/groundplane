package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/backingservices"
	"github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type provisioningWork struct {
	environment *environment.Work
	backing     *backingservices.Work
	admission   []workdispatch.Registration
	retry       []workdispatch.RetryRegistration
	retention   []operationretention.WorkRegistration
	completion  []workdispatch.CompletionRegistration
	starts      []workdispatch.AgentStartRegistration
}

func (platform *Platform) newProvisioningWork(queue *workqueue.Repository, dependencies ExecutionDependencies,
	diagnostics *executiondiagnostics.Repository,
) (provisioningWork, error) {
	records, err := environmentprovision.NewRepository(platform.dependencies.Store)
	if err != nil {
		return provisioningWork{}, err
	}
	owner, err := environment.NewWork(records, queue)
	if err != nil {
		return provisioningWork{}, err
	}
	if err := platform.dependencies.Channel.ConfigureEnvironmentProvision(owner); err != nil {
		return provisioningWork{}, err
	}
	backingRecords, err := backingprovisionwork.NewRepository(platform.dependencies.Store)
	if err != nil {
		return provisioningWork{}, err
	}
	backing, err := backingservices.NewWork(backingRecords, queue)
	if err != nil {
		return provisioningWork{}, err
	}
	reports, err := backingservices.NewWorkReports(queue, backingRecords, dependencies.CapturedFiles, dependencies.AttachFacts, diagnostics)
	if err != nil {
		return provisioningWork{}, err
	}
	if err := platform.dependencies.Channel.ConfigureBackingWork(reports); err != nil {
		return provisioningWork{}, err
	}
	return provisioningWork{environment: owner, backing: backing,
		admission: []workdispatch.Registration{
			{Operation: workexecution.Environment, Executor: workexecution.Agent, Owner: owner},
			{Operation: workexecution.BackingProvision, Executor: workexecution.Agent, Owner: backing}},
		retry:     []workdispatch.RetryRegistration{{Operation: workexecution.Environment, Owner: owner}, {Operation: workexecution.BackingProvision, Owner: backing}},
		retention: []operationretention.WorkRegistration{{Operation: workexecution.Environment, Owner: owner}, {Operation: workexecution.BackingProvision, Owner: backing}},
		completion: []workdispatch.CompletionRegistration{
			{Action: workexecution.ActionCreate, Operation: workexecution.Environment, Owner: owner},
			{Action: workexecution.ActionCreate, Operation: workexecution.BackingProvision, Owner: backing}},
		starts: []workdispatch.AgentStartRegistration{{Operation: workexecution.Environment, Owner: owner}, {Operation: workexecution.BackingProvision, Owner: backing}},
	}, nil
}
