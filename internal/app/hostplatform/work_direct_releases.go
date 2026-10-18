package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/releaseoperation"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type directReleaseExecution struct {
	input       []workdispatch.InputRegistration
	publication []workdispatch.PublicationRegistration
	completion  []workdispatch.CompletionRegistration
}

func (platform *Platform) newDirectReleaseWork(queue *workqueue.Repository, networks *networkwork.InputRepository,
	releases *blueprintrelease.Service,
) (directReleaseExecution, error) {
	ledger, err := etcd.NewReleaseLedger(platform.dependencies.Store, platform.dependencies.Tasks)
	if err != nil {
		return directReleaseExecution{}, err
	}
	inputs, err := releaseoperation.NewInputPreparation(ledger, queue, releases)
	if err != nil {
		return directReleaseExecution{}, err
	}
	publication, err := releaseoperation.NewGraphPublication(ledger, queue, networks)
	if err != nil {
		return directReleaseExecution{}, err
	}
	return directReleaseExecution{
		input: []workdispatch.InputRegistration{
			{Action: workexecution.ActionDeploy, Operation: workexecution.Release, Owner: inputs},
			{Action: workexecution.ActionRollback, Operation: workexecution.Release, Owner: inputs},
		},
		publication: []workdispatch.PublicationRegistration{
			{Action: workexecution.ActionDeploy, Owner: publication},
			{Action: workexecution.ActionRollback, Owner: publication},
		},
		completion: []workdispatch.CompletionRegistration{
			{Action: workexecution.ActionDeploy, Owner: publication},
			{Action: workexecution.ActionRollback, Owner: publication},
		},
	}, nil
}
