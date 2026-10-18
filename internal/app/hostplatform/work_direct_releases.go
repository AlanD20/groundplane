package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/grouprestoration"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	"github.com/AlanD20/groundplane/internal/controller/releaseoperation"
	"github.com/AlanD20/groundplane/internal/controller/taskcheckpoint"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type directReleaseExecution struct {
	restoration *grouprestoration.Service
	admission   []workdispatch.Registration
	retention   []operationretention.TaskRegistration
	input       []workdispatch.InputRegistration
	publication []workdispatch.PublicationRegistration
	completion  []workdispatch.CompletionRegistration
}

func (platform *Platform) newDirectReleaseWork(queue *workqueue.Repository, networks *networkwork.InputRepository,
	releases *blueprintrelease.Service, scripts *etcd.ScriptRepository, artifacts *taskplanning.ScriptArtifactService,
) (directReleaseExecution, error) {
	ledger, err := etcd.NewReleaseLedger(platform.dependencies.Store, platform.dependencies.Tasks)
	if err != nil {
		return directReleaseExecution{}, err
	}
	groups, err := releasegroups.NewExecutionRepository(platform.dependencies.Store)
	if err != nil {
		return directReleaseExecution{}, err
	}
	restoration, err := grouprestoration.New(ledger, groups, queue, artifacts)
	if err != nil {
		return directReleaseExecution{}, err
	}
	inputs, err := releaseoperation.NewInputPreparation(ledger, queue, releases, groups)
	if err != nil {
		return directReleaseExecution{}, err
	}
	preflight, err := releaseoperation.NewGroupPreflightPreparation(ledger, scripts, queue, groups)
	if err != nil {
		return directReleaseExecution{}, err
	}
	publication, err := releaseoperation.NewGraphPublication(ledger, queue, networks, groups)
	if err != nil {
		return directReleaseExecution{}, err
	}
	return directReleaseExecution{
		restoration: restoration,
		retention: []operationretention.TaskRegistration{
			{Action: workexecution.ActionDeploy, Owner: publication, Drafts: publication},
			{Action: workexecution.ActionRollback, Owner: publication},
		},
		admission: []workdispatch.Registration{
			{Operation: workexecution.ReleaseRestoration, Executor: workexecution.Agent, Owner: restoration},
		},
		input: []workdispatch.InputRegistration{
			{Action: workexecution.ActionDeploy, Operation: workexecution.Release, Owner: inputs},
			{Action: workexecution.ActionRollback, Operation: workexecution.Release, Owner: inputs},
			{Action: workexecution.ActionDeploy, Operation: workexecution.Script, Owner: preflight},
			{Action: workexecution.ActionRollback, Operation: workexecution.Script, Owner: preflight},
			{Action: workexecution.ActionDeploy, Operation: workexecution.ReleaseRestoration, Owner: restoration},
			{Action: workexecution.ActionRollback, Operation: workexecution.ReleaseRestoration, Owner: restoration},
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

func (platform *Platform) newReleaseCheckpoints(queue *workqueue.Repository, dependencies ExecutionDependencies,
	execution directReleaseExecution, diagnostics *executiondiagnostics.Repository,
) (*taskcheckpoint.ReleaseCheckpointService, error) {
	return taskcheckpoint.NewReleaseCheckpointService(queue, platform.dependencies.Store, dependencies.CapturedFiles,
		diagnostics, platform.dependencies.Logger, []taskcheckpoint.ReleaseCheckpointRegistration{
			{Operation: workexecution.Release, Forward: dependencies.Releases, Restoration: dependencies.Releases, Outcome: dependencies.Releases},
			{Operation: workexecution.ReleaseRestoration, Restoration: execution.restoration, Outcome: execution.restoration},
		})
}
