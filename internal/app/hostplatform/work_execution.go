package hostplatform

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/controller/attachreports"
	"github.com/AlanD20/groundplane/internal/controller/backingchecks"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/connectors"
	"github.com/AlanD20/groundplane/internal/controller/detachwork"
	"github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	"github.com/AlanD20/groundplane/internal/controller/edgechecks"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	"github.com/AlanD20/groundplane/internal/controller/scriptwork"
	"github.com/AlanD20/groundplane/internal/controller/taskcheckpoint"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	backingrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	edgerecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// WorkExecution is composed separately from the superseded Task runner during
// source migration. The final daemon cutover starts only the replacement runtime.
type WorkExecution struct {
	Runtime         *workdispatch.Runtime
	Queue           *workqueue.Repository
	scheduler       *workdispatch.Scheduler
	controller      *workdispatch.ControllerExecutor
	Retries         *workdispatch.Retries
	replayRetention *idempotency.RetentionRepository
	retention       *operationretention.Collector
	config          workdispatch.RuntimeConfig
}

// Run keeps Controller-local execution serial, as before, while Agent capacity
// remains independently configured on each host. Start owns process registration,
// reconciliation and lease release after all execution loops have joined.
func (execution *WorkExecution) Run(ctx context.Context) error {
	return execution.Runtime.Start(ctx, 1, execution.config)
}

func (platform *Platform) NewWorkExecution(dependencies ExecutionDependencies) (*WorkExecution, error) {
	maintenance, err := platform.dependencies.Config.Maintenance.Policy()
	if err != nil {
		return nil, err
	}
	queue, err := workqueue.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	scriptRecords, err := etcd.NewScriptRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	scripts, err := scriptwork.New(queue, scriptRecords, dependencies.ScriptArtifacts)
	if err != nil {
		return nil, err
	}
	checkRecords, err := connectorchecks.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	checks, err := connectors.NewCheckExecutor(checkRecords, platform.dependencies.Protector)
	if err != nil {
		return nil, err
	}
	backingRecords, err := backingrecord.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	backing, err := backingchecks.NewExecutor(backingRecords, platform.dependencies.Channel.Registry, platform.dependencies.Protector)
	if err != nil {
		return nil, err
	}
	dnsRecords, err := dnschecks.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	dns, err := dnsresolver.NewConnectionCheckExecutor(dnsRecords, platform.dependencies.Channel.Registry)
	if err != nil {
		return nil, err
	}
	edgeRecords, err := edgerecord.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	edge, err := edgechecks.NewExecutor(edgeRecords, platform.dependencies.Channel.Registry)
	if err != nil {
		return nil, err
	}
	runnerRecords, err := runnerchecks.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	blueprintLedger, err := blueprintunits.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	entryWork, err := blueprint.NewEntryWork(dependencies.Entries, blueprintLedger)
	if err != nil {
		return nil, err
	}
	definitionWork, err := blueprint.NewDefinitionWork(dependencies.Definitions, blueprintLedger)
	if err != nil {
		return nil, err
	}
	hierarchyRecords, err := etcd.NewHierarchyDeletionRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	hierarchyController, err := hierarchydeletion.NewControllerWork(hierarchyRecords)
	if err != nil {
		return nil, err
	}
	diagnostics, err := executiondiagnostics.New(platform.dependencies.Store, queue)
	if err != nil {
		return nil, err
	}
	backupRemoval, err := hierarchydeletion.NewBackupWork(hierarchyRecords, queue, dependencies.BackupRemoval, diagnostics)
	if err != nil {
		return nil, err
	}
	attachRemovals, err := attachplan.NewRemovalPlanner(dependencies.AttachFacts)
	if err != nil {
		return nil, err
	}
	hierarchyAgent, err := hierarchydeletion.NewAgentWork(queue, hierarchyRecords, attachRemovals)
	if err != nil {
		return nil, err
	}
	detachRecords, err := attachremoval.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	detaches, err := detachwork.New(queue, detachRecords, attachRemovals)
	if err != nil {
		return nil, err
	}
	volumeRecords, err := volumework.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	volumeAgent, err := volume.NewAgentWork(volumeRecords, queue)
	if err != nil {
		return nil, err
	}
	volumeReports, err := volume.NewWorkReports(queue, volumeRecords)
	if err != nil {
		return nil, err
	}
	networkRecords, err := networkwork.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	networkAgent, err := network.NewAgentWork(networkRecords, queue)
	if err != nil {
		return nil, err
	}
	networkReports, err := network.NewWorkReports(queue, networkRecords)
	if err != nil {
		return nil, err
	}
	scheduler, err := workdispatch.NewScheduler(queue, []workdispatch.Registration{
		{Operation: workexecution.Detach, Executor: workexecution.Agent, Owner: detaches},
		{Operation: workexecution.Detach, Executor: workexecution.Controller, Owner: detaches, DecisionOnly: true},
		{Operation: workexecution.Network, Executor: workexecution.Agent, Owner: networkAgent},
		{Operation: workexecution.Volume, Executor: workexecution.Agent, Owner: volumeAgent},
		{Operation: workexecution.RemovalBackupObject, Executor: workexecution.Controller, Owner: backupRemoval},
		{Operation: workexecution.Hierarchy, Executor: workexecution.Controller, Owner: hierarchyController, DecisionOnly: true},
		{Operation: workexecution.Hierarchy, Executor: workexecution.Agent, Owner: hierarchyAgent},
		{Operation: workexecution.Script, Executor: workexecution.Agent, Owner: scripts},
		{Operation: workexecution.Release, Executor: workexecution.Agent, Owner: dependencies.Releases},
		{Operation: workexecution.ResourceDefinition, Executor: workexecution.Controller, Owner: definitionWork, DecisionOnly: true},
		{Operation: workexecution.EntrySettlement, Executor: workexecution.Controller, Owner: entryWork, DecisionOnly: true},
		{Operation: workexecution.Image, Executor: workexecution.Controller, Owner: dependencies.Images},
		{Operation: workexecution.ConnectorCheck, Executor: workexecution.Controller, Owner: checks},
		{Operation: workexecution.BackingCheck, Executor: workexecution.Controller, Owner: backing},
		{Operation: workexecution.DNSCheck, Executor: workexecution.Controller, Owner: dns},
		{Operation: workexecution.EdgeCheck, Executor: workexecution.Controller, Owner: edge},
		{Operation: workexecution.RunnerCheck, Executor: workexecution.Controller, Owner: dependencies.RunnerChecks},
	})
	if err != nil {
		return nil, err
	}
	controller, err := workdispatch.NewControllerExecutor(queue, []workdispatch.ControllerRegistration{
		{Operation: workexecution.RemovalBackupObject, Owner: backupRemoval},
		{Operation: workexecution.Image, Owner: dependencies.Images},
		{Operation: workexecution.ConnectorCheck, Owner: checks},
		{Operation: workexecution.BackingCheck, Owner: backing},
		{Operation: workexecution.DNSCheck, Owner: dns},
		{Operation: workexecution.EdgeCheck, Owner: edge},
		{Operation: workexecution.RunnerCheck, Owner: dependencies.RunnerChecks},
	})
	if err != nil {
		return nil, err
	}
	retries, err := workdispatch.NewRetries(queue, []workdispatch.RetryRegistration{
		{Operation: workexecution.Detach, Owner: detaches},
		{Operation: workexecution.Network, Owner: networkAgent},
		{Operation: workexecution.Volume, Owner: volumeAgent},
		{Operation: workexecution.RemovalBackupObject, Owner: backupRemoval},
		{Operation: workexecution.Hierarchy, Owner: hierarchyAgent},
		{Operation: workexecution.Script, Owner: scripts},
		{Operation: workexecution.Release, Owner: dependencies.Releases},
		{Operation: workexecution.ConnectorCheck, Owner: checks},
		{Operation: workexecution.BackingCheck, Owner: backing},
		{Operation: workexecution.EdgeCheck, Owner: edge},
	})
	if err != nil {
		return nil, err
	}
	replayRetention, err := idempotency.NewRetentionRepository(platform.dependencies.Store, queue.GuardReplayRetention)
	if err != nil {
		return nil, err
	}
	imageRecords, err := imageoperations.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	hierarchyRetention, err := hierarchydeletion.NewRetention(hierarchyRecords)
	if err != nil {
		return nil, err
	}
	retention, err := operationretention.New(queue, checkRecords, backingRecords, dnsRecords, edgeRecords, runnerRecords, imageRecords, hierarchyRecords,
		operationretention.WorkRegistration{Operation: workexecution.Detach, Owner: detaches},
		operationretention.WorkRegistration{Operation: workexecution.Network, Owner: networkAgent},
		operationretention.WorkRegistration{Operation: workexecution.Volume, Owner: volumeAgent},
		operationretention.WorkRegistration{Operation: workexecution.RemovalBackupObject, Owner: hierarchyRetention},
		operationretention.WorkRegistration{Operation: workexecution.Hierarchy, Owner: hierarchyRetention},
		operationretention.WorkRegistration{Operation: workexecution.Script, Owner: scripts},
		operationretention.WorkRegistration{Operation: workexecution.Release, Owner: dependencies.Releases},
		operationretention.WorkRegistration{Operation: workexecution.EntrySettlement, Owner: entryWork},
		operationretention.WorkRegistration{Operation: workexecution.ResourceDefinition, Owner: definitionWork})
	if err != nil {
		return nil, err
	}
	blueprintCompletion, err := blueprintcoordinator.NewCompletion(blueprintLedger)
	if err != nil {
		return nil, err
	}
	removalCompletion, err := hierarchydeletion.NewRemovalCompletion(hierarchyRecords, volumeAgent, networkAgent)
	if err != nil {
		return nil, err
	}
	completions, err := workdispatch.NewCompletions(queue, []workdispatch.CompletionRegistration{
		{Action: workexecution.ActionDetach, Owner: detaches},
		{Action: workexecution.ActionCreate, Owner: volumeAgent},
		{Action: workexecution.ActionUpdate, Owner: volumeAgent},
		{Action: workexecution.ActionScript, Owner: scripts},
		{Action: workexecution.ActionApply, Owner: blueprintCompletion},
		{Action: workexecution.ActionCheck, Owner: workdispatch.CheckImageCompletion{}},
		{Action: workexecution.ActionFetch, Owner: workdispatch.CheckImageCompletion{}},
		{Action: workexecution.ActionRemove, Owner: removalCompletion},
	})
	if err != nil {
		return nil, err
	}
	blueprintPublication, err := blueprintcoordinator.NewPublication(blueprintLedger)
	if err != nil {
		return nil, err
	}
	hierarchyPublication, err := hierarchydeletion.NewWorkPublication(dependencies.Hierarchy, hierarchyRecords)
	if err != nil {
		return nil, err
	}
	removalPublication, err := workdispatch.NewRemovalPublication(hierarchyPublication, networkAgent)
	if err != nil {
		return nil, err
	}
	publications, err := workdispatch.NewPublications(queue, []workdispatch.PublicationRegistration{
		{Action: workexecution.ActionRemove, Owner: removalPublication},
		{Action: workexecution.ActionApply, Owner: blueprintPublication},
	})
	if err != nil {
		return nil, err
	}
	serviceInputs, err := blueprint.NewServiceWorkPreparation(dependencies.Blueprints, blueprintLedger, queue)
	if err != nil {
		return nil, err
	}
	volumeInputs, err := blueprint.NewVolumeWorkPreparation(dependencies.Blueprints, blueprintLedger, queue, volumeRecords)
	if err != nil {
		return nil, err
	}
	networkInputs, err := blueprint.NewNetworkWorkPreparation(dependencies.Blueprints, blueprintLedger, queue, networkRecords)
	if err != nil {
		return nil, err
	}
	inputs, err := workdispatch.NewInputs(queue, []workdispatch.InputRegistration{
		{Action: workexecution.ActionApply, Operation: workexecution.Release, Owner: serviceInputs},
		{Action: workexecution.ActionApply, Operation: workexecution.Volume, Owner: volumeInputs},
		{Action: workexecution.ActionApply, Operation: workexecution.Network, Owner: networkInputs},
	})
	if err != nil {
		return nil, err
	}
	runtime, err := workdispatch.NewRuntime(queue, scheduler, controller, completions, publications, inputs, retention, replayRetention, platform.dependencies.Logger)
	if err != nil {
		return nil, err
	}
	starts, err := workdispatch.NewAgentStarts(queue, []workdispatch.AgentStartRegistration{
		{Operation: workexecution.Detach, Owner: detaches},
		{Operation: workexecution.Network, Owner: networkAgent},
		{Operation: workexecution.Volume, Owner: volumeAgent},
		{Operation: workexecution.Hierarchy, Owner: hierarchyAgent},
		{Operation: workexecution.Script, Owner: scripts},
		{Operation: workexecution.Release, Owner: dependencies.Releases},
	})
	if err != nil {
		return nil, err
	}
	checkpoints, err := taskcheckpoint.NewReleaseCheckpointService(queue, platform.dependencies.Store, dependencies.CapturedFiles, diagnostics, platform.dependencies.Logger,
		[]taskcheckpoint.ReleaseCheckpointRegistration{{Operation: workexecution.Release, Owner: dependencies.Releases}})
	if err != nil {
		return nil, err
	}
	deliveries, err := workdispatch.NewAgentDeliveries(queue, scheduler, dependencies.Releases, scripts, hierarchyAgent, volumeAgent, networkAgent, detaches)
	if err != nil {
		return nil, err
	}
	removalReports, err := hierarchydeletion.NewReports(queue, hierarchyAgent)
	if err != nil {
		return nil, err
	}
	attachReports, err := attachreports.New(queue, []attachreports.Registration{
		{Operation: workexecution.Hierarchy, Owner: hierarchyAgent},
		{Operation: workexecution.Detach, Owner: detaches},
	})
	if err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureWorkExecution(platform.dependencies.Agents, starts, checkpoints, scripts, removalReports, deliveries); err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureVolumeWork(volumeReports); err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureNetworkWork(networkReports); err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureAttachWork(attachReports); err != nil {
		return nil, err
	}
	return &WorkExecution{Runtime: runtime, Queue: queue, scheduler: scheduler, controller: controller,
		Retries: retries, replayRetention: replayRetention, retention: retention,
		config: workdispatch.RuntimeConfig{Interval: platform.dependencies.Tick, CollectionInterval: maintenance.CleanupInterval,
			HistoryRetention: maintenance.DiagnosticRetention}}, nil
}
