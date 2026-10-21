package hostplatform

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachreports"
	"github.com/AlanD20/groundplane/internal/controller/attachwork"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	componentexecution "github.com/AlanD20/groundplane/internal/controller/componentwork"
	entryexecution "github.com/AlanD20/groundplane/internal/controller/entrywork"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// WorkExecution composes the native scheduling, operation and retention owners.
// The daemon starts one execution runtime, not parallel Task engines.
type WorkExecution struct {
	Runtime    *workdispatch.Runtime
	Queue      *workqueue.Repository
	scheduler  *workdispatch.Scheduler
	controller *workdispatch.ControllerExecutor
	Retries    *workdispatch.Retries
	retention  *operationretention.Collector
	config     workdispatch.RuntimeConfig
}

// Run keeps Controller-local execution serial, as before, while Agent capacity
// remains independently configured on each host. Start owns process registration,
// reconciliation and lease release after all execution loops have joined.
func (execution *WorkExecution) Run(ctx context.Context) error {
	return execution.Runtime.Start(ctx, 1, execution.config)
}

func (platform *Platform) NewWorkExecution(dependencies ExecutionDependencies) (*WorkExecution, error) {
	softwareWork := platform.softwareWork()
	maintenance, err := platform.dependencies.Config.Maintenance.Policy()
	if err != nil {
		return nil, err
	}
	queue, err := workqueue.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	owners, err := platform.newWorkOwners(queue, dependencies)
	if err != nil {
		return nil, err
	}
	dispatchers, err := owners.newDispatchers(platform, dependencies, queue, softwareWork)
	if err != nil {
		return nil, err
	}
	scheduler, controller, retries := dispatchers.scheduler, dispatchers.controller, dispatchers.retries
	imageRecords, err := imageoperations.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	hierarchyRetention, err := hierarchydeletion.NewRetention(owners.hierarchyRecords)
	if err != nil {
		return nil, err
	}
	componentRetention, err := componentexecution.NewRetention(
		owners.componentRecords,
		owners.componentDefinitions,
		dependencies.Releases,
	)
	if err != nil {
		return nil, err
	}
	retentionOwners := append(
		append(append(softwareWork.retention, owners.provisioning.retention...), owners.resolver.retention...),
		[]operationretention.WorkRegistration{
			{Operation: workexecution.ReleaseGroup, Owner: owners.groupRemoval},
			operationretention.WorkRegistration{Operation: workexecution.AgentLifecycle, Owner: owners.agentWork},
			operationretention.WorkRegistration{Operation: workexecution.AgentSoftware, Owner: owners.agentWork},
			operationretention.WorkRegistration{
				Operation: workexecution.ControllerSoftware,
				Owner:     platform.controllerWork,
			},
			operationretention.WorkRegistration{
				Operation: workexecution.SoftwarePreparation,
				Owner:     platform.preparations,
			},
			operationretention.WorkRegistration{Operation: workexecution.EtcdConfiguration, Owner: platform.EtcdConfig},
			operationretention.WorkRegistration{Operation: workexecution.SecretRotation, Owner: owners.keyRotations},
			operationretention.WorkRegistration{Operation: workexecution.BackupPrune, Owner: owners.backupReports},
			operationretention.WorkRegistration{Operation: workexecution.Backup, Owner: owners.backupReports},
			operationretention.WorkRegistration{Operation: workexecution.Restore, Owner: owners.backupReports},
			operationretention.WorkRegistration{
				Operation: workexecution.ServiceRemoval,
				Owner:     owners.serviceRemovalAgent,
			},
			operationretention.WorkRegistration{
				Operation: workexecution.ServiceLifecycle,
				Owner:     owners.lifecycleAgent,
			},
			operationretention.WorkRegistration{
				Operation: workexecution.ConnectorRemoval,
				Owner:     owners.connectorRemoval,
			},
			operationretention.WorkRegistration{Operation: workexecution.SecretRemoval, Owner: owners.secretRemoval},
			operationretention.WorkRegistration{Operation: workexecution.ScriptRemoval, Owner: owners.scriptRemoval},
			operationretention.WorkRegistration{Operation: workexecution.Runner, Owner: dependencies.RunnerLifecycle},
			operationretention.WorkRegistration{Operation: workexecution.Detach, Owner: owners.detaches},
			operationretention.WorkRegistration{Operation: workexecution.Attach, Owner: owners.provisionReports},
			operationretention.WorkRegistration{Operation: workexecution.Network, Owner: owners.networkAgent},
			operationretention.WorkRegistration{Operation: workexecution.Volume, Owner: owners.volumeAgent},
			operationretention.WorkRegistration{
				Operation: workexecution.RemovalBackupObject,
				Owner:     hierarchyRetention,
			},
			operationretention.WorkRegistration{Operation: workexecution.Hierarchy, Owner: hierarchyRetention},
			operationretention.WorkRegistration{Operation: workexecution.Script, Owner: owners.scripts},
			{
				Operation: workexecution.Release,
				Owner:     dependencies.Releases,
			}, {Operation: workexecution.ReleaseRestoration, Owner: owners.directReleases.restoration},
			operationretention.WorkRegistration{Operation: workexecution.EntrySettlement, Owner: owners.entryWork},
			operationretention.WorkRegistration{Operation: workexecution.EntryConfiguration, Owner: owners.entryAgent},
			operationretention.WorkRegistration{Operation: workexecution.Component, Owner: componentRetention},
			operationretention.WorkRegistration{
				Operation: workexecution.ResourceDefinition,
				Owner:     owners.definitionWork,
			}}...)
	retention, err := operationretention.New(
		queue,
		owners.connections.connectorRecords,
		owners.connections.backingRecords,
		owners.connections.dnsRecords,
		owners.connections.edgeRecords,
		owners.runnerRecords,
		imageRecords,
		owners.hierarchyRecords,
		owners.networkRecords,
		owners.directReleases.retention,
		retentionOwners...)
	if err != nil {
		return nil, err
	}
	blueprintCompletion, err := blueprintcoordinator.NewCompletion(owners.blueprintLedger, dependencies.Blueprints)
	if err != nil {
		return nil, err
	}
	removalCompletion, err := hierarchydeletion.NewRemovalCompletion(
		owners.hierarchyRecords,
		owners.volumeAgent,
		owners.networkAgent,
	)
	if err != nil {
		return nil, err
	}
	routeCompletions, err := componentexecution.RouteCompletions(owners.componentRecords)
	if err != nil {
		return nil, err
	}
	attachCompletions, err := attachwork.DirectCompletions(owners.attachRecords)
	if err != nil {
		return nil, err
	}
	completions, err := workdispatch.NewCompletions(
		queue,
		append(
			append(
				append(
					append(
						append(append(softwareWork.completion, routeCompletions...), attachCompletions...),
						owners.directReleases.completion...),
					owners.provisioning.completion...),
				owners.resolver.completion...),
			[]workdispatch.CompletionRegistration{
				{
					Action:    workexecution.ActionCreate,
					Operation: workexecution.EntryConfiguration,
					Owner:     owners.entryAgent,
				},
				{
					Action:    workexecution.ActionUpdate,
					Operation: workexecution.EntryConfiguration,
					Owner:     owners.entryAgent,
				},
				{
					Action:    workexecution.ActionRemove,
					Operation: workexecution.EntryConfiguration,
					Owner:     owners.entryAgent,
				},
				{Action: workexecution.ActionRemove, Operation: workexecution.ReleaseGroup, Owner: owners.groupRemoval},
				{Action: workexecution.ActionCreate, Operation: workexecution.AgentLifecycle, Owner: owners.agentWork},
				{Action: workexecution.ActionRemove, Operation: workexecution.AgentLifecycle, Owner: owners.agentWork},
				{Action: workexecution.ActionUpdate, Operation: workexecution.AgentSoftware, Owner: owners.agentWork},
				{
					Action:    workexecution.ActionUpdate,
					Operation: workexecution.ControllerSoftware,
					Owner:     platform.controllerWork,
				},
				{
					Action:    workexecution.ActionPrepare,
					Operation: workexecution.SoftwarePreparation,
					Owner:     platform.preparations,
				},
				{
					Action:    workexecution.ActionApply,
					Operation: workexecution.EtcdConfiguration,
					Owner:     platform.EtcdConfig,
				},
				{
					Action:    workexecution.ActionRotate,
					Operation: workexecution.SecretRotation,
					Owner:     owners.keyRotations,
				},
				{Action: workexecution.ActionBackup, Operation: workexecution.Backup, Owner: owners.backupReports},
				{Action: workexecution.ActionRestore, Operation: workexecution.Restore, Owner: owners.backupReports},
				{
					Action:    workexecution.ActionBackupPrune,
					Operation: workexecution.BackupPrune,
					Owner:     owners.backupReports,
				},
				{
					Action:    workexecution.ActionRemove,
					Operation: workexecution.ServiceRemoval,
					Owner:     owners.serviceRemovalAgent,
				},
				{
					Action:    workexecution.ActionStart,
					Operation: workexecution.ServiceLifecycle,
					Owner:     owners.lifecycleAgent,
				},
				{
					Action:    workexecution.ActionStop,
					Operation: workexecution.ServiceLifecycle,
					Owner:     owners.lifecycleAgent,
				},
				{
					Action:    workexecution.ActionDestroy,
					Operation: workexecution.ServiceLifecycle,
					Owner:     owners.lifecycleAgent,
				},
				{Action: workexecution.ActionDetach, Owner: owners.detaches},
				{Action: workexecution.ActionCreate, Owner: owners.volumeAgent},
				{
					Action:    workexecution.ActionCreate,
					Operation: workexecution.Runner,
					Owner:     dependencies.RunnerLifecycle,
				},
				{
					Action:    workexecution.ActionRemove,
					Operation: workexecution.Runner,
					Owner:     dependencies.RunnerLifecycle,
				},
				{
					Action:    workexecution.ActionRemove,
					Operation: workexecution.ConnectorRemoval,
					Owner:     owners.connectorRemoval,
				},
				{
					Action:    workexecution.ActionRemove,
					Operation: workexecution.SecretRemoval,
					Owner:     owners.secretRemoval,
				},
				{
					Action:    workexecution.ActionRemove,
					Operation: workexecution.ScriptRemoval,
					Owner:     owners.scriptRemoval,
				},
				{Action: workexecution.ActionUpdate, Owner: owners.volumeAgent},
				{Action: workexecution.ActionScript, Owner: owners.scripts},
				{Action: workexecution.ActionApply, Owner: blueprintCompletion},
				{Action: workexecution.ActionCheck, Owner: workdispatch.CheckImageCompletion{}},
				{Action: workexecution.ActionFetch, Owner: workdispatch.CheckImageCompletion{}},
				{Action: workexecution.ActionRemove, Owner: removalCompletion},
			}...),
	)
	if err != nil {
		return nil, err
	}
	blueprintPublication, err := blueprintcoordinator.NewPublication(owners.blueprintLedger)
	if err != nil {
		return nil, err
	}
	hierarchyPublication, err := hierarchydeletion.NewWorkPublication(
		dependencies.Hierarchy,
		owners.hierarchyRecords,
		queue,
		owners.attachRemovals,
		maintenance.DiagnosticRetention,
	)
	if err != nil {
		return nil, err
	}
	removalPublication, err := workdispatch.NewRemovalPublication(hierarchyPublication, owners.networkAgent)
	if err != nil {
		return nil, err
	}
	publications, err := workdispatch.NewPublications(
		queue,
		append(owners.directReleases.publication, []workdispatch.PublicationRegistration{
			{Action: workexecution.ActionRemove, Owner: removalPublication},
			{Action: workexecution.ActionApply, Owner: blueprintPublication},
		}...),
	)
	if err != nil {
		return nil, err
	}
	serviceInputs, err := blueprint.NewServiceWorkPreparation(dependencies.Blueprints, owners.blueprintLedger, queue)
	if err != nil {
		return nil, err
	}
	volumeInputs, err := blueprint.NewVolumeWorkPreparation(
		dependencies.Blueprints,
		owners.blueprintLedger,
		queue,
		owners.volumeRecords,
	)
	if err != nil {
		return nil, err
	}
	networkInputs, err := blueprint.NewNetworkWorkPreparation(
		dependencies.Blueprints,
		owners.blueprintLedger,
		queue,
		owners.networkRecords,
	)
	if err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureAttachProvision(owners.provisionReports); err != nil {
		return nil, err
	}
	componentInputs, err := blueprint.NewComponentWorkPreparation(
		dependencies.Blueprints,
		owners.blueprintLedger,
		queue,
		owners.componentRecords,
	)
	if err != nil {
		return nil, err
	}
	componentReports, err := componentexecution.NewWorkReports(
		queue,
		owners.componentRecords,
		owners.blueprintLedger,
		dependencies.CapturedFiles,
		owners.diagnostics,
	)
	if err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureComponentWork(componentReports); err != nil {
		return nil, err
	}
	entryReports, err := entryexecution.NewWorkReports(
		queue,
		owners.entryRecords,
		dependencies.CapturedFiles,
		owners.diagnostics,
	)
	if err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureEntryConfigurationWork(entryReports); err != nil {
		return nil, err
	}
	blueprintInputs, err := blueprintcoordinator.NewInputPreparations(
		queue, owners.blueprintLedger, []workdispatch.InputRegistration{
			{
				Action:    workexecution.ActionApply,
				Operation: workexecution.ServiceRemoval,
				Owner:     owners.serviceRemovalPreparation,
			},
			{Action: workexecution.ActionApply, Operation: workexecution.Component, Owner: componentInputs},
			{Action: workexecution.ActionApply, Operation: workexecution.Release, Owner: serviceInputs},
			{Action: workexecution.ActionApply, Operation: workexecution.Volume, Owner: volumeInputs},
			{Action: workexecution.ActionApply, Operation: workexecution.Network, Owner: networkInputs},
			{Action: workexecution.ActionApply, Operation: workexecution.Attach, Owner: owners.attachPreparation},
		},
	)
	if err != nil {
		return nil, err
	}
	inputs, err := workdispatch.NewInputs(queue,
		append(append(softwareWork.input, owners.directReleases.input...), blueprintInputs...))
	if err != nil {
		return nil, err
	}
	runtime, err := workdispatch.NewRuntime(
		queue,
		scheduler,
		controller,
		completions,
		publications,
		inputs,
		retention,
		owners.diagnostics,
		platform.dependencies.Logger,
	)
	if err != nil {
		return nil, err
	}
	starts, err := workdispatch.NewAgentStarts(
		queue,
		append(append(owners.provisioning.starts, owners.resolver.starts...), []workdispatch.AgentStartRegistration{
			{Operation: workexecution.ReleaseRestoration, Owner: owners.directReleases.restoration},
			{Operation: workexecution.Backup, Owner: owners.backupReports},
			{Operation: workexecution.Restore, Owner: owners.backupReports},
			{Operation: workexecution.BackupPrune, Owner: owners.backupReports},
			{Operation: workexecution.ServiceRemoval, Owner: owners.serviceRemovalAgent},
			{Operation: workexecution.ServiceLifecycle, Owner: owners.lifecycleAgent},
			{Operation: workexecution.Attach, Owner: owners.attachInputs},
			{Operation: workexecution.EntryConfiguration, Owner: owners.entryAgent},
			{Operation: workexecution.Component, Owner: owners.componentAgent},
			{Operation: workexecution.Detach, Owner: owners.detaches},
			{Operation: workexecution.Network, Owner: owners.networkAgent},
			{Operation: workexecution.Volume, Owner: owners.volumeAgent},
			{Operation: workexecution.Hierarchy, Owner: owners.hierarchyAgent},
			{Operation: workexecution.Script, Owner: owners.scripts},
			{Operation: workexecution.Release, Owner: dependencies.Releases},
		}...),
	)
	if err != nil {
		return nil, err
	}
	checkpoints, err := platform.newReleaseCheckpoints(queue, dependencies, owners.directReleases, owners.diagnostics)
	if err != nil {
		return nil, err
	}
	deliveries, err := workdispatch.NewAgentDeliveries(
		queue,
		scheduler,
		dependencies.Releases,
		owners.scripts,
		owners.hierarchyAgent,
		owners.volumeAgent,
		owners.networkAgent,
		owners.detaches,
		owners.componentAgent,
		owners.entryAgent,
		owners.provisionReports,
		owners.lifecycleAgent,
		owners.serviceRemovalAgent,
		owners.backupReports,
		owners.provisioning.environment,
		owners.directReleases.restoration,
		owners.provisioning.backing,
		owners.resolver.owner,
	)
	if err != nil {
		return nil, err
	}
	removalReports, err := hierarchydeletion.NewReports(queue, owners.hierarchyAgent)
	if err != nil {
		return nil, err
	}
	attachReports, err := attachreports.New(queue, []attachreports.Registration{
		{Operation: workexecution.Hierarchy, Owner: owners.hierarchyAgent},
		{Operation: workexecution.Detach, Owner: owners.detaches},
	})
	if err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureWorkExecution(platform.dependencies.Agents, starts, checkpoints, owners.scripts, removalReports, deliveries); err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureVolumeWork(owners.volumeReports); err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureServiceLifecycle(owners.memberReports); err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureServiceRemovalRecovery(owners.serviceRemovalReports); err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureNetworkWork(owners.networkReports); err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureAttachWork(attachReports); err != nil {
		return nil, err
	}
	return &WorkExecution{Runtime: runtime, Queue: queue, scheduler: scheduler, controller: controller,
		Retries: retries, retention: retention,
		config: workdispatch.RuntimeConfig{
			Interval:            platform.dependencies.Tick,
			CollectionInterval:  maintenance.CleanupInterval,
			HistoryRetention:    workexecution.TaskHistoryRetention,
			DiagnosticRetention: maintenance.DiagnosticRetention,
			CollectionBudget:    maintenance.CleanupTimeBudget,
		}}, nil
}
