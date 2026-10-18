package hostplatform

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/controller/attachreports"
	"github.com/AlanD20/groundplane/internal/controller/attachwork"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	componentexecution "github.com/AlanD20/groundplane/internal/controller/componentwork"
	"github.com/AlanD20/groundplane/internal/controller/connectors"
	"github.com/AlanD20/groundplane/internal/controller/detachwork"
	entryexecution "github.com/AlanD20/groundplane/internal/controller/entrywork"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	scriptremovalwork "github.com/AlanD20/groundplane/internal/controller/scripts"
	"github.com/AlanD20/groundplane/internal/controller/scriptwork"
	"github.com/AlanD20/groundplane/internal/controller/secrets"
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/internal/controller/serviceremoval"
	"github.com/AlanD20/groundplane/internal/controller/taskcheckpoint"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
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
	softwareWork := platform.softwareWork()
	maintenance, err := platform.dependencies.Config.Maintenance.Policy()
	if err != nil {
		return nil, err
	}
	queue, err := workqueue.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	backupReports, keyRotations, err := platform.newBackupWork(queue, dependencies)
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
	connections, err := platform.newConnectionWork()
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
	componentDefinitions, err := blueprint.NewComponentDefinitionWork(dependencies.Blueprints, blueprintLedger, queue)
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
	agentWork, err := platform.newAgentWork(queue, diagnostics)
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
	lifecycleRecords, err := servicelifecyclework.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	lifecycleAgent, err := servicelifecycle.NewAgentWork(lifecycleRecords, queue)
	if err != nil {
		return nil, err
	}
	lifecycleDecisions, err := servicelifecycle.NewDecisionWork(lifecycleRecords)
	if err != nil {
		return nil, err
	}
	lifecycleReports, err := servicelifecycle.NewWorkReports(queue, lifecycleRecords, dependencies.AttachFacts)
	if err != nil {
		return nil, err
	}
	serviceRemovalInputs, err := serviceremovalwork.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	serviceRemovalRecords, err := etcd.NewServiceRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	serviceRemovalAgent, err := serviceremoval.NewAgentWork(serviceRemovalInputs, queue, serviceRemovalRecords)
	if err != nil {
		return nil, err
	}
	serviceRemovalDecision, err := serviceremoval.NewDecisionWork(serviceRemovalRecords, serviceRemovalInputs, queue)
	if err != nil {
		return nil, err
	}
	serviceRemovalPreparation, err := serviceremoval.NewInputPreparation(serviceRemovalRecords)
	if err != nil {
		return nil, err
	}
	serviceRemovalReports, err := serviceremoval.NewWorkReports(queue, serviceRemovalInputs, serviceRemovalRecords, dependencies.CapturedFiles)
	if err != nil {
		return nil, err
	}
	memberReports, err := workdispatch.NewLifecycleReportRouter(queue, lifecycleReports, serviceRemovalReports)
	if err != nil {
		return nil, err
	}
	networkRecords, err := networkwork.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	directReleases, err := platform.newDirectReleaseWork(queue, networkRecords, dependencies.Releases, scriptRecords, dependencies.ScriptArtifacts)
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
	componentRecords, err := componentwork.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	entryRecords, err := entrywork.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	entryAgent, err := entryexecution.NewAgentWork(entryRecords, queue)
	if err != nil {
		return nil, err
	}
	componentAgent, err := componentexecution.NewAgentWork(componentRecords, queue, blueprintLedger)
	if err != nil {
		return nil, err
	}
	attachRecords, err := attachprovision.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	attachPlanner, err := attachplan.NewProvisionPlanner(dependencies.AttachFacts)
	if err != nil {
		return nil, err
	}
	attachInputs, err := attachwork.NewInputs(attachRecords, attachPlanner)
	if err != nil {
		return nil, err
	}
	attachPreparation, err := attachwork.NewPreparation(attachInputs, queue, blueprintLedger)
	if err != nil {
		return nil, err
	}
	provisionReports, err := attachwork.NewReports(queue, attachInputs, dependencies.AttachFacts, blueprintLedger, diagnostics)
	if err != nil {
		return nil, err
	}
	scriptRemovalRecords, err := scriptremoval.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	scriptRemoval, err := scriptremovalwork.NewRemovalWork(scriptRemovalRecords, queue)
	if err != nil {
		return nil, err
	}
	secretRemovalRecords, err := secretremoval.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	secretRemoval, err := secrets.NewRemovalWork(secretRemovalRecords, queue)
	if err != nil {
		return nil, err
	}
	connectorRemovalRecords, err := connectorremoval.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	connectorRemoval, err := connectors.NewRemovalWork(connectorRemovalRecords, queue)
	if err != nil {
		return nil, err
	}
	environmentWork, err := platform.newEnvironmentWork(queue)
	if err != nil {
		return nil, err
	}
	groupRemoval, err := platform.newReleaseGroupWork(queue)
	if err != nil {
		return nil, err
	}
	scheduler, err := workdispatch.NewScheduler(queue, append(softwareWork.admission, []workdispatch.Registration{
		{Operation: workexecution.ReleaseGroup, Executor: workexecution.Controller, Owner: groupRemoval, DecisionOnly: true},
		{Operation: workexecution.Environment, Executor: workexecution.Agent, Owner: environmentWork},
		{Operation: workexecution.AgentLifecycle, Executor: workexecution.Controller, Owner: agentWork},
		{Operation: workexecution.AgentSoftware, Executor: workexecution.Controller, Owner: agentWork},
		{Operation: workexecution.ControllerSoftware, Executor: workexecution.Controller, Owner: platform.controllerWork},
		{Operation: workexecution.SoftwarePreparation, Executor: workexecution.Controller, Owner: platform.preparations},
		{Operation: workexecution.EtcdConfiguration, Executor: workexecution.Controller, Owner: platform.EtcdConfig},
		{Operation: workexecution.SecretRotation, Executor: workexecution.Controller, Owner: keyRotations, DecisionOnly: true},
		{Operation: workexecution.Backup, Executor: workexecution.Agent, Owner: backupReports},
		{Operation: workexecution.Restore, Executor: workexecution.Agent, Owner: backupReports},
		{Operation: workexecution.BackupPrune, Executor: workexecution.Agent, Owner: backupReports},
		{Operation: workexecution.ServiceRemoval, Executor: workexecution.Controller, Owner: serviceRemovalDecision, DecisionOnly: true},
		{Operation: workexecution.ServiceRemoval, Executor: workexecution.Agent, Owner: serviceRemovalAgent},
		{Operation: workexecution.ConnectorRemoval, Executor: workexecution.Controller, Owner: connectorRemoval, DecisionOnly: true},
		{Operation: workexecution.SecretRemoval, Executor: workexecution.Controller, Owner: secretRemoval, DecisionOnly: true},
		{Operation: workexecution.ServiceLifecycle, Executor: workexecution.Controller, Owner: lifecycleDecisions, DecisionOnly: true},
		{Operation: workexecution.ServiceLifecycle, Executor: workexecution.Agent, Owner: lifecycleAgent},
		{Operation: workexecution.ScriptRemoval, Executor: workexecution.Controller, Owner: scriptRemoval, DecisionOnly: true},
		{Operation: workexecution.Runner, Executor: workexecution.Controller, Owner: dependencies.RunnerLifecycle},
		{Operation: workexecution.Attach, Executor: workexecution.Agent, Owner: provisionReports},
		{Operation: workexecution.Attach, Executor: workexecution.Controller, Owner: provisionReports, DecisionOnly: true},
		{Operation: workexecution.EntryConfiguration, Executor: workexecution.Agent, Owner: entryAgent},
		{Operation: workexecution.Component, Executor: workexecution.Agent, Owner: componentAgent},
		{Operation: workexecution.Detach, Executor: workexecution.Agent, Owner: detaches},
		{Operation: workexecution.Detach, Executor: workexecution.Controller, Owner: detaches, DecisionOnly: true},
		{Operation: workexecution.Network, Executor: workexecution.Agent, Owner: networkAgent},
		{Operation: workexecution.Volume, Executor: workexecution.Agent, Owner: volumeAgent},
		{Operation: workexecution.RemovalBackupObject, Executor: workexecution.Controller, Owner: backupRemoval},
		{Operation: workexecution.Hierarchy, Executor: workexecution.Controller, Owner: hierarchyController, DecisionOnly: true},
		{Operation: workexecution.Hierarchy, Executor: workexecution.Agent, Owner: hierarchyAgent},
		{Operation: workexecution.Script, Executor: workexecution.Agent, Owner: scripts},
		{Operation: workexecution.Release, Executor: workexecution.Agent, Owner: dependencies.Releases},
		{Operation: workexecution.ReleaseRestoration, Executor: workexecution.Agent, Owner: directReleases.restoration},
		{Operation: workexecution.ResourceDefinition, Executor: workexecution.Controller, Owner: definitionWork, DecisionOnly: true},
		{Operation: workexecution.Component, Executor: workexecution.Controller, Owner: componentDefinitions, DecisionOnly: true},
		{Operation: workexecution.EntrySettlement, Executor: workexecution.Controller, Owner: entryWork, DecisionOnly: true},
		{Operation: workexecution.Image, Executor: workexecution.Controller, Owner: dependencies.Images},
		{Operation: workexecution.ConnectorCheck, Executor: workexecution.Controller, Owner: connections.connectors},
		{Operation: workexecution.BackingCheck, Executor: workexecution.Controller, Owner: connections.backings},
		{Operation: workexecution.DNSCheck, Executor: workexecution.Controller, Owner: connections.dns},
		{Operation: workexecution.EdgeCheck, Executor: workexecution.Controller, Owner: connections.edges},
		{Operation: workexecution.RunnerCheck, Executor: workexecution.Controller, Owner: dependencies.RunnerChecks},
	}...))
	if err != nil {
		return nil, err
	}
	controller, err := workdispatch.NewControllerExecutor(queue, append(softwareWork.controller, []workdispatch.ControllerRegistration{
		{Operation: workexecution.AgentLifecycle, Owner: agentWork},
		{Operation: workexecution.AgentSoftware, Owner: agentWork},
		{Operation: workexecution.ControllerSoftware, Owner: platform.controllerWork},
		{Operation: workexecution.SoftwarePreparation, Owner: platform.preparations},
		{Operation: workexecution.EtcdConfiguration, Owner: platform.EtcdConfig},
		{Operation: workexecution.Runner, Owner: dependencies.RunnerLifecycle},
		{Operation: workexecution.RemovalBackupObject, Owner: backupRemoval},
		{Operation: workexecution.Image, Owner: dependencies.Images},
		{Operation: workexecution.ConnectorCheck, Owner: connections.connectors},
		{Operation: workexecution.BackingCheck, Owner: connections.backings},
		{Operation: workexecution.DNSCheck, Owner: connections.dns},
		{Operation: workexecution.EdgeCheck, Owner: connections.edges},
		{Operation: workexecution.RunnerCheck, Owner: dependencies.RunnerChecks},
	}...))
	if err != nil {
		return nil, err
	}
	retries, err := workdispatch.NewRetries(queue, append(softwareWork.retry, []workdispatch.RetryRegistration{
		{Operation: workexecution.Environment, Owner: environmentWork},
		{Operation: workexecution.AgentLifecycle, Owner: agentWork},
		{Operation: workexecution.AgentSoftware, Owner: agentWork},
		{Operation: workexecution.ControllerSoftware, Owner: platform.controllerWork},
		{Operation: workexecution.SoftwarePreparation, Owner: platform.preparations},
		{Operation: workexecution.EtcdConfiguration, Owner: platform.EtcdConfig},
		{Operation: workexecution.Backup, Owner: backupReports},
		{Operation: workexecution.Restore, Owner: backupReports},
		{Operation: workexecution.BackupPrune, Owner: backupReports},
		{Operation: workexecution.ServiceRemoval, Owner: serviceRemovalAgent},
		{Operation: workexecution.ServiceLifecycle, Owner: lifecycleAgent},
		{Operation: workexecution.Runner, Owner: dependencies.RunnerLifecycle},
		{Operation: workexecution.Attach, Owner: provisionReports},
		{Operation: workexecution.EntryConfiguration, Owner: entryAgent},
		{Operation: workexecution.Component, Owner: componentAgent},
		{Operation: workexecution.Detach, Owner: detaches},
		{Operation: workexecution.Network, Owner: networkAgent},
		{Operation: workexecution.Volume, Owner: volumeAgent},
		{Operation: workexecution.RemovalBackupObject, Owner: backupRemoval},
		{Operation: workexecution.Hierarchy, Owner: hierarchyAgent},
		{Operation: workexecution.Script, Owner: scripts},
		{Operation: workexecution.Release, Owner: dependencies.Releases},
		{Operation: workexecution.ConnectorCheck, Owner: connections.connectors},
		{Operation: workexecution.BackingCheck, Owner: connections.backings},
		{Operation: workexecution.EdgeCheck, Owner: connections.edges},
	}...))
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
	componentRetention, err := componentexecution.NewRetention(componentRecords, componentDefinitions, dependencies.Releases)
	if err != nil {
		return nil, err
	}
	retentionOwners := append(softwareWork.retention, []operationretention.WorkRegistration{
		{Operation: workexecution.Environment, Owner: environmentWork},
		{Operation: workexecution.ReleaseGroup, Owner: groupRemoval},
		operationretention.WorkRegistration{Operation: workexecution.AgentLifecycle, Owner: agentWork},
		operationretention.WorkRegistration{Operation: workexecution.AgentSoftware, Owner: agentWork},
		operationretention.WorkRegistration{Operation: workexecution.ControllerSoftware, Owner: platform.controllerWork},
		operationretention.WorkRegistration{Operation: workexecution.SoftwarePreparation, Owner: platform.preparations},
		operationretention.WorkRegistration{Operation: workexecution.EtcdConfiguration, Owner: platform.EtcdConfig},
		operationretention.WorkRegistration{Operation: workexecution.SecretRotation, Owner: keyRotations},
		operationretention.WorkRegistration{Operation: workexecution.BackupPrune, Owner: backupReports},
		operationretention.WorkRegistration{Operation: workexecution.Backup, Owner: backupReports},
		operationretention.WorkRegistration{Operation: workexecution.Restore, Owner: backupReports},
		operationretention.WorkRegistration{Operation: workexecution.ServiceRemoval, Owner: serviceRemovalAgent},
		operationretention.WorkRegistration{Operation: workexecution.ServiceLifecycle, Owner: lifecycleAgent},
		operationretention.WorkRegistration{Operation: workexecution.ConnectorRemoval, Owner: connectorRemoval},
		operationretention.WorkRegistration{Operation: workexecution.SecretRemoval, Owner: secretRemoval},
		operationretention.WorkRegistration{Operation: workexecution.ScriptRemoval, Owner: scriptRemoval},
		operationretention.WorkRegistration{Operation: workexecution.Runner, Owner: dependencies.RunnerLifecycle},
		operationretention.WorkRegistration{Operation: workexecution.Detach, Owner: detaches},
		operationretention.WorkRegistration{Operation: workexecution.Attach, Owner: provisionReports},
		operationretention.WorkRegistration{Operation: workexecution.Network, Owner: networkAgent},
		operationretention.WorkRegistration{Operation: workexecution.Volume, Owner: volumeAgent},
		operationretention.WorkRegistration{Operation: workexecution.RemovalBackupObject, Owner: hierarchyRetention},
		operationretention.WorkRegistration{Operation: workexecution.Hierarchy, Owner: hierarchyRetention},
		operationretention.WorkRegistration{Operation: workexecution.Script, Owner: scripts},
		operationretention.WorkRegistration{Operation: workexecution.Release, Owner: dependencies.Releases},
		operationretention.WorkRegistration{Operation: workexecution.EntrySettlement, Owner: entryWork},
		operationretention.WorkRegistration{Operation: workexecution.EntryConfiguration, Owner: entryAgent},
		operationretention.WorkRegistration{Operation: workexecution.Component, Owner: componentRetention},
		operationretention.WorkRegistration{Operation: workexecution.ResourceDefinition, Owner: definitionWork}}...)
	retention, err := operationretention.New(queue, connections.connectorRecords, connections.backingRecords, connections.dnsRecords, connections.edgeRecords, runnerRecords, imageRecords, hierarchyRecords, networkRecords, retentionOwners...)
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
	completions, err := workdispatch.NewCompletions(queue, append(append(softwareWork.completion, directReleases.completion...), []workdispatch.CompletionRegistration{
		{Action: workexecution.ActionCreate, Operation: workexecution.Environment, Owner: environmentWork},
		{Action: workexecution.ActionRemove, Operation: workexecution.ReleaseGroup, Owner: groupRemoval},
		{Action: workexecution.ActionCreate, Operation: workexecution.AgentLifecycle, Owner: agentWork},
		{Action: workexecution.ActionRemove, Operation: workexecution.AgentLifecycle, Owner: agentWork},
		{Action: workexecution.ActionUpdate, Operation: workexecution.AgentSoftware, Owner: agentWork},
		{Action: workexecution.ActionUpdate, Operation: workexecution.ControllerSoftware, Owner: platform.controllerWork},
		{Action: workexecution.ActionPrepare, Operation: workexecution.SoftwarePreparation, Owner: platform.preparations},
		{Action: workexecution.ActionApply, Operation: workexecution.EtcdConfiguration, Owner: platform.EtcdConfig},
		{Action: workexecution.ActionRotate, Operation: workexecution.SecretRotation, Owner: keyRotations},
		{Action: workexecution.ActionBackup, Operation: workexecution.Backup, Owner: backupReports},
		{Action: workexecution.ActionRestore, Operation: workexecution.Restore, Owner: backupReports},
		{Action: workexecution.ActionBackupPrune, Operation: workexecution.BackupPrune, Owner: backupReports},
		{Action: workexecution.ActionRemove, Operation: workexecution.ServiceRemoval, Owner: serviceRemovalAgent},
		{Action: workexecution.ActionStart, Operation: workexecution.ServiceLifecycle, Owner: lifecycleAgent},
		{Action: workexecution.ActionStop, Operation: workexecution.ServiceLifecycle, Owner: lifecycleAgent},
		{Action: workexecution.ActionDestroy, Operation: workexecution.ServiceLifecycle, Owner: lifecycleAgent},
		{Action: workexecution.ActionDetach, Owner: detaches},
		{Action: workexecution.ActionCreate, Owner: volumeAgent},
		{Action: workexecution.ActionCreate, Operation: workexecution.Runner, Owner: dependencies.RunnerLifecycle},
		{Action: workexecution.ActionRemove, Operation: workexecution.Runner, Owner: dependencies.RunnerLifecycle},
		{Action: workexecution.ActionRemove, Operation: workexecution.ConnectorRemoval, Owner: connectorRemoval},
		{Action: workexecution.ActionRemove, Operation: workexecution.SecretRemoval, Owner: secretRemoval},
		{Action: workexecution.ActionRemove, Operation: workexecution.ScriptRemoval, Owner: scriptRemoval},
		{Action: workexecution.ActionUpdate, Owner: volumeAgent},
		{Action: workexecution.ActionScript, Owner: scripts},
		{Action: workexecution.ActionApply, Owner: blueprintCompletion},
		{Action: workexecution.ActionCheck, Owner: workdispatch.CheckImageCompletion{}},
		{Action: workexecution.ActionFetch, Owner: workdispatch.CheckImageCompletion{}},
		{Action: workexecution.ActionRemove, Owner: removalCompletion},
	}...))
	if err != nil {
		return nil, err
	}
	blueprintPublication, err := blueprintcoordinator.NewPublication(blueprintLedger)
	if err != nil {
		return nil, err
	}
	hierarchyPublication, err := hierarchydeletion.NewWorkPublication(dependencies.Hierarchy, hierarchyRecords, queue, attachRemovals, maintenance.DiagnosticRetention)
	if err != nil {
		return nil, err
	}
	removalPublication, err := workdispatch.NewRemovalPublication(hierarchyPublication, networkAgent)
	if err != nil {
		return nil, err
	}
	publications, err := workdispatch.NewPublications(queue, append(directReleases.publication, []workdispatch.PublicationRegistration{
		{Action: workexecution.ActionRemove, Owner: removalPublication},
		{Action: workexecution.ActionApply, Owner: blueprintPublication},
	}...))
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
	if err := platform.dependencies.Channel.ConfigureAttachProvision(provisionReports); err != nil {
		return nil, err
	}
	componentInputs, err := blueprint.NewComponentWorkPreparation(dependencies.Blueprints, blueprintLedger, queue, componentRecords)
	if err != nil {
		return nil, err
	}
	componentReports, err := componentexecution.NewWorkReports(queue, componentRecords, blueprintLedger, dependencies.CapturedFiles, diagnostics)
	if err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureComponentWork(componentReports); err != nil {
		return nil, err
	}
	entryReports, err := entryexecution.NewWorkReports(queue, entryRecords, dependencies.CapturedFiles, diagnostics)
	if err != nil {
		return nil, err
	}
	if err := platform.dependencies.Channel.ConfigureEntryConfigurationWork(entryReports); err != nil {
		return nil, err
	}
	inputs, err := workdispatch.NewInputs(queue, append(append(softwareWork.input, directReleases.input...), []workdispatch.InputRegistration{
		{Action: workexecution.ActionApply, Operation: workexecution.ServiceRemoval, Owner: serviceRemovalPreparation},
		{Action: workexecution.ActionApply, Operation: workexecution.Component, Owner: componentInputs},
		{Action: workexecution.ActionApply, Operation: workexecution.Release, Owner: serviceInputs},
		{Action: workexecution.ActionApply, Operation: workexecution.Volume, Owner: volumeInputs},
		{Action: workexecution.ActionApply, Operation: workexecution.Network, Owner: networkInputs},
		{Action: workexecution.ActionApply, Operation: workexecution.Attach, Owner: attachPreparation},
	}...))
	if err != nil {
		return nil, err
	}
	runtime, err := workdispatch.NewRuntime(queue, scheduler, controller, completions, publications, inputs, retention, replayRetention, platform.dependencies.Logger)
	if err != nil {
		return nil, err
	}
	starts, err := workdispatch.NewAgentStarts(queue, []workdispatch.AgentStartRegistration{
		{Operation: workexecution.ReleaseRestoration, Owner: directReleases.restoration},
		{Operation: workexecution.Environment, Owner: environmentWork},
		{Operation: workexecution.Backup, Owner: backupReports},
		{Operation: workexecution.Restore, Owner: backupReports},
		{Operation: workexecution.BackupPrune, Owner: backupReports},
		{Operation: workexecution.ServiceRemoval, Owner: serviceRemovalAgent},
		{Operation: workexecution.ServiceLifecycle, Owner: lifecycleAgent},
		{Operation: workexecution.Attach, Owner: attachInputs},
		{Operation: workexecution.EntryConfiguration, Owner: entryAgent},
		{Operation: workexecution.Component, Owner: componentAgent},
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
	checkpoints, err := platform.newReleaseCheckpoints(queue, dependencies, directReleases, diagnostics)
	if err != nil {
		return nil, err
	}
	deliveries, err := workdispatch.NewAgentDeliveries(queue, scheduler, dependencies.Releases, scripts, hierarchyAgent, volumeAgent, networkAgent, detaches, componentAgent, entryAgent, provisionReports, lifecycleAgent, serviceRemovalAgent, backupReports, environmentWork, directReleases.restoration)
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
	platform.dependencies.Channel.ConfigureServiceLifecycle(memberReports)
	platform.dependencies.Channel.ConfigureServiceRemovalRecovery(serviceRemovalReports)
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
