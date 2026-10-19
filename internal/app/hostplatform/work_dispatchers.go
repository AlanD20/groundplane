package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// WorkExecution composes the native scheduling, operation and retention owners.
// The daemon starts one execution runtime, not parallel Task engines.
type executionDispatchers struct {
	scheduler  *workdispatch.Scheduler
	controller *workdispatch.ControllerExecutor
	retries    *workdispatch.Retries
}

func (owners *workOwners) newDispatchers(
	platform *Platform,
	dependencies ExecutionDependencies,
	queue *workqueue.Repository,
	softwareWork softwareWorkRegistrations,
) (executionDispatchers, error) {
	scheduler, err := workdispatch.NewScheduler(
		queue,
		append(
			append(
				append(
					append(softwareWork.admission, owners.directReleases.admission...),
					owners.provisioning.admission...),
				owners.resolver.admission...),
			[]workdispatch.Registration{
				{
					Operation:    workexecution.ReleaseGroup,
					Executor:     workexecution.Controller,
					Owner:        owners.groupRemoval,
					DecisionOnly: true,
				},
				{Operation: workexecution.AgentLifecycle, Executor: workexecution.Controller, Owner: owners.agentWork},
				{Operation: workexecution.AgentSoftware, Executor: workexecution.Controller, Owner: owners.agentWork},
				{
					Operation: workexecution.ControllerSoftware,
					Executor:  workexecution.Controller,
					Owner:     platform.controllerWork,
				},
				{
					Operation: workexecution.SoftwarePreparation,
					Executor:  workexecution.Controller,
					Owner:     platform.preparations,
				},
				{
					Operation: workexecution.EtcdConfiguration,
					Executor:  workexecution.Controller,
					Owner:     platform.EtcdConfig,
				},
				{
					Operation:    workexecution.SecretRotation,
					Executor:     workexecution.Controller,
					Owner:        owners.keyRotations,
					DecisionOnly: true,
				},
				{Operation: workexecution.Backup, Executor: workexecution.Agent, Owner: owners.backupReports},
				{Operation: workexecution.Restore, Executor: workexecution.Agent, Owner: owners.backupReports},
				{Operation: workexecution.BackupPrune, Executor: workexecution.Agent, Owner: owners.backupReports},
				{
					Operation:    workexecution.ServiceRemoval,
					Executor:     workexecution.Controller,
					Owner:        owners.serviceRemovalDecision,
					DecisionOnly: true,
				},
				{
					Operation: workexecution.ServiceRemoval,
					Executor:  workexecution.Agent,
					Owner:     owners.serviceRemovalAgent,
				},
				{
					Operation:    workexecution.ConnectorRemoval,
					Executor:     workexecution.Controller,
					Owner:        owners.connectorRemoval,
					DecisionOnly: true,
				},
				{
					Operation:    workexecution.SecretRemoval,
					Executor:     workexecution.Controller,
					Owner:        owners.secretRemoval,
					DecisionOnly: true,
				},
				{
					Operation:    workexecution.ServiceLifecycle,
					Executor:     workexecution.Controller,
					Owner:        owners.lifecycleDecisions,
					DecisionOnly: true,
				},
				{
					Operation: workexecution.ServiceLifecycle,
					Executor:  workexecution.Agent,
					Owner:     owners.lifecycleAgent,
				},
				{
					Operation:    workexecution.ScriptRemoval,
					Executor:     workexecution.Controller,
					Owner:        owners.scriptRemoval,
					DecisionOnly: true,
				},
				{
					Operation: workexecution.Runner,
					Executor:  workexecution.Controller,
					Owner:     dependencies.RunnerLifecycle,
				},
				{Operation: workexecution.Attach, Executor: workexecution.Agent, Owner: owners.provisionReports},
				{
					Operation:    workexecution.Attach,
					Executor:     workexecution.Controller,
					Owner:        owners.provisionReports,
					DecisionOnly: true,
				},
				{Operation: workexecution.EntryConfiguration, Executor: workexecution.Agent, Owner: owners.entryAgent},
				{Operation: workexecution.Component, Executor: workexecution.Agent, Owner: owners.componentAgent},
				{Operation: workexecution.Detach, Executor: workexecution.Agent, Owner: owners.detaches},
				{
					Operation:    workexecution.Detach,
					Executor:     workexecution.Controller,
					Owner:        owners.detaches,
					DecisionOnly: true,
				},
				{Operation: workexecution.Network, Executor: workexecution.Agent, Owner: owners.networkAgent},
				{Operation: workexecution.Volume, Executor: workexecution.Agent, Owner: owners.volumeAgent},
				{
					Operation: workexecution.RemovalBackupObject,
					Executor:  workexecution.Controller,
					Owner:     owners.backupRemoval,
				},
				{
					Operation:    workexecution.Hierarchy,
					Executor:     workexecution.Controller,
					Owner:        owners.hierarchyController,
					DecisionOnly: true,
				},
				{Operation: workexecution.Hierarchy, Executor: workexecution.Agent, Owner: owners.hierarchyAgent},
				{Operation: workexecution.Script, Executor: workexecution.Agent, Owner: owners.scripts},
				{Operation: workexecution.Release, Executor: workexecution.Agent, Owner: dependencies.Releases},
				{
					Operation:    workexecution.ResourceDefinition,
					Executor:     workexecution.Controller,
					Owner:        owners.definitionWork,
					DecisionOnly: true,
				},
				{
					Operation:    workexecution.Component,
					Executor:     workexecution.Controller,
					Owner:        owners.componentDefinitions,
					DecisionOnly: true,
				},
				{
					Operation:    workexecution.EntrySettlement,
					Executor:     workexecution.Controller,
					Owner:        owners.entryWork,
					DecisionOnly: true,
				},
				{Operation: workexecution.Image, Executor: workexecution.Controller, Owner: dependencies.Images},
				{
					Operation: workexecution.ConnectorCheck,
					Executor:  workexecution.Controller,
					Owner:     owners.connections.connectors,
				},
				{
					Operation: workexecution.BackingCheck,
					Executor:  workexecution.Controller,
					Owner:     owners.connections.backings,
				},
				{Operation: workexecution.DNSCheck, Executor: workexecution.Controller, Owner: owners.connections.dns},
				{
					Operation: workexecution.EdgeCheck,
					Executor:  workexecution.Controller,
					Owner:     owners.connections.edges,
				},
				{
					Operation: workexecution.RunnerCheck,
					Executor:  workexecution.Controller,
					Owner:     dependencies.RunnerChecks,
				},
			}...),
	)
	if err != nil {
		return executionDispatchers{}, err
	}
	controller, err := workdispatch.NewControllerExecutor(
		queue,
		append(softwareWork.controller, []workdispatch.ControllerRegistration{
			{Operation: workexecution.AgentLifecycle, Owner: owners.agentWork},
			{Operation: workexecution.AgentSoftware, Owner: owners.agentWork},
			{Operation: workexecution.ControllerSoftware, Owner: platform.controllerWork},
			{Operation: workexecution.SoftwarePreparation, Owner: platform.preparations},
			{Operation: workexecution.EtcdConfiguration, Owner: platform.EtcdConfig},
			{Operation: workexecution.Runner, Owner: dependencies.RunnerLifecycle},
			{Operation: workexecution.RemovalBackupObject, Owner: owners.backupRemoval},
			{Operation: workexecution.Image, Owner: dependencies.Images},
			{Operation: workexecution.ConnectorCheck, Owner: owners.connections.connectors},
			{Operation: workexecution.BackingCheck, Owner: owners.connections.backings},
			{Operation: workexecution.DNSCheck, Owner: owners.connections.dns},
			{Operation: workexecution.EdgeCheck, Owner: owners.connections.edges},
			{Operation: workexecution.RunnerCheck, Owner: dependencies.RunnerChecks},
		}...),
	)
	if err != nil {
		return executionDispatchers{}, err
	}
	retries, err := workdispatch.NewRetries(
		queue,
		append(
			append(append(softwareWork.retry, owners.provisioning.retry...), owners.resolver.retry...),
			[]workdispatch.RetryRegistration{
				{Operation: workexecution.Image, Owner: dependencies.Images},
				{Operation: workexecution.ReleaseRestoration, Owner: owners.directReleases.restoration},
				{Operation: workexecution.AgentLifecycle, Owner: owners.agentWork},
				{Operation: workexecution.AgentSoftware, Owner: owners.agentWork},
				{Operation: workexecution.ControllerSoftware, Owner: platform.controllerWork},
				{Operation: workexecution.SoftwarePreparation, Owner: platform.preparations},
				{Operation: workexecution.EtcdConfiguration, Owner: platform.EtcdConfig},
				{Operation: workexecution.Backup, Owner: owners.backupReports},
				{Operation: workexecution.Restore, Owner: owners.backupReports},
				{Operation: workexecution.BackupPrune, Owner: owners.backupReports},
				{Operation: workexecution.ServiceRemoval, Owner: owners.serviceRemovalAgent},
				{Operation: workexecution.ServiceLifecycle, Owner: owners.lifecycleAgent},
				{Operation: workexecution.Runner, Owner: dependencies.RunnerLifecycle},
				{Operation: workexecution.Attach, Owner: owners.provisionReports},
				{Operation: workexecution.EntryConfiguration, Owner: owners.entryAgent},
				{Operation: workexecution.Component, Owner: owners.componentAgent},
				{Operation: workexecution.Detach, Owner: owners.detaches},
				{Operation: workexecution.Network, Owner: owners.networkAgent},
				{Operation: workexecution.Volume, Owner: owners.volumeAgent},
				{Operation: workexecution.RemovalBackupObject, Owner: owners.backupRemoval},
				{Operation: workexecution.Hierarchy, Owner: owners.hierarchyAgent},
				{Operation: workexecution.Script, Owner: owners.scripts},
				{Operation: workexecution.Release, Owner: dependencies.Releases},
				{Operation: workexecution.ConnectorCheck, Owner: owners.connections.connectors},
				{Operation: workexecution.BackingCheck, Owner: owners.connections.backings},
				{Operation: workexecution.EdgeCheck, Owner: owners.connections.edges},
			}...),
	)
	if err != nil {
		return executionDispatchers{}, err
	}

	return executionDispatchers{scheduler: scheduler, controller: controller, retries: retries}, nil
}
