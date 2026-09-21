package app

import (
	"context"
	"fmt"
	"time"

	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/scheduler"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type controllerStaleTaskMaintenance interface {
	ExpireStaleAgentTasks(context.Context, time.Time) (int, error)
}

type controllerHTTPDependencies struct {
	bootstrap               controllerBootstrapComposition
	store                   etcd.EnvironmentBlueprintStore
	authority               controllerAuthorityComposition
	dataServices            controllerDataComposition
	execution               controllerExecutionComposition
	platform                *controllerPlatform
	agentRuntime            *channeltransport.Runtime
	runner                  *controllerRunnerComposition
	backup                  *controllerBackupComposition
	hierarchy               *controllerHierarchyMutations
	connectors              *controllerConnectorComposition
	backingServiceMutations handlers.BackingServiceMutator
	componentReads          handlers.ComponentReader
	componentMutations      handlers.ComponentMutator
	network                 *networkcontroller.Service
	serviceMutations        handlers.ServiceMutator
	releaseGroupMutations   handlers.ReleaseGroupMutator
	releaseOperations       handlers.ReleaseOperator
	scriptMutations         handlers.ScriptMutator
	entryMutations          handlers.EntryMutator
	secretMutations         handlers.SecretMutator
	secretDeletions         handlers.SecretDeleter
	volumeMutations         handlers.VolumeMutator
	environmentBlueprints   handlers.EnvironmentBlueprintService
	hierarchyDeletions      handlers.HierarchyDeletionService
	attachMutations         *attachments.MutationService
	taskMutations           handlers.TaskRetrier
	taskAborts              handlers.TaskAborter
	controllerTasks         *controllertask.Runner
	staleTasks              controllerStaleTaskMaintenance
}

func newControllerHTTPComposition(dependencies controllerHTTPDependencies) (*Controller, error) {
	serviceReads, err := newServiceReadResources(
		dependencies.authority.hierarchyRecords,
		dependencies.dataServices.serviceRecords,
		dependencies.dataServices.zoneRecords,
		dependencies.authority.releaseLedger,
		dependencies.authority.agents,
		dependencies.agentRuntime.Registry,
	)
	if err != nil {
		// Preserve the initialization error; cleanup is best-effort.
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Service reads: %w", err)
	}
	server := handlers.New(dependencies.store, dependencies.bootstrap.logger, handlers.Options{
		Host: dependencies.platform.host, ControllerConfig: dependencies.bootstrap.controllerConfig,
		ControllerUpdates: dependencies.platform.upgrades,
		OnHTTPReady:       dependencies.platform.readiness.MarkHTTPReady, MutationAdmission: dependencies.platform.upgrades,
		Agents: dependencies.platform.reads, AgentMutations: dependencies.platform.mutations,
		Tenants:                 dependencies.authority.hierarchyService,
		Projects:                dependencies.authority.hierarchyService,
		ProjectMutations:        dependencies.hierarchy.projectMutations,
		ProjectChanges:          dependencies.hierarchy.projectChanges,
		BackingServices:         dependencies.dataServices.backingServiceReads,
		BackingServiceMutations: dependencies.backingServiceMutations,
		Components:              dependencies.componentReads,
		ComponentMutations:      dependencies.componentMutations,
		Environments:            serviceReads.environments,
		Services:                serviceReads.services,
		ServiceObservations:     serviceReads.observations,
		ServiceMutations:        dependencies.serviceMutations,
		Zones:                   dependencies.network,
		ZoneMutations:           dependencies.network,
		Routes:                  dependencies.network,
		RouteMutations:          dependencies.network,
		ReleaseGroups:           dependencies.authority.releaseGroups,
		ReleaseGroupMutations:   dependencies.releaseGroupMutations,
		Releases:                dependencies.authority.releaseLedger,
		ReleaseOperations:       dependencies.releaseOperations,
		Scripts:                 dependencies.dataServices.scriptReads,
		ScriptMutations:         dependencies.scriptMutations,
		Entries:                 dependencies.dataServices.entryReads,
		EntryMutations:          dependencies.entryMutations,
		Secrets:                 dependencies.dataServices.secretReads,
		SecretMutations:         dependencies.secretMutations,
		SecretDeletions:         dependencies.secretDeletions,
		Connectors:              dependencies.dataServices.connectorReads,
		ConnectorMutations:      dependencies.connectors.mutations,
		ConnectorDeletions:      dependencies.connectors.deletions,
		Runners:                 dependencies.authority.runnerRecords,
		RunnerProvisioning:      dependencies.runner.provisioning,
		RunnerMutations:         dependencies.runner.mutations,
		RunnerRemovals:          dependencies.runner.removals,
		BackupPolicies:          dependencies.backup.policies,
		BackupPolicyMutations:   dependencies.backup.policies,
		RecoveryPoints:          dependencies.backup.points,
		BackupRuns:              dependencies.backup.runs,
		BackupKeyMutations:      dependencies.backup.keys,
		BackupKeyExports:        dependencies.backup.keys,
		Volumes:                 dependencies.dataServices.volumeReads,
		VolumeMutations:         dependencies.volumeMutations,
		EnvironmentMutations:    dependencies.hierarchy.environmentMutations,
		EnvironmentChanges:      dependencies.hierarchy.environmentChanges,
		EnvironmentBlueprints:   dependencies.environmentBlueprints,
		HierarchyDeletions:      dependencies.hierarchyDeletions,
		AttachMutations:         dependencies.attachMutations,
		AttachFacts:             dependencies.execution.attachFactReads,
		TaskMutations:           dependencies.taskMutations,
		TaskAborts:              dependencies.taskAborts,
		ControllerTaskWake:      dependencies.controllerTasks.Wake,
		AgentTaskWake:           dependencies.agentRuntime.Registry.WakeTaskDispatch,
		TenantMutations:         dependencies.hierarchy.tenantMutations,
		TenantChanges:           dependencies.hierarchy.tenantChanges,
		Console:                 dependencies.bootstrap.consoleAssets,
		Tasks:                   dependencies.authority.tasks,
		Logs:                    serviceReads.logs,
	})
	return &Controller{
		Config: dependencies.bootstrap.config,
		Logger: dependencies.bootstrap.logger,
		server: server,
		agent:  dependencies.agentRuntime,
		scheduler: scheduler.New(
			dependencies.bootstrap.logger,
			dependencies.platform.upgrades,
			dependencies.bootstrap.tick,
			dependencies.authority.tasks,
			dependencies.authority.idempotency,
			dependencies.staleTasks,
			dependencies.backup.schedules,
		),
		controllerTasks: dependencies.controllerTasks,
		localAgent:      dependencies.platform.reconciliation,
		attachMutations: dependencies.attachMutations,
		container:       dependencies.platform,
		etcdContainer:   dependencies.bootstrap.etcdLifecycle,
		store:           dependencies.store,
	}, nil
}
