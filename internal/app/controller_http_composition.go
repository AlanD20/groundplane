package app

import (
	"context"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/internal/app/hostplatform"
	"github.com/AlanD20/groundplane/internal/app/softwareplatform"
	commonrunner "github.com/AlanD20/groundplane/internal/common/runner"
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/controller/environmentdns"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	"github.com/AlanD20/groundplane/internal/controller/hostterminal"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/scheduler"
	taskoperations "github.com/AlanD20/groundplane/internal/controller/tasks"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type controllerStaleTaskMaintenance interface {
	ExpireStaleAgentTasks(context.Context, time.Time) (int, error)
}

type controllerBlueprintService interface {
	handlers.EnvironmentBlueprintService
	blueprintcoordinator.Planner
	blueprintcoordinator.ChildPreparer
}

type controllerHTTPDependencies struct {
	bootstrap               controllerBootstrapComposition
	store                   etcd.EnvironmentBlueprintStore
	authority               controllerAuthorityComposition
	dataServices            controllerDataComposition
	execution               controllerExecutionComposition
	platform                *hostplatform.Platform
	software                *softwareplatform.Composition
	images                  *imagedelivery.Service
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
	environmentBlueprints   controllerBlueprintService
	hierarchyDeletions      handlers.HierarchyDeletionService
	attachMutations         *attachments.MutationService
	taskMutations           handlers.TaskRetrier
	controllerTasks         *controllertask.Runner
	staleTasks              controllerStaleTaskMaintenance
}

func newControllerHTTPComposition(dependencies controllerHTTPDependencies) (*Controller, error) {
	environmentDNS, err := environmentdns.NewService(dependencies.authority.environmentBlueprintRecords,
		dependencies.authority.idempotency, dependencies.authority.intentCoordinator)
	if err != nil {
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Environment DNS: %w", err)
	}
	edgeChecks, err := newControllerEdgeCheckService(dependencies)
	if err != nil {
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Tunnel connection checks: %w", err)
	}
	dnsChecks, err := newControllerDNSCheckService(dependencies)
	if err != nil {
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize DNS connection checks: %w", err)
	}
	backingChecks, err := newControllerBackingCheckService(dependencies)
	if err != nil {
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Backing access checks: %w", err)
	}
	blueprintCoordinator, err := newBlueprintCoordinatorRuntime(
		dependencies.store, dependencies.authority.tasks,
		dependencies.environmentBlueprints, dependencies.environmentBlueprints,
		dependencies.agentRuntime.Registry, dependencies.bootstrap.tick, dependencies.bootstrap.logger,
	)
	if err != nil {
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Blueprint coordinator: %w", err)
	}
	terminalInactivity, err := dependencies.bootstrap.config.ParsedHostTerminalInputInactivity()
	if err != nil {
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize host terminal policy: %w", err)
	}
	hostTerminals, err := hostterminal.New(commonrunner.New(dependencies.bootstrap.logger), terminalInactivity)
	if err != nil {
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize host terminal service: %w", err)
	}
	taskAborts, err := taskoperations.NewAbortService(
		dependencies.authority.tasks, dependencies.agentRuntime.Registry, dependencies.controllerTasks,
		blueprintCoordinator,
	)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Task abort service: %w", err)
	}
	serviceReads, err := newServiceReadResources(
		dependencies.authority.hierarchyRecords,
		dependencies.dataServices.serviceRecords,
		dependencies.dataServices.zoneRecords,
		dependencies.authority.releaseLedger,
		dependencies.authority.agents,
		dependencies.agentRuntime.Registry,
	)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close() // Preserve the initialization error; cleanup is best-effort.
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Service reads: %w", err)
	}
	server := handlers.New(dependencies.store, dependencies.bootstrap.logger, handlers.Options{
		Host: dependencies.platform.Host, HostTerminals: hostTerminals,
		ControllerConfig: dependencies.bootstrap.controllerConfig,
		EtcdConfig:       dependencies.bootstrap.etcdLifecycle.Configuration(), EtcdConfigApplier: dependencies.platform.EtcdConfig,
		ControllerUpdates: dependencies.platform.Upgrades,
		SoftwarePreparer:  dependencies.software.Preparations, SoftwareReleases: dependencies.software.Releases,
		SoftwareActivator: dependencies.software.Activations,
		Images:            dependencies.images,
		OnHTTPReady:       dependencies.platform.Readiness.MarkHTTPReady, MutationAdmission: dependencies.platform.Upgrades,
		Agents: dependencies.platform.AgentReads, AgentMutations: dependencies.platform.AgentMutations,
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
		ConnectorChecks:         dependencies.connectors.checks,
		Runners:                 dependencies.authority.runnerRecords,
		RunnerProvisioning:      dependencies.runner.provisioning,
		RunnerChecks:            dependencies.runner.checks,
		RunnerMutations:         dependencies.runner.mutations,
		RunnerRemovals:          dependencies.runner.removals,
		BackupPolicies:          dependencies.backup.policies,
		BackupPolicyMutations:   dependencies.backup.policies,
		RecoveryPoints:          dependencies.backup.points,
		RecoveryPointRemover:    dependencies.backup.retention,
		BackupRuns:              dependencies.backup.runs,
		BackupRestores:          dependencies.backup.restores,
		BackupRestorePreviews:   dependencies.backup.restores,
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
		BackingChecks:           backingChecks,
		EdgeChecks:              edgeChecks,
		DNSChecks:               dnsChecks,
		EnvironmentDNS:          environmentDNS,
		TaskMutations:           dependencies.taskMutations,
		TaskAborts:              taskAborts,
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
			dependencies.platform.Upgrades,
			dependencies.bootstrap.tick,
			dependencies.authority.tasks,
			dependencies.authority.idempotency.Retention,
			dependencies.dataServices.serviceDesiredRevisionRecords,
			dependencies.staleTasks,
			dependencies.backup.schedules,
			dependencies.backup.retention,
			dependencies.backup.orphans,
		),
		controllerTasks: dependencies.controllerTasks,
		blueprints:      blueprintCoordinator,
		software:        dependencies.software,
		localAgent:      dependencies.platform,
		attachMutations: dependencies.attachMutations,
		container:       dependencies.platform,
		etcdContainer:   dependencies.bootstrap.etcdLifecycle,
		store:           dependencies.store,
	}, nil
}
