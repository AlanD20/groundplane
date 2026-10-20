package app

import (
	"fmt"

	"github.com/AlanD20/groundplane/internal/app/hostplatform"
	"github.com/AlanD20/groundplane/internal/app/softwareplatform"
	commonrunner "github.com/AlanD20/groundplane/internal/common/runner"
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/environmentdns"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/hostterminal"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/scheduler"
	"github.com/AlanD20/groundplane/internal/controller/taskinspection"
	taskoperations "github.com/AlanD20/groundplane/internal/controller/tasks"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
)

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
	blueprintReleases       *blueprintrelease.Service
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
	environmentBlueprints   *blueprint.Service
	hierarchyDeletions      *hierarchydeletion.Service
	attachMutations         *attachments.MutationService
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
	workRuntime, err := dependencies.platform.NewWorkExecution(hostplatform.ExecutionDependencies{
		BackupSecrets:   dependencies.execution.backupSecrets,
		Hierarchy:       dependencies.hierarchyDeletions,
		AttachFacts:     dependencies.execution.attachFactValues,
		BackupRemoval:   dependencies.backup.cleanup,
		Keys:            dependencies.backup.keys,
		ScriptArtifacts: dependencies.execution.scriptArtifacts,
		Releases:        dependencies.blueprintReleases,
		CapturedFiles:   dependencies.execution.materializationResolver,
		Blueprints:      dependencies.environmentBlueprints,
		Entries:         dependencies.environmentBlueprints,
		Definitions:     dependencies.environmentBlueprints,
		Images:          dependencies.images, RunnerChecks: dependencies.runner.checkExecutor,
		RunnerLifecycle: dependencies.runner.lifecycle,
	})
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize execution storage: %w", err)
	}
	executionQueue := workRuntime.Queue
	taskMutations, err := taskoperations.NewRetryService(executionQueue, workRuntime.Retries,
		dependencies.authority.idempotency, dependencies.authority.intentCoordinator)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize execution Retry: %w", err)
	}
	taskAborts, err := taskoperations.NewAbortService(executionQueue)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Task abort service: %w", err)
	}
	imageRecords, err := imageoperations.NewRepository(dependencies.store)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize image execution inspection: %w", err)
	}
	executionDiagnostics, err := executiondiagnostics.New(dependencies.store, executionQueue)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize execution diagnostics: %w", err)
	}
	hierarchyInspection, err := etcd.NewHierarchyDeletionRepository(dependencies.store)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize removal inspection: %w", err)
	}
	componentInspection, err := componentwork.NewInputRepository(dependencies.store)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Component inspection: %w", err)
	}
	runnerInspection, err := runnerwork.NewRepository(dependencies.store)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Runner inspection: %w", err)
	}
	etcdInspection, err := etcdconfiguration.New(dependencies.store)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize etcd activation inspection: %w", err)
	}
	preparationInspection, err := preparationrecord.NewRepository(dependencies.store)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize software preparation inspection: %w", err)
	}
	controllerInspection, err := controllersoftware.New(dependencies.store)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Controller activation inspection: %w", err)
	}
	agentInspection, err := agentoperations.New(dependencies.store)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Agent lifecycle inspection: %w", err)
	}
	activationInspection, err := activationrecord.NewRepository(dependencies.store)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize software staging inspection: %w", err)
	}

	environmentInspection, err := environmentprovision.NewRepository(dependencies.store)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Environment provisioning inspection: %w", err)
	}
	taskInspection, err := taskinspection.New(
		executionQueue,
		imageRecords,
		executionDiagnostics,
		workRuntime.Retries,
		hierarchyInspection,
		componentInspection,
		runnerInspection,
		etcdInspection,
		preparationInspection,
		controllerInspection,
		agentInspection,
		activationInspection,
		environmentInspection,
	)
	if err != nil {
		_ = hostTerminals.Close()
		_ = dependencies.platform.Close()
		_ = dependencies.store.Close()
		return nil, fmt.Errorf("controller: initialize Task inspection: %w", err)
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
		StorageAdmission: dependencies.bootstrap.storageMaintenance,
		Agents:           dependencies.platform.AgentReads, AgentMutations: dependencies.platform.AgentMutations,
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
		TaskMutations:           taskMutations,
		TaskAborts:              taskAborts,
		TaskInspection:          taskInspection,
		AgentTaskWake:           dependencies.agentRuntime.Registry.WakeTaskDispatch,
		TenantMutations:         dependencies.hierarchy.tenantMutations,
		TenantChanges:           dependencies.hierarchy.tenantChanges,
		Console:                 dependencies.bootstrap.consoleAssets,
		Logs:                    serviceReads.logs,
	})
	maintenanceScheduler := scheduler.New(
		dependencies.bootstrap.logger, dependencies.platform.Upgrades, dependencies.bootstrap.tick,
		dependencies.authority.tasks, dependencies.authority.idempotency.Retention,
		dependencies.dataServices.serviceDesiredRevisionRecords,
		dependencies.backup.schedules, dependencies.backup.retention, dependencies.backup.orphans,
	)
	maintenanceScheduler.ConfigureMaintenance(dependencies.bootstrap.storageMaintenance)
	maintenanceScheduler.ConfigureStorageAdmission(dependencies.bootstrap.storageMaintenance)
	return &Controller{
		Config:          dependencies.bootstrap.config,
		Logger:          dependencies.bootstrap.logger,
		server:          server,
		agent:           dependencies.agentRuntime,
		scheduler:       maintenanceScheduler,
		workExecution:   workRuntime,
		localAgent:      dependencies.platform,
		attachMutations: dependencies.attachMutations,
		container:       dependencies.platform,
		etcdContainer:   dependencies.bootstrap.etcdLifecycle,
		store:           dependencies.store,
	}, nil
}
