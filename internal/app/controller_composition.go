package app

import (
	"context"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/logging"
	"github.com/AlanD20/groundplane/internal/componentregistration"
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/attachplanning"
	"github.com/AlanD20/groundplane/internal/controller/backingservices"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	componentcapability "github.com/AlanD20/groundplane/internal/controller/component"
	taskdispatch "github.com/AlanD20/groundplane/internal/controller/controllertask/dispatch"
	desiredrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	controllerdns "github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	entryoperations "github.com/AlanD20/groundplane/internal/controller/entry/operations"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	handlers "github.com/AlanD20/groundplane/internal/controller/handlers"
	agentruntime "github.com/AlanD20/groundplane/internal/controller/localagent/runtime"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/releasegroup"
	releaseoperation "github.com/AlanD20/groundplane/internal/controller/releaseoperation"
	"github.com/AlanD20/groundplane/internal/controller/scheduler"
	"github.com/AlanD20/groundplane/internal/controller/secrets"
	taskoperations "github.com/AlanD20/groundplane/internal/controller/tasks"
	controllerconfigstore "github.com/AlanD20/groundplane/internal/infra/controllerconfig"
	"github.com/AlanD20/groundplane/internal/infra/docker/etcdcontainer"
	"github.com/AlanD20/groundplane/internal/infra/environmentroot"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	networketcd "github.com/AlanD20/groundplane/internal/infra/etcd/network"
	"github.com/AlanD20/groundplane/pkg/errs"
	"os"
	"time"
)

// NewController constructs the Controller composition root.
func NewController(ctx context.Context, configPath string) (*Controller, error) {
	registerAdapters()
	componentCatalog, err := componentregistration.EnvironmentCatalog()
	if err != nil {
		return nil, fmt.Errorf("controller: initialize registered Components: %w", err)
	}

	cfg, startupDocument, err := loadControllerConfigDocument(ctx, configPath)
	if err != nil {
		return nil, err
	}
	controllerConfig, err := controllerconfigstore.New(ctx, configPath, startupDocument)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Controller config store: %w", err)
	}
	if _, err := environmentroot.Validate(ctx, cfg.Storage.VolumeRoot); err != nil {
		return nil, fmt.Errorf("controller: validate Environment volume root: %w", err)
	}
	tick, err := time.ParseDuration(cfg.Scheduler.TickInterval)
	if err != nil {
		return nil, fmt.Errorf("controller: parse scheduler tick interval: %w", err)
	}
	if tick <= 0 {
		return nil, fmt.Errorf("controller: scheduler tick interval must be positive")
	}

	level, err := logging.ResolveLevel(false, false, os.Getenv("GROUNDPLANE_LOG_LEVEL"), cfg.Log.Level)
	if err != nil {
		return nil, err
	}
	logger, err := logging.Setup(logging.Options{
		Level:   level,
		Console: logging.ConsoleConfig{Enabled: cfg.Log.Console.Enabled},
		File:    logging.FileConfig{Enabled: cfg.Log.File.Enabled, Path: cfg.Log.File.Path},
	})
	if err != nil {
		return nil, err
	}
	consoleAssets, err := controllerConsoleAssets()
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("controller: load embedded Console: %w", err))
	}

	etcdLifecycle, err := etcdcontainer.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize etcd container lifecycle: %w", err)
	}
	keepEtcdLifecycle := false
	defer func() {
		if !keepEtcdLifecycle {
			_ = etcdLifecycle.Close()
		}
	}()
	if err := etcdLifecycle.Reconcile(ctx); err != nil {
		return nil, fmt.Errorf("controller: reconcile etcd container: %w", err)
	}
	etcdEndpoints := etcdcontainer.Endpoints()
	store, err := etcd.New(ctx, etcdEndpoints, cfg.Etcd.KeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize etcd: %w", err)
	}
	if err := bootstrapRunnerNetworkPool(ctx, store, cfg); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: reserve Runner network pool: %w", err)
	}
	authority, err := newControllerAuthorityComposition(ctx, cfg, store)
	if err != nil {
		return nil, err
	}
	dataServices, err := newControllerDataComposition(ctx, store, authority)
	if err != nil {
		return nil, err
	}
	execution, err := newControllerExecutionComposition(ctx, cfg, store, authority, dataServices, componentCatalog)
	if err != nil {
		return nil, err
	}
	resolverComposition, err := newControllerResolverComposition(
		ctx, cfg.Storage.VolumeRoot, dataServices.componentRecords, dataServices.resolverBaselines, dataServices.resolutionProjections, authority.tasks, authority.intentCoordinator, execution.planResolver,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	agentRuntime := channeltransport.New(
		authority.authenticator, authority.tasks, execution.planResolver, execution.materializationResolver, execution.backupSecrets, dataServices.backupCheckpoints,
		resolverComposition.executionPlanner, execution.scriptArtifacts, dataServices.scriptCheckpoints, execution.backingHookCheckpoints,
	)
	blueprintReleases, err := blueprintrelease.NewService(
		authority.releaseLedger, dataServices.scriptRecords, execution.planResolver, execution.scriptArtifacts, execution.scriptSourceReferences,
		authority.agents, agentRuntime.Registry,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Blueprint candidate-release service: %w", err)
	}
	staleTasks, err := agentruntime.NewStaleTaskMaintenance(authority.agents, agentRuntime.Registry, authority.tasks)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize stale Agent task maintenance: %w", err)
	}
	runnerComposition, err := newControllerRunnerComposition(
		cfg, authority.runnerRecords, authority.tasks, authority.hierarchyRecords, authority.idempotency, authority.intentCoordinator,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	releaseGroupMutations, err := releasegroup.NewMutationService(
		authority.releaseGroups, authority.hierarchyRecords, authority.tasks, authority.idempotency, authority.intentCoordinator,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release group mutation service: %w", err)
	}
	releaseExecutionTimeout, err := cfg.ParsedReleaseExecutionTimeout()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release execution timeout: %w", err)
	}
	releaseOperations, err := releaseoperation.NewService(
		authority.releaseLedger, dataServices.serviceRecords, authority.releaseGroups, authority.idempotency, authority.intentCoordinator,
		execution.planResolver, dataServices.scriptRecords, execution.scriptArtifacts, releaseExecutionTimeout,
		authority.agents, agentRuntime.Registry,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release operation service: %w", err)
	}
	hierarchyDeletions, err := newHierarchyDeletionRuntime(store, authority.idempotency, authority.intentCoordinator)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize hierarchy deletion runtime: %w", err)
	}
	backupComposition, err := newControllerBackupComposition(
		store, logger, authority.hierarchyRecords, dataServices.backupPolicyRecords, dataServices.backupPolicyRepository,
		dataServices.backupRuntimeRecords, dataServices.backupKeyRecords, execution.attachFactValues, authority.intentCoordinator,
		authority.idempotency, authority.intentProtector, authority.controllerKey,
	)
	if err != nil {
		return nil, err
	}
	networkRecords, err := networketcd.NewRepository(
		authority.hierarchyRecords,
		dataServices.serviceRecords,
		dataServices.zoneRecords,
		dataServices.routeRecords,
		execution.attachRecords,
		authority.tasks,
		authority.idempotency,
		execution.attachFactValues,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Network persistence adapter: %w", err)
	}
	if err := networkRecords.EnableDesiredRevisions(dataServices.serviceDesiredRevisionRecords); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Network desired revision persistence: %w", err)
	}
	componentReads, err := newComponentReadService(dataServices.componentRecords, dataServices.resolverBaselines, dataServices.resolutionProjections, networkRecords, componentCatalog)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Component reads: %w", err)
	}
	networkCapability, err := networkcontroller.NewEtcdService(
		networkRecords,
		execution.planResolver,
		authority.intentCoordinator,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Network capability: %w", err)
	}
	serviceMutations, err := newControllerServiceMutations(
		dataServices.serviceMutationRepository, execution.planResolver, execution.attachFactValues, authority.intentCoordinator, authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	scriptMutations, err := newControllerScriptMutations(
		dataServices.scriptMutationRepository, execution.scriptArtifacts, authority.agents, agentRuntime.Registry, authority.intentCoordinator, authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	entryGeneration, err := entrygeneration.NewEntryGenerationService(dataServices.secretRecords, execution.attachFactValues, authority.intentProtector)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry generation: %w", err)
	}
	entryCreationIdempotency, err := entryoperations.NewCreationIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry creation idempotency: %w", err)
	}
	entryBulkUpsertIdempotency, err := entryoperations.NewBulkUpsertIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry bulk upsert idempotency: %w", err)
	}
	entryEditIdempotency, err := entryoperations.NewEditIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry edit idempotency: %w", err)
	}
	entryRemovalIdempotency, err := entryoperations.NewRemovalIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry removal idempotency: %w", err)
	}
	secretCreationIdempotency, err := secrets.NewCreationIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret creation idempotency: %w", err)
	}
	secretMutations, err := secrets.NewCreationService(
		dataServices.secretReadRepository,
		authority.intentProtector,
		secretCreationIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret creation service: %w", err)
	}
	connectorComposition, err := newControllerConnectorComposition(
		store, authority.hierarchyRecords, dataServices.secretRecords, dataServices.connectorRecords, authority.intentCoordinator, authority.idempotency, authority.intentProtector,
	)
	if err != nil {
		return nil, err
	}
	secretDeletionIdempotency, err := secrets.NewDeletionIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret deletion idempotency: %w", err)
	}
	secretDeletions, err := secrets.NewDeletionService(dataServices.secretReadRepository, secretDeletionIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret deletion service: %w", err)
	}
	attachMutationRecords, err := attachments.NewRepository(
		authority.hierarchyRecords, dataServices.serviceRecords, execution.attachRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach mutation repository: %w", err)
	}
	attachMutationIdempotency, err := attachments.NewMutationIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach mutation idempotency: %w", err)
	}
	attachMutationPlans, err := attachplanning.New(cfg.Storage.VolumeRoot,
		execution.attachRecords, attachMutationRecords, execution.attachFactValues, componentCatalog)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach draft plan sealer: %w", err)
	}
	attachMutations, err := attachments.NewMutationService(
		attachMutationRecords, execution.attachFactValues, attachMutationPlans, execution.planResolver, attachMutationIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach mutation service: %w", err)
	}
	taskRetryIdempotency, err := taskoperations.NewRetryIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Task retry idempotency: %w", err)
	}
	taskMutations, err := taskoperations.NewRetryService(authority.tasks, taskRetryIdempotency, backupComposition.runs)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Task retry service: %w", err)
	}
	backingZoneCascades, err := networkCapability.NewBackingZoneCascadeExecutor(
		attachMutations,
		taskMutations,
		execution.planResolver,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backing Zone cascade: %w", err)
	}
	environmentBlueprintIdempotency, err := desiredrevision.NewIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint idempotency: %w", err)
	}
	desiredRevisionRecords, err := desiredrevisionstore.NewRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize desired revision repository: %w", err)
	}
	environmentBlueprintRepository, err := blueprint.NewRepository(
		authority.environmentBlueprintRecords,
		desiredRevisionRecords,
		dataServices.zoneRecords,
		dataServices.serviceRecords,
		dataServices.routeRecords,
		dataServices.entryRecords,
		dataServices.entryValues,
		execution.attachRecords,
		dataServices.componentRecords,
		dataServices.scriptRecords,
		dataServices.backupPolicyRecords,
		dataServices.connectorRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint repositories: %w", err)
	}
	entryDesiredMutations, err := entryoperations.NewDesiredMutationService(
		cfg.Storage.VolumeRoot, environmentBlueprintRepository, entryGeneration, execution.materializationResolver,
		entryCreationIdempotency, entryEditIdempotency, entryRemovalIdempotency, execution.planResolver, authority.hierarchyRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry desired mutation service: %w", err)
	}
	entryBulkUpserts, err := entryoperations.NewBulkUpsertService(entryDesiredMutations, entryBulkUpsertIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry bulk upsert service: %w", err)
	}
	entryMutations, err := entryoperations.NewMutationService(entryDesiredMutations, entryBulkUpserts)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry mutation service: %w", err)
	}
	releaseGroupBlueprints, err := releasegroup.NewReleaseGroupBlueprintPlanner(
		authority.releaseGroups,
		authority.hierarchyRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Release Group Blueprint planner: %w", err)
	}
	environmentBlueprints, err := blueprint.NewService(
		cfg.Storage.VolumeRoot, cfg.EnvironmentPool,
		environmentBlueprintRepository, environmentBlueprintIdempotency, execution.materializationResolver,
		releaseGroupBlueprints, blueprintReleases, entryGeneration, execution.attachFactValues, componentCatalog,
		environmentBlueprintRepository, backupComposition.policyKeys,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint service: %w", err)
	}
	backingServiceCreations, err := backingservices.NewCreationService(
		cfg.Storage.VolumeRoot,
		runnerComposition.pools.Environment,
		environmentBlueprintRepository,
		environmentBlueprintIdempotency,
		authority.intentProtector,
		execution.planResolver,
		execution.attachFactValues,
		componentCatalog,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Backing-service creation: %w", err)
	}
	componentCredentials, err := componentcapability.NewCredentialReferenceResolver(
		authority.hierarchyRecords,
		dataServices.secretReads,
		secretMutations,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Component credentials: %w", err)
	}
	platformComponentMutations, err := controllerdns.NewPlatformMutationService(
		dataServices.componentRecords, authority.tasks, authority.idempotency, authority.intentCoordinator, resolverComposition.renderer, resolverComposition.renderPlanner,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Platform Component mutations: %w", err)
	}
	componentMutations, err := componentcapability.NewMutationService(
		dataServices.componentRecords, environmentBlueprints,
		componentCredentials, platformComponentMutations,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Component mutations: %w", err)
	}
	backingServiceMutations, err := backingservices.NewMutationService(
		dataServices.backingServiceReads,
		serviceMutations,
		backingServiceCreations,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Backing-service mutations: %w", err)
	}
	volumeMutations, err := configureVolumeMutationPlans(
		cfg.Storage.VolumeRoot,
		environmentBlueprintRepository,
		authority.intentCoordinator,
		authority.idempotency,
		dataServices.volumeReads, dataServices.backupPolicyRecords, store, execution.planResolver, agentRuntime,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Volume mutations: %w", err)
	}
	hierarchyMutations, err := newControllerHierarchyMutations(
		cfg, authority.hierarchyRecords, dataServices.zoneRecords, authority.intentCoordinator, authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	platform, err := newControllerPlatform(ctx, controllerPlatformDependencies{
		Config: cfg, Key: authority.controllerKey, Store: store, Agents: authority.agents, Tasks: authority.tasks,
		Intents: authority.intentCoordinator, Idempotency: authority.idempotency, Channel: agentRuntime,
		EtcdEndpoints: etcdEndpoints, Tick: tick, Logger: logger,
	})
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize platform runtime: %w", err)
	}
	runnerLifecycle, err := newRunnerLifecycleExecutor(logger, authority.runnerRecords, runnerComposition.tokens, cfg, runnerComposition.pools)
	if err != nil {
		_ = platform.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Runner lifecycle: %w", err)
	}
	controllerTaskHandler, err := taskdispatch.NewResourceHandler(
		platform.agents,
		backingZoneCascades,
		authority.runnerRecords,
		runnerLifecycle,
	)
	if err != nil {
		_ = platform.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Controller Task handler: %w", err)
	}
	controllerTaskRunner, err := newControllerTaskRuntime(
		ctx, authority.tasks, controllerTaskHandler, backupComposition.keys, hierarchyDeletions,
		platform.agents, agentRuntime.Registry, platform.native, tick, logger,
	)
	if err != nil {
		_ = platform.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Controller Task runner: %w", err)
	}
	taskAborts, err := taskoperations.NewAbortService(authority.tasks, agentRuntime.Registry, controllerTaskRunner)
	if err != nil {
		_ = platform.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Task abort service: %w", err)
	}
	serviceReads, err := newServiceReadResources(
		authority.hierarchyRecords, dataServices.serviceRecords, dataServices.zoneRecords, authority.releaseLedger, authority.agents, agentRuntime.Registry,
	)
	if err != nil {
		// Preserve the initialization error; cleanup is best-effort.
		_ = platform.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service reads: %w", err)
	}
	srv := handlers.New(store, logger, handlers.Options{
		Host: platform.host, ControllerConfig: controllerConfig, ControllerUpdates: platform.upgrades,
		OnHTTPReady: platform.readiness.MarkHTTPReady, MutationAdmission: platform.upgrades,
		Agents: platform.reads, AgentMutations: platform.mutations, Tenants: authority.hierarchyService,
		Projects:                authority.hierarchyService,
		ProjectMutations:        hierarchyMutations.projectMutations,
		ProjectChanges:          hierarchyMutations.projectChanges,
		BackingServices:         dataServices.backingServiceReads,
		BackingServiceMutations: backingServiceMutations,
		Components:              componentReads,
		ComponentMutations:      componentMutations,
		Environments:            serviceReads.environments,
		Services:                serviceReads.services, ServiceObservations: serviceReads.observations,
		ServiceMutations:      serviceMutations,
		Zones:                 networkCapability,
		ZoneMutations:         networkCapability,
		Routes:                networkCapability,
		RouteMutations:        networkCapability,
		ReleaseGroups:         authority.releaseGroups,
		ReleaseGroupMutations: releaseGroupMutations,
		Releases:              authority.releaseLedger,
		ReleaseOperations:     releaseOperations,
		Scripts:               dataServices.scriptReads,
		ScriptMutations:       scriptMutations,
		Entries:               dataServices.entryReads,
		EntryMutations:        entryMutations,
		Secrets:               dataServices.secretReads,
		SecretMutations:       secretMutations,
		SecretDeletions:       secretDeletions,
		Connectors:            dataServices.connectorReads,
		ConnectorMutations:    connectorComposition.mutations,
		ConnectorDeletions:    connectorComposition.deletions,
		Runners:               authority.runnerRecords,
		RunnerProvisioning:    runnerComposition.provisioning,
		RunnerMutations:       runnerComposition.mutations,
		RunnerRemovals:        runnerComposition.removals,
		BackupPolicies:        backupComposition.policies,
		BackupPolicyMutations: backupComposition.policies,
		RecoveryPoints:        backupComposition.points,
		BackupRuns:            backupComposition.runs,
		BackupKeyMutations:    backupComposition.keys,
		BackupKeyExports:      backupComposition.keys,
		Volumes:               dataServices.volumeReads,
		VolumeMutations:       volumeMutations,
		EnvironmentMutations:  hierarchyMutations.environmentMutations,
		EnvironmentChanges:    hierarchyMutations.environmentChanges,
		EnvironmentBlueprints: environmentBlueprints,
		HierarchyDeletions:    hierarchyDeletions,
		AttachMutations:       attachMutations,
		AttachFacts:           execution.attachFactReads,
		TaskMutations:         taskMutations,
		TaskAborts:            taskAborts,
		ControllerTaskWake:    controllerTaskRunner.Wake,
		AgentTaskWake:         agentRuntime.Registry.WakeTaskDispatch,
		TenantMutations:       hierarchyMutations.tenantMutations,
		TenantChanges:         hierarchyMutations.tenantChanges,
		Console:               consoleAssets, Tasks: authority.tasks, Logs: serviceReads.logs,
	})

	wired := &Controller{
		Config:          cfg,
		Logger:          logger,
		server:          srv,
		agent:           agentRuntime,
		scheduler:       scheduler.New(logger, platform.upgrades, tick, authority.tasks, authority.idempotency, staleTasks, backupComposition.schedules),
		controllerTasks: controllerTaskRunner,
		localAgent:      platform.reconciliation,
		attachMutations: attachMutations,
		container:       platform,
		etcdContainer:   etcdLifecycle,
		store:           store,
	}
	keepEtcdLifecycle = true
	return wired, nil
}
