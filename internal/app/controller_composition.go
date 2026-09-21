package app

import (
	"context"
	"fmt"
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
	agentruntime "github.com/AlanD20/groundplane/internal/controller/localagent/runtime"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/releasegroup"
	releaseoperation "github.com/AlanD20/groundplane/internal/controller/releaseoperation"
	"github.com/AlanD20/groundplane/internal/controller/secrets"
	taskoperations "github.com/AlanD20/groundplane/internal/controller/tasks"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	networketcd "github.com/AlanD20/groundplane/internal/infra/etcd/network"
)

// NewController constructs the Controller composition root.
func NewController(ctx context.Context, configPath string) (*Controller, error) {
	bootstrap, err := newControllerBootstrapComposition(ctx, configPath)
	if err != nil {
		return nil, err
	}
	cfg := bootstrap.config
	store := bootstrap.store
	keepEtcdLifecycle := false
	defer func() {
		if !keepEtcdLifecycle {
			_ = bootstrap.etcdLifecycle.Close()
		}
	}()
	authority, err := newControllerAuthorityComposition(ctx, cfg, store)
	if err != nil {
		return nil, err
	}
	dataServices, err := newControllerDataComposition(ctx, store, authority)
	if err != nil {
		return nil, err
	}
	execution, err := newControllerExecutionComposition(
		ctx,
		cfg,
		store,
		authority,
		dataServices,
		bootstrap.componentCatalog,
	)
	if err != nil {
		return nil, err
	}
	resolverComposition, err := newControllerResolverComposition(
		ctx,
		cfg.Storage.VolumeRoot,
		dataServices.componentRecords,
		dataServices.resolverBaselines,
		dataServices.resolutionProjections,
		authority.tasks,
		authority.intentCoordinator,
		execution.planResolver,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	agentRuntime := channeltransport.New(
		authority.authenticator,
		authority.tasks,
		execution.planResolver,
		execution.materializationResolver,
		execution.backupSecrets,
		dataServices.backupCheckpoints,
		resolverComposition.executionPlanner,
		execution.scriptArtifacts,
		dataServices.scriptCheckpoints,
		execution.backingHookCheckpoints,
	)
	blueprintReleases, err := blueprintrelease.NewService(
		authority.releaseLedger,
		dataServices.scriptRecords,
		execution.planResolver,
		execution.scriptArtifacts,
		execution.scriptSourceReferences,
		authority.agents,
		agentRuntime.Registry,
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
		cfg,
		authority.runnerRecords,
		authority.tasks,
		authority.hierarchyRecords,
		authority.idempotency,
		authority.intentCoordinator,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	releaseGroupMutations, err := releasegroup.NewMutationService(
		authority.releaseGroups,
		authority.hierarchyRecords,
		authority.tasks,
		authority.idempotency,
		authority.intentCoordinator,
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
		authority.releaseLedger,
		dataServices.serviceRecords,
		authority.releaseGroups,
		authority.idempotency,
		authority.intentCoordinator,
		execution.planResolver,
		dataServices.scriptRecords,
		execution.scriptArtifacts,
		releaseExecutionTimeout,
		authority.agents,
		agentRuntime.Registry,
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
		store,
		bootstrap.logger,
		authority.hierarchyRecords,
		dataServices.backupPolicyRecords,
		dataServices.backupPolicyRepository,
		dataServices.backupRuntimeRecords,
		dataServices.backupKeyRecords,
		execution.attachFactValues,
		authority.intentCoordinator,
		authority.idempotency,
		authority.intentProtector,
		authority.controllerKey,
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
	componentReads, err := newComponentReadService(
		dataServices.componentRecords,
		dataServices.resolverBaselines,
		dataServices.resolutionProjections,
		networkRecords,
		bootstrap.componentCatalog,
	)
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
		dataServices.serviceMutationRepository,
		execution.planResolver,
		execution.attachFactValues,
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	scriptMutations, err := newControllerScriptMutations(
		dataServices.scriptMutationRepository,
		execution.scriptArtifacts,
		authority.agents,
		agentRuntime.Registry,
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	entryGeneration, err := entrygeneration.NewEntryGenerationService(
		dataServices.secretRecords,
		execution.attachFactValues,
		authority.intentProtector,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry generation: %w", err)
	}
	entryCreationIdempotency, err := entryoperations.NewCreationIdempotency(
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry creation idempotency: %w", err)
	}
	entryBulkUpsertIdempotency, err := entryoperations.NewBulkUpsertIdempotency(
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry bulk upsert idempotency: %w", err)
	}
	entryEditIdempotency, err := entryoperations.NewEditIdempotency(authority.intentCoordinator, authority.idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry edit idempotency: %w", err)
	}
	entryRemovalIdempotency, err := entryoperations.NewRemovalIdempotency(
		authority.intentCoordinator,
		authority.idempotency,
	)
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
		store,
		authority.hierarchyRecords,
		dataServices.secretRecords,
		dataServices.connectorRecords,
		authority.intentCoordinator,
		authority.idempotency,
		authority.intentProtector,
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
	attachMutationIdempotency, err := attachments.NewMutationIdempotency(
		authority.intentCoordinator,
		authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach mutation idempotency: %w", err)
	}
	attachMutationPlans, err := attachplanning.New(cfg.Storage.VolumeRoot,
		execution.attachRecords,
		attachMutationRecords,
		execution.attachFactValues,
		bootstrap.componentCatalog,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach draft plan sealer: %w", err)
	}
	attachMutations, err := attachments.NewMutationService(
		attachMutationRecords,
		execution.attachFactValues,
		attachMutationPlans,
		execution.planResolver,
		attachMutationIdempotency,
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
	environmentBlueprintIdempotency, err := desiredrevision.NewIdempotency(
		authority.intentCoordinator,
		authority.idempotency,
	)
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
		cfg.Storage.VolumeRoot,
		environmentBlueprintRepository,
		entryGeneration,
		execution.materializationResolver,
		entryCreationIdempotency,
		entryEditIdempotency,
		entryRemovalIdempotency,
		execution.planResolver,
		authority.hierarchyRecords,
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
		releaseGroupBlueprints,
		blueprintReleases,
		entryGeneration,
		execution.attachFactValues,
		bootstrap.componentCatalog,
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
		bootstrap.componentCatalog,
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
		dataServices.componentRecords,
		authority.tasks,
		authority.idempotency,
		authority.intentCoordinator,
		resolverComposition.renderer,
		resolverComposition.renderPlanner,
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
		EtcdEndpoints: bootstrap.etcdEndpoints, Tick: bootstrap.tick, Logger: bootstrap.logger,
	})
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize platform runtime: %w", err)
	}
	runnerLifecycle, err := newRunnerLifecycleExecutor(
		bootstrap.logger,
		authority.runnerRecords,
		runnerComposition.tokens,
		cfg,
		runnerComposition.pools,
	)
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
		platform.agents, agentRuntime.Registry, platform.native, bootstrap.tick, bootstrap.logger,
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
	wired, err := newControllerHTTPComposition(controllerHTTPDependencies{
		bootstrap:               bootstrap,
		store:                   store,
		authority:               authority,
		dataServices:            dataServices,
		execution:               execution,
		platform:                platform,
		agentRuntime:            agentRuntime,
		runner:                  runnerComposition,
		backup:                  backupComposition,
		hierarchy:               hierarchyMutations,
		connectors:              connectorComposition,
		backingServiceMutations: backingServiceMutations,
		componentReads:          componentReads,
		componentMutations:      componentMutations,
		network:                 networkCapability,
		serviceMutations:        serviceMutations,
		releaseGroupMutations:   releaseGroupMutations,
		releaseOperations:       releaseOperations,
		scriptMutations:         scriptMutations,
		entryMutations:          entryMutations,
		secretMutations:         secretMutations,
		secretDeletions:         secretDeletions,
		volumeMutations:         volumeMutations,
		environmentBlueprints:   environmentBlueprints,
		hierarchyDeletions:      hierarchyDeletions,
		attachMutations:         attachMutations,
		taskMutations:           taskMutations,
		taskAborts:              taskAborts,
		controllerTasks:         controllerTaskRunner,
		staleTasks:              staleTasks,
	})
	if err != nil {
		return nil, err
	}
	keepEtcdLifecycle = true
	return wired, nil
}
