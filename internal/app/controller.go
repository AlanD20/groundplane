package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/internal/componentregistration"
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	backupcapability "github.com/AlanD20/groundplane/internal/controller/backup"
	taskdispatch "github.com/AlanD20/groundplane/internal/controller/controllertask/dispatch"
	handlers "github.com/AlanD20/groundplane/internal/controller/handlers"
	agentruntime "github.com/AlanD20/groundplane/internal/controller/localagent/runtime"
	"github.com/AlanD20/groundplane/internal/controller/scheduler"
	taskcheckpoint "github.com/AlanD20/groundplane/internal/controller/taskcheckpoint"
	"log/slog"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/adapters/custom"
	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/adapters/valkey9"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/logging"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/attachplanning"
	"github.com/AlanD20/groundplane/internal/controller/backingservices"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	componentcapability "github.com/AlanD20/groundplane/internal/controller/component"
	desiredrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	controllerdns "github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	entryoperations "github.com/AlanD20/groundplane/internal/controller/entry/operations"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"

	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/releasegroup"
	releaseoperation "github.com/AlanD20/groundplane/internal/controller/releaseoperation"
	"github.com/AlanD20/groundplane/internal/controller/secrets"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	taskoperations "github.com/AlanD20/groundplane/internal/controller/tasks"
	controllerconfigstore "github.com/AlanD20/groundplane/internal/infra/controllerconfig"
	"github.com/AlanD20/groundplane/internal/infra/docker/etcdcontainer"
	"github.com/AlanD20/groundplane/internal/infra/environmentroot"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	networketcd "github.com/AlanD20/groundplane/internal/infra/etcd/network"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const DefaultControllerConfigPath = "/etc/groundplane/controller.yaml"

// RunController constructs and runs the local Controller process using the
// same environment override as the dedicated Controller binary. It is the
// app-layer operation injected into `groundplane controller serve`.
func RunController(ctx context.Context) error {
	c, err := NewController(ctx, ControllerConfigPath())
	if err != nil {
		return err
	}
	return c.Run(ctx)
}

// ControllerConfigPath resolves the one Controller configuration path used by
// every foreground entry point.
func ControllerConfigPath() string {
	if value := os.Getenv("GROUNDPLANE_CONTROLLER_CONFIG"); value != "" {
		return value
	}
	return DefaultControllerConfigPath
}

// Controller is the wired Controller binary: config, logging, the
// adapter registry, the etcd store, the HTTP server, the local Agent channel,
// and the scheduler.
// cmd/controller/main.go is nothing but NewController + Run.
type Controller struct {
	Config config.ControllerConfig
	Logger *slog.Logger

	server          controllerServer
	agent           controllerAgentChannel
	scheduler       controllerScheduler
	controllerTasks controllerScheduler
	localAgent      controllerScheduler
	attachMutations *attachments.MutationService
	container       ownedStore
	etcdContainer   controllerEtcdLifecycle
	store           ownedStore
}

type controllerServer interface {
	Serve(ctx context.Context, addresses []string) error
}

type controllerScheduler interface {
	Run(ctx context.Context)
}

type controllerAgentChannel interface {
	Run(ctx context.Context) error
}

type ownedStore interface {
	Close() error
}

type controllerEtcdLifecycle interface {
	Run(context.Context) error
	Close() error
}

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
	attachRecords, err := etcd.NewAttachRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach repository: %w", err)
	}
	attachFactValues, err := attachments.NewFactService(attachRecords, authority.intentProtector)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach fact service: %w", err)
	}
	if err := attachFactValues.EnableBackingHookInputs(dataServices.secretRecords); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backing hook inputs: %w", err)
	}
	attachFactReads, err := attachments.NewFactReadService(attachRecords, attachFactValues)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach fact reads: %w", err)
	}
	planResolver, err := taskplanning.NewTaskPlanResolverWithAttachments(
		cfg.Storage.VolumeRoot,
		authority.hierarchyRecords,
		attachRecords,
		dataServices.serviceRecords,
		attachFactValues,
		componentCatalog,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize execution plan resolver: %w", err)
	}
	if err := componentregistration.ConfigureReleasePlans(planResolver, authority.releaseLedger); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release plan resolver: %w", err)
	}
	if err := planResolver.EnableScriptPlans(dataServices.scriptRecords); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script plan resolver: %w", err)
	}
	if err := planResolver.EnableBackupPlans(dataServices.backupRuntimeRecords); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup plan resolver: %w", err)
	}
	backingHookCheckpoints, err := taskcheckpoint.NewBackingHookCheckpointService(
		authority.tasks, planResolver, attachRecords, attachFactValues,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backing hook checkpoint service: %w", err)
	}
	materializationResolver, err := initializeTaskMaterializationResolver(
		store, authority.hierarchyRecords, dataServices.entryValues, dataServices.secretRecords, planResolver, authority.intentProtector,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	scriptArtifacts, err := taskplanning.NewScriptArtifactService(dataServices.scriptRecords, materializationResolver)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script artifact service: %w", err)
	}
	scriptSourceReferences, err := initializeExecutionSourceReferences(ctx, store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script source-reference authority: %w", err)
	}
	backupSecretEvidence, err := etcd.NewBackupSecretResolutionReader(store)
	if err != nil {
		// Rationale: initialization is already failing; store shutdown is
		// best-effort and must not replace the primary typed error.
		_ = store.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	backupSecrets, err := backupcapability.NewBackupSecretResolver(backupSecretEvidence, authority.intentProtector)
	if err != nil {
		// Rationale: initialization is already failing; store shutdown is
		// best-effort and must not replace the primary typed error.
		_ = store.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	resolverComposition, err := newControllerResolverComposition(
		ctx, cfg.Storage.VolumeRoot, dataServices.componentRecords, dataServices.resolverBaselines, dataServices.resolutionProjections, authority.tasks, authority.intentCoordinator, planResolver,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	agentRuntime := channeltransport.New(
		authority.authenticator, authority.tasks, planResolver, materializationResolver, backupSecrets, dataServices.backupCheckpoints,
		resolverComposition.executionPlanner, scriptArtifacts, dataServices.scriptCheckpoints, backingHookCheckpoints,
	)
	blueprintReleases, err := blueprintrelease.NewService(
		authority.releaseLedger, dataServices.scriptRecords, planResolver, scriptArtifacts, scriptSourceReferences,
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
		planResolver, dataServices.scriptRecords, scriptArtifacts, releaseExecutionTimeout,
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
		dataServices.backupRuntimeRecords, dataServices.backupKeyRecords, attachFactValues, authority.intentCoordinator,
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
		attachRecords,
		authority.tasks,
		authority.idempotency,
		attachFactValues,
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
		planResolver,
		authority.intentCoordinator,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Network capability: %w", err)
	}
	serviceMutations, err := newControllerServiceMutations(
		dataServices.serviceMutationRepository, planResolver, attachFactValues, authority.intentCoordinator, authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	scriptMutations, err := newControllerScriptMutations(
		dataServices.scriptMutationRepository, scriptArtifacts, authority.agents, agentRuntime.Registry, authority.intentCoordinator, authority.idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	entryGeneration, err := entrygeneration.NewEntryGenerationService(dataServices.secretRecords, attachFactValues, authority.intentProtector)
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
		authority.hierarchyRecords, dataServices.serviceRecords, attachRecords,
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
		attachRecords, attachMutationRecords, attachFactValues, componentCatalog)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach draft plan sealer: %w", err)
	}
	attachMutations, err := attachments.NewMutationService(
		attachMutationRecords, attachFactValues, attachMutationPlans, planResolver, attachMutationIdempotency,
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
		planResolver,
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
		attachRecords,
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
		cfg.Storage.VolumeRoot, environmentBlueprintRepository, entryGeneration, materializationResolver,
		entryCreationIdempotency, entryEditIdempotency, entryRemovalIdempotency, planResolver, authority.hierarchyRecords,
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
		environmentBlueprintRepository, environmentBlueprintIdempotency, materializationResolver,
		releaseGroupBlueprints, blueprintReleases, entryGeneration, attachFactValues, componentCatalog,
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
		planResolver,
		attachFactValues,
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
		dataServices.volumeReads, dataServices.backupPolicyRecords, store, planResolver, agentRuntime,
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
		AttachFacts:           attachFactReads,
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

func registerAdapters() {
	if _, registered := adapters.Get("postgres:16"); !registered {
		postgres16.Register()
	}
	if _, registered := adapters.Get("valkey:9"); !registered {
		valkey9.Register()
	}
	if _, registered := adapters.Get("custom"); !registered {
		custom.Register()
	}
}

// Run supervises HTTP, the local Agent channel, and the scheduler as one
// Controller lifetime. It joins every runtime before closing the etcd Store
// that NewController created.
func (c *Controller) Run(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "controller run context is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		c.scheduler.Run(runCtx)
	}()
	controllerTasksDone := make(chan struct{})
	go func() {
		defer close(controllerTasksDone)
		c.controllerTasks.Run(runCtx)
	}()
	localAgentDone := make(chan struct{})
	go func() {
		defer close(localAgentDone)
		c.localAgent.Run(runCtx)
	}()

	type runtimeResult struct {
		name string
		err  error
	}
	runtimeDone := make(chan runtimeResult, 3)
	go func() {
		err := c.server.Serve(runCtx, c.Config.Listen.HTTP)
		if err == nil && runCtx.Err() == nil {
			err = errors.New("http server stopped unexpectedly")
		}
		runtimeDone <- runtimeResult{name: "serve HTTP", err: err}
	}()
	go func() {
		err := c.agent.Run(runCtx)
		if err == nil && runCtx.Err() == nil {
			err = errors.New("agent channel stopped unexpectedly")
		}
		runtimeDone <- runtimeResult{name: "serve Agent channel", err: err}
	}()
	go func() {
		err := c.etcdContainer.Run(runCtx)
		if err == nil && runCtx.Err() == nil {
			err = errors.New("etcd container reconciliation stopped unexpectedly")
		}
		runtimeDone <- runtimeResult{name: "reconcile etcd container", err: err}
	}()

	runtimeErrors := make(map[string]error, 3)
	for index := 0; index < 3; index++ {
		result := <-runtimeDone
		runtimeErrors[result.name] = result.err
		if index == 0 {
			cancel()
		}
	}
	<-schedulerDone
	<-controllerTasksDone
	<-localAgentDone

	containerCloseErr := c.container.Close()
	closeErr := c.store.Close()
	etcdContainerCloseErr := c.etcdContainer.Close()
	joined := errors.Join(
		wrapControllerRunError("serve HTTP", runtimeErrors["serve HTTP"]),
		wrapControllerRunError("serve Agent channel", runtimeErrors["serve Agent channel"]),
		wrapControllerRunError("reconcile etcd container", runtimeErrors["reconcile etcd container"]),
		wrapControllerRunError("close platform runtime", containerCloseErr),
		wrapControllerRunError("close etcd", closeErr),
		wrapControllerRunError("close etcd container lifecycle", etcdContainerCloseErr),
	)
	if joined != nil {
		return errs.Wrap(errs.KindInternal, joined)
	}
	return nil
}

func wrapControllerRunError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("controller: %s: %w", operation, err)
}
