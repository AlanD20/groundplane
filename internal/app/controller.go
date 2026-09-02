package app

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/adapters/manual"
	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/adapters/valkey9"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/logging"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	componentcapability "github.com/AlanD20/groundplane/internal/controller/component"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	desiredrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	controllerdns "github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"
	hierarchycontroller "github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/agentcredential"
	controllerconfigstore "github.com/AlanD20/groundplane/internal/infra/controllerconfig"
	"github.com/AlanD20/groundplane/internal/infra/docker/agentcontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/etcdcontainer"
	"github.com/AlanD20/groundplane/internal/infra/environmentroot"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	environmentetcd "github.com/AlanD20/groundplane/internal/infra/etcd/environment"
	networketcd "github.com/AlanD20/groundplane/internal/infra/etcd/network"
	etcdreleasegroup "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	"github.com/AlanD20/groundplane/internal/infra/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
	"github.com/AlanD20/groundplane/internal/volume"
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
	attachMutations *attachMutationService
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
	componentCatalog, err := registeredEnvironmentComponentCatalog()
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
	runnerRecords, err := etcd.NewRunnerRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Runner repository: %w", err)
	}
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Task repository: %w", err)
	}
	if err := tasks.EnsureTaskJournalSchema(ctx); err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("validate Task journal schema", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	idempotency, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize idempotency repository: %w", err)
	}
	hierarchyRecords, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize hierarchy repository: %w", err)
	}
	environmentBlueprintRecords, err := etcd.NewEnvironmentBlueprintRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint repository: %w", err)
	}
	releaseGroups, err := etcdreleasegroup.New(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release group repository: %w", err)
	}
	releaseLedger, err := etcd.NewReleaseLedger(store, tasks)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release ledger: %w", err)
	}
	if err := hierarchyRecords.ValidateEnvironmentVolumeDirs(ctx, cfg.Storage.VolumeRoot); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: validate persisted environment volume directories: %w", err)
	}
	hierarchyRepository, err := hierarchycontroller.NewEtcdRepository(hierarchyRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize hierarchy adapter: %w", err)
	}
	hierarchyService, err := hierarchycontroller.NewService(hierarchyRepository)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize hierarchy service: %w", err)
	}
	agents, err := etcd.NewLocalAgentRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize local Agent repository: %w", err)
	}
	authenticator, err := newAgentChannelAuthenticator(agents)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent channel authenticator: %w", err)
	}
	controllerKey := &ageinfra.ControllerKey{Path: cfg.AgeKeyPath}
	if err := controllerKey.Load(ctx); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: load Controller age key: %w", err)
	}
	intentProtector, err := newSecretValueProtector(controllerKey)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize idempotent intent protector: %w", err)
	}
	intentCoordinator, err := idempotentintent.NewCoordinator(intentProtector)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize idempotent intent coordinator: %w", err)
	}
	entryValues, err := etcd.NewEntryValueGenerationRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry value generation repository: %w", err)
	}
	entryRecords, err := etcd.NewEntryRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry repository: %w", err)
	}
	entryReadRepository, err := newDurableEntryReadRepository(hierarchyRecords, entryRecords, entryValues)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry read repositories: %w", err)
	}
	entryReads, err := newEntryReadService(entryReadRepository, intentProtector)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry reads: %w", err)
	}
	serviceRecords, err := etcd.NewServiceRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service repository: %w", err)
	}
	routeRecords, err := etcd.NewRouteRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Route repository: %w", err)
	}
	scriptRecords, err := etcd.NewScriptRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script repository: %w", err)
	}
	scriptReadRepository, err := newDurableScriptReadRepository(hierarchyRecords, serviceRecords, scriptRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script read repositories: %w", err)
	}
	scriptReads, err := newScriptReadService(scriptReadRepository)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script reads: %w", err)
	}
	zoneRecords, err := etcd.NewZoneRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Zone repository: %w", err)
	}
	serviceDesiredRevisionRecords, err := desiredrevisionstore.NewRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service desired revision repository: %w", err)
	}
	serviceMutationRepository, err := newDurableServiceMutationRepository(
		hierarchyRecords, serviceRecords, zoneRecords, serviceDesiredRevisionRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service mutation repositories: %w", err)
	}
	scriptMutationRepository, err := newDurableScriptMutationRepository(
		hierarchyRecords, serviceRecords, scriptRecords, releaseLedger,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script mutation repositories: %w", err)
	}
	componentRecords, err := etcd.NewComponentRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Component repository: %w", err)
	}
	platformComponents, err := etcd.DefaultPlatformComponents(detectTailnetDelegationDefault())
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize platform Components: %w", err)
	}
	if _, err := componentRecords.EnsurePlatformComponents(ctx, platformComponents); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: bootstrap platform Components: %w", err)
	}
	managedConfigProjector, err := newRegisteredCoreDNSManagedConfigProjector(componentRecords, componentRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize managed Component config projection: %w", err)
	}
	componentReads, err := componentcapability.NewReadService(componentRecords, managedConfigProjector)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Component reads: %w", err)
	}
	serviceReadRepository, err := newDurableServiceReadRepository(hierarchyRecords, serviceRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service read repositories: %w", err)
	}
	serviceReads, err := newServiceReadService(serviceReadRepository)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service reads: %w", err)
	}
	backingServiceRecords, err := etcd.NewBackingServiceRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backing-service repository: %w", err)
	}
	backingServiceReads, err := newBackingServiceReadService(backingServiceRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backing-service reads: %w", err)
	}
	secretRecords, err := etcd.NewSecretRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret repository: %w", err)
	}
	secretReadRepository, err := newDurableSecretReadRepository(hierarchyRecords, secretRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret read repositories: %w", err)
	}
	secretReads, err := newSecretReadService(secretReadRepository, intentProtector)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret reads: %w", err)
	}
	connectorRecords, err := etcd.NewConnectorRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Connector repository: %w", err)
	}
	connectorReadRepository, err := newDurableConnectorReadRepository(hierarchyRecords, connectorRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Connector read repositories: %w", err)
	}
	connectorReads, err := newConnectorReadService(connectorReadRepository)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Connector reads: %w", err)
	}
	volumeReads, err := volume.NewReadService(hierarchyRecords)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize volume reads", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupPolicyRecords, err := etcd.NewBackupPolicyRepository(store)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup policy repository", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupRuntimeRecords, err := etcd.NewBackupRuntimeRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup runtime repository: %w", err)
	}
	backupCheckpoints, err := controller.NewBackupCheckpointService(backupRuntimeRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Backup checkpoint service: %w", err)
	}
	scriptCheckpoints, err := controller.NewScriptCheckpointService(scriptRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script checkpoint service: %w", err)
	}
	backupPolicyRepository, err := controller.NewDurableBackupPolicyRepository(backupPolicyRecords)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup policy repository adapter", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	attachRecords, err := etcd.NewAttachRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach repository: %w", err)
	}
	attachFactValues, err := NewAttachFactService(attachRecords, intentProtector)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach fact service: %w", err)
	}
	attachFactReads, err := newAttachFactReadService(attachRecords, attachFactValues)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach fact reads: %w", err)
	}
	planResolver, err := controller.NewTaskPlanResolverWithAttachments(
		cfg.Storage.VolumeRoot,
		hierarchyRecords,
		attachRecords,
		serviceRecords,
		attachFactValues,
		componentCatalog,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize execution plan resolver: %w", err)
	}
	if err := planResolver.EnableReleasePlans(releaseLedger); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release plan resolver: %w", err)
	}
	if err := planResolver.EnableScriptPlans(scriptRecords); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script plan resolver: %w", err)
	}
	if err := planResolver.EnableBackupPlans(backupRuntimeRecords); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup plan resolver: %w", err)
	}
	materializationResolver, err := controller.NewTaskMaterializationResolver(
		hierarchyRecords,
		entryValues,
		secretRecords,
		planResolver,
		intentProtector,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize materialization value resolver: %w", err)
	}
	scriptArtifacts, err := controller.NewScriptArtifactService(scriptRecords, materializationResolver)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script artifact service: %w", err)
	}
	scriptSourceReferences, err := etcd.NewScriptSourceReferenceAuthority(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script source-reference authority: %w", err)
	}
	blueprintReleases, err := blueprintrelease.NewService(
		releaseLedger,
		scriptRecords,
		planResolver,
		scriptArtifacts,
		scriptSourceReferences,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Blueprint candidate-release service: %w", err)
	}
	backupSecretEvidence, err := etcd.NewBackupSecretResolutionReader(store)
	if err != nil {
		// Rationale: initialization is already failing; store shutdown is
		// best-effort and must not replace the primary typed error.
		_ = store.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	backupSecrets, err := controller.NewBackupSecretResolver(backupSecretEvidence, intentProtector)
	if err != nil {
		// Rationale: initialization is already failing; store shutdown is
		// best-effort and must not replace the primary typed error.
		_ = store.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	coreDNSRenderer, err := registeredCoreDNSRenderer()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize CoreDNS renderer: %w", err)
	}
	actionCatalog, err := newRegisteredActionCatalog()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize registered Component action catalog: %w", err)
	}
	platformRenderPlanner, err := controllerdns.NewPlatformRenderPlanner(
		componentRecords,
		componentRecords,
		componentRecords,
		controllerdns.BaselineCapture(hostresolution.CaptureBaseline),
		coreDNSRenderer,
		actionCatalog,
		actionCatalog,
		managedConfigActivateAction,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize platform Component render planner: %w", err)
	}
	if err := tasks.SetPlatformResolverTaskPreparer(func(
		ctx context.Context,
		current etcd.Versioned[etcd.ComponentRecord],
		projection etcd.HostResolutionProjectionRecord,
		task etcd.TaskRecord,
		priorObservation *etcd.ComponentObservationRecord,
	) (etcd.PlatformComponentTaskRenderInput, error) {
		desired, err := etcd.ProjectComponentRecord(current.Record)
		if err != nil {
			return etcd.PlatformComponentTaskRenderInput{}, err
		}
		return platformRenderPlanner.PrepareConfigTaskAtProjection(
			ctx, current, desired, task, projection, priorObservation,
		)
	}); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: configure platform resolver Task preparer: %w", err)
	}
	if err := tasks.SetPlatformResolverComponentSelector(platformRenderPlanner.SelectResolver); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: configure platform resolver Component selector: %w", err)
	}
	if err := controllerdns.EnsurePlatformResolverTask(
		ctx, componentRecords, tasks, platformRenderPlanner, intentCoordinator,
	); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize platform resolver projection: %w", err)
	}
	platformComponentExecution, err := controllerdns.NewPlatformComponentExecutionPlanner(
		cfg.Storage.VolumeRoot, componentRecords, actionCatalog,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Platform Component execution planner: %w", err)
	}
	if err := planResolver.EnableComponentPlans(platformComponentExecution); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Platform Component plan resolver: %w", err)
	}
	agentRuntime := newAgentChannelRuntimeWithManagedConfigAndScripts(
		authenticator, tasks, planResolver, materializationResolver, backupSecrets, backupCheckpoints,
		platformComponentExecution, scriptArtifacts, scriptCheckpoints,
	)
	staleTasks, err := newStaleAgentTaskMaintenance(agents, agentRuntime.registry, tasks)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize stale Agent task maintenance: %w", err)
	}
	runnerPools, err := cfg.AllocationPools()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Runner allocation: %w", err)
	}
	runnerTokens := runnercapability.NewTokenBroker()
	runnerProvisioning, err := runnercapability.NewProvisioningService(
		runnerRecords, tasks, hierarchyRecords, idempotency, intentCoordinator, runnerTokens,
		runnerallocation.RunnerAllocationConfigFromPools(runnerPools), cfg.Runner.Image,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Runner provisioning service: %w", err)
	}
	runnerMutations, err := runnercapability.NewMutationService(runnerRecords, idempotency, intentCoordinator)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Runner mutation service: %w", err)
	}
	runnerRemovals, err := runnercapability.NewRemovalService(runnerRecords, idempotency, intentCoordinator)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Runner removal service: %w", err)
	}
	releaseGroupMutations, err := newReleaseGroupMutationService(
		releaseGroups, hierarchyRecords, tasks, idempotency, intentCoordinator,
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
	releaseOperations, err := newReleaseOperationService(
		releaseLedger, serviceRecords, releaseGroups, idempotency, intentCoordinator,
		planResolver, scriptRecords, scriptArtifacts, releaseExecutionTimeout,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize release operation service: %w", err)
	}
	hierarchyDeletions, err := newHierarchyDeletionRuntime(store, idempotency, intentCoordinator)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize hierarchy deletion runtime: %w", err)
	}
	backupPolicyIdempotency, err := controller.NewDurableBackupPolicyIdempotency(intentCoordinator, idempotency)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup policy idempotency", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupPolicyKeys, err := controller.NewAgeBackupPolicyKeyFactory(intentProtector)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup policy key factory", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupPolicies, err := controller.NewBackupPolicyService(
		backupPolicyRepository,
		backupPolicyKeys,
		backupPolicyIdempotency,
	)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup policy service", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupPointReads, err := controller.NewRecoveryPointReadService(
		hierarchyRecords,
		backupRuntimeRecords,
		&secretValueCipher{key: controllerKey},
	)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize recovery point reads", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	backupRunRepository, err := controller.NewDurableBackupRunRepository(
		backupRuntimeRecords,
		attachFactValues.ResolveBackupIdentity,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup run repository: %w", err)
	}
	backupRunIdempotency, err := controller.NewDurableBackupRunIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup run idempotency: %w", err)
	}
	backupRuns := controller.NewBackupRunService(
		backupRunRepository,
		controller.BackupRunPlanBuilderFunc(controller.BuildBackupRunPlan),
		backupRunIdempotency,
	)
	backupSchedules, err := controller.NewBackupScheduleService(backupRuntimeRecords, backupRuns, logger)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup scheduler: %w", err)
	}
	backupKeys, err := backupkey.NewService(
		backupPolicyRecords,
		backupPolicyKeys,
		intentCoordinator,
		idempotency,
		intentProtector,
	)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize backup key service", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	networkRecords, err := networketcd.NewRepository(
		hierarchyRecords,
		serviceRecords,
		zoneRecords,
		routeRecords,
		attachRecords,
		tasks,
		idempotency,
		attachFactValues,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Network persistence adapter: %w", err)
	}
	if err := networkRecords.EnableDesiredRevisions(serviceDesiredRevisionRecords); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Network desired revision persistence: %w", err)
	}
	networkCapability, err := networkcontroller.NewEtcdService(
		networkRecords,
		planResolver,
		intentCoordinator,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Network capability: %w", err)
	}
	serviceMutationIdempotency, err := newDurableServiceMutationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service mutation idempotency: %w", err)
	}
	serviceMutations, err := newServiceMutationService(serviceMutationRepository, serviceMutationIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service mutation service: %w", err)
	}
	serviceLifecycleIdempotency, err := newDurableServiceLifecycleIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service lifecycle idempotency: %w", err)
	}
	serviceMutations.lifecycle, err = newServiceLifecycleService(
		serviceMutationRepository,
		planResolver,
		serviceLifecycleIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service lifecycle service: %w", err)
	}
	scriptMutationIdempotency, err := newDurableScriptMutationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script mutation idempotency: %w", err)
	}
	scriptMutations, err := newScriptMutationService(
		scriptMutationRepository, scriptMutationIdempotency, scriptArtifacts,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script mutation service: %w", err)
	}
	scriptDeletionIdempotency, err := newDurableScriptDeletionIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script deletion idempotency: %w", err)
	}
	scriptDeletions, err := newScriptDeletionService(scriptMutationRepository, scriptDeletionIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script deletion service: %w", err)
	}
	scriptMutations.deletions = scriptDeletions
	agentConfigIdempotency, err := newDurableLocalAgentConfigIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent config idempotency: %w", err)
	}
	repositoryAdapter, err := newLocalAgentRepositoryAdapter(agents, agentConfigIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize local Agent repository adapter: %w", err)
	}
	sessionsAdapter, err := newLocalAgentSessionsAdapter(agentRuntime.registry)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize local Agent session adapter: %w", err)
	}
	tasksAdapter, err := newLocalAgentTasksAdapter(tasks, agentRuntime.registry)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize local Agent Task adapter: %w", err)
	}
	entryGeneration, err := NewEntryGenerationService(secretRecords, attachFactValues, intentProtector)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry generation: %w", err)
	}
	entryCreationIdempotency, err := newDurableEntryCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry creation idempotency: %w", err)
	}
	entryBulkUpsertIdempotency, err := newDurableEntryBulkUpsertIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry bulk upsert idempotency: %w", err)
	}
	entryEditIdempotency, err := newDurableEntryEditIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry edit idempotency: %w", err)
	}
	entryRemovalIdempotency, err := newDurableEntryDesiredRemovalIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry removal idempotency: %w", err)
	}
	secretCreationIdempotency, err := newDurableSecretCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret creation idempotency: %w", err)
	}
	secretMutations, err := newSecretCreationService(
		secretReadRepository,
		intentProtector,
		secretCreationIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret creation service: %w", err)
	}
	connectorCreationRepository, err := newDurableConnectorCreationRepository(
		hierarchyRecords,
		secretRecords,
		connectorRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Connector creation repositories: %w", err)
	}
	connectorCreationIdempotency, err := newDurableConnectorCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Connector creation idempotency: %w", err)
	}
	connectorMutations, err := newConnectorCreationService(
		connectorCreationRepository,
		intentProtector,
		connectorCreationIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Connector creation service: %w", err)
	}
	connectorDeletionRepository, err := newDurableConnectorDeletionRepository(hierarchyRecords, connectorRecords)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize Connector deletion repositories", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	connectorDeletionIdempotency, err := newDurableConnectorDeletionIdempotency(intentCoordinator, idempotency)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize Connector deletion idempotency", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	connectorDeletions, err := newConnectorDeletionService(
		connectorDeletionRepository,
		connectorDeletionIdempotency,
	)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize Connector deletion service", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	secretDeletionIdempotency, err := newDurableSecretDeletionIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret deletion idempotency: %w", err)
	}
	secretDeletions, err := newSecretDeletionService(secretReadRepository, secretDeletionIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Secret deletion service: %w", err)
	}
	attachMutationRecords, err := newDurableAttachMutationRepository(
		hierarchyRecords, serviceRecords, attachRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach mutation repository: %w", err)
	}
	attachMutationIdempotency, err := newDurableAttachMutationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach mutation idempotency: %w", err)
	}
	attachMutationPlans, err := newDraftAttachPlanSealer(
		cfg.Storage.VolumeRoot, attachMutationRecords, attachFactValues, componentCatalog,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach draft plan sealer: %w", err)
	}
	attachMutations, err := newAttachMutationService(
		attachMutationRecords, attachFactValues, attachMutationPlans, attachMutationIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Attach mutation service: %w", err)
	}
	taskRetryIdempotency, err := newDurableTaskRetryIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Task retry idempotency: %w", err)
	}
	taskMutations, err := newTaskRetryService(tasks, taskRetryIdempotency, backupRuns)
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
	environmentBlueprintIdempotency, err := desiredrevision.NewIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint idempotency: %w", err)
	}
	desiredRevisionRecords, err := desiredrevisionstore.NewRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize desired revision repository: %w", err)
	}
	environmentBlueprintRepository, err := newDurableEnvironmentBlueprintRepository(
		environmentBlueprintRecords,
		desiredRevisionRecords,
		zoneRecords,
		serviceRecords,
		routeRecords,
		entryRecords,
		entryValues,
		attachRecords,
		componentRecords,
		scriptRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint repositories: %w", err)
	}
	environmentBlueprintRepository.backups, environmentBlueprintRepository.connectors = backupPolicyRecords, connectorRecords
	entryDesiredMutations, err := newEntryDesiredMutationService(
		cfg.Storage.VolumeRoot, environmentBlueprintRepository, entryGeneration, materializationResolver,
		entryCreationIdempotency, entryEditIdempotency, entryRemovalIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry desired mutation service: %w", err)
	}
	entryBulkUpserts, err := newEntryBulkUpsertService(entryDesiredMutations, entryBulkUpsertIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry bulk upsert service: %w", err)
	}
	entryMutations, err := newEntryMutationService(entryDesiredMutations, entryBulkUpserts)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry mutation service: %w", err)
	}
	releaseGroupBlueprints, err := controller.NewReleaseGroupBlueprintPlanner(
		releaseGroups,
		hierarchyRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Release Group Blueprint planner: %w", err)
	}
	environmentBlueprints, err := newEnvironmentBlueprintService(
		cfg.Storage.VolumeRoot,
		cfg.EnvironmentPool,
		environmentBlueprintRepository,
		environmentBlueprintIdempotency,
		materializationResolver,
		releaseGroupBlueprints,
		blueprintReleases,
		entryGeneration,
		attachFactValues,
		componentCatalog,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint service: %w", err)
	}
	environmentBlueprints.backups, environmentBlueprints.backupKeys = environmentBlueprintRepository, backupPolicyKeys
	backingServiceCreations, err := newBackingServiceCreationService(
		cfg.Storage.VolumeRoot,
		runnerPools.Environment,
		environmentBlueprintRepository,
		environmentBlueprintIdempotency,
		intentProtector,
		componentCatalog,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Backing-service creation: %w", err)
	}
	componentCredentials, err := componentcapability.NewCredentialReferenceResolver(
		hierarchyRecords,
		secretReads,
		secretMutations,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Component credentials: %w", err)
	}
	platformComponentMutations, err := controllerdns.NewPlatformMutationService(
		componentRecords, tasks, idempotency, intentCoordinator, coreDNSRenderer, platformRenderPlanner,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Platform Component mutations: %w", err)
	}
	componentMutations, err := componentcapability.NewMutationService(
		componentRecords, environmentBlueprintRepository, environmentBlueprints,
		componentCredentials, platformComponentMutations,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Component mutations: %w", err)
	}
	backingServiceMutations, err := newBackingServiceMutationService(
		backingServiceReads,
		serviceMutations,
		backingServiceCreations,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Backing-service mutations: %w", err)
	}
	volumeMutations, err := volume.NewMutationService(
		cfg.Storage.VolumeRoot,
		environmentBlueprintRepository,
		intentCoordinator,
		idempotency,
		volumeReads,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Volume mutations: %w", err)
	}
	tenantCreationIdempotency, err := newDurableTenantCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Tenant creation idempotency: %w", err)
	}
	tenantMutations, err := newTenantCreationService(hierarchyRecords, tenantCreationIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Tenant creation service: %w", err)
	}
	tenantChangeIdempotency, err := newDurableTenantChangeIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Tenant change idempotency: %w", err)
	}
	tenantChanges, err := newTenantChangeService(hierarchyRecords, tenantChangeIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Tenant change service: %w", err)
	}
	projectCreationIdempotency, err := newDurableProjectCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Project creation idempotency: %w", err)
	}
	projectMutations, err := newProjectCreationService(hierarchyRecords, projectCreationIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Project creation service: %w", err)
	}
	projectChangeIdempotency, err := newDurableProjectChangeIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Project change idempotency: %w", err)
	}
	projectChanges, err := newProjectChangeService(hierarchyRecords, projectChangeIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Project change service: %w", err)
	}
	environmentChangeIdempotency, err := environmentcapability.NewDurableChangeIdempotency(
		intentCoordinator,
		idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment change idempotency: %w", err)
	}
	environmentChanges, err := environmentcapability.NewChangeService(
		cfg.EnvironmentPool,
		hierarchyRecords,
		zoneRecords,
		environmentChangeIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment change service: %w", err)
	}
	environmentCreationIdempotency, err := environmentcapability.NewDurableCreationIdempotency(
		intentCoordinator,
		idempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment creation idempotency: %w", err)
	}
	environmentMutations, err := environmentcapability.NewCreationService(
		cfg.Storage.VolumeRoot,
		cfg.EnvironmentPool,
		hierarchyRecords,
		environmentCreationIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment creation service: %w", err)
	}
	agentEnrollmentIdempotency, err := newDurableAgentEnrollmentIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent enrollment idempotency: %w", err)
	}
	agentEnrollments, err := newAgentEnrollmentService(
		cfg.Agent.Image,
		localagent.Config{
			PullIntervalSeconds: cfg.Agent.Runtime.PullIntervalSeconds,
			MaxConcurrentTasks:  cfg.Agent.Runtime.MaxConcurrentTasks,
			Labels:              cfg.Agent.Runtime.Labels,
		},
		tasks,
		agentEnrollmentIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent enrollment service: %w", err)
	}
	agentRemovalIdempotency, err := newDurableAgentRemovalIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent removal idempotency: %w", err)
	}
	agentUpdateIdempotency, err := newDurableAgentUpdateIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent update idempotency: %w", err)
	}
	credentialCipher, err := newCredentialCipher(controllerKey)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent credential cipher: %w", err)
	}
	credentialManager, err := agentcredential.New(rand.Reader, credentialCipher, credentialCipher)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent credential runtime: %w", err)
	}
	runtimeAdapter, err := newLocalAgentRuntimeAdapter(credentialManager, cfg.Log, cfg.Storage.VolumeRoot)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize local Agent runtime adapter: %w", err)
	}
	containerManager, err := agentcontainer.New(ctx)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize local Agent Docker lifecycle: %w", err)
	}
	containerAdapter, err := newLocalAgentContainerAdapter(containerManager)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize local Agent container adapter: %w", err)
	}
	localAgentManager, err := localagent.New(localagent.Dependencies{
		Repository: repositoryAdapter,
		Runtime:    runtimeAdapter,
		Container:  containerAdapter,
		Sessions:   sessionsAdapter,
		Tasks:      tasksAdapter,
		Clock:      localagent.SystemClock{},
	})
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize local Agent lifecycle: %w", err)
	}
	agentRemovals, err := newAgentRemovalService(localAgentManager, tasks, agentRemovalIdempotency)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent removal service: %w", err)
	}
	agentUpdates, err := newAgentUpdateService(
		cfg.Agent.Image,
		localAgentManager,
		tasks,
		agentUpdateIdempotency,
	)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent update service: %w", err)
	}
	agentMutations, err := newAgentMutationService(agentEnrollments, agentUpdates, agentRemovals)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Agent mutations: %w", err)
	}
	localAgentReconciliation, err := newLocalAgentReconciliation(localAgentManager, tick, logger)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize local Agent reconciliation: %w", err)
	}
	runnerLifecycle, err := newRunnerLifecycleExecutor(logger, runnerRecords, runnerTokens, cfg, runnerPools)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Runner lifecycle: %w", err)
	}
	controllerTaskHandler, err := newControllerTaskHandler(
		localAgentManager,
		backingZoneCascades,
		runnerRecords,
		runnerLifecycle,
	)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Controller Task handler: %w", err)
	}
	backupKeyTaskDispatcher, err := controllertask.NewDispatcher(controllerTaskHandler, backupKeys)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backup key Task dispatcher: %w", err)
	}
	hierarchyTaskDispatcher, err := newHierarchyDeletionTaskDispatcher(backupKeyTaskDispatcher, hierarchyDeletions)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize hierarchy deletion Task dispatcher: %w", err)
	}
	controllerTaskRunner, err := controllertask.New(tasks, hierarchyTaskDispatcher, tick, logger)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Controller Task runner: %w", err)
	}
	taskAborts, err := newTaskAbortService(tasks, agentRuntime.registry, controllerTaskRunner)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Task abort service: %w", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: resolve local hostname: %w", err)
	}
	agentReads, err := newLocalAgentReadService(localAgentManager, tasks, hostname)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize local Agent reads: %w", err)
	}
	hostReads, err := controller.NewHostService(controller.HostDependencies{
		System: &hostSystemSnapshotSource{system: hoststats.New(), docker: containerManager},
		Etcd: &hostEtcdSnapshotSource{
			endpoints: append([]string(nil), etcdEndpoints...),
			probe:     etcd.ProbeEndpoints,
		},
		Agent: &hostAgentSnapshotSource{
			health: localAgentManager,
			fallback: localagent.Config{
				PullIntervalSeconds: cfg.Agent.Runtime.PullIntervalSeconds,
				MaxConcurrentTasks:  cfg.Agent.Runtime.MaxConcurrentTasks,
				Labels:              cfg.Agent.Runtime.Labels,
			},
		},
		ControllerService: hostControllerUnit,
		ControllerVersion: version.Value,
	})
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Host reads: %w", err)
	}

	environmentReads := environmentcapability.NewEtcdReader(
		environmentetcd.NewRepository(hierarchyRecords, zoneRecords),
	)
	logService := controller.NewLogService(environmentReads, serviceReads, releaseLedger, agentRuntime.registry)
	srv := controller.New(store, logger, controller.Options{
		Host: hostReads, ControllerConfig: controllerConfig,
		Agents: agentReads, AgentMutations: agentMutations, Tenants: hierarchyService,
		Projects:                hierarchyService,
		ProjectMutations:        projectMutations,
		ProjectChanges:          projectChanges,
		BackingServices:         backingServiceReads,
		BackingServiceMutations: backingServiceMutations,
		Components:              componentReads,
		ComponentMutations:      componentMutations,
		Environments:            environmentReads,
		Services:                serviceReads,
		ServiceMutations:        serviceMutations,
		Zones:                   networkCapability,
		ZoneMutations:           networkCapability,
		Routes:                  networkCapability,
		RouteMutations:          networkCapability,
		ReleaseGroups:           releaseGroups,
		ReleaseGroupMutations:   releaseGroupMutations,
		Releases:                releaseLedger,
		ReleaseOperations:       releaseOperations,
		Scripts:                 scriptReads,
		ScriptMutations:         scriptMutations,
		Entries:                 entryReads,
		EntryMutations:          entryMutations,
		Secrets:                 secretReads,
		SecretMutations:         secretMutations,
		SecretDeletions:         secretDeletions,
		Connectors:              connectorReads,
		ConnectorMutations:      connectorMutations,
		ConnectorDeletions:      connectorDeletions,
		Runners:                 runnerRecords,
		RunnerProvisioning:      runnerProvisioning,
		RunnerMutations:         runnerMutations,
		RunnerRemovals:          runnerRemovals,
		BackupPolicies:          backupPolicies,
		BackupPolicyMutations:   backupPolicies,
		RecoveryPoints:          backupPointReads,
		BackupRuns:              backupRuns,
		BackupKeyMutations:      backupKeys,
		BackupKeyExports:        backupKeys,
		Volumes:                 volumeReads,
		VolumeMutations:         volumeMutations,
		EnvironmentMutations:    environmentMutations,
		EnvironmentChanges:      environmentChanges,
		EnvironmentBlueprints:   environmentBlueprints,
		HierarchyDeletions:      hierarchyDeletions,
		AttachMutations:         attachMutations,
		AttachFacts:             attachFactReads,
		TaskMutations:           taskMutations,
		TaskAborts:              taskAborts,
		ControllerTaskWake:      controllerTaskRunner.Wake,
		AgentTaskWake:           agentRuntime.registry.WakeTaskDispatch,
		TenantMutations:         tenantMutations,
		TenantChanges:           tenantChanges,
		Console:                 consoleAssets, Tasks: tasks, Logs: logService,
	})

	wired := &Controller{
		Config:          cfg,
		Logger:          logger,
		server:          srv,
		agent:           agentRuntime,
		scheduler:       controller.NewScheduler(srv, tick, tasks, idempotency, staleTasks, backupSchedules),
		controllerTasks: controllerTaskRunner,
		localAgent:      localAgentReconciliation,
		attachMutations: attachMutations,
		container:       containerManager,
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
	if _, registered := adapters.Get("manual"); !registered {
		manual.Register()
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
		wrapControllerRunError("close local Agent Docker lifecycle", containerCloseErr),
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
