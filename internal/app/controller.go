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
	"github.com/AlanD20/groundplane/internal/common/version"
	agentcomponent "github.com/AlanD20/groundplane/internal/components/agent"
	"github.com/AlanD20/groundplane/internal/components/caddy"
	"github.com/AlanD20/groundplane/internal/components/cloudflaretunnel"
	controllercomponent "github.com/AlanD20/groundplane/internal/components/controller"
	"github.com/AlanD20/groundplane/internal/components/coredns"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	hierarchycontroller "github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/agentcredential"
	"github.com/AlanD20/groundplane/internal/infra/docker/agentcontainer"
	"github.com/AlanD20/groundplane/internal/infra/environmentroot"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
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
	store           ownedStore
}

type controllerServer interface {
	Serve(ctx context.Context, addr string) error
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

// NewController performs every piece of this binary's DI wiring, once,
// explicitly — including adapter and component registration
// (postgres16.Register(), valkey9.Register(), manual.Register(),
// and all five component registrations), which replaces the
// init()-based self-registration architecture.md originally sketched:
// this project bans init() globals (docs/standards.md, section 11), so
// the "one package + one registration line" extensibility promise is
// kept by putting the registration line here instead of behind a blank
// import's side effect.
func NewController(ctx context.Context, configPath string) (*Controller, error) {
	registerAdapters()
	registerComponents()

	cfg, err := loadControllerConfig(ctx, configPath)
	if err != nil {
		return nil, err
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

	store, err := etcd.New(ctx, cfg.Etcd.Endpoints, cfg.Etcd.KeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize etcd: %w", err)
	}
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Task repository: %w", err)
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
	routeReadRepository, err := newDurableRouteReadRepository(hierarchyRecords, routeRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Route read repositories: %w", err)
	}
	routeReads, err := newRouteReadService(routeReadRepository)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Route reads: %w", err)
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
	zoneReadRepository, err := newDurableZoneReadRepository(hierarchyRecords, zoneRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Zone read repositories: %w", err)
	}
	zoneReads, err := newZoneReadService(zoneReadRepository)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Zone reads: %w", err)
	}
	serviceMutationRepository, err := newDurableServiceMutationRepository(hierarchyRecords, serviceRecords, zoneRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Service mutation repositories: %w", err)
	}
	zoneCreationRepository, err := newDurableZoneCreationRepository(hierarchyRecords, zoneRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Zone creation repositories: %w", err)
	}
	zoneDeletionRepository, err := newDurableZoneDeletionRepository(hierarchyRecords, zoneRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Zone deletion repositories: %w", err)
	}
	routeMutationRepository, err := newDurableRouteMutationRepository(
		hierarchyRecords, serviceRecords, routeRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Route mutation repositories: %w", err)
	}
	scriptMutationRepository, err := newDurableScriptMutationRepository(
		hierarchyRecords, serviceRecords, scriptRecords,
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
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize execution plan resolver: %w", err)
	}
	materializationResolver, err := controller.NewTaskMaterializationResolver(
		hierarchyRecords,
		entryValues,
		planResolver,
		intentProtector,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize materialization value resolver: %w", err)
	}
	agentRuntime := newAgentChannelRuntime(authenticator, tasks, planResolver, materializationResolver)
	staleTasks, err := newStaleAgentTaskMaintenance(agents, agentRuntime.registry, tasks)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize stale Agent task maintenance: %w", err)
	}
	intentCoordinator, err := idempotentintent.NewCoordinator(intentProtector)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize idempotent intent coordinator: %w", err)
	}
	zoneCreationIdempotency, err := newDurableZoneCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Zone creation idempotency: %w", err)
	}
	zoneMutations, err := newZoneCreationService(zoneCreationRepository, zoneCreationIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Zone creation service: %w", err)
	}
	zoneDeletionIdempotency, err := newDurableZoneDeletionIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Zone deletion idempotency: %w", err)
	}
	zoneDeletions, err := newZoneDeletionService(
		zoneDeletionRepository,
		planResolver,
		zoneDeletionIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Zone deletion service: %w", err)
	}
	zoneImpacts, err := newZoneRemovalImpactService(
		zoneDeletionRepository,
		attachRecords,
		serviceRecords,
		attachFactValues,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Zone removal impact service: %w", err)
	}
	zoneMutations.deletions = zoneDeletions
	zoneMutations.impacts = zoneImpacts
	zoneDeletions.impacts = zoneImpacts
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
	routeMutationIdempotency, err := newDurableRouteMutationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Route mutation idempotency: %w", err)
	}
	routeMutations, err := newRouteMutationService(routeMutationRepository, routeMutationIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Route mutation service: %w", err)
	}
	scriptMutationIdempotency, err := newDurableScriptMutationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Script mutation idempotency: %w", err)
	}
	scriptMutations, err := newScriptMutationService(scriptMutationRepository, scriptMutationIdempotency)
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
	routeDeletionIdempotency, err := newDurableRouteDeletionIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Route deletion idempotency: %w", err)
	}
	routeDeletions, err := newRouteDeletionService(
		routeMutationRepository,
		planResolver,
		routeDeletionIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Route deletion service: %w", err)
	}
	routeMutations.deletions = routeDeletions
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
	entryCreationRepository, err := newDurableEntryCreationRepository(
		hierarchyRecords,
		entryRecords,
		serviceRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry creation repositories: %w", err)
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
	entryCreationMutations, err := newEntryCreationService(
		entryCreationRepository,
		entryGeneration,
		entryCreationIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry creation service: %w", err)
	}
	entryEditRepository, err := newDurableEntryEditRepository(entryCreationRepository)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry edit repositories: %w", err)
	}
	entryEditIdempotency, err := newDurableEntryEditIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry edit idempotency: %w", err)
	}
	entryEditMutations, err := newEntryEditService(
		entryEditRepository,
		entryGeneration,
		entryEditIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry edit service: %w", err)
	}
	entryDeletionRepository, err := newDurableEntryDeletionRepository(entryCreationRepository, componentRecords)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry deletion repositories: %w", err)
	}
	entryDeletionIdempotency, err := newDurableEntryDeletionIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry deletion idempotency: %w", err)
	}
	entryDeletions, err := newEntryDeletionService(
		entryDeletionRepository,
		planResolver,
		materializationResolver,
		entryDeletionIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry deletion service: %w", err)
	}
	entryMutations, err := newEntryMutationService(entryCreationMutations, entryEditMutations, entryDeletions)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Entry mutation service: %w", err)
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
		cfg.Storage.VolumeRoot, attachMutationRecords, attachFactValues,
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
	taskMutations, err := newTaskRetryService(tasks, taskRetryIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Task retry service: %w", err)
	}
	backingZoneCascades, err := newBackingZoneCascadeService(
		&durableBackingZoneCascadeRepository{
			hierarchy: hierarchyRecords,
			zones:     zoneRecords,
			attaches:  attachRecords,
			tasks:     tasks,
		},
		attachMutations,
		taskMutations,
		planResolver,
		zoneDeletionIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize backing Zone cascade: %w", err)
	}
	environmentBlueprintIdempotency, err := newDurableEnvironmentBlueprintIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint idempotency: %w", err)
	}
	environmentBlueprintRepository, err := newDurableEnvironmentBlueprintRepository(
		hierarchyRecords,
		zoneRecords,
		serviceRecords,
		routeRecords,
		entryRecords,
		componentRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint repositories: %w", err)
	}
	environmentBlueprints, err := newEnvironmentBlueprintService(
		cfg.Storage.VolumeRoot,
		environmentBlueprintRepository,
		environmentBlueprintIdempotency,
		materializationResolver,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment Blueprint service: %w", err)
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
	environmentChangeIdempotency, err := newDurableEnvironmentChangeIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment change idempotency: %w", err)
	}
	environmentChanges, err := newEnvironmentChangeService(hierarchyRecords, environmentChangeIdempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment change service: %w", err)
	}
	environmentDeletionIdempotency, err := newDurableEnvironmentDeletionIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment deletion idempotency: %w", err)
	}
	environmentDeletions, err := newEnvironmentDeletionService(
		hierarchyRecords,
		planResolver,
		environmentDeletionIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment deletion service: %w", err)
	}
	environmentCreationIdempotency, err := newDurableEnvironmentCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Environment creation idempotency: %w", err)
	}
	environmentMutations, err := newEnvironmentCreationService(
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
	controllerTaskHandler, err := newControllerTaskHandler(localAgentManager, backingZoneCascades)
	if err != nil {
		_ = containerManager.Close()
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Controller Task handler: %w", err)
	}
	controllerTaskRunner, err := controllertask.New(tasks, controllerTaskHandler, tick, logger)
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
			endpoints: append([]string(nil), cfg.Etcd.Endpoints...),
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

	srv := controller.New(store, logger, controller.Options{
		Host: hostReads, Agents: agentReads, AgentMutations: agentMutations, Tenants: hierarchyService,
		Projects:              hierarchyService,
		ProjectMutations:      projectMutations,
		ProjectChanges:        projectChanges,
		BackingServices:       backingServiceReads,
		Environments:          hierarchyRecords,
		Services:              serviceReads,
		ServiceMutations:      serviceMutations,
		Zones:                 zoneReads,
		ZoneMutations:         zoneMutations,
		Routes:                routeReads,
		RouteMutations:        routeMutations,
		Scripts:               scriptReads,
		ScriptMutations:       scriptMutations,
		Entries:               entryReads,
		EntryMutations:        entryMutations,
		Secrets:               secretReads,
		SecretMutations:       secretMutations,
		SecretDeletions:       secretDeletions,
		Connectors:            connectorReads,
		ConnectorMutations:    connectorMutations,
		EnvironmentMutations:  environmentMutations,
		EnvironmentChanges:    environmentChanges,
		EnvironmentBlueprints: environmentBlueprints,
		EnvironmentDeletions:  environmentDeletions,
		AttachMutations:       attachMutations,
		AttachFacts:           attachFactReads,
		TaskMutations:         taskMutations,
		TaskAborts:            taskAborts,
		TenantMutations:       tenantMutations,
		TenantChanges:         tenantChanges,
		Console:               consoleAssets, Tasks: tasks,
	})

	return &Controller{
		Config:          cfg,
		Logger:          logger,
		server:          srv,
		agent:           agentRuntime,
		scheduler:       controller.NewScheduler(srv, tick, tasks, idempotency, staleTasks),
		controllerTasks: controllerTaskRunner,
		localAgent:      localAgentReconciliation,
		attachMutations: attachMutations,
		container:       containerManager,
		store:           store,
	}, nil
}

// registerAdapters is the ONE explicit registration point — the
// extensibility seam's actual "one line" per adapter, called from here
// instead of relying on package-import side effects.
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

// registerComponents is registerAdapters' twin for the owner-aware component
// seam.
func registerComponents() {
	caddy.Register()
	cloudflaretunnel.Register()
	coredns.Register()
	controllercomponent.Register()
	agentcomponent.Register()
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
	runtimeDone := make(chan runtimeResult, 2)
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

	runtimeErrors := make(map[string]error, 2)
	for index := 0; index < 2; index++ {
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
	joined := errors.Join(
		wrapControllerRunError("serve HTTP", runtimeErrors["serve HTTP"]),
		wrapControllerRunError("serve Agent channel", runtimeErrors["serve Agent channel"]),
		wrapControllerRunError("close local Agent Docker lifecycle", containerCloseErr),
		wrapControllerRunError("close etcd", closeErr),
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
