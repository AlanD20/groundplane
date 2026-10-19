package app

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/AlanD20/groundplane/internal/agent/backupruntime"

	"github.com/AlanD20/groundplane/internal/agent/attachruntime"
	"github.com/AlanD20/groundplane/internal/agent/backingadapter"
	"github.com/AlanD20/groundplane/internal/agent/backingruntime"
	"github.com/AlanD20/groundplane/internal/agent/componentruntime"
	"github.com/AlanD20/groundplane/internal/agent/entryruntime"
	"github.com/AlanD20/groundplane/internal/agent/environmentruntime"
	"github.com/AlanD20/groundplane/internal/agent/hierarchyruntime"
	"github.com/AlanD20/groundplane/internal/agent/lifecycleruntime"
	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/agent/networkruntime"
	"github.com/AlanD20/groundplane/internal/agent/releaseruntime"
	"github.com/AlanD20/groundplane/internal/agent/removalruntime"
	"github.com/AlanD20/groundplane/internal/agent/resolverruntime"
	scriptruntime "github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/agent/volumeruntime"
	"github.com/AlanD20/groundplane/internal/app/adaptercompiler"
	"github.com/AlanD20/groundplane/internal/app/agentplatform"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"github.com/AlanD20/groundplane/internal/infra/agentcredential"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/agent/componentaction"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/logging"
	commonrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/backingcommand"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelpercontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/composeobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/containerlogs"
	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/hostresolutionhelpercontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/internal/infra/docker/scriptrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const DefaultAgentConfigPath = agentprotocol.RuntimeConfigPath

// Agent is the wired Agent binary: config, logging, and the gRPC
// client. cmd/agent/main.go is nothing but NewAgent + Run.
type Agent struct {
	Config    config.AgentConfig
	Logger    *slog.Logger
	Client    *agent.Client
	resources *agentRuntimeResources
}

func NewAgent(ctx context.Context, configPath string) (result *Agent, resultErr error) {
	adaptercompiler.Register()
	cfg := config.DefaultAgentConfig()
	if err := config.Load(ctx, configPath, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ownership, err := agentownership.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if result == nil {
			resultErr = preferAgentComposeCleanup(resultErr, ownership.Close())
		}
	}()

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

	token, err := agentcredential.ReadChannelToken(ctx, agentprotocol.TokenPath, 0)
	if err != nil {
		return nil, err
	}
	defer clear(token)
	image := os.Getenv(agentprotocol.AgentImageEnv)
	resources, err := newAgentComposeResources(
		image,
		func(image string) (ownedComposeHelper, error) {
			return composehelpercontainer.New(image)
		},
		func() (ownedComposeObserver, error) {
			return composeobserver.New()
		},
	)
	if err != nil {
		return nil, err
	}
	materializerHelper, err := materializerrunner.New(ctx, image, cfg.Storage.VolumeRoot)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, resources.Close())
	}
	managedConfigs, err := managedconfighelpercontainer.New(image)
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(materializerHelper.Close(), resources.Close()),
		)
	}
	actionCatalog, err := componentregistration.NewCatalog()
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(managedConfigs.Close(), materializerHelper.Close(), resources.Close()),
		)
	}
	componentFiles, err := agentplatform.NewComponentFileValidator(actionCatalog, managedConfigs)
	if err != nil {
		return nil, preferAgentComposeCleanup(err,
			errors.Join(managedConfigs.Close(), materializerHelper.Close(), resources.Close()))
	}
	materializer, err := filematerialization.New(materializerHelper, componentFiles)
	if err != nil {
		return nil, preferAgentComposeCleanup(err,
			errors.Join(managedConfigs.Close(), materializerHelper.Close(), resources.Close()))
	}
	dnsObserver, err := dnsresolverobserver.New()
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(
				dnsObserver.Close(),
				managedConfigs.Close(),
				materializerHelper.Close(),
				resources.Close(),
			),
		)
	}
	componentActions, err := componentaction.New(
		actionCatalog, dnsObserver,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(
				dnsObserver.Close(),
				managedConfigs.Close(),
				materializerHelper.Close(),
				resources.Close(),
			),
		)
	}
	hostResolution, err := hostresolutionhelpercontainer.New(image)
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(managedConfigs.Close(), materializerHelper.Close(), resources.Close()),
		)
	}
	logReader, err := containerlogs.New()
	if err != nil {
		return nil, preferAgentComposeCleanup(
			errs.Wrap(errs.KindInternal, err),
			errors.Join(
				hostResolution.Close(),
				dnsObserver.Close(),
				managedConfigs.Close(),
				materializerHelper.Close(),
				resources.Close(),
			),
		)
	}
	scriptEngine, err := scriptrunner.New(ctx)
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(
				logReader.Close(),
				hostResolution.Close(),
				dnsObserver.Close(),
				managedConfigs.Close(),
				materializerHelper.Close(),
				resources.Close(),
			),
		)
	}
	scripts, err := scriptruntime.New(scriptEngine)
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(
				scriptEngine.Close(),
				logReader.Close(),
				hostResolution.Close(),
				dnsObserver.Close(),
				managedConfigs.Close(),
				materializerHelper.Close(),
				resources.Close(),
			),
		)
	}
	runtimeResources := &agentRuntimeResources{
		ownership: ownership,
		compose:   resources, materializer: materializerHelper,
		managedConfigs: managedConfigs, dnsResolverObserver: dnsObserver,
		hostResolution: hostResolution, logs: logReader, scripts: scripts,
	}
	client, err := agent.NewClient(
		agentprotocol.SocketPath,
		cfg.AgentID,
		token,
		cfg.Storage.VolumeRoot,
		logger,
		logReader,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetProcessOwnership(ownership); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	workJournal, err := agentworkjournal.Open(agentprotocol.StatePath + "/work")
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	runtimeResources.workJournal = workJournal
	if err := client.SetWorkJournal(workJournal); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	scriptExecutor, err := scriptruntime.NewExecutor(scripts, workJournal)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetScriptExecutor(scriptExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	releaseExecutor, err := releaseruntime.NewExecutor(
		cfg.Storage.VolumeRoot,
		workJournal,
		materializer,
		scripts,
		resources.helper,
		resources.observer,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetReleaseExecutor(releaseExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}

	environmentExecutor, err := environmentruntime.NewExecutor(cfg.Storage.VolumeRoot, workJournal, resources.helper)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetEnvironmentExecutor(environmentExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	hierarchyHost, err := hierarchyruntime.NewHost(cfg.Storage.VolumeRoot, workJournal, resources.helper, ownership)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	hierarchyExecutor, err := hierarchyruntime.NewExecutor(cfg.Storage.VolumeRoot, workJournal, hierarchyHost)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetHierarchyExecutor(hierarchyExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	processes := commonrunner.New(logger)
	entryExecutor, err := entryruntime.NewExecutor(workJournal, materializer, resources.helper, ownership)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetEntryExecutor(entryExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	componentExecutor, err := componentruntime.NewExecutor(workJournal, materializer, resources.helper, ownership)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetComponentExecutor(componentExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	networkHost, err := networkruntime.NewHost(workJournal, resources.helper, ownership)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	networkExecutor, err := networkruntime.NewExecutor(workJournal, networkHost)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetNetworkExecutor(networkExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	volumeHost, err := volumeruntime.NewHost(cfg.Storage.VolumeRoot, workJournal, resources.helper, ownership)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	volumeExecutor, err := volumeruntime.NewExecutor(cfg.Storage.VolumeRoot, workJournal, volumeHost)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetVolumeExecutor(volumeExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	runtimeResources.dockerReads, err = agentplatform.ConfigureDockerReads(ctx, client, componentActions)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	backingCommands, err := backingcommand.New(runtimeResources.dockerReads)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	backingExecutor, err := backingruntime.NewExecutor(
		cfg.Storage.VolumeRoot,
		workJournal,
		materializer,
		resources.helper,
		backingCommands,
		ownership,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetBackingExecutor(backingExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	resolverExecutor, err := resolverruntime.New(workJournal, resources.helper, ownership)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetResolverExecutor(resolverExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	lifecycleHost, err := lifecycleruntime.NewHost(
		cfg.Storage.VolumeRoot,
		workJournal,
		resources.helper,
		ownership,
		backingCommands,
		processes,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	lifecycleExecutor, err := lifecycleruntime.NewExecutor(cfg.Storage.VolumeRoot, workJournal, lifecycleHost)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetLifecycleExecutor(lifecycleExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	backupExecutor, err := backupruntime.NewExecutor(
		workJournal,
		cfg.Storage.VolumeRoot,
		resources.helper,
		materializer,
		ownership,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetBackupExecutor(backupExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}

	removalRecovery, err := removalruntime.NewRuntime(
		cfg.Storage.VolumeRoot,
		workJournal,
		resources.helper,
		materializer,
		ownership,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetRemovalRecovery(removalRecovery); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	backingAdapter := backingadapter.New(processes, adaptercompiler.Compile)
	provisionHost, err := attachruntime.NewBackingHost(
		workJournal,
		backingAdapter,
		ownership,
		backingCommands,
		resources.helper,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	provisionExecutor, err := attachruntime.NewExecutor(workJournal, provisionHost)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetAttachProvisionExecutor(provisionExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	attachHost, err := hierarchyruntime.NewBackingHost(workJournal, backingAdapter, ownership, backingCommands)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	attachRuntime, err := hierarchyruntime.NewAttachRuntimeHost(attachHost, workJournal, resources.helper, ownership)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	attachExecutor, err := hierarchyruntime.NewAttachExecutor(workJournal, attachRuntime)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetAttachRemovalExecutor(attachExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	recoveryExecutor, err := releaseruntime.NewRecoveryExecutor(
		cfg.Storage.VolumeRoot,
		workJournal,
		materializer,
		scripts,
		resources.helper,
		ownership,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetReleaseRecoveryExecutor(recoveryExecutor); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetDNSConnectionChecker(componentActions); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	return &Agent{Config: cfg, Logger: logger, Client: client, resources: runtimeResources}, nil
}

func (a *Agent) Run(ctx context.Context) error {
	if a.resources == nil {
		return errs.New(errs.KindStateConflict, "Agent runtime ownership is unavailable")
	}
	if err := a.resources.ownership.Check(); err != nil {
		return preferAgentComposeCleanup(err, a.resources.Close())
	}
	runErr := a.Client.Run(ctx)
	return preferAgentComposeCleanup(runErr, a.resources.Close())
}
