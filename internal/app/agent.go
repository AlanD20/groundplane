package app

import (
	"context"
	"errors"
	composeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	directoryruntime "github.com/AlanD20/groundplane/internal/agent/environmentdirectory"
	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	scriptruntime "github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/componentregistration"
	"github.com/AlanD20/groundplane/internal/infra/agentcredential"
	"io"
	"log/slog"
	"os"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/agent/componentaction"
	"github.com/AlanD20/groundplane/internal/agent/hostresolution"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/logging"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelpercontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/composeobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/containerlogs"
	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelpercontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/hostresolutionhelpercontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/internal/infra/docker/scriptrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
	mobyclient "github.com/moby/moby/client"
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

type ownedComposeHelper interface {
	composeruntime.Helper
	io.Closer
}

type ownedComposeObserver interface {
	composeruntime.Observer
	io.Closer
}

type ownedEnvironmentDirectoryHelper interface {
	directoryruntime.Helper
	io.Closer
}

type ownedMaterializationHelper interface {
	filematerialization.Helper
	io.Closer
}

type agentRuntimeResources struct {
	compose                *agentComposeResources
	environmentDirectories ownedEnvironmentDirectoryHelper
	materializer           ownedMaterializationHelper
	managedConfigs         *managedconfighelpercontainer.Executor
	dnsResolverObserver    *dnsresolverobserver.Executor
	hostResolution         *hostresolutionhelpercontainer.Executor
	logs                   *containerlogs.Reader
	scripts                *scriptruntime.Runtime
	dockerReads            *mobyclient.Client
}

type agentComposeResources struct {
	helper   ownedComposeHelper
	observer ownedComposeObserver
}

func NewAgent(ctx context.Context, configPath string) (*Agent, error) {
	registerAdapters()
	cfg := config.DefaultAgentConfig()
	if err := config.Load(ctx, configPath, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
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

	token, err := agentcredential.ReadChannelToken(ctx, agentprotocol.TokenPath, 0)
	if err != nil {
		return nil, err
	}
	defer clear(token)
	image := os.Getenv(agentprotocol.AgentImageEnv)
	compose, resources, err := newAgentComposeRuntime(
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
	directoryHelper, err := environmentdirectoryhelpercontainer.New(
		os.Getenv(agentprotocol.AgentImageEnv),
		cfg.Storage.VolumeRoot,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, resources.Close())
	}
	directories, err := directoryruntime.New(directoryHelper)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, errors.Join(directoryHelper.Close(), resources.Close()))
	}
	materializerHelper, err := materializerrunner.New(ctx, image, cfg.Storage.VolumeRoot)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, errors.Join(directoryHelper.Close(), resources.Close()))
	}
	managedConfigs, err := managedconfighelpercontainer.New(image)
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(materializerHelper.Close(), directoryHelper.Close(), resources.Close()),
		)
	}
	actionCatalog, err := componentregistration.NewCatalog()
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(managedConfigs.Close(), materializerHelper.Close(), directoryHelper.Close(), resources.Close()),
		)
	}
	componentFiles, err := newComponentFileValidator(actionCatalog, managedConfigs)
	if err != nil {
		return nil, preferAgentComposeCleanup(err,
			errors.Join(managedConfigs.Close(), materializerHelper.Close(), directoryHelper.Close(), resources.Close()))
	}
	materializer, err := filematerialization.New(materializerHelper, componentFiles)
	if err != nil {
		return nil, preferAgentComposeCleanup(err,
			errors.Join(managedConfigs.Close(), materializerHelper.Close(), directoryHelper.Close(), resources.Close()))
	}
	dnsObserver, err := dnsresolverobserver.New()
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(
				dnsObserver.Close(),
				managedConfigs.Close(),
				materializerHelper.Close(),
				directoryHelper.Close(),
				resources.Close(),
			),
		)
	}
	componentActions, err := componentaction.New(
		actionCatalog, managedConfigs, resources.helper, dnsObserver,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(
				dnsObserver.Close(),
				managedConfigs.Close(),
				materializerHelper.Close(),
				directoryHelper.Close(),
				resources.Close(),
			),
		)
	}
	hostResolution, err := hostresolutionhelpercontainer.New(image)
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(managedConfigs.Close(), materializerHelper.Close(), directoryHelper.Close(), resources.Close()),
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
				directoryHelper.Close(),
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
				directoryHelper.Close(),
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
				directoryHelper.Close(),
				resources.Close(),
			),
		)
	}
	runtimeResources := &agentRuntimeResources{
		compose: resources, environmentDirectories: directoryHelper, materializer: materializerHelper,
		managedConfigs: managedConfigs, dnsResolverObserver: dnsObserver,
		hostResolution: hostResolution, logs: logReader, scripts: scripts,
	}
	client, err := agent.NewClientWithLogReader(
		agentprotocol.SocketPath,
		cfg.AgentID,
		token,
		cfg.Storage.VolumeRoot,
		logger,
		compose,
		directories,
		materializer,
		logReader,
	)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetComponentActionRuntime(componentActions); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetHostResolutionRuntime(hostresolution.New(hostResolution)); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := client.SetScriptRuntime(scripts); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}
	if err := configureAgentDockerReads(ctx, client, runtimeResources); err != nil {
		return nil, preferAgentComposeCleanup(err, runtimeResources.Close())
	}

	return &Agent{Config: cfg, Logger: logger, Client: client, resources: runtimeResources}, nil
}

func (a *Agent) Run(ctx context.Context) error {
	runErr := a.Client.Run(ctx)
	if a.resources == nil {
		return runErr
	}
	return preferAgentComposeCleanup(runErr, a.resources.Close())
}

func (resources *agentRuntimeResources) Close() error {
	if resources == nil {
		return nil
	}
	var logErr, scriptErr, hostResolutionErr, managedConfigErr, dnsObserverErr, materializerErr, directoryErr, composeErr error
	var imageErr error
	if resources.dockerReads != nil {
		imageErr = resources.dockerReads.Close()
	}
	if resources.logs != nil {
		logErr = resources.logs.Close()
	}
	if resources.scripts != nil {
		scriptErr = resources.scripts.Close()
	}
	if resources.materializer != nil {
		materializerErr = resources.materializer.Close()
	}
	if resources.managedConfigs != nil {
		managedConfigErr = resources.managedConfigs.Close()
	}
	if resources.dnsResolverObserver != nil {
		dnsObserverErr = resources.dnsResolverObserver.Close()
	}
	if resources.hostResolution != nil {
		hostResolutionErr = resources.hostResolution.Close()
	}
	if resources.environmentDirectories != nil {
		directoryErr = resources.environmentDirectories.Close()
	}
	if resources.compose != nil {
		composeErr = resources.compose.Close()
	}
	if joined := errors.Join(
		imageErr,
		logErr,
		scriptErr,
		hostResolutionErr,
		managedConfigErr,
		dnsObserverErr,
		materializerErr,
		directoryErr,
		composeErr,
	); joined != nil {
		return errs.Wrap(errs.KindInternal, joined)
	}
	return nil
}

func newAgentComposeRuntime(
	image string,
	newHelper func(string) (ownedComposeHelper, error),
	newObserver func() (ownedComposeObserver, error),
) (*composeruntime.Runtime, *agentComposeResources, error) {
	if image == "" {
		return nil, nil, errs.New(errs.KindValidationFailed, "agent: managed Agent image is required")
	}
	if newHelper == nil || newObserver == nil {
		return nil, nil, errs.New(errs.KindInternal, "agent: Compose runtime factories are required")
	}
	helper, err := newHelper(image)
	if err != nil {
		return nil, nil, err
	}
	observer, err := newObserver()
	if err != nil {
		return nil, nil, preferAgentComposeCleanup(err, helper.Close())
	}
	resources := &agentComposeResources{helper: helper, observer: observer}
	runtime, err := composeruntime.New(helper, observer)
	if err != nil {
		return nil, nil, preferAgentComposeCleanup(err, resources.Close())
	}
	return runtime, resources, nil
}

func (resources *agentComposeResources) Close() error {
	if resources == nil {
		return nil
	}
	var observerErr, helperErr error
	if resources.observer != nil {
		observerErr = resources.observer.Close()
	}
	if resources.helper != nil {
		helperErr = resources.helper.Close()
	}
	if joined := errors.Join(observerErr, helperErr); joined != nil {
		return errs.Wrap(errs.KindInternal, joined)
	}
	return nil
}

func preferAgentComposeCleanup(operationErr, cleanupErr error) error {
	if cleanupErr == nil {
		return operationErr
	}
	if operationErr == nil || errors.Is(operationErr, context.Canceled) ||
		errors.Is(operationErr, context.DeadlineExceeded) {
		return cleanupErr
	}
	return errs.Wrap(errs.KindInternal, errors.Join(operationErr, cleanupErr))
}
