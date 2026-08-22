package app

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/logging"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelpercontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/composeobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelpercontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
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

type ownedComposeHelper interface {
	agent.ComposeHelper
	io.Closer
}

type ownedComposeObserver interface {
	agent.ComposeObserver
	io.Closer
}

type ownedEnvironmentDirectoryHelper interface {
	agent.EnvironmentDirectoryHelper
	io.Closer
}

type ownedMaterializationHelper interface {
	agent.MaterializationHelper
	io.Closer
}

type agentRuntimeResources struct {
	compose                *agentComposeResources
	environmentDirectories ownedEnvironmentDirectoryHelper
	materializer           ownedMaterializationHelper
}

type agentComposeResources struct {
	helper   ownedComposeHelper
	observer ownedComposeObserver
}

func NewAgent(ctx context.Context, configPath string) (*Agent, error) {
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

	token, err := readAgentToken(ctx, agentprotocol.TokenPath)
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
	directories, err := agent.NewEnvironmentDirectoryRuntime(directoryHelper)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, errors.Join(directoryHelper.Close(), resources.Close()))
	}
	materializerHelper, err := materializerrunner.New(ctx, image, cfg.Storage.VolumeRoot)
	if err != nil {
		return nil, preferAgentComposeCleanup(err, errors.Join(directoryHelper.Close(), resources.Close()))
	}
	materializer, err := agent.NewMaterializationRuntime(materializerHelper)
	if err != nil {
		return nil, preferAgentComposeCleanup(
			err,
			errors.Join(materializerHelper.Close(), directoryHelper.Close(), resources.Close()),
		)
	}
	runtimeResources := &agentRuntimeResources{
		compose: resources, environmentDirectories: directoryHelper, materializer: materializerHelper,
	}
	client, err := agent.NewClientWithRuntimes(
		agentprotocol.SocketPath,
		cfg.AgentID,
		token,
		cfg.Storage.VolumeRoot,
		logger,
		compose,
		directories,
		materializer,
	)
	if err != nil {
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
	var materializerErr, directoryErr, composeErr error
	if resources.materializer != nil {
		materializerErr = resources.materializer.Close()
	}
	if resources.environmentDirectories != nil {
		directoryErr = resources.environmentDirectories.Close()
	}
	if resources.compose != nil {
		composeErr = resources.compose.Close()
	}
	if joined := errors.Join(materializerErr, directoryErr, composeErr); joined != nil {
		return errs.Wrap(errs.KindInternal, joined)
	}
	return nil
}

func newAgentComposeRuntime(
	image string,
	newHelper func(string) (ownedComposeHelper, error),
	newObserver func() (ownedComposeObserver, error),
) (*agent.ComposeRuntime, *agentComposeResources, error) {
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
	runtime, err := agent.NewComposeRuntime(helper, observer)
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
	if operationErr == nil || errors.Is(operationErr, context.Canceled) || errors.Is(operationErr, context.DeadlineExceeded) {
		return cleanupErr
	}
	return errs.Wrap(errs.KindInternal, errors.Join(operationErr, cleanupErr))
}

func readAgentToken(ctx context.Context, tokenPath string) ([]byte, error) {
	return readAgentTokenForUID(ctx, tokenPath, 0)
}

func readAgentTokenForUID(ctx context.Context, tokenPath string, expectedUID uint32) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(tokenPath) || filepath.Clean(tokenPath) != tokenPath {
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid channel token path")
	}
	directory, err := os.OpenRoot(filepath.Dir(tokenPath))
	if err != nil {
		return nil, errs.New(errs.KindInternal, "agent: read channel token")
	}
	defer directory.Close()
	name := filepath.Base(tokenPath)
	entry, err := directory.Lstat(name)
	if err != nil || !secureTokenFile(entry, expectedUID) {
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid channel token file")
	}
	file, err := directory.Open(name)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "agent: read channel token")
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(entry, opened) || !secureTokenFile(opened, expectedUID) {
		// Best effort: preserve the validation failure if closing also fails.
		_ = file.Close()
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid channel token file")
	}
	encoded, readErr := io.ReadAll(io.LimitReader(file, agentprotocol.EncodedTokenBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		clear(encoded)
		return nil, errs.New(errs.KindInternal, "agent: read channel token")
	}
	defer clear(encoded)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(encoded) != agentprotocol.EncodedTokenBytes {
		return nil, errs.New(errs.KindValidationFailed, "agent: channel token has invalid encoding")
	}
	token := make([]byte, agentprotocol.RawTokenBytes)
	written, err := base64.RawURLEncoding.Strict().Decode(token, encoded)
	if err != nil || written != agentprotocol.RawTokenBytes {
		clear(token)
		return nil, errs.New(errs.KindValidationFailed, "agent: channel token has invalid encoding")
	}
	return token, nil
}

func secureTokenFile(info os.FileInfo, expectedUID uint32) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == expectedUID
}
