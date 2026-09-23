package app

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/app/adaptercompiler"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/logging"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	controllerconfigstore "github.com/AlanD20/groundplane/internal/infra/controllerconfig"
	"github.com/AlanD20/groundplane/internal/infra/docker/etcdcontainer"
	"github.com/AlanD20/groundplane/internal/infra/environmentroot"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type controllerBootstrapComposition struct {
	config           config.ControllerConfig
	controllerConfig *controllerconfigstore.Store
	tick             time.Duration
	logger           *slog.Logger
	consoleAssets    fs.FS
	etcdLifecycle    *etcdcontainer.Manager
	etcdEndpoints    []string
	store            controllerStore
	componentCatalog []componentrender.EnvironmentComponentRegistration
}

type controllerStore interface {
	etcd.EnvironmentBlueprintStore
	volumeEvidenceStore
}

// newControllerBootstrapComposition owns the native resources needed before
// capability construction. A successful return transfers their cleanup to
// NewController; every failure here closes the resources already acquired.
func newControllerBootstrapComposition(
	ctx context.Context,
	configPath string,
) (controllerBootstrapComposition, error) {
	adaptercompiler.Register()
	componentCatalog, err := componentregistration.EnvironmentCatalog()
	if err != nil {
		return controllerBootstrapComposition{}, fmt.Errorf(
			"controller: initialize registered Components: %w",
			err,
		)
	}
	cfg, startupDocument, err := loadControllerConfigDocument(ctx, configPath)
	if err != nil {
		return controllerBootstrapComposition{}, err
	}
	controllerConfig, err := controllerconfigstore.New(ctx, configPath, startupDocument)
	if err != nil {
		return controllerBootstrapComposition{}, fmt.Errorf(
			"controller: initialize Controller config store: %w",
			err,
		)
	}
	if _, err := environmentroot.Validate(ctx, cfg.Storage.VolumeRoot); err != nil {
		return controllerBootstrapComposition{}, fmt.Errorf(
			"controller: validate Environment volume root: %w",
			err,
		)
	}
	tick, err := time.ParseDuration(cfg.Scheduler.TickInterval)
	if err != nil {
		return controllerBootstrapComposition{}, fmt.Errorf(
			"controller: parse scheduler tick interval: %w",
			err,
		)
	}
	if tick <= 0 {
		return controllerBootstrapComposition{}, fmt.Errorf(
			"controller: scheduler tick interval must be positive",
		)
	}
	level, err := logging.ResolveLevel(false, false, os.Getenv("GROUNDPLANE_LOG_LEVEL"), cfg.Log.Level)
	if err != nil {
		return controllerBootstrapComposition{}, err
	}
	logger, err := logging.Setup(logging.Options{
		Level:   level,
		Console: logging.ConsoleConfig{Enabled: cfg.Log.Console.Enabled},
		File:    logging.FileConfig{Enabled: cfg.Log.File.Enabled, Path: cfg.Log.File.Path},
	})
	if err != nil {
		return controllerBootstrapComposition{}, err
	}
	consoleAssets, err := controllerConsoleAssets()
	if err != nil {
		return controllerBootstrapComposition{}, errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("controller: load embedded Console: %w", err),
		)
	}
	etcdLifecycle, err := etcdcontainer.New(ctx)
	if err != nil {
		return controllerBootstrapComposition{}, fmt.Errorf(
			"controller: initialize etcd container lifecycle: %w",
			err,
		)
	}
	keepEtcdLifecycle := false
	defer func() {
		if !keepEtcdLifecycle {
			_ = etcdLifecycle.Close()
		}
	}()
	if err := etcdLifecycle.Reconcile(ctx); err != nil {
		return controllerBootstrapComposition{}, fmt.Errorf("controller: reconcile etcd container: %w", err)
	}
	etcdEndpoints := etcdcontainer.Endpoints()
	store, err := etcd.New(ctx, etcdEndpoints, cfg.Etcd.KeyPrefix)
	if err != nil {
		return controllerBootstrapComposition{}, fmt.Errorf("controller: initialize etcd: %w", err)
	}
	if err := bootstrapRunnerNetworkPool(ctx, store, cfg); err != nil {
		_ = store.Close()
		return controllerBootstrapComposition{}, fmt.Errorf("controller: reserve Runner network pool: %w", err)
	}
	keepEtcdLifecycle = true
	return controllerBootstrapComposition{
		config:           cfg,
		controllerConfig: controllerConfig,
		tick:             tick,
		logger:           logger,
		consoleAssets:    consoleAssets,
		etcdLifecycle:    etcdLifecycle,
		etcdEndpoints:    etcdEndpoints,
		store:            store,
		componentCatalog: componentCatalog,
	}, nil
}
