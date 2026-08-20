package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters/manual"
	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/adapters/valkey9"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/logging"
	agentcomponent "github.com/AlanD20/groundplane/internal/components/agent"
	"github.com/AlanD20/groundplane/internal/components/caddy"
	"github.com/AlanD20/groundplane/internal/components/cloudflaretunnel"
	controllercomponent "github.com/AlanD20/groundplane/internal/components/controller"
	"github.com/AlanD20/groundplane/internal/components/coredns"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
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
// adapter registry, the etcd store, the HTTP server, and the scheduler.
// cmd/controller/main.go is nothing but NewController + Run.
type Controller struct {
	Config config.ControllerConfig
	Logger *slog.Logger

	server    controllerServer
	scheduler controllerScheduler
	store     ownedStore
}

type controllerServer interface {
	Serve(ctx context.Context, addr string) error
}

type controllerScheduler interface {
	Run(ctx context.Context)
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

	cfg := config.DefaultControllerConfig()
	if err := config.Load(ctx, configPath, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
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

	store, err := etcd.New(ctx, cfg.Etcd.Endpoints, cfg.Etcd.KeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize etcd: %w", err)
	}

	srv := controller.New(store, logger, controller.Options{
		EtcdEndpoints: cfg.Etcd.Endpoints,
	})

	return &Controller{
		Config:    cfg,
		Logger:    logger,
		server:    srv,
		scheduler: controller.NewScheduler(srv, tick),
		store:     store,
	}, nil
}

// registerAdapters is the ONE explicit registration point — the
// extensibility seam's actual "one line" per adapter, called from here
// instead of relying on package-import side effects.
func registerAdapters() {
	postgres16.Register()
	valkey9.Register()
	manual.Register()
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

// Run starts the scheduler and blocks serving HTTP until ctx is cancelled.
// Once serving stops, Run joins the scheduler before closing the etcd Store
// that NewController created.
func (c *Controller) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		c.scheduler.Run(runCtx)
	}()

	serveErr := c.server.Serve(runCtx, c.Config.Listen.HTTP)
	cancel()
	<-schedulerDone

	closeErr := c.store.Close()
	joined := errors.Join(
		wrapControllerRunError("serve", serveErr),
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
