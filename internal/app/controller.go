package app

import (
	"context"
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
)

const DefaultControllerConfigPath = "/etc/groundplane/controller.yaml"

// Controller is the wired Controller binary: config, logging, the
// adapter registry, the etcd store, the HTTP server, and the scheduler.
// cmd/controller/main.go is nothing but NewController + Run.
type Controller struct {
	Config config.ControllerConfig
	Logger *slog.Logger
	Server *controller.Server
	Sched  *controller.Scheduler
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

	logger, err := logging.Setup(logging.Options{
		Level:   logging.ResolveLevel(false, false, os.Getenv("GROUNDPLANE_LOG_LEVEL"), cfg.Log.Level),
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

	tick, err := time.ParseDuration(cfg.Scheduler.TickInterval)
	if err != nil {
		return nil, fmt.Errorf("controller: parse scheduler tick interval: %w", err)
	}
	if tick <= 0 {
		return nil, fmt.Errorf("controller: scheduler tick interval must be positive")
	}

	return &Controller{
		Config: cfg,
		Logger: logger,
		Server: srv,
		Sched:  controller.NewScheduler(srv, tick),
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

// Run starts the scheduler and blocks serving HTTP until ctx is
// cancelled.
func (c *Controller) Run(ctx context.Context) error {
	go c.Sched.Run(ctx)
	if err := c.Server.Serve(ctx, c.Config.Listen.HTTP); err != nil {
		return fmt.Errorf("controller: serve: %w", err)
	}
	return nil
}
