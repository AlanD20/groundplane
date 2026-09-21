package app

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"log/slog"
	"os"
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
