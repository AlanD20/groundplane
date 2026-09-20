// Package scriptrunner executes one sealed Script runner projection through
// the local Docker Engine without using exec, copy, or container discovery.
package scriptrunner

import (
	"context"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
	"time"
)

const (
	dockerHost     = "unix:///var/run/docker.sock"
	bodyTarget     = "/groundplane-script-body"
	cleanupTimeout = 60 * time.Second
	stopSeconds    = 10
)

type networkConnector interface {
	NetworkConnect(context.Context, string, client.NetworkConnectOptions) (client.NetworkConnectResult, error)
}

type engineClient interface {
	networkConnector
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerAttach(context.Context, string, client.ContainerAttachOptions) (client.ContainerAttachResult, error)
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerWait(context.Context, string, client.ContainerWaitOptions) client.ContainerWaitResult
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	ContainerStop(context.Context, string, client.ContainerStopOptions) (client.ContainerStopResult, error)
	ContainerKill(context.Context, string, client.ContainerKillOptions) (client.ContainerKillResult, error)
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
	Close() error
}

type Runner struct {
	client engineClient
	bodies *bodyStore
}

func New(ctx context.Context) (*Runner, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "Script runner: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bodies, err := newBodyStore(agentprotocol.StatePath, 0, 0)
	if err != nil {
		return nil, err
	}
	engine, err := client.New(client.WithHost(dockerHost))
	if err != nil {
		_ = bodies.Close()
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("script runner: create Docker client: %w", err))
	}
	return &Runner{client: engine, bodies: bodies}, nil
}

func (runner *Runner) Close() error {
	if runner == nil {
		return nil
	}
	var engineErr, bodyErr error
	if runner.client != nil {
		engineErr = runner.client.Close()
	}
	if runner.bodies != nil {
		bodyErr = runner.bodies.Close()
	}
	if joined := errors.Join(engineErr, bodyErr); joined != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("script runner: close runtime: %w", joined))
	}
	return nil
}
