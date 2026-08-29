// Package hostresolutionhelpercontainer runs the fixed resolver procedure in a
// short-lived networkless helper container.
package hostresolutionhelpercontainer

import (
	"context"
	"errors"
	"fmt"
	"time"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/infra/docker/hostresolutionhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Operation string

const (
	OperationApply   Operation = "apply"
	OperationRestore Operation = "restore"
	dockerSocketPath           = "/var/run/docker.sock"
	helperArgument             = "host-resolution-helper"
	cleanupTimeout             = 30 * time.Second
)

type Engine interface {
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerWait(context.Context, string, client.ContainerWaitOptions) client.ContainerWaitResult
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerStop(context.Context, string, client.ContainerStopOptions) (client.ContainerStopResult, error)
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
	Close() error
}

type Executor struct {
	engine Engine
	image  string
}

func New(image string) (*Executor, error) {
	engine, err := client.New(client.WithHost("unix://" + dockerSocketPath))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	executor, err := NewWithEngine(engine, image)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, errors.Join(err, engine.Close()))
	}
	return executor, nil
}

func NewWithEngine(engine Engine, image string) (*Executor, error) {
	if engine == nil || !imageref.IsDigestPinned(image) {
		return nil, errs.New(errs.KindValidationFailed, "host resolution helper container configuration is invalid")
	}
	return &Executor{engine: engine, image: image}, nil
}

func (executor *Executor) Close() error {
	if executor == nil || executor.engine == nil {
		return nil
	}
	if err := executor.engine.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func (executor *Executor) Execute(ctx context.Context, operation Operation) (resultErr error) {
	if executor == nil || executor.engine == nil || ctx == nil ||
		operation != OperationApply && operation != OperationRestore {
		return errs.New(errs.KindInternal, "host resolution helper execution is invalid")
	}
	created, err := executor.engine.ContainerCreate(ctx, createOptions(executor.image, operation))
	if err != nil {
		return operationError(ctx, "create", err)
	}
	if created.ID == "" {
		return errs.New(errs.KindInternal, "host resolution helper create returned an empty container id")
	}
	containerID := created.ID
	defer func() {
		if cleanupErr := executor.remove(containerID); cleanupErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, cleanupErr))
		}
	}()
	wait := executor.engine.ContainerWait(ctx, containerID, client.ContainerWaitOptions{
		Condition: container.WaitConditionNextExit,
	})
	if wait.Result == nil || wait.Error == nil {
		return errs.New(errs.KindInternal, "host resolution helper wait channels are not configured")
	}
	if _, err := executor.engine.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		return operationError(ctx, "start", err)
	}
	select {
	case response, open := <-wait.Result:
		if !open || response.Error != nil || response.StatusCode != 0 {
			return errs.New(errs.KindInternal, "host resolution helper process failed")
		}
		return nil
	case waitErr, open := <-wait.Error:
		if !open || waitErr == nil {
			return errs.New(errs.KindInternal, "host resolution helper wait failed")
		}
		return operationError(ctx, "wait", waitErr)
	case <-ctx.Done():
		return errors.Join(ctx.Err(), executor.stop(containerID))
	}
}

func createOptions(image string, operation Operation) client.ContainerCreateOptions {
	return client.ContainerCreateOptions{
		Config: &container.Config{
			Image: image, User: "0", Cmd: []string{helperArgument, string(operation)},
			WorkingDir: "/", NetworkDisabled: true,
		},
		HostConfig: &container.HostConfig{
			NetworkMode: container.NetworkMode("none"),
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
			ReadonlyRootfs: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges"},
			Mounts: []mount.Mount{
				{Type: mount.TypeBind, Source: hostresolutionhelper.ResolverSourcePath, Target: hostresolutionhelper.ResolverMountPath},
				{Type: mount.TypeBind, Source: agentprotocol.StatePath, Target: agentprotocol.StatePath},
			},
		},
	}
}

func (executor *Executor) stop(containerID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	timeout := 0
	_, err := executor.engine.ContainerStop(ctx, containerID, client.ContainerStopOptions{Timeout: &timeout})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func (executor *Executor) remove(containerID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	_, err := executor.engine.ContainerRemove(ctx, containerID, client.ContainerRemoveOptions{Force: true})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func operationError(ctx context.Context, operation string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("host resolution helper container: %s: %w", operation, err))
}
