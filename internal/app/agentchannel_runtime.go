package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/infra/agentlistener"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
)

const (
	agentChannelControllerMaximumReceiveMessageBytes = 16 * 1024 * 1024
	agentChannelControllerMaximumSendMessageBytes    = 5 * 1024 * 1024
)

type agentChannelRuntime struct {
	registry          *agentchannel.Registry
	listen            func(context.Context) (net.Listener, error)
	newServer         func(*agentchannel.Registry) agentChannelGRPCServer
	volumeCheckpoints agentchannel.VolumeRemovalCheckpointer
}

type agentChannelGRPCServer interface {
	Serve(net.Listener) error
	Stop()
}

func newAgentChannelRuntime(
	authenticator agentchannel.Authenticator,
	tasks agentchannel.TaskStore,
	plans agentchannel.PlanResolver,
	materials agentchannel.MaterializationResolver,
	secrets agentchannel.BackupSecretSlotResolver,
	checkpoints agentchannel.BackupCheckpointer,
) *agentChannelRuntime {
	return newAgentChannelRuntimeWithManagedConfig(
		authenticator, tasks, plans, materials, secrets, checkpoints, nil,
	)
}

func newAgentChannelRuntimeWithManagedConfig(
	authenticator agentchannel.Authenticator,
	tasks agentchannel.TaskStore,
	plans agentchannel.PlanResolver,
	materials agentchannel.MaterializationResolver,
	secrets agentchannel.BackupSecretSlotResolver,
	checkpoints agentchannel.BackupCheckpointer,
	managed agentchannel.ManagedConfigResolver,
) *agentChannelRuntime {
	return newAgentChannelRuntimeWithManagedConfigAndScripts(
		authenticator, tasks, plans, materials, secrets, checkpoints, managed, nil, nil,
	)
}

func newAgentChannelRuntimeWithManagedConfigAndScripts(
	authenticator agentchannel.Authenticator,
	tasks agentchannel.TaskStore,
	plans agentchannel.PlanResolver,
	materials agentchannel.MaterializationResolver,
	secrets agentchannel.BackupSecretSlotResolver,
	checkpoints agentchannel.BackupCheckpointer,
	managed agentchannel.ManagedConfigResolver,
	scripts agentchannel.ScriptArtifactResolver,
	scriptCheckpoints agentchannel.ScriptCheckpointer,
) *agentChannelRuntime {
	runtime := &agentChannelRuntime{registry: agentchannel.NewRegistry(), listen: agentlistener.Listen}
	runtime.newServer = func(registry *agentchannel.Registry) agentChannelGRPCServer {
		server := grpc.NewServer(
			grpc.MaxRecvMsgSize(agentChannelControllerMaximumReceiveMessageBytes),
			grpc.MaxSendMsgSize(agentChannelControllerMaximumSendMessageBytes),
		)
		channel := agentchannel.NewWithScriptRuntimeServices(
			authenticator, registry, tasks, plans, materials, secrets, checkpoints, managed,
			scripts, scriptCheckpoints, runtime.volumeCheckpoints,
		)
		agentpb.RegisterAgentChannelServer(server, channel)
		return server
	}
	return runtime
}

func (runtime *agentChannelRuntime) Run(ctx context.Context) (resultErr error) {
	if ctx == nil {
		return errs.New(errs.KindInternal, "agent channel context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil
	}
	if runtime == nil || runtime.registry == nil || runtime.listen == nil || runtime.newServer == nil {
		return errs.New(errs.KindInternal, "agent channel runtime is not configured")
	}

	listener, err := runtime.listen(ctx)
	if err != nil {
		return agentChannelRuntimeError(ctx, "listen", err)
	}
	owned := &ownedAgentListener{Listener: listener}
	defer func() {
		if closeErr := owned.Close(); closeErr != nil {
			resultErr = preferAgentChannelCleanup(resultErr, closeErr)
		}
	}()

	server := runtime.newServer(runtime.registry)
	if server == nil {
		return errs.New(errs.KindInternal, "agent channel gRPC server is not configured")
	}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(owned)
	}()

	select {
	case serveErr := <-serveDone:
		server.Stop()
		if ctx.Err() != nil && errors.Is(serveErr, grpc.ErrServerStopped) {
			return nil
		}
		if serveErr == nil {
			return errs.New(errs.KindInternal, "agent channel gRPC server stopped unexpectedly")
		}
		return agentChannelRuntimeError(ctx, "serve", serveErr)
	case <-ctx.Done():
		server.Stop()
		serveErr := <-serveDone
		if serveErr == nil || errors.Is(serveErr, grpc.ErrServerStopped) {
			return nil
		}
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent channel: stop serving: %w", serveErr))
	}
}

func agentChannelRuntimeError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("agent channel: %s: %w", operation, err))
}

func preferAgentChannelCleanup(operationErr, cleanupErr error) error {
	if cleanupErr == nil {
		return operationErr
	}
	if errors.Is(cleanupErr, context.Canceled) || errors.Is(cleanupErr, context.DeadlineExceeded) {
		return errs.New(errs.KindInternal, "agent channel listener cleanup failed")
	}
	if operationErr == nil || errors.Is(operationErr, context.Canceled) ||
		errors.Is(operationErr, context.DeadlineExceeded) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("agent channel: close listener: %w", cleanupErr))
	}
	return errs.Wrap(errs.KindInternal, errors.Join(
		operationErr,
		fmt.Errorf("agent channel: close listener: %w", cleanupErr),
	))
}

type ownedAgentListener struct {
	net.Listener
	once sync.Once
	err  error
}

func (listener *ownedAgentListener) Close() error {
	if listener == nil || listener.Listener == nil {
		return nil
	}
	listener.once.Do(func() {
		listener.err = listener.Listener.Close()
	})
	return listener.err
}
