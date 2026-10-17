package transport

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

type Runtime struct {
	Registry         *agentchannel.Registry
	listen           func(context.Context) (net.Listener, error)
	newServer        func(*agentchannel.Registry) (agentChannelGRPCServer, error)
	workIdentities   agentchannel.WorkIdentityResolver
	workStarts       agentchannel.WorkStarter
	workCheckpoints  agentchannel.ReleaseWorkCheckpointer
	scriptCompletion agentchannel.ScriptWorkCompleter
	hierarchyReports agentchannel.HierarchyReporter
	attachReports    agentchannel.AttachReporter
	networkReports   agentchannel.NetworkReporter
	volumeReports    agentchannel.VolumeReporter
	workDispatch     agentchannel.WorkDispatcher
	OnReady          func()
}

type agentChannelGRPCServer interface {
	Serve(net.Listener) error
	Stop()
}

func New(
	authenticator agentchannel.Authenticator,
	tasks agentchannel.TaskStore,
	plans agentchannel.PlanResolver,
	materials agentchannel.MaterializationResolver,
	secrets agentchannel.BackupSecretSlotResolver,
	checkpoints agentchannel.BackupCheckpointer,
	managed agentchannel.ManagedConfigResolver,
	scripts agentchannel.ScriptArtifactResolver,
	scriptCheckpoints agentchannel.ScriptCheckpointer,
	backingHookCheckpoints agentchannel.BackingHookCheckpointer,
) *Runtime {
	runtime := &Runtime{Registry: agentchannel.NewRegistry(), listen: agentlistener.Listen}
	runtime.newServer = func(registry *agentchannel.Registry) (agentChannelGRPCServer, error) {
		server := grpc.NewServer(
			grpc.MaxRecvMsgSize(agentChannelControllerMaximumReceiveMessageBytes),
			grpc.MaxSendMsgSize(agentChannelControllerMaximumSendMessageBytes),
		)
		channel := agentchannel.NewWithScriptRuntimeServices(
			authenticator, registry, tasks, plans, materials, secrets, checkpoints, managed,
			scripts, scriptCheckpoints,
		)
		if backingHookCheckpoints != nil {
			if err := channel.EnableBackingHookCheckpoints(backingHookCheckpoints); err != nil {
				server.Stop()
				return nil, err
			}
		}
		if err := channel.ConfigureWorkExecution(runtime.workIdentities, runtime.workStarts, runtime.workCheckpoints, runtime.scriptCompletion, runtime.hierarchyReports, runtime.workDispatch); err != nil {
			server.Stop()
			return nil, err
		}
		if runtime.volumeReports == nil {
			server.Stop()
			return nil, errs.New(errs.KindInternal, "native Volume reporting is not configured")
		}
		channel.ConfigureVolumeWork(runtime.volumeReports)
		if runtime.networkReports == nil {
			server.Stop()
			return nil, errs.New(errs.KindInternal, "native Network reporting is not configured")
		}
		channel.ConfigureNetworkWork(runtime.networkReports)
		if runtime.attachReports == nil {
			server.Stop()
			return nil, errs.New(errs.KindInternal, "native Attach reporting is not configured")
		}
		channel.ConfigureAttachWork(runtime.attachReports)
		agentpb.RegisterAgentChannelServer(server, channel)
		return server, nil
	}
	return runtime
}

// ConfigureWorkExecution is composition-only and must precede Run. Native work
// is mandatory; there is no fallback to the superseded assignment engine.
func (runtime *Runtime) ConfigureWorkExecution(identities agentchannel.WorkIdentityResolver, starts agentchannel.WorkStarter,
	checkpoints agentchannel.ReleaseWorkCheckpointer, scripts agentchannel.ScriptWorkCompleter, removals agentchannel.HierarchyReporter, dispatch agentchannel.WorkDispatcher,
) error {
	if runtime == nil || identities == nil || starts == nil || checkpoints == nil || scripts == nil || removals == nil || dispatch == nil || runtime.workIdentities != nil {
		return errs.New(errs.KindInternal, "Agent work transport dependencies are missing or already configured")
	}
	runtime.workIdentities, runtime.workStarts = identities, starts
	runtime.workCheckpoints, runtime.workDispatch = checkpoints, dispatch
	runtime.scriptCompletion = scripts
	runtime.hierarchyReports = removals
	return nil
}

func (runtime *Runtime) ConfigureVolumeWork(reports agentchannel.VolumeReporter) error {
	if runtime == nil || reports == nil || runtime.volumeReports != nil {
		return errs.New(errs.KindInternal, "Volume reporting dependencies are missing or already configured")
	}
	runtime.volumeReports = reports
	return nil
}

func (runtime *Runtime) ConfigureNetworkWork(reports agentchannel.NetworkReporter) error {
	if runtime == nil || reports == nil || runtime.networkReports != nil {
		return errs.New(errs.KindInternal, "Network reporting dependencies are missing or already configured")
	}
	runtime.networkReports = reports
	return nil
}

func (runtime *Runtime) ConfigureAttachWork(reports agentchannel.AttachReporter) error {
	if runtime == nil || reports == nil || runtime.attachReports != nil {
		return errs.New(errs.KindInternal, "Attach reporting dependencies are missing or already configured")
	}
	runtime.attachReports = reports
	return nil
}

func (runtime *Runtime) Run(ctx context.Context) (resultErr error) {
	if ctx == nil {
		return errs.New(errs.KindInternal, "agent channel context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil
	}
	if runtime == nil || runtime.Registry == nil || runtime.listen == nil || runtime.newServer == nil {
		return errs.New(errs.KindInternal, "agent channel runtime is not configured")
	}

	listener, err := runtime.listen(ctx)
	if err != nil {
		return agentChannelRuntimeError(ctx, "listen", err)
	}
	owned := &ownedAgentListener{Listener: listener, onReady: runtime.OnReady}
	defer func() {
		if closeErr := owned.Close(); closeErr != nil {
			resultErr = preferAgentChannelCleanup(resultErr, closeErr)
		}
	}()

	server, err := runtime.newServer(runtime.Registry)
	if err != nil {
		return err
	}
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
	once      sync.Once
	err       error
	readyOnce sync.Once
	onReady   func()
}

func (listener *ownedAgentListener) Accept() (net.Conn, error) {
	if listener.onReady != nil {
		listener.readyOnce.Do(listener.onReady)
	}
	return listener.Listener.Accept()
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
