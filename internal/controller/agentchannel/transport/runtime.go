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
	Registry               *agentchannel.Registry
	listen                 func(context.Context) (net.Listener, error)
	newServer              func(*agentchannel.Registry) (agentChannelGRPCServer, error)
	workIdentities         agentchannel.WorkIdentityResolver
	workStarts             agentchannel.WorkStarter
	workCheckpoints        agentchannel.ReleaseWorkCheckpointer
	scriptCompletion       agentchannel.ScriptWorkCompleter
	hierarchyReports       agentchannel.HierarchyReporter
	attachReports          agentchannel.AttachReporter
	attachProvisionReports agentchannel.AttachProvisionReporter
	networkReports         agentchannel.NetworkReporter
	componentReports       agentchannel.ComponentReporter
	backingReports         agentchannel.BackingReporter
	resolverReports        agentchannel.ResolverReporter
	entryReports           agentchannel.EntryReporter
	volumeReports          agentchannel.VolumeReporter
	environmentProvision   agentchannel.EnvironmentProvisionReporter
	lifecycleReports       agentchannel.LifecycleReporter
	removalRecoveryReports agentchannel.RemovalRecoveryReporter
	backupWorkReports      agentchannel.BackupWorkReporter
	backupWorkSecrets      agentchannel.BackupWorkSecretResolver
	workDispatch           agentchannel.WorkDispatcher
	OnReady                func()
}

type agentChannelGRPCServer interface {
	Serve(net.Listener) error
	Stop()
}

func New(
	authenticator agentchannel.Authenticator,
	staging agentchannel.BackupStagingStore,
	secrets agentchannel.RestoreIdentityReleaser,
	scriptCheckpoints agentchannel.ScriptCheckpointer,
) *Runtime {
	runtime := &Runtime{Registry: agentchannel.NewRegistry(), listen: agentlistener.Listen}
	runtime.newServer = func(registry *agentchannel.Registry) (agentChannelGRPCServer, error) {
		channel := agentchannel.New(authenticator, registry, staging, secrets, scriptCheckpoints)
		if err := channel.ConfigureWorkExecution(runtime.workIdentities, runtime.workStarts, runtime.workCheckpoints, runtime.scriptCompletion, runtime.hierarchyReports, runtime.workDispatch); err != nil {
			return nil, err
		}
		if runtime.volumeReports == nil {
			return nil, errs.New(errs.KindInternal, "native Volume reporting is not configured")
		}
		channel.ConfigureVolumeWork(runtime.volumeReports)
		if runtime.networkReports == nil {
			return nil, errs.New(errs.KindInternal, "native Network reporting is not configured")
		}
		channel.ConfigureNetworkWork(runtime.networkReports)
		if runtime.componentReports == nil {
			return nil, errs.New(errs.KindInternal, "native Component reporting is not configured")
		}
		channel.ConfigureComponentWork(runtime.componentReports)
		if runtime.backingReports == nil {
			return nil, errs.New(errs.KindInternal, "native Backing creation reporting is not configured")
		}
		channel.ConfigureBackingWork(runtime.backingReports)
		if runtime.resolverReports == nil {
			return nil, errs.New(errs.KindInternal, "native DNS resolver reporting is not configured")
		}
		channel.ConfigureResolverWork(runtime.resolverReports)
		if runtime.entryReports == nil {
			return nil, errs.New(errs.KindInternal, "native Entry reporting is not configured")
		}
		channel.ConfigureEntryConfigurationWork(runtime.entryReports)
		if err := channel.ConfigureEnvironmentProvision(runtime.environmentProvision); err != nil {
			return nil, err
		}
		if runtime.lifecycleReports == nil || runtime.removalRecoveryReports == nil {
			return nil, errs.New(
				errs.KindInternal,
				"native Service lifecycle and removal recovery reporting are not configured",
			)
		}
		channel.ConfigureServiceLifecycle(runtime.lifecycleReports)
		channel.ConfigureServiceRemovalRecovery(runtime.removalRecoveryReports)
		if runtime.attachReports == nil {
			return nil, errs.New(errs.KindInternal, "native Attach reporting is not configured")
		}
		channel.ConfigureAttachWork(runtime.attachReports)
		if runtime.attachProvisionReports == nil {
			return nil, errs.New(errs.KindInternal, "native Attach provisioning reporting is not configured")
		}
		channel.ConfigureAttachProvision(runtime.attachProvisionReports)
		if err := channel.ConfigureBackupWork(runtime.backupWorkReports); err != nil {
			return nil, err
		}
		if err := channel.ConfigureBackupWorkSecrets(runtime.backupWorkSecrets); err != nil {
			return nil, err
		}
		return newChannelGRPCServer(channel), nil
	}
	return runtime
}

func newChannelGRPCServer(channel agentpb.AgentChannelServer) *grpc.Server {
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(agentChannelControllerMaximumReceiveMessageBytes),
		grpc.MaxSendMsgSize(agentChannelControllerMaximumSendMessageBytes),
	)
	agentpb.RegisterAgentChannelServer(server, channel)
	return server
}

func (runtime *Runtime) ConfigureBackupWork(reports agentchannel.BackupWorkReporter) error {
	if runtime == nil || reports == nil || runtime.backupWorkReports != nil {
		return errs.New(errs.KindInternal, "native Backup reporting configuration is invalid")
	}
	runtime.backupWorkReports = reports
	return nil
}

func (runtime *Runtime) ConfigureBackupWorkSecrets(resolver agentchannel.BackupWorkSecretResolver) error {
	if runtime == nil || resolver == nil || runtime.backupWorkSecrets != nil {
		return errs.New(errs.KindInternal, "native Backup credential configuration is invalid")
	}
	runtime.backupWorkSecrets = resolver
	return nil
}

// ConfigureWorkExecution is composition-only and must precede Run. Native work
// is mandatory; there is no fallback to the superseded assignment engine.
func (runtime *Runtime) ConfigureWorkExecution(
	identities agentchannel.WorkIdentityResolver,
	starts agentchannel.WorkStarter,
	checkpoints agentchannel.ReleaseWorkCheckpointer,
	scripts agentchannel.ScriptWorkCompleter,
	removals agentchannel.HierarchyReporter,
	dispatch agentchannel.WorkDispatcher,
) error {
	if runtime == nil || identities == nil || starts == nil || checkpoints == nil || scripts == nil ||
		removals == nil ||
		dispatch == nil ||
		runtime.workIdentities != nil {
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

func (runtime *Runtime) ConfigureComponentWork(reports agentchannel.ComponentReporter) error {
	if runtime == nil || reports == nil || runtime.componentReports != nil {
		return errs.New(errs.KindInternal, "Component reporting dependencies are missing or already configured")
	}
	runtime.componentReports = reports
	return nil
}

func (runtime *Runtime) ConfigureResolverWork(reports agentchannel.ResolverReporter) error {
	if runtime == nil || reports == nil || runtime.resolverReports != nil {
		return errs.New(errs.KindInternal, "native DNS resolver reporting configuration is invalid")
	}
	runtime.resolverReports = reports
	return nil
}

func (runtime *Runtime) ConfigureBackingWork(reports agentchannel.BackingReporter) error {
	if runtime == nil || reports == nil || runtime.backingReports != nil {
		return errs.New(errs.KindInternal, "native Backing creation reporting configuration is invalid")
	}
	runtime.backingReports = reports
	return nil
}

func (runtime *Runtime) ConfigureAttachProvision(reports agentchannel.AttachProvisionReporter) error {
	if runtime == nil || reports == nil || runtime.attachProvisionReports != nil {
		return errs.New(
			errs.KindInternal,
			"Attach provisioning reporting dependencies are missing or already configured",
		)
	}
	runtime.attachProvisionReports = reports
	return nil
}

func (runtime *Runtime) ConfigureAttachWork(reports agentchannel.AttachReporter) error {
	if runtime == nil || reports == nil || runtime.attachReports != nil {
		return errs.New(errs.KindInternal, "Attach reporting dependencies are missing or already configured")
	}
	runtime.attachReports = reports
	return nil
}

func (runtime *Runtime) ConfigureEntryConfigurationWork(reports agentchannel.EntryReporter) error {
	if runtime == nil || reports == nil || runtime.entryReports != nil {
		return errs.New(errs.KindInternal, "Entry reporting dependencies are missing or already configured")
	}
	runtime.entryReports = reports
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
