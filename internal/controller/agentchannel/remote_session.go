package agentchannel

import (
	"context"
	"crypto/tls"

	hostidentity "github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/remotehost"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// RemoteWorkIdentityResolver must join the credential revision from the
// authenticated peer to the Host binding and executor conditions in each write.
// A local-only resolver cannot open a Remote channel.
type RemoteWorkIdentityResolver interface {
	RemoteConfiguration(context.Context, hostidentity.Authorization) (*agentpb.AgentConfig, error)
	RegisterRemoteWorkProcess(
		context.Context,
		hostidentity.Authorization,
		[16]byte,
		workexecution.AgentConcurrency,
		[]workexecution.Operation,
	) (workexecution.Identity, error)
	ResolveRemoteWorkIdentity(
		context.Context,
		hostidentity.Authorization,
		[16]byte,
	) (agentregistration.WorkAuthorization, error)
}

type remoteSession struct {
	initial   hostidentity.Authorization
	authorize func(context.Context, tls.ConnectionState) (hostidentity.Authorization, error)
	peer      tls.ConnectionState
	backend   RemoteWorkIdentityResolver
}

func (session *remoteSession) current(ctx context.Context) (hostidentity.Authorization, error) {
	current, err := session.authorize(ctx, session.peer)
	if err != nil || current.HostRevision <= 0 || current.Identity != session.initial.Identity {
		return hostidentity.Authorization{}, unauthenticated()
	}
	return current, nil
}

type readChannelConfiguration func(context.Context) (*agentpb.AgentConfig, error)

func (server *Server) authorizeChannel(ctx context.Context, agentID string, token Token,
	remote *remoteSession) (Authorization, readChannelConfiguration, error) {
	if remote == nil {
		authorization, err := server.auth.Authenticate(ctx, agentID, token)
		return authorization, func(ctx context.Context) (*agentpb.AgentConfig, error) {
			return server.auth.Configuration(ctx, agentID, authorization.Generation)
		}, err
	}
	if remote.initial.Identity.AgentID != agentID {
		return Authorization{}, nil, unauthenticated()
	}
	configuration := func(ctx context.Context) (*agentpb.AgentConfig, error) {
		current, err := remote.current(ctx)
		if err != nil {
			return nil, err
		}
		return remote.backend.RemoteConfiguration(ctx, current)
	}
	config, err := configuration(ctx)
	return Authorization{Generation: remote.initial.Identity.Generation, Config: config}, configuration, err
}

func (server *Server) registerChannelProcess(ctx context.Context, agentID string, generation uint64,
	process [16]byte, limits workexecution.AgentConcurrency, remote *remoteSession) (workexecution.Identity, error) {
	operations := server.workStarts.Operations()
	if remote == nil {
		return server.workIdentities.RegisterWorkProcess(ctx, agentID, generation, process, limits, operations)
	}
	current, err := remote.workPeer(ctx, agentID, generation)
	if err != nil {
		return workexecution.Identity{}, err
	}
	registered, err := remote.backend.RegisterRemoteWorkProcess(ctx, current, process, limits, operations)
	if err == nil && registered.HostID != current.Identity.HostID {
		err = unauthenticated()
	}
	return registered, err
}

func (server *Server) resolveChannelProcess(ctx context.Context, agentID string, generation uint64,
	process [16]byte, remote *remoteSession) (agentregistration.WorkAuthorization, error) {
	if remote == nil {
		return server.workIdentities.ResolveWorkIdentity(ctx, agentID, generation, process)
	}
	current, err := remote.workPeer(ctx, agentID, generation)
	if err != nil {
		return agentregistration.WorkAuthorization{}, err
	}
	resolved, err := remote.backend.ResolveRemoteWorkIdentity(ctx, current, process)
	if err == nil && resolved.Identity().HostID != current.Identity.HostID {
		err = unauthenticated()
	}
	return resolved, err
}

func (remote *remoteSession) workPeer(ctx context.Context, agentID string,
	generation uint64) (hostidentity.Authorization, error) {
	current, err := remote.current(ctx)
	if err != nil || current.Identity.AgentID != agentID || current.Identity.Generation != generation {
		return hostidentity.Authorization{}, unauthenticated()
	}
	return current, nil
}

// RemoteChannel is the TLS listener's only registered service. Local listeners
// register Server directly and never enter the remote authentication branch.
func (server *Server) RemoteChannel(
	authority *remotehost.Service,
	backend RemoteWorkIdentityResolver,
) (agentpb.AgentChannelServer, error) {
	if server == nil || authority == nil || backend == nil || server.workStarts == nil || server.workDispatch == nil ||
		server.workIdentities == nil || server.sessions == nil {
		return nil, errs.New(errs.KindInternal, "Remote Agent channel is not configured")
	}
	return &remoteChannel{server: server, authority: authority, backend: backend}, nil
}

type remoteChannel struct {
	agentpb.UnimplementedAgentChannelServer
	server    *Server
	authority *remotehost.Service
	backend   RemoteWorkIdentityResolver
}

func (channel *remoteChannel) Connect(stream agentpb.AgentChannel_ConnectServer) error {
	transport, ok := peer.FromContext(stream.Context())
	if !ok {
		return unauthenticated()
	}
	security, ok := transport.AuthInfo.(credentials.TLSInfo)
	if !ok || len(security.State.PeerCertificates) == 0 {
		return unauthenticated()
	}
	identity, valid := hostidentity.CertificateIdentity(security.State.PeerCertificates[0])
	if !valid {
		return unauthenticated()
	}
	first, err := stream.Recv()
	if err != nil {
		return unauthenticated()
	}
	authenticate, _, err := validateChannelAuthenticate(first, 0)
	if err != nil || authenticate.AgentId != identity.AgentID {
		return unauthenticated()
	}
	// The first verified client handshake with the staged certificate proves
	// key possession. Confirmation never publishes an authority from message fields.
	confirmed, err := channel.authority.Confirm(stream.Context(), security.State)
	if err != nil || confirmed != identity {
		return unauthenticated()
	}
	authorization, err := channel.authority.Authorize(stream.Context(), security.State)
	if err != nil || authorization.Identity != identity || authorization.HostRevision <= 0 {
		return unauthenticated()
	}
	session := &remoteSession{
		initial:   authorization,
		authorize: channel.authority.Authorize,
		peer:      security.State,
		backend:   channel.backend,
	}
	wrapped := &remoteStream{AgentChannel_ConnectServer: stream, session: session, first: first}
	return channel.server.connect(wrapped, session)
}

type remoteStream struct {
	agentpb.AgentChannel_ConnectServer
	session  *remoteSession
	executor workexecution.Identity
	first    *agentpb.AgentMessage
}

func (stream *remoteStream) Recv() (*agentpb.AgentMessage, error) {
	message := stream.first
	stream.first = nil
	if message == nil {
		var err error
		message, err = stream.AgentChannel_ConnectServer.Recv()
		if err != nil {
			return nil, err
		}
	}
	if _, err := stream.session.current(stream.Context()); err != nil {
		return nil, err
	}
	return message, nil
}

func (stream *remoteStream) Send(message *agentpb.ControllerMessage) error {
	if _, err := stream.session.current(stream.Context()); err != nil {
		return err
	}
	if registration := message.GetWorkExecutorRegistration(); registration != nil {
		identity := stream.session.initial.Identity
		if registration.AgentId != identity.AgentID || registration.HostId != identity.HostID ||
			registration.ExecutorGeneration == 0 || stream.executor.Generation != 0 {
			return unauthenticated()
		}
		stream.executor = workexecution.Identity{Executor: workexecution.Agent, AgentID: identity.AgentID,
			HostID: identity.HostID, Generation: registration.ExecutorGeneration}
	}
	if attempt, assignment := controllerAssignment(message); assignment {
		if _, err := workexecution.DecodeAgentAttempt(attempt, stream.executor); err != nil {
			return unauthenticated()
		}
	}
	return stream.AgentChannel_ConnectServer.Send(message)
}

func controllerAssignment(message *agentpb.ControllerMessage) (*agentpb.WorkAttemptIdentity, bool) {
	switch payload := message.GetPayload().(type) {
	case *agentpb.ControllerMessage_ReleaseWorkAssignment:
		return payload.ReleaseWorkAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_ReleaseRecoveryAssignment:
		return payload.ReleaseRecoveryAssignment.GetWork().GetAttempt(), true
	case *agentpb.ControllerMessage_ScriptWorkAssignment:
		return payload.ScriptWorkAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_ScriptRecoveryAssignment:
		return payload.ScriptRecoveryAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_EnvironmentRemovalAssignment:
		return payload.EnvironmentRemovalAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_AttachRemovalAssignment:
		return payload.AttachRemovalAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_AttachRecoveryAssignment:
		return payload.AttachRecoveryAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_VolumeWorkAssignment:
		return payload.VolumeWorkAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_NetworkWorkAssignment:
		return payload.NetworkWorkAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_ComponentWorkAssignment:
		return payload.ComponentWorkAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_EntryConfigurationWorkAssignment:
		return payload.EntryConfigurationWorkAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_AttachProvisionAssignment:
		return payload.AttachProvisionAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_AttachProvisionRecoveryAssignment:
		return payload.AttachProvisionRecoveryAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_ServiceLifecycleAssignment:
		return payload.ServiceLifecycleAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_ServiceRemovalRecoveryAssignment:
		return payload.ServiceRemovalRecoveryAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_BackupWorkAssignment:
		return payload.BackupWorkAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_BackupRecoveryAssignment:
		return payload.BackupRecoveryAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_EnvironmentProvisionAssignment:
		return payload.EnvironmentProvisionAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_PublicIngressAssignment:
		return payload.PublicIngressAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_RemoteImageAssignment:
		return payload.RemoteImageAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_BackingProvisionAssignment:
		return payload.BackingProvisionAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_ResolverWorkAssignment:
		return payload.ResolverWorkAssignment.GetAttempt(), true
	case *agentpb.ControllerMessage_WorkCancel:
		return payload.WorkCancel.GetAttempt(), true
	default:
		return nil, false
	}
}
