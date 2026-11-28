package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/netip"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/remotehost"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// RunRemote starts the optional mTLS connection for already enrolled remote
// Agents. The caller supplies a concrete listener address and a Controller
// certificate. Local Agent channel does not pass through this listener.
func RunRemote(ctx context.Context, address netip.AddrPort, certificate tls.Certificate,
	authority *remotehost.Service, channel *agentchannel.Server, backend *RemoteWorkResolver) error {
	if ctx == nil || authority == nil || !address.IsValid() || address.Addr().IsUnspecified() ||
		address.Addr().IsMulticast() || address.Port() == 0 ||
		len(certificate.Certificate) == 0 || certificate.PrivateKey == nil {
		return errs.New(
			errs.KindValidationFailed,
			"Remote Agent listener requires a concrete address and TLS credentials",
		)
	}
	if backend == nil {
		return errs.New(errs.KindValidationFailed, "Remote Agent listener requires an execution authority")
	}
	remote, err := channel.RemoteChannel(authority, backend)
	if err != nil {
		return err
	}
	var bind net.ListenConfig
	listener, err := bind.Listen(ctx, "tcp", address.String())
	if err != nil {
		return agentChannelRuntimeError(ctx, "listen for remote Agents", err)
	}
	owned := &ownedAgentListener{Listener: listener}
	server := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{certificate},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    authority.ClientRoots(),
		})),
		grpc.ConnectionTimeout(10*time.Second),
		grpc.MaxRecvMsgSize(agentChannelControllerMaximumReceiveMessageBytes),
		grpc.MaxSendMsgSize(agentChannelControllerMaximumSendMessageBytes),
	)
	agentpb.RegisterAgentChannelServer(server, remote)
	stop := context.AfterFunc(ctx, server.Stop)
	defer stop()
	defer server.Stop()
	serveErr := server.Serve(owned)
	if ctx.Err() != nil && (serveErr == nil || errors.Is(serveErr, grpc.ErrServerStopped)) {
		return preferAgentChannelCleanup(nil, owned.Close())
	}
	if serveErr == nil {
		serveErr = errs.New(errs.KindInternal, "Remote Agent listener stopped unexpectedly")
	}
	return preferAgentChannelCleanup(agentChannelRuntimeError(ctx, "serve remote Agents", serveErr), owned.Close())
}
