package agent

import (
	"bytes"
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// RemoteChannel contains the locally provisioned Controller endpoint, trust
// roots, and Agent identity. Certificate storage belongs to machine bootstrap.
type RemoteChannel struct {
	Endpoint            string
	ControllerName      string
	Roots               *x509.CertPool
	EnrollmentAuthority *x509.Certificate
	Certificate         tls.Certificate `json:"-"`
	Identity            remotehost.Identity
}

func NewRemoteClient(remote RemoteChannel, volumeRoot string, logger *slog.Logger,
	logReader agentprotocol.LogReader) (*Client, error) {
	host, rawPort, err := net.SplitHostPort(remote.Endpoint)
	port, portErr := strconv.ParseUint(rawPort, 10, 16)
	if err != nil || portErr != nil || host == "" || port == 0 || !remote.Identity.Valid() ||
		remote.Roots == nil || remote.EnrollmentAuthority == nil ||
		strings.TrimSpace(remote.ControllerName) != remote.ControllerName || remote.ControllerName == "" ||
		len(remote.Certificate.Certificate) == 0 || remote.Certificate.PrivateKey == nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Remote Agent requires a Controller endpoint, TLS credentials, and enrolled identity",
		)
	}
	if err := VerifyRemoteCredential(remote.Certificate, remote.EnrollmentAuthority, remote.Identity); err != nil {
		return nil, err
	}
	client, err := newClient(remote.Identity.AgentID, volumeRoot, logger, logReader)
	if err != nil {
		return nil, err
	}
	client.remoteHostID = remote.Identity.HostID
	security := credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		ServerName:   remote.ControllerName,
		RootCAs:      remote.Roots.Clone(),
		Certificates: []tls.Certificate{remote.Certificate},
	})
	endpoint := remote.Endpoint
	client.connect = func(ctx context.Context, _ string) (agentStream, io.Closer, error) {
		return connectRemoteGRPC(ctx, endpoint, security)
	}
	return client, nil
}

// VerifyRemoteCredential checks the expected durable identity before any dial.
func VerifyRemoteCredential(
	certificate tls.Certificate,
	authority *x509.Certificate,
	expected remotehost.Identity,
) error {
	signer, usable := certificate.PrivateKey.(crypto.Signer)
	if len(certificate.Certificate) == 0 || !usable || authority == nil {
		return errs.New(errs.KindValidationFailed, "Remote Agent credential is incomplete")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	identity, ok := remotehost.CertificateIdentity(leaf)
	now := time.Now()
	if err != nil || !ok || identity != expected || leaf.CheckSignatureFrom(authority) != nil ||
		now.Before(
			leaf.NotBefore,
		) || !now.Before(leaf.NotAfter) || now.Before(authority.NotBefore) || !now.Before(authority.NotAfter) {
		return errs.New(
			errs.KindValidationFailed,
			"Remote Agent certificate does not identify this enrolled Host and generation",
		)
	}
	publicKey, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil || !bytes.Equal(leaf.RawSubjectPublicKeyInfo, publicKey) {
		return errs.New(errs.KindValidationFailed, "Remote Agent private key does not match the enrolled certificate")
	}
	return nil
}

func connectRemoteGRPC(
	ctx context.Context,
	endpoint string,
	security credentials.TransportCredentials,
) (agentStream, io.Closer, error) {
	connection, err := grpc.NewClient(
		"passthrough:///groundplane-remote-agent",
		grpc.WithTransportCredentials(security),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(agentChannelAgentMaximumReceiveMessageBytes),
			grpc.MaxCallSendMsgSize(agentChannelAgentMaximumSendMessageBytes),
		),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", endpoint)
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	stream, err := agentpb.NewAgentChannelClient(connection).Connect(ctx)
	if err != nil {
		// Best effort: preserve the stream creation failure.
		_ = connection.Close()
		return nil, nil, err
	}
	return stream, connection, nil
}
