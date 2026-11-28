package remoteagentplatform

import (
	"crypto"
	"fmt"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	"github.com/AlanD20/groundplane/internal/controller/remotehost"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remotehosts"
	"github.com/AlanD20/groundplane/internal/infra/remotetls"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ConfigureController(cfg config.ControllerConfig, store remotehosts.Store, agents *agentregistration.Repository,
	channel *channeltransport.Runtime) (*remotehost.Service, error) {
	remote := cfg.RemoteAgent
	if remote == nil {
		return nil, nil
	}
	address, err := remote.ListenAddress()
	if err != nil {
		return nil, err
	}
	server, err := remotetls.KeyPair(remote.Certificate, remote.PrivateKey)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if server.Leaf == nil || server.Leaf.IsCA || now.Before(server.Leaf.NotBefore) ||
		!now.Before(server.Leaf.NotAfter) {
		return nil, errs.New(errs.KindValidationFailed, "Remote Controller TLS certificate is invalid or expired")
	}
	ca, err := remotetls.KeyPair(remote.EnrollmentCACertificate, remote.EnrollmentCAPrivateKey)
	if err != nil {
		return nil, err
	}
	signer, ok := ca.PrivateKey.(crypto.Signer)
	if !ok || ca.Leaf == nil || now.Before(ca.Leaf.NotBefore) || !now.Before(ca.Leaf.NotAfter) {
		return nil, errs.New(errs.KindValidationFailed, "Remote Agent enrollment CA is invalid or expired")
	}
	authority, err := remotehost.NewAuthority(ca.Leaf, signer)
	if err != nil {
		return nil, err
	}
	enrollment, err := remotehost.New(remotehosts.NewRepository(store), authority)
	if err != nil {
		return nil, err
	}
	work, err := channeltransport.NewRemoteWorkResolver(
		agents,
		channeltransport.RemoteRuntimeConfig(cfg.Agent.Runtime),
		[]workexecution.Operation{
			workexecution.Environment,
			workexecution.Network,
			workexecution.Component,
			workexecution.Release,
			workexecution.ReleaseRestoration,
			workexecution.RemoteImage,
		},
	)
	if err != nil {
		return nil, err
	}
	if err := channel.ConfigureRemote(address, server, enrollment, work); err != nil {
		return nil, err
	}
	return enrollment, nil
}

func AgentChannel(cfg config.AgentConfig) (*agent.RemoteChannel, error) {
	remote := cfg.Remote
	expected := os.Getenv("GROUNDPLANE_REMOTE_EXPECTED_IDENTITY")
	if remote == nil {
		if expected != "" {
			return nil, errs.New(errs.KindStateConflict, "Installed Remote Agent companion configuration is missing")
		}
		return nil, nil
	}
	certificate, err := remotetls.KeyPair(remote.Certificate, remote.PrivateKey)
	if err != nil {
		return nil, err
	}
	authority, err := remotetls.Authority(remote.EnrollmentCACertificate)
	if err != nil {
		return nil, err
	}
	identity := remote.Identity(cfg.AgentID)
	if expected != "" && expected != fmt.Sprintf("%s,%s,%d", identity.HostID, identity.AgentID, identity.Generation) {
		return nil, errs.New(errs.KindStateConflict, "Remote Agent configuration changed from its installed identity")
	}
	if err := agent.VerifyRemoteCredential(certificate, authority, identity); err != nil {
		return nil, err
	}
	roots, err := remotetls.Roots(remote.ControllerCACertificate)
	if err != nil {
		return nil, err
	}
	return &agent.RemoteChannel{Endpoint: remote.Endpoint, ControllerName: remote.ControllerServerName, Roots: roots,
		EnrollmentAuthority: authority, Certificate: certificate, Identity: identity}, nil
}
