package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/remotehost"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type remoteListener struct {
	address     netip.AddrPort
	certificate tls.Certificate
	authority   *remotehost.Service
	work        *RemoteWorkResolver
}

// ConfigureRemote is an opt-in startup setting. Both listeners share the same
// session registry and typed work owners and remain separately authenticated.
func (runtime *Runtime) ConfigureRemote(address netip.AddrPort, certificate tls.Certificate,
	authority *remotehost.Service, work *RemoteWorkResolver) error {
	if runtime == nil || runtime.remote != nil || runtime.newChannel == nil || !address.IsValid() ||
		address.Addr().
			IsUnspecified() ||
		address.Addr().IsMulticast() || address.Port() == 0 || authority == nil || work == nil ||
		len(certificate.Certificate) == 0 || certificate.PrivateKey == nil {
		return errs.New(errs.KindValidationFailed, "Remote Agent runtime configuration is invalid")
	}
	runtime.remote = &remoteListener{address: address, certificate: certificate, authority: authority, work: work}
	return nil
}

func (runtime *Runtime) runWithRemote(ctx context.Context) error {
	if ctx == nil || runtime.Registry == nil {
		return errs.New(errs.KindInternal, "Remote Agent runtime requires context and registry")
	}
	if ctx.Err() != nil {
		return nil
	}
	channel, err := runtime.newChannel(runtime.Registry)
	if err != nil {
		return err
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	results := make(chan error, 2)
	go func() {
		results <- runtime.runLocal(runCtx, func(*agentchannel.Registry) (agentChannelGRPCServer, error) {
			return newChannelGRPCServer(channel), nil
		})
	}()
	go func() {
		remote := runtime.remote
		results <- RunRemote(runCtx, remote.address, remote.certificate, remote.authority, channel, remote.work)
	}()
	first := <-results
	stop()
	return errors.Join(first, <-results)
}
