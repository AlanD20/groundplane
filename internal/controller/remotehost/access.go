package remotehost

import (
	"context"
	"crypto/tls"
	"time"

	record "github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) Authorize(ctx context.Context, peer tls.ConnectionState) (record.Authorization, error) {
	current, identity, err := service.currentPeer(ctx, peer)
	if err != nil {
		return record.Authorization{}, err
	}
	return record.Authorization{Identity: identity, HostRevision: current.Revision}, nil
}

func (service *Service) currentPeer(
	ctx context.Context,
	peer tls.ConnectionState,
) (record.VersionedHost, record.Identity, error) {
	current, identity, digest, err := service.peerHost(ctx, peer)
	if err != nil {
		return record.VersionedHost{}, record.Identity{}, err
	}
	agent := current.Host.Agent
	if agent == nil || agent.AgentID != identity.AgentID ||
		!activeCertificate(&agent.Certificate, digest, service.now()) {
		return record.VersionedHost{}, record.Identity{}, denied()
	}
	return current, identity, nil
}

func (service *Service) peerHost(
	ctx context.Context,
	peer tls.ConnectionState,
) (record.VersionedHost, record.Identity, [32]byte, error) {
	identity, digest, err := service.authority.peer(peer, service.now())
	if err != nil {
		return record.VersionedHost{}, record.Identity{}, [32]byte{}, err
	}
	current, err := service.read(ctx, identity.HostID)
	if err != nil {
		return record.VersionedHost{}, record.Identity{}, [32]byte{}, concealHost(err)
	}
	if current.Host.Generation != identity.Generation {
		return record.VersionedHost{}, record.Identity{}, [32]byte{}, denied()
	}
	return current, identity, digest, nil
}

func activeCertificate(certificate *record.CertificateCredential, digest [32]byte, now time.Time) bool {
	return certificate != nil && certificate.Digest == digest && now.Before(certificate.ExpiresAt)
}

// Confirm is the machine-only promotion of a staged enrollment or renewal.
// A retried confirmation for the active certificate is harmless.
func (service *Service) Confirm(ctx context.Context, peer tls.ConnectionState) (record.Identity, error) {
	current, identity, digest, err := service.peerHost(ctx, peer)
	if err != nil {
		return record.Identity{}, err
	}
	next := current.Host
	now := service.now()
	if pending := next.Pending; pending != nil {
		if pending.AgentID != identity.AgentID || !now.Before(pending.ExpiresAt) ||
			!activeCertificate(pending.Certificate, digest, now) {
			return record.Identity{}, denied()
		}
		next.Pending = nil
		next.Agent = &record.AgentCredential{AgentID: identity.AgentID, Certificate: *pending.Certificate}
		attached, err := service.repository.AttachAgent(ctx, current.Revision, identity, next)
		if err != nil {
			return record.Identity{}, err
		}
		if !attached {
			return record.Identity{}, staleHost()
		}
		return identity, nil
	}
	agent := next.Agent
	if agent == nil || agent.AgentID != identity.AgentID {
		return record.Identity{}, denied()
	}
	if activeCertificate(&agent.Certificate, digest, now) {
		return identity, nil
	}
	if !activeCertificate(agent.Renewal, digest, now) {
		return record.Identity{}, denied()
	}
	next.Agent = &record.AgentCredential{AgentID: identity.AgentID, Certificate: *agent.Renewal}
	if err := service.save(ctx, current, next); err != nil {
		return record.Identity{}, err
	}
	return identity, nil
}

// Revoke cancels the named generation. An active Agent detaches only once the
// scheduler and execution store prove removal can be fenced safely.
func (service *Service) Revoke(ctx context.Context, hostID string, generation uint64) (HostView, error) {
	current, err := service.read(ctx, hostID)
	if err != nil {
		return HostView{}, err
	}
	next := current.Host
	if generation == 0 || next.Generation != generation {
		return HostView{}, staleHost()
	}
	if next.Pending != nil {
		next.Pending = nil
		if err := service.save(ctx, current, next); err != nil {
			return HostView{}, err
		}
		return hostView(next), nil
	}
	if agent := next.Agent; agent != nil {
		next.Agent = nil
		removed, err := service.repository.DetachAgent(ctx, current.Revision, next.Identity(agent.AgentID), next)
		if err != nil {
			return HostView{}, err
		}
		if !removed {
			return HostView{}, staleHost()
		}
	}
	return hostView(next), nil
}

func denied() error {
	return errs.New(errs.KindRequestForbidden, "Remote Agent credential is not authorized")
}

func concealHost(err error) error {
	kind, _ := errs.KindOf(err)
	if kind == errs.KindRequestNotFound || kind == errs.KindValidationFailed {
		return denied()
	}
	return err
}
