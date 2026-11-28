// Package remotehost owns remote Agent enrollment. Local Agent enrollment uses
// its existing Controller path.
package remotehost

import (
	"context"
	"time"

	record "github.com/AlanD20/groundplane/internal/common/remotehost"
)

// Repository provides linearizable reads and compare-and-swap host updates.
// AttachAgent must publish host ownership in the same transaction as recording
// the first accepted certificate. DetachAgent fences both assignment and active
// execution in the same transaction as removal and returns a conflict while
// unsafe. Credential renewal preserves generation and uses UpdateHost.
type Repository interface {
	CreateHost(ctx context.Context, host record.Host) error
	ReadHost(ctx context.Context, hostID string) (record.VersionedHost, error)
	UpdateHost(ctx context.Context, expectedRevision int64, host record.Host) (bool, error)
	AttachAgent(ctx context.Context, expectedRevision int64, identity record.Identity, host record.Host) (bool, error)
	DetachAgent(ctx context.Context, expectedRevision int64, identity record.Identity, host record.Host) (bool, error)
}

type HostState string

const (
	HostUnenrolled HostState = "unenrolled"
	HostEnrolling  HostState = "enrolling"
	HostEnrolled   HostState = "enrolled"
)

// HostView is the operator-readable projection. Enrollment secrets, credential
// digests, and certificate bytes never appear in this type.
type HostView struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	State            HostState `json:"state"`
	AgentID          string    `json:"agent_id,omitempty"`
	Generation       uint64    `json:"generation"`
	CredentialExpiry time.Time `json:"credential_expiry,omitzero"`
}

func hostView(host record.Host) HostView {
	view := HostView{ID: host.ID, Name: host.Name, State: HostUnenrolled, Generation: host.Generation}
	if pending := host.Pending; pending != nil {
		view.State, view.AgentID, view.CredentialExpiry = HostEnrolling, pending.AgentID, pending.ExpiresAt
	}
	if agent := host.Agent; agent != nil {
		view.State, view.AgentID, view.CredentialExpiry = HostEnrolled, agent.AgentID, agent.Certificate.ExpiresAt
	}
	return view
}
