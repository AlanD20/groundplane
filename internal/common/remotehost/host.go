// Package remotehost defines the durable identity and credentials of a remotely enrolled Agent.
package remotehost

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Identity belongs to a particular Agent enrollment on a stable host. A
// replacement gets a new Agent ID and a higher host-scoped generation.
type Identity struct {
	HostID     string `json:"host_id"`
	AgentID    string `json:"agent_id"`
	Generation uint64 `json:"generation"`
}

func (identity Identity) Valid() bool {
	return ids.Validate(ids.KindHost, identity.HostID) == nil &&
		ids.Validate(ids.KindAgent, identity.AgentID) == nil && identity.Generation > 0
}

// Host is the persisted enrollment record. Store only token and certificate
// digests; private keys stay with their owners.
type Host struct {
	ID         string             `json:"id"`
	Name       string             `json:"name"`
	Generation uint64             `json:"generation"`
	Pending    *PendingEnrollment `json:"pending,omitempty"`
	Agent      *AgentCredential   `json:"agent,omitempty"`
}

type PendingEnrollment struct {
	AgentID     string                 `json:"agent_id"`
	TokenDigest [32]byte               `json:"token_digest"`
	KeyDigest   [32]byte               `json:"key_digest"`
	ExpiresAt   time.Time              `json:"expires_at"`
	Certificate *CertificateCredential `json:"certificate,omitempty"`
}

type AgentCredential struct {
	AgentID     string                 `json:"agent_id"`
	Certificate CertificateCredential  `json:"certificate"`
	Renewal     *CertificateCredential `json:"renewal,omitempty"`
}

type CertificateCredential struct {
	Digest    [32]byte  `json:"digest"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (credential CertificateCredential) valid() bool {
	return credential.Digest != [32]byte{} && !credential.ExpiresAt.IsZero()
}

func (host Host) Validate() error {
	if ids.Validate(ids.KindHost, host.ID) != nil || !validHostName(host.Name) ||
		(host.Pending != nil && host.Agent != nil) {
		return errs.New(errs.KindValidationFailed, "Remote Host identity is invalid")
	}
	if pending := host.Pending; pending != nil && (!host.Identity(pending.AgentID).Valid() ||
		pending.TokenDigest == [32]byte{} || pending.KeyDigest == [32]byte{} || pending.ExpiresAt.IsZero() ||
		(pending.Certificate != nil && !pending.Certificate.valid())) {
		return errs.New(errs.KindValidationFailed, "Remote Host enrollment is invalid")
	}
	if agent := host.Agent; agent != nil && (!host.Identity(agent.AgentID).Valid() ||
		!agent.Certificate.valid() || (agent.Renewal != nil && (!agent.Renewal.valid() ||
		agent.Renewal.Digest == agent.Certificate.Digest))) {
		return errs.New(errs.KindValidationFailed, "Remote Agent credential is invalid")
	}
	return nil
}

func (host Host) Identity(agentID string) Identity {
	return Identity{HostID: host.ID, AgentID: agentID, Generation: host.Generation}
}

func validHostName(name string) bool {
	return name != "" && len(name) <= 253 && utf8.ValidString(name) &&
		strings.TrimSpace(name) == name && strings.IndexFunc(name, unicode.IsControl) == -1
}

type VersionedHost struct {
	Host     Host
	Revision int64
}

// Authorization ties the Agent's credential identity to a linearizable host
// record revision. Work publication compares that revision with the live key.
type Authorization struct {
	Identity     Identity
	HostRevision int64
}
