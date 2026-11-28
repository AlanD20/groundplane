// Package servingendpoint tracks the acknowledged edge targets of one Service
// port independently of placement and the Agent connection lifecycle.
package servingendpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/controllerendpoint"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Target is one Groundplane Service port. The Controller resolves its owner
// before admitting any serving evidence or including it in an edge route.
type Target struct {
	EnvironmentID string `json:"environment_id"`
	ServiceID     string `json:"service_id"`
	Port          uint16 `json:"port"`
}

func (target Target) valid() bool {
	return target.Port != 0 && ids.Validate(ids.KindEnvironment, target.EnvironmentID) == nil &&
		ids.Validate(ids.KindService, target.ServiceID) == nil
}

// Endpoint identifies a stable host proxy for a particular serving Release.
// Agent generation fences observations; forwarding connects to the proxy.
type Endpoint struct {
	HostID          string         `json:"host_id"`
	AgentID         string         `json:"agent_id"`
	AgentGeneration uint64         `json:"agent_generation"`
	ReleaseID       string         `json:"release_id"`
	Address         netip.AddrPort `json:"address"`
}

func (endpoint Endpoint) valid() bool {
	ip := endpoint.Address.Addr()
	return endpoint.Address.Port() != 0 && ip.Zone() == "" && ip.Unmap() == ip &&
		(ip.IsPrivate() || controllerendpoint.IsTailnet(ip)) && endpoint.AgentGeneration != 0 &&
		ids.Validate(ids.KindHost, endpoint.HostID) == nil &&
		ids.Validate(ids.KindAgent, endpoint.AgentID) == nil &&
		ids.Validate(ids.KindDeployment, endpoint.ReleaseID) == nil
}

// ServingAuthority must come from the Controller's acknowledged serving
// Release and current authenticated host generation at one durable revision.
// An assigned or healthy candidate is insufficient to construct it.
type ServingAuthority struct {
	Target   Target
	Endpoint Endpoint
}

// ServingProof is an authenticated observation of that same host proxy and
// release. Address reachability and trust from the edge are checked by the
// runtime; a private address alone is not authorization.
type ServingProof struct {
	Target     Target
	Endpoint   Endpoint
	ObservedAt time.Time
	Ready      bool
}

// Revision is published to the edge as an immutable snapshot. Endpoints are
// ordered by Host ID so identical state produces the same acknowledgement.
type Revision struct {
	Number    uint64     `json:"number"`
	Endpoints []Endpoint `json:"endpoints"`
}

type EdgeAck struct {
	Number uint64 `json:"number"`
	Digest string `json:"digest"`
}

// Witness binds acknowledgement to this target and the exact published proxy
// set. The caller accepts it only from the authenticated applied edge revision.
func (revision Revision) Witness(target Target) EdgeAck {
	var witness strings.Builder
	fmt.Fprintf(&witness, "groundplane-serving-endpoints/v1\n%d\n%s\n%s\n%d\n",
		revision.Number, target.EnvironmentID, target.ServiceID, target.Port)
	for _, endpoint := range revision.Endpoints {
		fmt.Fprintf(&witness, "%s\n%s\n%d\n%s\n%s\n", endpoint.HostID, endpoint.AgentID,
			endpoint.AgentGeneration, endpoint.ReleaseID, endpoint.Address)
	}
	digest := sha256.Sum256([]byte(witness.String()))
	return EdgeAck{Number: revision.Number, Digest: "sha256:" + hex.EncodeToString(digest[:])}
}

func (revision Revision) endpoint(hostID string) (Endpoint, bool) {
	for _, endpoint := range revision.Endpoints {
		if endpoint.HostID == hostID {
			return endpoint, true
		}
	}
	return Endpoint{}, false
}

func (revision Revision) valid() bool {
	if revision.Number == 0 && len(revision.Endpoints) != 0 {
		return false
	}
	addresses := make(map[netip.AddrPort]struct{}, len(revision.Endpoints))
	previousHost := ""
	for _, endpoint := range revision.Endpoints {
		if !endpoint.valid() || previousHost >= endpoint.HostID {
			return false
		}
		if _, duplicate := addresses[endpoint.Address]; duplicate {
			return false
		}
		addresses[endpoint.Address] = struct{}{}
		previousHost = endpoint.HostID
	}
	return true
}

// Fence retains the newest admitted host generation and observation, including
// after a withdrawal. An old observation cannot republish a retired proxy.
type Fence struct {
	HostID          string    `json:"host_id"`
	AgentID         string    `json:"agent_id"`
	AgentGeneration uint64    `json:"agent_generation"`
	ObservedAt      time.Time `json:"observed_at"`
}

// State retains desired and last-applied edge configuration for one port.
// Mutations are pure. The Controller must commit a transition under a durable
// compare against both this state and its serving authority.
type State struct {
	Target     Target   `json:"target"`
	Desired    Revision `json:"desired"`
	Applied    Revision `json:"applied"`
	LastIssued uint64   `json:"last_issued"`
	Draining   []Drain  `json:"draining"`
	Fences     []Fence  `json:"fences"`
}

func New(target Target) (State, error) {
	if !target.valid() {
		return State{}, errs.New(errs.KindValidationFailed, "serving endpoint target is invalid")
	}
	return State{Target: target}, nil
}

// Validate rejects corrupted restored state before the Controller publishes or
// replays an edge operation. The edge can retain Applied while a change waits.
func (state State) Validate() error {
	const invalid = "serving endpoint state is invalid"
	if !state.Target.valid() || !state.Applied.valid() || !state.Desired.valid() ||
		state.Applied.Number > state.Desired.Number || state.Desired.Number > state.LastIssued ||
		state.pending() && state.Desired.Number != state.LastIssued ||
		state.Applied.Number == state.Desired.Number &&
			!slices.Equal(state.Applied.Endpoints, state.Desired.Endpoints) {
		return errs.New(errs.KindStateConflict, invalid)
	}
	fences := make(map[string]Fence, len(state.Fences))
	for _, fence := range state.Fences {
		if _, exists := fences[fence.HostID]; exists || fence.AgentGeneration == 0 || fence.ObservedAt.IsZero() ||
			ids.Validate(ids.KindHost, fence.HostID) != nil || ids.Validate(ids.KindAgent, fence.AgentID) != nil {
			return errs.New(errs.KindStateConflict, invalid)
		}
		fences[fence.HostID] = fence
	}
	for _, revision := range []Revision{state.Applied, state.Desired} {
		for _, endpoint := range revision.Endpoints {
			if !covers(fences[endpoint.HostID], endpoint) {
				return errs.New(errs.KindStateConflict, invalid)
			}
		}
	}
	return state.validateDrains(fences)
}

func covers(fence Fence, endpoint Endpoint) bool {
	return fence.AgentGeneration > endpoint.AgentGeneration ||
		fence.AgentGeneration == endpoint.AgentGeneration && fence.AgentID == endpoint.AgentID
}
