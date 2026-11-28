package servingendpoint

import (
	"net/netip"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type WithdrawalMode string

const (
	WithdrawalMove      WithdrawalMode = "move"
	WithdrawalUnpublish WithdrawalMode = "unpublish"
)

// Drain retains a withdrawn endpoint's reservation until the edge has
// stopped new traffic and all remaining connections are accounted for.
type Drain struct {
	Endpoint    Endpoint  `json:"endpoint"`
	Withdrawal  EdgeAck   `json:"withdrawal"`
	RequestedAt time.Time `json:"requested_at"`
}

type DrainProof struct {
	Target               Target
	Endpoint             Endpoint
	Withdrawal           EdgeAck
	ObservedAt           time.Time
	RemainingConnections *uint32
}

type Withdrawal struct {
	Mode        WithdrawalMode
	Replacement *ServingProof
	MaxProofAge time.Duration
}

// BeginDrain stages an edge withdrawal. A move needs another already-applied
// endpoint and fresh evidence that it still serves. Explicitly unpublishing a
// target can withdraw the last endpoint.
func (state State) BeginDrain(hostID string, withdrawal Withdrawal, now time.Time) (State, error) {
	if err := state.Validate(); err != nil {
		return state, err
	}
	if withdrawal.Mode != WithdrawalMove && withdrawal.Mode != WithdrawalUnpublish || now.IsZero() {
		return state, errs.New(errs.KindValidationFailed, "serving endpoint withdrawal is invalid")
	}
	for _, drain := range state.Draining {
		if drain.Endpoint.HostID == hostID {
			return state, nil
		}
	}
	if state.pending() {
		return state, errs.New(
			errs.KindStateConflict,
			"wait for the edge to apply its previous revision before withdrawing an endpoint",
		)
	}
	endpoint, found := state.Applied.endpoint(hostID)
	if !found {
		return state, errs.New(errs.KindStateConflict, "applied serving endpoint was not found")
	}
	if withdrawal.Mode == WithdrawalMove &&
		!state.replacementReady(hostID, withdrawal.Replacement, now, withdrawal.MaxProofAge) {
		return state, errs.New(
			errs.KindStateConflict,
			"a ready and acknowledged serving replacement is required before withdrawing this endpoint",
		)
	}
	index := slices.IndexFunc(state.Fences, func(fence Fence) bool { return fence.HostID == hostID })
	if index < 0 || now.Before(state.Fences[index].ObservedAt) {
		return state, errs.New(errs.KindStateConflict, "endpoint withdrawal is fenced by a newer host observation")
	}
	endpoints := slices.DeleteFunc(slices.Clone(state.Applied.Endpoints), func(existing Endpoint) bool {
		return existing.HostID == hostID
	})
	staged, err := state.stage(endpoints)
	if err != nil {
		return state, err
	}
	staged.Draining = append(slices.Clone(state.Draining), Drain{
		Endpoint:    endpoint,
		Withdrawal:  staged.Desired.Witness(state.Target),
		RequestedAt: now,
	})
	staged.Fences = slices.Clone(state.Fences)
	staged.Fences[index].ObservedAt = now
	return staged, nil
}

func (state State) replacementReady(leavingHost string, proof *ServingProof, now time.Time, maxAge time.Duration) bool {
	if proof == nil || proof.Target != state.Target || proof.Endpoint.HostID == leavingHost ||
		!proof.fresh(now, maxAge) {
		return false
	}
	endpoint, found := state.Applied.endpoint(proof.Endpoint.HostID)
	if !found || proof.Endpoint != endpoint {
		return false
	}
	for _, fence := range state.Fences {
		if fence.HostID == endpoint.HostID {
			return endpoint.AgentGeneration == fence.AgentGeneration && endpoint.AgentID == fence.AgentID &&
				!proof.ObservedAt.Before(fence.ObservedAt)
		}
	}
	return false
}

// FinishDrain accepts authenticated runtime evidence of zero remaining
// connections after the exact withdrawal applied. The reservation remains
// held across a failed or uncertain drain.
func (state State) FinishDrain(proof DrainProof) (State, error) {
	if err := state.Validate(); err != nil {
		return state, err
	}
	index := slices.IndexFunc(state.Draining, func(item Drain) bool { return item.Endpoint == proof.Endpoint })
	if proof.Target != state.Target || index < 0 {
		return state, errs.New(errs.KindStateConflict, "drain evidence does not match a withdrawing serving endpoint")
	}
	drain := state.Draining[index]
	if proof.Withdrawal != drain.Withdrawal || state.Applied.Number < drain.Withdrawal.Number ||
		proof.RemainingConnections == nil || *proof.RemainingConnections != 0 || proof.ObservedAt.Before(drain.RequestedAt) {
		return state, errs.New(
			errs.KindStateConflict,
			"the edge has not acknowledged withdrawal and complete connection draining",
		)
	}
	state.Draining = slices.Delete(slices.Clone(state.Draining), index, index+1)
	for index, fence := range state.Fences {
		if fence.HostID == drain.Endpoint.HostID && proof.ObservedAt.After(fence.ObservedAt) {
			state.Fences = slices.Clone(state.Fences)
			state.Fences[index].ObservedAt = proof.ObservedAt
			break
		}
	}
	return state, nil
}

func (state State) validateDrains(fences map[string]Fence) error {
	const invalid = "serving endpoint drain state is invalid"
	hosts := make(map[string]struct{}, len(state.Draining))
	addresses := make(map[netip.AddrPort]struct{}, len(state.Desired.Endpoints)+len(state.Draining))
	for _, endpoint := range state.Desired.Endpoints {
		addresses[endpoint.Address] = struct{}{}
		hosts[endpoint.HostID] = struct{}{}
	}
	for _, drain := range state.Draining {
		endpoint := drain.Endpoint
		withdrawal := drain.Withdrawal.Number
		if !endpoint.valid() || withdrawal == 0 || withdrawal > state.Desired.Number ||
			drain.RequestedAt.IsZero() || drain.Withdrawal.Digest == "" ||
			!covers(fences[endpoint.HostID], endpoint) || fences[endpoint.HostID].ObservedAt.Before(drain.RequestedAt) {
			return errs.New(errs.KindStateConflict, invalid)
		}
		if _, exists := hosts[endpoint.HostID]; exists {
			return errs.New(errs.KindStateConflict, invalid)
		}
		if _, exists := addresses[endpoint.Address]; exists {
			return errs.New(errs.KindStateConflict, invalid)
		}
		hosts[endpoint.HostID] = struct{}{}
		addresses[endpoint.Address] = struct{}{}
		applied, found := state.Applied.endpoint(endpoint.HostID)
		if state.Applied.Number >= withdrawal && found ||
			state.Applied.Number < withdrawal && (!found || applied != endpoint || withdrawal != state.Desired.Number) {
			return errs.New(errs.KindStateConflict, invalid)
		}
		if withdrawal == state.Desired.Number && drain.Withdrawal != state.Desired.Witness(state.Target) {
			return errs.New(errs.KindStateConflict, invalid)
		}
	}
	for _, applied := range state.Applied.Endpoints {
		desired, found := state.Desired.endpoint(applied.HostID)
		if found && desired.Address != applied.Address {
			return errs.New(errs.KindStateConflict, invalid)
		}
		if !found {
			if _, withdrawing := hosts[applied.HostID]; !withdrawing {
				return errs.New(errs.KindStateConflict, invalid)
			}
		}
	}
	return nil
}
