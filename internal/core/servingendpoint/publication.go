package servingendpoint

import (
	"math"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Publish accepts fresh serving evidence matching current authority and stages a
// revision for edge application. Active listeners cannot change address until
// their old binding is withdrawn and drained.
func (state State) Publish(
	authority ServingAuthority,
	proof ServingProof,
	now time.Time,
	maxProofAge time.Duration,
) (State, error) {
	if err := state.Validate(); err != nil {
		return state, err
	}
	endpoint := proof.Endpoint
	if !endpoint.valid() || authority.Target != state.Target || proof.Target != authority.Target ||
		authority.Endpoint != endpoint {
		return state, errs.New(
			errs.KindStateConflict,
			"endpoint evidence differs from the acknowledged serving Release",
		)
	}
	if !proof.fresh(now, maxProofAge) {
		return state, errs.New(errs.KindStateConflict, "serving endpoint readiness evidence is unavailable or stale")
	}
	previous, alreadyPublished := state.Desired.endpoint(endpoint.HostID)
	index := slices.IndexFunc(state.Fences, func(fence Fence) bool { return fence.HostID == endpoint.HostID })
	if index >= 0 {
		fence := state.Fences[index]
		if alreadyPublished && previous == endpoint && fence.ObservedAt.Equal(proof.ObservedAt) {
			return state, nil
		}
		if endpoint.AgentGeneration < fence.AgentGeneration ||
			endpoint.AgentGeneration == fence.AgentGeneration && endpoint.AgentID != fence.AgentID ||
			!proof.ObservedAt.After(fence.ObservedAt) {
			return state, errs.New(
				errs.KindStateConflict,
				"serving endpoint evidence is fenced by a newer host observation",
			)
		}
	}
	if state.pending() {
		return state, errs.New(
			errs.KindStateConflict,
			"the previous edge revision must settle before publishing another endpoint",
		)
	}
	if err := state.checkAddress(endpoint, previous, alreadyPublished); err != nil {
		return state, err
	}
	if !alreadyPublished || previous != endpoint {
		endpoints := slices.DeleteFunc(slices.Clone(state.Desired.Endpoints), func(existing Endpoint) bool {
			return existing.HostID == endpoint.HostID
		})
		endpoints = append(endpoints, endpoint)
		slices.SortFunc(endpoints, func(a, b Endpoint) int { return strings.Compare(a.HostID, b.HostID) })
		staged, err := state.stage(endpoints)
		if err != nil {
			return state, err
		}
		state = staged
	}
	fence := Fence{
		HostID:          endpoint.HostID,
		AgentID:         endpoint.AgentID,
		AgentGeneration: endpoint.AgentGeneration,
		ObservedAt:      proof.ObservedAt,
	}
	state.Fences = slices.Clone(state.Fences)
	if index < 0 {
		state.Fences = append(state.Fences, fence)
	} else {
		state.Fences[index] = fence
	}
	return state, nil
}

func (proof ServingProof) fresh(now time.Time, maxProofAge time.Duration) bool {
	return proof.Ready && !now.IsZero() && maxProofAge > 0 && !proof.ObservedAt.IsZero() &&
		!proof.ObservedAt.After(now) && now.Sub(proof.ObservedAt) <= maxProofAge
}

func (state State) checkAddress(endpoint, previous Endpoint, alreadyPublished bool) error {
	if alreadyPublished && previous.Address != endpoint.Address {
		return errs.New(errs.KindStateConflict, "an active host proxy address must be withdrawn before replacement")
	}
	for _, other := range state.Desired.Endpoints {
		if other.HostID != endpoint.HostID && other.Address == endpoint.Address {
			return errs.New(errs.KindStateConflict, "serving proxy address is reserved by another host")
		}
	}
	for _, drain := range state.Draining {
		if drain.Endpoint.HostID == endpoint.HostID || drain.Endpoint.Address == endpoint.Address {
			return errs.New(errs.KindStateConflict, "the previous host proxy must finish draining before reuse")
		}
	}
	return nil
}

// Acknowledge advances last-applied state only for the exact configuration
// the edge prepared and applied. Duplicate acknowledgement of Applied is safe.
func (state State) Acknowledge(ack EdgeAck) (State, error) {
	if err := state.Validate(); err != nil {
		return state, err
	}
	if ack == state.Applied.Witness(state.Target) {
		return state, nil
	}
	if ack != state.Desired.Witness(state.Target) {
		return state, errs.New(errs.KindStateConflict, "edge acknowledgement does not match a published revision")
	}
	state.Applied = state.Desired
	return state, nil
}

// RestorePrevious requires explicit proof that the pending edge apply is
// permanently fenced and that the exact previous configuration is serving.
// Its revision number is never reused, so a late acknowledgement cannot apply
// a subsequently published change.
func (state State) RestorePrevious(rejected, live EdgeAck) (State, error) {
	if err := state.Validate(); err != nil {
		return state, err
	}
	if !state.pending() || rejected != state.Desired.Witness(state.Target) ||
		live != state.Applied.Witness(state.Target) {
		return state, errs.New(
			errs.KindStateConflict,
			"edge restoration does not match the pending and previous revisions",
		)
	}
	state.Draining = slices.DeleteFunc(slices.Clone(state.Draining), func(drain Drain) bool {
		return drain.Withdrawal.Number == rejected.Number
	})
	state.Desired = state.Applied
	return state, nil
}

func (state State) pending() bool { return state.Desired.Number != state.Applied.Number }

func (state State) stage(endpoints []Endpoint) (State, error) {
	if state.LastIssued == math.MaxUint64 {
		return state, errs.New(errs.KindStateConflict, "serving endpoint revisions are exhausted")
	}
	state.LastIssued++
	state.Desired = Revision{Number: state.LastIssued, Endpoints: endpoints}
	return state, nil
}
