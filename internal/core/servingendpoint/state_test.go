package servingendpoint

import (
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestMoveWaitsForEdgeAndRetainsDrainingProxy(t *testing.T) {
	t.Parallel()
	state, now, origin, destination := fixtures(t)
	state = publishProof(t, state, origin, now)
	if len(state.Applied.Endpoints) != 0 {
		t.Fatal("publication changed the applied edge before acknowledgement")
	}
	state = acknowledgeDesired(t, state)
	state = publishProof(t, state, destination, now.Add(time.Second))
	ready := ServingProof{Target: state.Target, Endpoint: destination, Ready: true, ObservedAt: now.Add(time.Second)}
	move := Withdrawal{Mode: WithdrawalMove, Replacement: &ready, MaxProofAge: time.Minute}
	if _, err := state.BeginDrain(origin.HostID, move, now.Add(2*time.Second)); err == nil {
		t.Fatal("origin could drain before the new edge accepted the destination")
	}
	state = acknowledgeDesired(t, state)
	if _, err := state.BeginDrain(origin.HostID, move, now.Add(2*time.Minute)); err == nil {
		t.Fatal("stale destination health was accepted for the move")
	}
	state, err := state.BeginDrain(origin.HostID, move, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Draining) != 1 || len(state.Applied.Endpoints) != 2 || len(state.Desired.Endpoints) != 1 {
		t.Fatalf("unexpected withdrawing state %#v", state)
	}
	remaining := uint32(0)
	proof := DrainProof{Target: state.Target, Endpoint: origin, Withdrawal: state.Draining[0].Withdrawal,
		ObservedAt: now.Add(3 * time.Second), RemainingConnections: &remaining}
	if _, err := state.FinishDrain(proof); err == nil {
		t.Fatal("release of the old reservation did not wait for edge withdrawal")
	}
	state = acknowledgeDesired(t, state)
	remaining = 1
	if _, err := state.FinishDrain(proof); err == nil {
		t.Fatal("released an address whose proxy still has an established connection")
	}
	remaining = 0
	state, err = state.FinishDrain(proof)
	if err != nil || len(state.Draining) != 0 || len(state.Applied.Endpoints) != 1 ||
		state.Applied.Endpoints[0] != destination {
		t.Fatalf("finish draining = %#v, %v", state, err)
	}
	oldReady := ServingProof{Target: state.Target, Endpoint: origin, Ready: true, ObservedAt: now}
	oldAuthority := ServingAuthority{Target: state.Target, Endpoint: origin}
	if _, err := state.Publish(oldAuthority, oldReady, now.Add(4*time.Second), time.Minute); err == nil {
		t.Fatal("old readiness could republish a retired host proxy")
	}
}

func TestRejectedApplyCannotBeAcknowledgedAfterRetry(t *testing.T) {
	t.Parallel()
	state, now, origin, destination := fixtures(t)
	state = acknowledgeDesired(t, publishProof(t, state, origin, now))
	state = publishProof(t, state, destination, now.Add(time.Second))
	rejected := state.Desired.Witness(state.Target)
	live := state.Applied.Witness(state.Target)
	state, err := state.RestorePrevious(rejected, live)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Applied.Endpoints) != 1 || state.Applied.Endpoints[0] != origin {
		t.Fatalf("restoration lost the live origin: %#v", state.Applied)
	}
	state = publishProof(t, state, destination, now.Add(2*time.Second))
	if _, err := state.Acknowledge(rejected); err == nil {
		t.Fatal("a late acknowledgement accepted the retried revision")
	}
	state = acknowledgeDesired(t, state)
	if len(state.Applied.Endpoints) != 2 {
		t.Fatalf("current acknowledgement lost a healthy endpoint: %#v", state.Applied)
	}
}

func TestCandidateAndReplacedAgentCannotPublishEndpoint(t *testing.T) {
	t.Parallel()
	state, now, origin, candidate := fixtures(t)
	kept := ServingAuthority{Target: state.Target, Endpoint: origin}
	claimed := ServingProof{Target: state.Target, Endpoint: candidate, Ready: true, ObservedAt: now}
	if _, err := state.Publish(kept, claimed, now, time.Minute); err == nil {
		t.Fatal("publication accepted a healthy candidate before it served")
	}
	state = acknowledgeDesired(t, publishProof(t, state, origin, now))
	replacement := origin
	replacement.AgentID = candidate.AgentID
	replacement.AgentGeneration++
	state = acknowledgeDesired(t, publishProof(t, state, replacement, now.Add(time.Second)))
	oldReport := ServingProof{Target: state.Target, Endpoint: origin, Ready: true, ObservedAt: now.Add(2 * time.Second)}
	if _, err := state.Publish(kept, oldReport, oldReport.ObservedAt, time.Minute); err == nil {
		t.Fatal("observation from replaced Agent could regain serving identity")
	}
}

func fixtures(t *testing.T) (State, time.Time, Endpoint, Endpoint) {
	t.Helper()
	now := time.Date(2026, time.November, 27, 12, 0, 0, 0, time.UTC)
	state, err := New(
		Target{
			EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 1),
			ServiceID:     ids.NewAt(ids.KindService, now, 1),
			Port:          8080,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return state, now,
		Endpoint{HostID: ids.NewAt(ids.KindHost, now, 1), AgentID: ids.NewAt(ids.KindAgent, now, 1), AgentGeneration: 1,
			ReleaseID: ids.NewAt(ids.KindDeployment, now, 1), Address: netip.MustParseAddrPort("10.10.1.5:8400")},
		Endpoint{HostID: ids.NewAt(ids.KindHost, now, 2), AgentID: ids.NewAt(ids.KindAgent, now, 2), AgentGeneration: 1,
			ReleaseID: ids.NewAt(ids.KindDeployment, now, 2), Address: netip.MustParseAddrPort("10.10.2.5:8400")}
}

func publishProof(t *testing.T, state State, endpoint Endpoint, now time.Time) State {
	t.Helper()
	target := state.Target
	updated, err := state.Publish(ServingAuthority{Target: target, Endpoint: endpoint},
		ServingProof{Target: target, Endpoint: endpoint, Ready: true, ObservedAt: now}, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return updated
}

func acknowledgeDesired(t *testing.T, state State) State {
	t.Helper()
	updated, err := state.Acknowledge(state.Desired.Witness(state.Target))
	if err != nil {
		t.Fatal(err)
	}
	return updated
}
