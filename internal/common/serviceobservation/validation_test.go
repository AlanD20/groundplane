package serviceobservation

import (
	"math"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func observationRequest() *agentpb.ObserveServices {
	return &agentpb.ObserveServices{
		RequestId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Targets: []*agentpb.ServiceObservationTarget{{
			EnvironmentId: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			ReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			RenderGeneration: 3, ComposeName: "api--blue", RuntimeRole: "slot", Slot: "blue",
		}},
	}
}

func observationResult(request *agentpb.ObserveServices) *agentpb.ServiceObservationResult {
	return &agentpb.ServiceObservationResult{
		RequestId: request.RequestId,
		Observations: []*agentpb.ServiceObservationRow{{
			ServiceId: request.Targets[0].ServiceId, ReleaseId: request.Targets[0].ReleaseId,
			Outcome: &agentpb.ServiceObservationRow_Replicas{Replicas: &agentpb.ServiceReplicaCounts{Healthy: 2}},
		}},
	}
}

// Rationale: a read-only channel still must reject malformed, mixed-owner or
// unbounded requests before the Agent observes Docker.
func TestObservationRequestIsClosedAndBounded(t *testing.T) {
	if err := ValidateRequest(observationRequest()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentpb.ObserveServices){
		"no targets":      func(r *agentpb.ObserveServices) { r.Targets = nil },
		"nil target":      func(r *agentpb.ObserveServices) { r.Targets[0] = nil },
		"duplicate":       func(r *agentpb.ObserveServices) { r.Targets = append(r.Targets, proto.CloneOf(r.Targets[0])) },
		"bad correlation": func(r *agentpb.ObserveServices) { r.RequestId = "read" },
		"mixed Environment": func(r *agentpb.ObserveServices) {
			other := proto.CloneOf(r.Targets[0])
			other.EnvironmentId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			other.ServiceId, other.ComposeName = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW", "worker"
			r.Targets = append(r.Targets, other)
		},
		"unknown request": func(r *agentpb.ObserveServices) { r.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown target":  func(r *agentpb.ObserveServices) { r.Targets[0].ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"no plan":         func(r *agentpb.ObserveServices) { r.Targets[0].PlanId = "" },
		"zero generation": func(r *agentpb.ObserveServices) { r.Targets[0].RenderGeneration = 0 },
		"proxy":           func(r *agentpb.ObserveServices) { r.Targets[0].RuntimeRole = "proxy" },
		"invalid slot":    func(r *agentpb.ObserveServices) { r.Targets[0].Slot = "candidate" },
		"mixed role":      func(r *agentpb.ObserveServices) { r.Targets[0].RuntimeRole = "singleton" },
		"Compose path":    func(r *agentpb.ObserveServices) { r.Targets[0].ComposeName = "../../host" },
		"too many": func(r *agentpb.ObserveServices) {
			r.Targets = make([]*agentpb.ServiceObservationTarget, MaximumTargets+1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := observationRequest()
			mutate(request)
			if err := ValidateRequest(request); err == nil {
				t.Fatal("accepted invalid observation request")
			}
		})
	}
}

// Rationale: a stale, partial, reordered or corrupt reply must never become a
// plausible health count, including uint32 wraparound and unknown wire fields.
func TestObservationResultCannotSubstituteOrOverflow(t *testing.T) {
	request := observationRequest()
	if err := ValidateResult(request, observationResult(request)); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentpb.ServiceObservationResult){
		"wrong request":   func(r *agentpb.ServiceObservationResult) { r.RequestId = "01ARZ3NDEKTSV4RRFFQ69G5FAW" },
		"partial":         func(r *agentpb.ServiceObservationResult) { r.Observations = nil },
		"nil row":         func(r *agentpb.ServiceObservationResult) { r.Observations[0] = nil },
		"foreign Service": func(r *agentpb.ServiceObservationResult) { r.Observations[0].ServiceId = "svc_other" },
		"wrong Release":   func(r *agentpb.ServiceObservationResult) { r.Observations[0].ReleaseId = "dep_old" },
		"no outcome":      func(r *agentpb.ServiceObservationResult) { r.Observations[0].Outcome = nil },
		"nil counts": func(r *agentpb.ServiceObservationResult) {
			r.Observations[0].Outcome = &agentpb.ServiceObservationRow_Replicas{}
		},
		"false unavailable": func(r *agentpb.ServiceObservationResult) {
			r.Observations[0].Outcome = &agentpb.ServiceObservationRow_Unavailable{}
		},
		"overflow": func(r *agentpb.ServiceObservationResult) {
			r.Observations[0].GetReplicas().Running = math.MaxUint32
		},
		"over bound": func(r *agentpb.ServiceObservationResult) {
			r.Observations[0].GetReplicas().Healthy = MaximumContainers + 1
		},
		"unknown result": func(r *agentpb.ServiceObservationResult) { r.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown row": func(r *agentpb.ServiceObservationResult) {
			r.Observations[0].ProtoReflect().SetUnknown([]byte{0x78, 1})
		},
		"unknown counts": func(r *agentpb.ServiceObservationResult) {
			r.Observations[0].GetReplicas().ProtoReflect().SetUnknown([]byte{0x78, 1})
		},
	} {
		t.Run(name, func(t *testing.T) {
			result := observationResult(request)
			mutate(result)
			if err := ValidateResult(request, result); err == nil {
				t.Fatal("accepted unusable observation result")
			}
		})
	}
	unavailable := observationResult(request)
	unavailable.Observations[0].Outcome = &agentpb.ServiceObservationRow_Unavailable{Unavailable: true}
	if err := ValidateResult(request, unavailable); err != nil {
		t.Fatalf("closed unavailable outcome: %v", err)
	}
}

// Rationale: OBS-05; the proxy proof is a closed digest-bound extension, not
// a wire path/command or an optional state that can silently disappear.
func TestObservationProxyWireRequiresCompleteAuthorityAndOutcome(t *testing.T) {
	request := observationRequest()
	target := request.Targets[0]
	target.ProxyComposeName = "api"
	target.ProxyPlanId = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	target.ProxyRenderGeneration = 4
	target.ProxyConfigSha256 = make([]byte, 32)
	result := observationResult(request)
	result.Observations[0].ProxyState =
		agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MATCHING
	if err := ValidateResult(request, result); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentpb.ObserveServices, *agentpb.ServiceObservationResult){
		"missing digest": func(request *agentpb.ObserveServices, _ *agentpb.ServiceObservationResult) {
			request.Targets[0].ProxyConfigSha256 = nil
		},
		"missing result": func(_ *agentpb.ObserveServices, result *agentpb.ServiceObservationResult) {
			result.Observations[0].ProxyState =
				agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_UNSPECIFIED
		},
		"unknown result": func(_ *agentpb.ObserveServices, result *agentpb.ServiceObservationResult) {
			result.Observations[0].ProxyState = agentpb.ServiceProxyObservationState(99)
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalidRequest := proto.CloneOf(request)
			invalidResult := proto.CloneOf(result)
			mutate(invalidRequest, invalidResult)
			if err := ValidateResult(invalidRequest, invalidResult); err == nil {
				t.Fatal("accepted incomplete proxy evidence")
			}
		})
	}
}

// Rationale: process liveness without a healthcheck is not application health,
// and an undeployed desired scale may not excuse missing serving replicas.
func TestObservationStateUsesExactSealedReplicaCount(t *testing.T) {
	for _, test := range []struct {
		name     string
		counts   *agentpb.ServiceReplicaCounts
		expected uint32
		want     State
	}{
		{"missing evidence", nil, 2, Unavailable},
		{"no authority", &agentpb.ServiceReplicaCounts{}, 0, Unavailable},
		{"absent", &agentpb.ServiceReplicaCounts{}, 2, Absent},
		{"healthy", &agentpb.ServiceReplicaCounts{Healthy: 2}, 2, Healthy},
		{"missing replica", &agentpb.ServiceReplicaCounts{Healthy: 1}, 2, Degraded},
		{"extra replica", &agentpb.ServiceReplicaCounts{Healthy: 3}, 2, Degraded},
		{"no checks", &agentpb.ServiceReplicaCounts{Running: 2}, 2, Running},
		{"mixed checks", &agentpb.ServiceReplicaCounts{Healthy: 1, Running: 1}, 2, Running},
		{"unhealthy", &agentpb.ServiceReplicaCounts{Healthy: 1, Unhealthy: 1}, 2, Degraded},
		{"starting", &agentpb.ServiceReplicaCounts{Transitional: 1, Starting: 1}, 2, Starting},
		{"stopped", &agentpb.ServiceReplicaCounts{Stopped: 2}, 2, Stopped},
		{"failed", &agentpb.ServiceReplicaCounts{Failed: 2}, 2, Failed},
		{"mixed failure", &agentpb.ServiceReplicaCounts{Stopped: 1, Failed: 1}, 2, Degraded},
		{"overflow", &agentpb.ServiceReplicaCounts{Healthy: math.MaxUint32, Running: 1}, 2, Unavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := Summarize(test.counts, test.expected); got != test.want {
				t.Fatalf("state = %q, want %q", got, test.want)
			}
		})
	}
}

// Rationale: OBS-05; proxy evidence changes the public aggregate only when a
// workload-only count would otherwise claim healthy or running service.
func TestObservationStateIncludesStableProxyEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		counts *agentpb.ServiceReplicaCounts
		proxy  agentpb.ServiceProxyObservationState
		want   State
	}{
		{"matching healthy", &agentpb.ServiceReplicaCounts{Healthy: 1}, agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MATCHING, Healthy},
		{"missing proxy", &agentpb.ServiceReplicaCounts{Healthy: 1}, agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MISSING, Degraded},
		{"stopped proxy", &agentpb.ServiceReplicaCounts{Running: 1}, agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_STOPPED, Degraded},
		{"wrong target", &agentpb.ServiceReplicaCounts{Healthy: 1}, agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_CONFIG_MISMATCH, Degraded},
		{"workload absent", &agentpb.ServiceReplicaCounts{}, agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MISSING, Absent},
		{"missing authority", &agentpb.ServiceReplicaCounts{Healthy: 1}, agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_UNSPECIFIED, Unavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := SummarizeProxy(test.counts, 1, test.proxy); got != test.want {
				t.Fatalf("state = %q, want %q", got, test.want)
			}
		})
	}
}
