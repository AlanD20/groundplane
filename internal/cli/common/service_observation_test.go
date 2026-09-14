package common

import (
	"reflect"
	"slices"
	"testing"
	"time"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// QA: OBS-01, OBS-02, UI-01; pure projection only, not live container observation or API rendering.
// Rationale: the Service collection must show runtime intent separately from
// every exact serving-workload count without losing the nested JSON snapshot.
func TestServiceObservationTableFormatsExactProjection(t *testing.T) {
	t.Parallel()
	observedAt := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	expiresAt := observedAt.Add(serviceObservationFreshness)
	releaseID := "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	expected := uint32(28)
	services := []apiTypes.Service{{
		ID: "svc_1", Name: "api", RuntimeIntent: apiTypes.ServiceRuntimeIntentStopped,
		Observation: &apiTypes.ServiceObservation{
			State: apiTypes.ServiceObservationDegraded, ObservedAt: &observedAt, ExpiresAt: &expiresAt,
			ServingReleaseID: &releaseID, ExpectedReplicas: &expected,
			Replicas: &apiTypes.ServiceReplicaCounts{
				Running: 1, Healthy: 2, Starting: 3, Unhealthy: 4,
				Transitional: 5, Stopped: 6, Failed: 7,
			},
		},
	}}

	presented, headers, rows := ServiceObservationTable(services, observedAt.Add(time.Second))
	wantHeaders := []string{
		"ID", "NAME", "RUNTIME_INTENT", "OBSERVATION_STATE", "OBSERVED_AT", "EXPIRES_AT",
		"SERVING_RELEASE_ID", "EXPECTED_REPLICAS", "RUNNING", "HEALTHY", "STARTING",
		"UNHEALTHY", "TRANSITIONAL", "STOPPED", "FAILED",
	}
	wantRow := []string{
		"svc_1", "api", "stopped", "degraded", "2026-09-12T12:00:00Z", "2026-09-12T12:00:15Z",
		releaseID, "28", "1", "2", "3", "4", "5", "6", "7",
	}
	wantObservation := &apiTypes.ServiceObservation{
		State: apiTypes.ServiceObservationDegraded, ObservedAt: &observedAt, ExpiresAt: &expiresAt,
		ServingReleaseID: &releaseID, ExpectedReplicas: &expected,
		Replicas: &apiTypes.ServiceReplicaCounts{
			Running: 1, Healthy: 2, Starting: 3, Unhealthy: 4,
			Transitional: 5, Stopped: 6, Failed: 7,
		},
	}
	if !slices.Equal(headers, wantHeaders) || len(rows) != 1 || !slices.Equal(rows[0], wantRow) ||
		!reflect.DeepEqual(presented[0].Observation, wantObservation) {
		t.Fatalf("Service observation table = %#v / %#v / %#v", presented, headers, rows)
	}
}

// QA: OBS-02, OBS-03, UI-01; local clock and schema checks only, not stalled refresh or live health.
// Rationale: expiry is exclusive at the exact boundary, and incomplete,
// contradictory, over-bounded, or falsely enriched evidence must never appear
// as current workload health.
func TestPresentServiceObservationRejectsExpiredOrMalformedEvidence(t *testing.T) {
	t.Parallel()
	observedAt := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	expiresAt := observedAt.Add(serviceObservationFreshness)
	releaseID := "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	expected := uint32(1)
	fresh := func() *apiTypes.ServiceObservation {
		return &apiTypes.ServiceObservation{
			State: apiTypes.ServiceObservationHealthy, ObservedAt: &observedAt, ExpiresAt: &expiresAt,
			ServingReleaseID: &releaseID, ExpectedReplicas: &expected,
			Replicas: &apiTypes.ServiceReplicaCounts{Healthy: 1},
		}
	}

	got := PresentServiceObservation(fresh(), expiresAt.Add(-time.Nanosecond))
	if got.State != "healthy" || got.Healthy != "1" {
		t.Fatalf("fresh observation = %#v", got)
	}
	// OBS-05: a proxy mismatch degrades healthy workloads without inventing
	// an unhealthy replica or discarding the Controller's valid snapshot.
	proxyFailure := fresh()
	proxyFailure.State = apiTypes.ServiceObservationDegraded
	if got := PresentServiceObservation(proxyFailure, observedAt); got.State != "degraded" || got.Healthy != "1" {
		t.Fatalf("proxy degradation lost: %#v", got)
	}
	tests := []struct {
		name        string
		observation *apiTypes.ServiceObservation
		now         time.Time
	}{
		{name: "nil", now: observedAt},
		{name: "future timestamp", observation: fresh(), now: observedAt.Add(-time.Nanosecond)},
		{name: "expired at boundary", observation: fresh(), now: expiresAt},
		{name: "expired after boundary", observation: fresh(), now: expiresAt.Add(time.Nanosecond)},
		{name: "missing counts", observation: func() *apiTypes.ServiceObservation {
			observation := fresh()
			observation.Replicas = nil
			return observation
		}(), now: observedAt},
		{name: "wrong state", observation: func() *apiTypes.ServiceObservation {
			observation := fresh()
			observation.State = apiTypes.ServiceObservationFailed
			return observation
		}(), now: observedAt},
		{name: "wrong freshness window", observation: func() *apiTypes.ServiceObservation {
			observation := fresh()
			changed := expiresAt.Add(time.Second)
			observation.ExpiresAt = &changed
			return observation
		}(), now: observedAt},
		{name: "over bounded counts", observation: func() *apiTypes.ServiceObservation {
			observation := fresh()
			observation.State = apiTypes.ServiceObservationDegraded
			observation.Replicas.Healthy = 4097
			return observation
		}(), now: observedAt},
		{name: "enriched unavailable", observation: func() *apiTypes.ServiceObservation {
			observation := fresh()
			observation.State = apiTypes.ServiceObservationUnavailable
			return observation
		}(), now: observedAt},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := PresentServiceObservation(test.observation, test.now)
			if got.State != "unavailable" || got.Observation == nil ||
				got.Observation.State != apiTypes.ServiceObservationUnavailable ||
				got.Observation.ObservedAt != nil || got.Healthy != "" {
				t.Fatalf("unsafe observation = %#v", got)
			}
		})
	}
}

// QA: OBS-01, UI-01; pure empty-list projection only, not an API response serialization test.
// Rationale: normal empty list responses remain JSON arrays, not null, and
// still expose stable table headers without inventing a Service observation.
func TestServiceObservationTablePreservesEmptyArray(t *testing.T) {
	t.Parallel()
	services, headers, rows := ServiceObservationTable([]apiTypes.Service{}, time.Now())
	if services == nil || len(services) != 0 || len(headers) == 0 || len(rows) != 0 {
		t.Fatalf("empty table changed list shape: %+v, %+v, %+v", services, headers, rows)
	}
}
