package controller

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/api"
)

type routeServiceObserver struct {
	result api.ServiceObservation
	calls  [][]etcd.Versioned[etcd.ServiceRecord]
}

func (observer *routeServiceObserver) ObserveServices(
	_ context.Context, records []etcd.Versioned[etcd.ServiceRecord],
) []api.ServiceObservation {
	observer.calls = append(observer.calls, records)
	result := make([]api.ServiceObservation, len(records))
	for index := range result {
		result[index] = observer.result
	}
	return result
}

// Rationale: existing list/show must serialize fresh serving evidence next to,
// not in place of, undeployed desired replicas and Controller runtime intent.
func TestServiceObservationListAndShowHTTP(t *testing.T) {
	t.Parallel()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	record := etcd.Versioned[etcd.ServiceRecord]{Record: etcd.ServiceRecord{
		EnvironmentID: environmentID,
		Desired:       core.Service{ID: serviceID, Name: "api", Image: "api:desired", Replicas: 9},
		Runtime:       core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	}, ReadRevision: 40}
	started := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	expires, releaseID, expected := started.Add(15*time.Second), "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", uint32(2)
	observer := &routeServiceObserver{result: api.ServiceObservation{
		State: api.ServiceObservationHealthy, ObservedAt: &started, ExpiresAt: &expires,
		ServingReleaseID: &releaseID, ExpectedReplicas: &expected, Replicas: &api.ServiceReplicaCounts{Healthy: 2},
	}}
	reader := &fakeServiceReader{record: record, native: "services: {}", page: etcd.Page[etcd.ServiceRecord]{
		Items: []etcd.Versioned[etcd.ServiceRecord]{record}, Revision: 40,
	}}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		Services: reader, ServiceObservations: observer, Releases: &fakeServiceReleaseReader{},
	})
	for _, path := range []string{"/api/v1/services?environment=" + environmentID, "/api/v1/services/" + serviceID} {
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", path, response.Code, response.Body.String())
		}
		var got api.Service
		if path == "/api/v1/services/"+serviceID {
			if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
		} else {
			var page api.Page[api.Service]
			if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil || len(page.Items) != 1 {
				t.Fatalf("bad Service page: %+v, %v", page, err)
			}
			got = page.Items[0]
		}
		if got.Replicas != 9 || got.RuntimeIntent != api.ServiceRuntimeIntentRunning || got.Observation == nil ||
			got.Observation.State != api.ServiceObservationHealthy || *got.Observation.ExpectedReplicas != 2 ||
			got.Observation.Replicas.Healthy != 2 || *got.Observation.ServingReleaseID != releaseID {
			t.Fatalf("observation and desired state not preserved: %+v", got)
		}
	}
	if len(observer.calls) != 2 || observer.calls[0][0].ReadRevision != 40 || observer.calls[1][0].ReadRevision != 40 {
		t.Fatalf("handler changed the desired-read snapshot: %+v", observer.calls)
	}
}
