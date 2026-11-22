package apiclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestShowMonitoringUsesGeneratedOperationAndPreservesUnsignedValues(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/api/v1/monitoring/service/svc_test" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		query := request.URL.Query()
		if query.Get("range") != "6h" || query.Get("points") != "120" || query.Get("limit") != "50" ||
			query.Get("cursor") != "container-next" || query.Get("consumer_cursor") != "consumer-next" {
			t.Errorf("query = %v", query)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{
  "scope":{"kind":"service","id":"svc_test","name":"api"},
  "window":{"range":"6h","start":"2026-11-22T10:00:00Z","end":"2026-11-22T16:00:00Z","bucket_seconds":180},
  "history_status":"available",
  "service":{
    "containers_status":"available",
    "live":{"status":"fresh","observed_at":"2026-11-22T15:59:55Z","coverage":{"eligible_resources":2,"sampled_resources":1,"eligible_volumes":1,"sampled_volumes":1},"values":{"cpu_cores":{"average":1.5,"minimum":1,"maximum":2,"valid_samples":1,"expected_samples":1,"valid_contributions":1,"expected_contributions":2}}},
    "history":[],
    "containers":{"items":[{"id":"container","replica":2,"restart_count":4,"live":{"status":"fresh","observed_at":null,"coverage":{"eligible_resources":1,"sampled_resources":1,"eligible_volumes":0,"sampled_volumes":0},"values":{}}}],"next_cursor":"container-next","revision":42},
    "volumes":[{"id":"vol_data","observed_at":"2026-11-22T15:59:00Z","used_bytes":4096}]
  }
}`))
	}))
	defer server.Close()

	got, err := New(server.URL).ShowMonitoring(
		context.Background(), "service", "svc_test", "6h", 120, 50, "container-next", "consumer-next",
	)
	if err != nil {
		t.Fatalf("ShowMonitoring() error = %v", err)
	}
	if got.Scope.Kind != "service" || got.Window.BucketSeconds != 180 || got.Service == nil {
		t.Fatalf("ShowMonitoring() = %#v", got)
	}
	service := got.Service
	if service.Live.Coverage.EligibleResources != 2 || service.Live.Values.CPUCores == nil ||
		service.Live.Values.CPUCores.ExpectedContributions != 2 ||
		len(service.Containers.Items) != 1 || service.Containers.Items[0].Replica != 2 ||
		service.Containers.Items[0].RestartCount != 4 || service.Containers.Revision != 42 ||
		len(service.Volumes) != 1 || service.Volumes[0].UsedBytes == nil || *service.Volumes[0].UsedBytes != 4096 {
		t.Fatalf("mapped Service monitoring = %#v", service)
	}
}

func TestShowMonitoringRejectsNegativeUnsignedWireValues(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{
  "scope":{"kind":"host","id":"local","name":"host"},
  "window":{"range":"1h","start":"2026-11-22T15:00:00Z","end":"2026-11-22T16:00:00Z","bucket_seconds":-1},
  "history_status":"unavailable",
  "host":{"live":{"status":"unavailable","observed_at":null,"coverage":{"eligible_resources":0,"sampled_resources":0,"eligible_volumes":0,"sampled_volumes":0},"values":{}},"history":[],"interfaces":[],"devices":[]}
}`))
	}))
	defer server.Close()

	_, err := New(server.URL).ShowMonitoring(context.Background(), "host", "local", "1h", 240, 25, "", "")
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("ShowMonitoring() error = %v, want internal", err)
	}
}
