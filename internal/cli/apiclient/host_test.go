package apiclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestShowHostUsesGeneratedHumanOperation(t *testing.T) {
	// Rationale: host show must use the generated host.show operation while
	// preserving the exact public nested model for JSON and table output.
	t.Parallel()

	want := apiTypes.Host{
		Hostname: "groundplane-test-1", Arch: "amd64", OS: "Ubuntu 24.04.3 LTS", Uptime: "2 days",
		CPU:    apiTypes.HostCPU{Model: "Example CPU", Cores: 4, Load: 25},
		Memory: apiTypes.HostResource{Total: "8 GiB", Used: "4 GiB", UsedPct: 50},
		Disk:   apiTypes.HostResource{Total: "80 GiB", Used: "20 GiB", UsedPct: 25},
		Swap:   apiTypes.HostResource{Total: "0 B", Used: "0 B", UsedPct: 0},
		Docker: "29.1.3",
		Etcd:   apiTypes.HostEtcd{Node: "single-node", Status: apiTypes.HealthHealthy, DBSize: "18 MiB"},
		Controller: apiTypes.HostController{
			Service: "groundplane-controller.service", Status: apiTypes.HealthHealthy, Version: "0.1.1",
		},
		Agent: apiTypes.HostAgent{
			Status: apiTypes.HealthHealthy, PullInterval: "2s", MaxConcurrent: 3,
			Labels: []string{},
		},
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/api/v1/host" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		response.Header().Set("Content-Type", "application/json")
		if _, err := response.Write(
			[]byte(
				`{"hostname":"groundplane-test-1","arch":"amd64","os":"Ubuntu 24.04.3 LTS","uptime":"2 days","cpu":{"model":"Example CPU","cores":4,"load":25},"memory":{"total":"8 GiB","used":"4 GiB","used_pct":50},"disk":{"total":"80 GiB","used":"20 GiB","used_pct":25},"swap":{"total":"0 B","used":"0 B","used_pct":0},"docker":"29.1.3","etcd":{"node":"single-node","status":"healthy","db_size":"18 MiB"},"controller":{"service":"groundplane-controller.service","status":"healthy","version":"0.1.1"},"agent":{"status":"healthy","pull_interval":"2s","max_concurrent":3,"labels":[]}}`,
			),
		); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	got, err := New(server.URL).ShowHost(context.Background())
	if err != nil {
		t.Fatalf("ShowHost() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ShowHost() = %#v, want %#v", got, want)
	}
}
