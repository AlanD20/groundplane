package runner

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// RUN-05: a Docker 404 must cause creation, while a replay must retain the
// owned network. Unwrapping past Docker's typed error used to prevent creation.
func TestEnsureNetworkCreatesMissingNetworkAndReusesOwnedNetwork(t *testing.T) {
	plan := validRuntimePlan(t)
	creates := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/networks/create"):
			creates++
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"Id":"owned-network"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/networks/"+plan.Network.Name):
			if creates == 0 {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"message":"network not found"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(network.Inspect{
				Network: network.Network{
					ID: "owned-network", Name: plan.Network.Name, Driver: "bridge",
					Labels: map[string]string{
						"groundplane.runner.id":     plan.RunnerID,
						"groundplane.runtime.epoch": strconv.FormatUint(plan.RuntimeEpoch, 10),
					},
					IPAM: network.IPAM{
						Config: []network.IPAMConfig{{Subnet: plan.Network.Subnet, Gateway: plan.Network.Gateway}},
					},
					Options: map[string]string{"com.docker.network.bridge.name": plan.Network.BridgeName},
				},
			})
		default:
			t.Errorf("unexpected Docker request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	engine, err := client.New(client.WithHost(server.URL), client.WithAPIVersion("1.52"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	for range 2 {
		if err := ensureNetwork(t.Context(), engine, plan); err != nil {
			t.Fatal(err)
		}
	}
	if creates != 1 {
		t.Fatalf("network creates = %d, want one", creates)
	}
}
