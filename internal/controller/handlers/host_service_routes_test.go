package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type hostReaderFunc func(context.Context) (api.Host, error)

func (reader hostReaderFunc) Show(ctx context.Context) (api.Host, error) {
	return reader(ctx)
}

func TestHostEndpointEmitsExactConsoleContract(t *testing.T) {
	t.Parallel()

	want := api.Host{
		Hostname: "qa-workload-groundplane",
		Arch:     "arm64",
		OS:       "Debian 12 (bookworm)",
		Uptime:   "34 days",
		CPU:      api.HostCPU{Model: "BCM2712 Cortex-A76", Cores: 4, Load: 38},
		Memory:   api.HostResource{Total: "8 GB", Used: "5.1 GB", UsedPct: 64},
		Disk:     api.HostResource{Total: "256 GB", Used: "96 GB", UsedPct: 38},
		Swap:     api.HostResource{Total: "4 GB", Used: "0.3 GB", UsedPct: 8},
		Docker:   "27.3.1",
		Etcd: api.HostEtcd{
			Node: "single-node", Status: api.HealthHealthy, DBSize: "18 MB",
		},
		Controller: api.HostController{
			Service: "groundplane-controller.service", Status: api.HealthHealthy, Version: "v0.4.2",
		},
		Agent: api.HostAgent{
			Status: api.HealthHealthy, PullInterval: "2s", MaxConcurrent: 3,
			Labels: []string{"qa-workload", "arm64"},
		},
	}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		Host: hostReaderFunc(func(context.Context) (api.Host, error) { return want, nil }),
	})
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/host", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /host status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	var got api.Host
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode GET /host: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GET /host body = %#v, want %#v", got, want)
	}
	encoded, err := json.Marshal(got.Etcd)
	if err != nil {
		t.Fatalf("marshal etcd projection: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("decode etcd projection: %v", err)
	}
	if len(fields) != 3 || fields["node"] == nil || fields["status"] == nil || fields["db_size"] == nil {
		t.Fatalf("etcd fields = %v, want node/status/db_size only", reflect.ValueOf(fields).MapKeys())
	}
}

func TestHostEndpointFailsClosedWithoutService(t *testing.T) {
	t.Parallel()

	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	_, err := server.showHost(context.Background(), &struct{}{})
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("showHost() error = %v, want %q", err, errs.CodeInternal)
	}
}
