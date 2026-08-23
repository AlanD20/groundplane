package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type systemSnapshotFunc func(context.Context) (SystemSnapshot, error)

func (function systemSnapshotFunc) SystemSnapshot(ctx context.Context) (SystemSnapshot, error) {
	return function(ctx)
}

type etcdSnapshotFunc func(context.Context) (EtcdSnapshot, error)

func (function etcdSnapshotFunc) EtcdSnapshot(ctx context.Context) (EtcdSnapshot, error) {
	return function(ctx)
}

type agentSnapshotFunc func(context.Context) (AgentSnapshot, error)

func (function agentSnapshotFunc) AgentSnapshot(ctx context.Context) (AgentSnapshot, error) {
	return function(ctx)
}

func TestHostServiceProjectsAcceptedHealthyShape(t *testing.T) {
	// Rationale: the Console store is the Host API contract, so the Controller
	// must project every accepted field without retaining the obsolete endpoint list.
	t.Parallel()

	want := acceptedHostFixture(api.HealthHealthy, api.HealthHealthy)
	service := hostServiceFor(t, want, nil, nil, nil)
	got, err := service.Show(context.Background())
	if err != nil {
		t.Fatalf("Show() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Show() = %#v, want %#v", got, want)
	}
	got.Agent.Labels[0] = "mutated"
	if want.Agent.Labels[0] == "mutated" {
		t.Fatal("Show() returned the Agent source's label slice")
	}
}

func TestHostServicePreservesEmptyAgentLabelsAsArray(t *testing.T) {
	// Rationale: the Host API has one stable array contract for Agent labels;
	// an empty durable label map must not become JSON null.
	t.Parallel()

	want := acceptedHostFixture(api.HealthHealthy, api.HealthHealthy)
	want.Agent.Labels = []string{}
	service := hostServiceFor(t, want, nil, nil, nil)
	got, err := service.Show(context.Background())
	if err != nil {
		t.Fatalf("Show() error = %v", err)
	}
	if got.Agent.Labels == nil || len(got.Agent.Labels) != 0 {
		t.Fatalf("Show() Agent labels = %#v, want non-nil empty array", got.Agent.Labels)
	}
	encoded, err := json.Marshal(got.Agent)
	if err != nil {
		t.Fatalf("marshal Host Agent: %v", err)
	}
	if !strings.Contains(string(encoded), `"labels":[]`) {
		t.Fatalf("Host Agent JSON = %s, want empty labels array", encoded)
	}
}

func TestHostServicePreservesExpectedUnhealthySnapshots(t *testing.T) {
	// Rationale: a health endpoint must return observable failed/stopped state
	// as data instead of turning an unhealthy dependency into a failed request.
	t.Parallel()

	want := acceptedHostFixture(api.HealthFailed, api.HealthStopped)
	service := hostServiceFor(t, want, nil, nil, nil)
	got, err := service.Show(context.Background())
	if err != nil {
		t.Fatalf("Show() error = %v", err)
	}
	if got.Etcd.Status != api.HealthFailed || got.Agent.Status != api.HealthStopped {
		t.Fatalf("unhealthy projection = etcd %q, agent %q", got.Etcd.Status, got.Agent.Status)
	}
	if got.Controller.Status != api.HealthHealthy {
		t.Fatalf("serving Controller status = %q, want %q", got.Controller.Status, api.HealthHealthy)
	}
}

func TestHostServiceStopsOnCancellation(t *testing.T) {
	// Rationale: request cancellation must stop Host collection before later
	// probes perform I/O and must retain the context sentinel for server handling.
	t.Parallel()

	var systemCalls, etcdCalls, agentCalls int
	service, err := NewHostService(HostDependencies{
		System: systemSnapshotFunc(func(ctx context.Context) (SystemSnapshot, error) {
			systemCalls++
			return SystemSnapshot{}, nil
		}),
		Etcd: etcdSnapshotFunc(func(ctx context.Context) (EtcdSnapshot, error) {
			etcdCalls++
			return EtcdSnapshot{}, nil
		}),
		Agent: agentSnapshotFunc(func(ctx context.Context) (AgentSnapshot, error) {
			agentCalls++
			return AgentSnapshot{}, nil
		}),
		ControllerService: "groundplane-controller.service",
		ControllerVersion: "v0.4.2",
	})
	if err != nil {
		t.Fatalf("NewHostService() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Show(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Show() error = %v, want context.Canceled", err)
	}
	if systemCalls != 0 || etcdCalls != 0 || agentCalls != 0 {
		t.Fatalf("probe calls = system %d, etcd %d, agent %d; want zero", systemCalls, etcdCalls, agentCalls)
	}
}

func TestHostServiceNormalizesSourceErrors(t *testing.T) {
	// Rationale: raw infrastructure errors must not cross the human API, while
	// the one closed error taxonomy must retain an already-classified failure.
	t.Parallel()

	want := acceptedHostFixture(api.HealthHealthy, api.HealthHealthy)
	rawCause := errors.New("private endpoint diagnostic")
	rawService := hostServiceFor(t, want, rawCause, nil, nil)
	if _, err := rawService.Show(
		context.Background(),
	); !errors.Is(err, errs.New(errs.KindInternal, "")) ||
		!errors.Is(err, rawCause) {
		t.Fatalf("raw source error = %v, want wrapped internal cause", err)
	}
	for _, dependencyCause := range []error{context.Canceled, context.DeadlineExceeded} {
		dependencyService := hostServiceFor(t, want, dependencyCause, nil, nil)
		if _, err := dependencyService.Show(context.Background()); !errors.Is(err, errs.New(errs.KindInternal, "")) ||
			!errors.Is(err, dependencyCause) {
			t.Errorf("dependency context error = %v, want privately wrapped internal cause", err)
		}
	}

	classified := errs.New(errs.KindStorageUnavailable, "etcd quorum unavailable")
	classifiedService := hostServiceFor(t, want, nil, classified, nil)
	if _, err := classifiedService.Show(context.Background()); !errors.Is(err, classified) {
		t.Fatalf("classified source error = %v, want preserved taxonomy", err)
	}
}

func TestHostEndpointEmitsExactConsoleContract(t *testing.T) {
	// Rationale: host show, GET /host, and Platform Host are one capability;
	// the HTTP document must use the Console's exact snake_case shape.
	t.Parallel()

	want := acceptedHostFixture(api.HealthHealthy, api.HealthHealthy)
	service := hostServiceFor(t, want, nil, nil, nil)
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Host: service})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/host", nil)
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
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
	// Rationale: incomplete application wiring must be visible as a canonical
	// internal problem and must never fall back to invented Host values.
	t.Parallel()

	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	_, err := server.showHost(context.Background(), &struct{}{})
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("showHost() error = %v, want %q", err, errs.CodeInternal)
	}
}

func acceptedHostFixture(etcdStatus, agentStatus api.HealthState) api.Host {
	return api.Host{
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
			Node:   "single-node",
			Status: etcdStatus,
			DBSize: "18 MB",
		},
		Controller: api.HostController{
			Service: "groundplane-controller.service",
			Status:  api.HealthHealthy,
			Version: "v0.4.2",
		},
		Agent: api.HostAgent{
			Status:        agentStatus,
			PullInterval:  "2s",
			MaxConcurrent: 3,
			Labels:        []string{"qa-workload", "arm64"},
		},
	}
}

func hostServiceFor(
	t *testing.T,
	host api.Host,
	systemErr error,
	etcdErr error,
	agentErr error,
) *HostService {
	t.Helper()
	service, err := NewHostService(HostDependencies{
		System: systemSnapshotFunc(func(context.Context) (SystemSnapshot, error) {
			return SystemSnapshot{
				Hostname: host.Hostname,
				Arch:     host.Arch,
				OS:       host.OS,
				Uptime:   host.Uptime,
				CPU:      host.CPU,
				Memory:   host.Memory,
				Disk:     host.Disk,
				Swap:     host.Swap,
				Docker:   host.Docker,
			}, systemErr
		}),
		Etcd: etcdSnapshotFunc(func(context.Context) (EtcdSnapshot, error) {
			return EtcdSnapshot{Node: host.Etcd.Node, Status: host.Etcd.Status, DBSize: host.Etcd.DBSize}, etcdErr
		}),
		Agent: agentSnapshotFunc(func(context.Context) (AgentSnapshot, error) {
			return AgentSnapshot{
				Status:        host.Agent.Status,
				PullInterval:  host.Agent.PullInterval,
				MaxConcurrent: host.Agent.MaxConcurrent,
				Labels:        host.Agent.Labels,
			}, agentErr
		}),
		ControllerService: host.Controller.Service,
		ControllerVersion: host.Controller.Version,
	})
	if err != nil {
		t.Fatalf("NewHostService() error = %v", err)
	}
	return service
}
