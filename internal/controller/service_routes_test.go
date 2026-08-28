package controller

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeServiceReader struct {
	record      etcd.Versioned[etcd.ServiceRecord]
	native      string
	page        etcd.Page[etcd.ServiceRecord]
	wantRequest etcd.PageRequest
	wantEnv     string
	listed      bool
}

func (fake *fakeServiceReader) GetServiceNativeCompose(
	_ context.Context,
	_ string,
	_ string,
) (string, error) {
	return fake.native, nil
}

func (fake *fakeServiceReader) GetService(
	_ context.Context,
	serviceID string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	if fake.record.Record.Desired.ID == serviceID {
		return fake.record, nil
	}
	return etcd.Versioned[etcd.ServiceRecord]{}, nil
}

func (fake *fakeServiceReader) ListServices(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	fake.listed = environmentID == fake.wantEnv && request == fake.wantRequest
	return fake.page, nil
}

func TestShowServiceProjectsStableOwnerAndFullDesiredState(t *testing.T) {
	// Rationale: detail is the Console edit source, so it must project the
	// stable Environment owner and complete durable desired/runtime record.
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, serviceRouteTestTime(), 11)
	serviceID := ids.NewAt(ids.KindService, serviceRouteTestTime(), 12)
	record := etcd.ServiceRecord{
		EnvironmentID: environmentID,
		Desired: core.Service{
			ID:          serviceID,
			Name:        "api",
			Image:       "app:stable",
			Zones:       []string{"backend"},
			Strategy:    core.StrategyRecreate,
			OnFailure:   core.OnFailureSwitchBack,
			Healthcheck: core.Healthcheck{HTTP: "/up", Interval: "10s", Retries: 3},
			Resources:   core.Resources{Mem: "512m", CPUs: 0.5},
			Expose:      []string{"8080"},
			Replicas:    2,
		},
		Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	}
	server := &Server{services: &fakeServiceReader{
		record: etcd.Versioned[etcd.ServiceRecord]{Record: record},
		native: "services:\n  api:\n    image: app:stable\n",
	}}
	output, err := server.showService(context.Background(), &serviceShowInput{ID: serviceID})
	if err != nil || output.Body.EnvironmentID != environmentID || output.Body.Healthcheck == nil ||
		output.Body.Healthcheck.HTTP != "/up" ||
		output.Body.Resources.CPUs != 0.5 || output.Body.NativeCompose == "" {
		t.Fatalf("showService() = %#v, %v", output, err)
	}
}

// Rationale: the Service API must expose Controller-owned runtime intent alongside the desired
// projection while preserving stable ids and opaque Environment pagination.
func TestListServicesProjectsDesiredAndRuntimeState(t *testing.T) {
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, serviceRouteTestTime(), 1)
	serviceID := ids.NewAt(ids.KindService, serviceRouteTestTime(), 2)
	record := etcd.ServiceRecord{
		EnvironmentID: environmentID,
		Desired: core.Service{
			ID: serviceID, Name: "api", Image: "app:stable", Zones: []string{"backend"},
			Strategy: core.StrategyRecreate, OnFailure: core.OnFailureSwitchBack, Replicas: 2,
		},
		Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentStopped},
	}
	request := etcd.PageRequest{Limit: 3, Cursor: "opaque"}
	reader := &fakeServiceReader{
		page: etcd.Page[etcd.ServiceRecord]{
			Items: []etcd.Versioned[etcd.ServiceRecord]{{Record: record}}, NextCursor: "next", Revision: 71,
		},
		wantRequest: request, wantEnv: environmentID,
	}
	server := &Server{services: reader}
	output, err := server.listServices(context.Background(), &serviceListInput{
		Environment: environmentID, Limit: request.Limit, Cursor: request.Cursor,
	})
	if err != nil || !reader.listed || len(output.Body.Items) != 1 {
		t.Fatalf("listServices() = %#v, %v, listed %t", output, err, reader.listed)
	}
	want := serviceResponse(record)
	if !reflect.DeepEqual(output.Body.Items[0], want) || output.Body.NextCursor != "next" {
		t.Fatalf("Service page = %#v, want %#v", output.Body, want)
	}
}

func serviceRouteTestTime() time.Time {
	return time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
}
