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
	page        etcd.Page[etcd.ServiceRecord]
	wantRequest etcd.PageRequest
	wantEnv     string
	listed      bool
}

func (fake *fakeServiceReader) ListServices(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	fake.listed = environmentID == fake.wantEnv && request == fake.wantRequest
	return fake.page, nil
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
