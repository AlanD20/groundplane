package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

type emptyServiceRuntimeFixture struct{}

func joinServiceFixture(ctx context.Context, store *memoryHierarchyStore,
	projection keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection], serviceID, fenceKey string,
) (keyvalue.Versioned[services.ServiceRecord], error) {
	return services.ReadJoined(ctx, store, services.DesiredSelection{
		Services: projection.Record.DesiredServices, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
	}, serviceID, fenceKey)
}

func refenceServiceFixture(
	t *testing.T,
	store *memoryHierarchyStore,
	service keyvalue.Versioned[services.ServiceRecord],
	key string,
	revision int64,
) keyvalue.Versioned[services.ServiceRecord] {
	t.Helper()
	joined, err := services.ReadJoined(t.Context(), store, services.DesiredSelection{
		Services: []services.EnvironmentServiceProjection{
			{
				EnvironmentID:    service.Record.EnvironmentID,
				BackingNetworkID: service.Record.BackingNetworkID,
				Desired:          service.Record.Desired,
			},
		},
		Revision: revision, ReadRevision: revision,
	}, service.Record.Desired.ID, key)
	if err != nil {
		t.Fatal(err)
	}
	return joined
}

func (emptyServiceRuntimeFixture) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	return &keyvalue.GetManyResult{
		Values:       make([]*keyvalue.KeyValue, len(request.Keys)),
		ReadRevision: request.Revision,
	}, nil
}

// Establish desired-head authority through the production join, rather than
// manufacturing the private fence fields in a Service record.
func selectedServiceFixture(
	t *testing.T,
	environmentID, serviceID, backingNetworkID string,
	revision int64,
) keyvalue.Versioned[services.ServiceRecord] {
	t.Helper()
	desired := core.Service{ID: serviceID, Name: "app", Image: "example/app:1"}
	if backingNetworkID != "" {
		desired.Adapter = "postgres:16"
	}
	service, err := services.ReadJoined(t.Context(), emptyServiceRuntimeFixture{}, services.DesiredSelection{
		Services: []services.EnvironmentServiceProjection{
			{EnvironmentID: environmentID, BackingNetworkID: backingNetworkID, Desired: desired},
		},
		Revision: revision, ReadRevision: revision,
	}, serviceID, blueprints.EnvironmentBlueprintHeadKey(environmentID))
	if err != nil {
		t.Fatalf("join Service fixture: %v", err)
	}
	return service
}
