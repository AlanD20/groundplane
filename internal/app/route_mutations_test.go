package app

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type fakeRouteMutationRepository struct {
	routeMutationRepository
	environment etcd.Versioned[etcd.EnvironmentRecord]
	project     etcd.Versioned[etcd.ProjectRecord]
	target      etcd.Versioned[etcd.ServiceRecord]
	record      etcd.RouteRecord
	marker      etcd.IdempotencyMarker
}

func (fake *fakeRouteMutationRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeRouteMutationRepository) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return fake.project, nil
}

func (fake *fakeRouteMutationRepository) GetService(
	context.Context,
	string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	return fake.target, nil
}

func (fake *fakeRouteMutationRepository) CreateRouteIdempotent(
	_ context.Context,
	_ etcd.Versioned[etcd.EnvironmentRecord],
	_ etcd.Versioned[etcd.ProjectRecord],
	_ etcd.Versioned[etcd.ServiceRecord],
	record etcd.RouteRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	fake.record = record
	fake.marker = marker
	fake.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	fake.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeRouteMutationIdempotency struct {
	evidence routeMutationEvidence
}

func (fake *fakeRouteMutationIdempotency) Prepare(
	context.Context,
	routeMutationIntent,
) (routeMutationEvidence, error) {
	return fake.evidence, nil
}

func (fake *fakeRouteMutationIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	routeMutationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{}, false, nil
}

func (fake *fakeRouteMutationIdempotency) ResolveKnown(
	context.Context,
	routeMutationEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (fake *fakeRouteMutationIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	routeMutationEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

func TestRouteCreationDerivesIdentityAndCommitsExactReplayResponse(t *testing.T) {
	// Rationale: the Controller alone allocates Route identity and must publish
	// the exact public response in the same transaction as its stable match.
	t.Parallel()
	at := time.Date(2026, time.August, 22, 18, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	targetID := ids.NewAt(ids.KindService, at, 3)
	repository := &fakeRouteMutationRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{ID: environmentID, ProjectID: projectID}, Revision: 7, ReadRevision: 7,
		},
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{ID: projectID, Kind: etcd.ProjectKindTenant}, Revision: 8, ReadRevision: 8,
		},
		target: etcd.Versioned[etcd.ServiceRecord]{
			Record: etcd.ServiceRecord{EnvironmentID: environmentID}, Revision: 9, ReadRevision: 9,
		},
	}
	repository.target.Record.Desired.ID = targetID
	idempotency := &fakeRouteMutationIdempotency{
		evidence: routeMutationEvidence{durable: projectCreationTestEvidence().durable},
	}
	service, err := newRouteMutationService(repository, idempotency)
	if err != nil {
		t.Fatalf("newRouteMutationService() error = %v", err)
	}
	now := at.Add(time.Hour)
	service.now = func() time.Time { return now }
	input := apiTypes.RouteCreate{
		EnvironmentID: environmentID, Host: "app.example.com", Path: "/api/*",
		Exposure: "public", TargetServiceID: targetID, TargetPort: 8080,
	}
	response, err := service.CreateRoute(context.Background(), input, "route-create-key-0001")
	if err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	var route apiTypes.Route
	if err := json.Unmarshal(response.Body, &route); err != nil {
		t.Fatalf("CreateRoute() body = %s, %v", response.Body, err)
	}
	if response.Status != http.StatusCreated || route.ID == "" || route.EnvironmentID != environmentID ||
		route.Host != input.Host || route.Path != input.Path || route.Exposure != input.Exposure ||
		route.TargetServiceID != targetID || route.TargetPort != input.TargetPort {
		t.Fatalf("CreateRoute() response = %#v/%#v", response, route)
	}
	if repository.record.Desired.ID != route.ID || repository.record.EnvironmentID != environmentID ||
		repository.marker.Locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		repository.marker.Locator.ScopeID != environmentID || repository.marker.Locator.Method != http.MethodPost ||
		repository.marker.Locator.Route != routeCreationRoute || repository.marker.Locator.Key != "route-create-key-0001" ||
		repository.marker.RetainUntil != now.Add(90*24*time.Hour) ||
		!reflect.DeepEqual(repository.marker.Response, response) {
		t.Fatalf("persisted Route/marker = %#v/%#v", repository.record, repository.marker)
	}
}
