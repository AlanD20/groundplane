package network

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeRouteMutationRepository struct {
	routeMutationRepository
	environment etcd.Versioned[etcd.EnvironmentRecord]
	project     etcd.Versioned[etcd.ProjectRecord]
	target      etcd.Versioned[etcd.ServiceRecord]
	record      etcd.RouteRecord
	marker      etcd.IdempotencyMarker
	createCalls int
	task        etcd.TaskRecord
	intent      etcd.RouteMutationIntent
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

func (fake *fakeRouteMutationRepository) BeginRouteMutationWithTask(
	_ context.Context,
	_ etcd.Versioned[etcd.EnvironmentRecord],
	_ etcd.Versioned[etcd.ProjectRecord],
	_ etcd.Versioned[etcd.ServiceRecord],
	_ *etcd.Versioned[etcd.RouteRecord],
	record etcd.RouteRecord,
	intent etcd.RouteMutationIntent,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	fake.createCalls++
	fake.record, fake.intent, fake.task, fake.marker = record, intent, task, marker
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
	tenantID := ids.NewAt(ids.KindTenant, at, 4)
	targetID := ids.NewAt(ids.KindService, at, 3)
	repository := &fakeRouteMutationRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{ID: environmentID, ProjectID: projectID}, Revision: 7, ReadRevision: 7,
		},
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{ID: projectID, TenantID: tenantID, Kind: etcd.ProjectKindTenant}, Revision: 8, ReadRevision: 8,
		},
		target: etcd.Versioned[etcd.ServiceRecord]{
			Record: etcd.ServiceRecord{EnvironmentID: environmentID}, Revision: 9, ReadRevision: 9,
		},
	}
	repository.target.Record.Desired.ID = targetID
	repository.target.Record.Desired.Expose = []string{"8080"}
	idempotency := &fakeRouteMutationIdempotency{
		evidence: routeMutationEvidence{durable: networkTestProtectedIntent()},
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
	var accepted apiTypes.RouteTaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatalf("CreateRoute() body = %s, %v", response.Body, err)
	}
	route := accepted.Route
	if response.Status != http.StatusAccepted || accepted.TaskID == "" || route.ID == "" || route.EnvironmentID != environmentID ||
		route.Host != input.Host || route.Path != input.Path || route.Exposure != input.Exposure ||
		route.TargetServiceID != targetID || route.TargetPort != input.TargetPort {
		t.Fatalf("CreateRoute() response = %#v/%#v", response, route)
	}
	if repository.record.Desired.ID != route.ID || repository.record.EnvironmentID != environmentID ||
		repository.record.Observed.Status != etcd.RouteObservedUnserved || repository.task.ID != accepted.TaskID ||
		repository.task.Executor != etcd.TaskExecutorController || repository.intent.TaskID != accepted.TaskID ||
		repository.marker.Locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		repository.marker.Locator.ScopeID != environmentID || repository.marker.Locator.Method != http.MethodPost ||
		repository.marker.Locator.Route != routeCreationRoute || repository.marker.Locator.Key != "route-create-key-0001" ||
		repository.marker.RetainUntil != now.Add(90*24*time.Hour) ||
		!reflect.DeepEqual(repository.marker.Response, response) {
		t.Fatalf("persisted Route/marker = %#v/%#v", repository.record, repository.marker)
	}
}

func TestRouteCreationRejectsUnsafePathBeforePersistence(t *testing.T) {
	t.Parallel()
	repository := &fakeRouteMutationRepository{}
	service, err := newRouteMutationService(repository, &fakeRouteMutationIdempotency{})
	if err != nil {
		t.Fatalf("newRouteMutationService() error = %v", err)
	}
	input := apiTypes.RouteCreate{
		EnvironmentID: ids.New(ids.KindEnvironment), Host: "app.example.com",
		Path: "/ok\n}\nrespond 200\n", Exposure: "public",
		TargetServiceID: ids.New(ids.KindService), TargetPort: 8080,
	}
	_, err = service.CreateRoute(context.Background(), input, "route-create-key-unsafe")
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("CreateRoute() error = %v, want validation_failed", err)
	}
	if repository.createCalls != 0 {
		t.Fatalf("CreateRoute() persistence calls = %d, want 0", repository.createCalls)
	}
}

func TestRouteCreationRejectsUnexposedTargetPort(t *testing.T) {
	t.Parallel()
	environmentID := ids.New(ids.KindEnvironment)
	projectID := ids.New(ids.KindProject)
	tenantID := ids.New(ids.KindTenant)
	targetID := ids.New(ids.KindService)
	repository := &fakeRouteMutationRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{ID: environmentID, ProjectID: projectID}, Revision: 7, ReadRevision: 7,
		},
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{ID: projectID, TenantID: tenantID, Kind: etcd.ProjectKindTenant}, Revision: 8, ReadRevision: 8,
		},
		target: etcd.Versioned[etcd.ServiceRecord]{
			Record: etcd.ServiceRecord{EnvironmentID: environmentID}, Revision: 9, ReadRevision: 9,
		},
	}
	repository.target.Record.Desired.ID = targetID
	repository.target.Record.Desired.Expose = []string{"9000", "8080/udp"}
	service, err := newRouteMutationService(repository, &fakeRouteMutationIdempotency{
		evidence: routeMutationEvidence{durable: networkTestProtectedIntent()},
	})
	if err != nil {
		t.Fatalf("newRouteMutationService() error = %v", err)
	}
	_, err = service.CreateRoute(context.Background(), apiTypes.RouteCreate{
		EnvironmentID: environmentID, Host: "app.example.com", Path: "/api/*", Exposure: "public",
		TargetServiceID: targetID, TargetPort: 8080,
	}, "route-create-unexposed")
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("CreateRoute() error = %v, want validation_failed", err)
	}
	if repository.createCalls != 0 {
		t.Fatalf("CreateRoute() persistence calls = %d, want 0", repository.createCalls)
	}
}

// Rationale: templated entity routes must bind the stable target id into the
// protected intent or every Route edit fails before reaching persistence.
func TestRouteEditMutationIntentBindsStableTarget(t *testing.T) {
	routeID := ids.New(ids.KindRoute)
	intent := routeEditMutationIntent(
		routeID,
		ids.New(ids.KindEnvironment),
		apiTypes.RouteEdit{Exposure: "tunnel"},
	)
	if len(intent.path) != 1 || intent.path[0].Name != "id" || intent.path[0].Value != routeID {
		t.Fatalf("route edit path bindings = %#v", intent.path)
	}
}
