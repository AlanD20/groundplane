package network

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

func TestZoneCreationServiceDerivesOwnershipAndPersistsExactResponse(t *testing.T) {
	// Rationale: callers choose the Environment and subnet, while the
	// Controller alone derives stable ownership and publishes the replay body.
	t.Parallel()
	at := time.Date(2026, time.August, 22, 14, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	repository := &fakeZoneCreationRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record:   etcd.EnvironmentRecord{ID: environmentID, ProjectID: projectID},
			Revision: 7, ReadRevision: 7,
		},
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record:   etcd.ProjectRecord{ID: projectID, Kind: etcd.ProjectKindTenant},
			Revision: 8, ReadRevision: 8,
		},
	}
	idempotency := &fakeZoneCreationIdempotency{
		evidence:   zoneCreationEvidence{durable: networkTestProtectedIntent()},
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := newZoneCreationService(repository, idempotency)
	if err != nil {
		t.Fatalf("newZoneCreationService() error = %v", err)
	}
	now := time.Date(2026, time.August, 22, 15, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	input := apiTypes.ZoneCreate{
		EnvironmentID: environmentID, Name: "frontend.v2", Subnet: "10.34.20.0/24", Internal: true,
	}
	response, err := service.CreateZone(context.Background(), input, "zone-create-key-0001")
	if err != nil {
		t.Fatalf("CreateZone() error = %v", err)
	}
	var zone apiTypes.Zone
	if err := json.Unmarshal(response.Body, &zone); err != nil {
		t.Fatalf("CreateZone() body = %s, %v", response.Body, err)
	}
	if response.Status != http.StatusCreated || response.ContentKind != "application/json" ||
		zone.ID == "" || zone.EnvironmentID != environmentID || zone.Name != input.Name ||
		zone.Subnet != input.Subnet || !zone.Internal || zone.OwnerKind != apiTypes.ZoneOwnerEnvironment ||
		zone.OwnerID != environmentID {
		t.Fatalf("CreateZone() response/Zone = %#v/%#v", response, zone)
	}
	if repository.record.Desired.ID != zone.ID || repository.record.Desired.OwnerID != environmentID ||
		repository.record.Desired.Name != input.Name || repository.calls != 1 {
		t.Fatalf("persisted Zone/calls = %#v/%d", repository.record, repository.calls)
	}
	if repository.marker.Locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		repository.marker.Locator.ScopeID != environmentID ||
		repository.marker.Locator.Method != http.MethodPost ||
		repository.marker.Locator.Route != zoneCreationRoute ||
		repository.marker.Locator.Key != "zone-create-key-0001" ||
		repository.marker.RetainUntil != now.Add(90*24*time.Hour) ||
		!reflect.DeepEqual(repository.marker.Response, response) {
		t.Fatalf("persisted marker = %#v", repository.marker)
	}
}

func TestZoneCreationServiceReplaysBeforeHierarchyReads(t *testing.T) {
	// Rationale: an exact retry remains replayable even if the hierarchy has
	// changed after the original synchronous mutation committed.
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, time.Date(2026, time.August, 22, 16, 0, 0, 0, time.UTC), 1)
	want := etcd.IdempotencyResponse{
		Status:      http.StatusCreated,
		ContentKind: "application/json",
		Body: []byte(
			`{"id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAV","environment_id":"` + environmentID + `","name":"frontend","subnet":"10.34.20.0/24","internal":false,"owner_kind":"environment","owner_id":"` + environmentID + `"}`,
		),
	}
	repository := &fakeZoneCreationRepository{}
	service, err := newZoneCreationService(repository, &fakeZoneCreationIdempotency{
		evidence:   zoneCreationEvidence{durable: networkTestProtectedIntent()},
		existing:   true,
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionReplay, Response: want},
	})
	if err != nil {
		t.Fatalf("newZoneCreationService() error = %v", err)
	}
	got, err := service.CreateZone(context.Background(), apiTypes.ZoneCreate{
		EnvironmentID: environmentID, Name: "frontend", Subnet: "10.34.20.0/24",
	}, "zone-create-key-0002")
	if err != nil || !reflect.DeepEqual(got, want) || repository.reads != 0 || repository.calls != 0 {
		t.Fatalf("CreateZone(replay) = %#v, %v, reads/calls %d/%d", got, err, repository.reads, repository.calls)
	}
}

type fakeZoneCreationRepository struct {
	environment etcd.Versioned[etcd.EnvironmentRecord]
	project     etcd.Versioned[etcd.ProjectRecord]
	record      etcd.ZoneRecord
	marker      etcd.IdempotencyMarker
	result      etcd.IdempotencyTransactionResult
	reads       int
	calls       int
}

func (repository *fakeZoneCreationRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	repository.reads++
	return repository.environment, nil
}

func (repository *fakeZoneCreationRepository) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	repository.reads++
	return repository.project, nil
}

func (repository *fakeZoneCreationRepository) CreateZoneIdempotent(
	_ context.Context,
	_ etcd.Versioned[etcd.EnvironmentRecord],
	_ etcd.Versioned[etcd.ProjectRecord],
	record etcd.ZoneRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.calls++
	repository.record = record
	repository.marker = marker
	repository.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return repository.result, nil
}

type fakeZoneCreationIdempotency struct {
	evidence   zoneCreationEvidence
	resolution idempotentintent.Resolution
	existing   bool
}

func (idempotency *fakeZoneCreationIdempotency) Prepare(
	context.Context,
	apiTypes.ZoneCreate,
) (zoneCreationEvidence, error) {
	return idempotency.evidence, nil
}

func (idempotency *fakeZoneCreationIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	zoneCreationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.existing, nil
}

func (idempotency *fakeZoneCreationIdempotency) ResolveKnown(
	context.Context,
	zoneCreationEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeZoneCreationIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	zoneCreationEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}
