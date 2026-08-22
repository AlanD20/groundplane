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

type fakeServiceMutationRepository struct {
	serviceMutationRepository
	environment etcd.Versioned[etcd.EnvironmentRecord]
	project     etcd.Versioned[etcd.ProjectRecord]
	zones       etcd.Page[etcd.ZoneRecord]
	record      etcd.ServiceRecord
	references  etcd.ServiceMutationReferences
	marker      etcd.IdempotencyMarker
}

func (fake *fakeServiceMutationRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeServiceMutationRepository) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return fake.project, nil
}

func (fake *fakeServiceMutationRepository) ListZones(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.ZoneRecord], error) {
	return fake.zones, nil
}

func (fake *fakeServiceMutationRepository) ListServices(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return etcd.Page[etcd.ServiceRecord]{}, nil
}

func (fake *fakeServiceMutationRepository) CreateServiceIdempotent(
	_ context.Context,
	_ etcd.Versioned[etcd.EnvironmentRecord],
	_ etcd.Versioned[etcd.ProjectRecord],
	record etcd.ServiceRecord,
	references etcd.ServiceMutationReferences,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	fake.record = record
	fake.references = references
	fake.marker = marker
	fake.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	fake.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeServiceMutationIdempotency struct{ evidence serviceMutationEvidence }

func (fake *fakeServiceMutationIdempotency) Prepare(
	context.Context,
	serviceMutationIntent,
) (serviceMutationEvidence, error) {
	return fake.evidence, nil
}

func (fake *fakeServiceMutationIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	serviceMutationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{}, false, nil
}

func (fake *fakeServiceMutationIdempotency) ResolveKnown(
	context.Context,
	serviceMutationEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (fake *fakeServiceMutationIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	serviceMutationEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

func TestServiceCreationCommitsExactResponseAndZoneFence(t *testing.T) {
	// Rationale: a direct Service create must allocate stable identity, start at
	// running intent, and atomically bind its exact response to every live Zone.
	t.Parallel()
	at := time.Date(2026, time.August, 23, 1, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	zoneID := ids.NewAt(ids.KindNetwork, at, 3)
	repository := &fakeServiceMutationRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{
				ID:                environmentID,
				ProjectID:         projectID,
				ProvisioningState: etcd.EnvironmentProvisioningReady,
			},
			Revision:     7,
			ReadRevision: 7,
		},
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record:       etcd.ProjectRecord{ID: projectID, Kind: etcd.ProjectKindTenant},
			Revision:     8,
			ReadRevision: 8,
		},
		zones: etcd.Page[etcd.ZoneRecord]{
			Items: []etcd.Versioned[etcd.ZoneRecord]{
				{Record: etcd.ZoneRecord{EnvironmentID: environmentID}, Revision: 9, ReadRevision: 9},
			},
		},
	}
	repository.zones.Items[0].Record.Desired.ID = zoneID
	repository.zones.Items[0].Record.Desired.Name = "backend"
	idempotency := &fakeServiceMutationIdempotency{
		evidence: serviceMutationEvidence{durable: projectCreationTestEvidence().durable},
	}
	service, err := newServiceMutationService(repository, idempotency)
	if err != nil {
		t.Fatalf("newServiceMutationService() error = %v", err)
	}
	now := at.Add(time.Hour)
	service.now = func() time.Time { return now }
	input := apiTypes.ServiceCreate{
		EnvironmentID: environmentID,
		Name:          "api",
		Image:         "app:stable",
		Zones:         []string{"backend"},
		Strategy:      "recreate",
		OnFailure:     apiTypes.OnFailureSwitchBack,
		Resources:     apiTypes.ServiceResources{Mem: "512m", CPUs: 0.5},
		Expose:        []string{"8080"},
		Restart:       "unless-stopped",
		Replicas:      1,
	}
	response, err := service.CreateService(context.Background(), input, "service-create-key-0001")
	if err != nil {
		t.Fatalf("CreateService() error = %v", err)
	}
	var created apiTypes.Service
	if err := json.Unmarshal(response.Body, &created); err != nil {
		t.Fatalf("CreateService() body = %s, %v", response.Body, err)
	}
	if response.Status != http.StatusCreated || created.ID == "" || created.EnvironmentID != environmentID ||
		created.RuntimeIntent != apiTypes.ServiceRuntimeIntentRunning ||
		created.Name != input.Name ||
		len(repository.references.Zones) != 1 ||
		repository.references.Zones[0].Record.Desired.ID != zoneID {
		t.Fatalf("created Service/references = %#v/%#v", created, repository.references)
	}
	if repository.record.Desired.ID != created.ID || repository.marker.Locator.Route != serviceCreationRoute ||
		repository.marker.Locator.Key != "service-create-key-0001" ||
		repository.marker.RetainUntil != now.Add(90*24*time.Hour) ||
		!reflect.DeepEqual(repository.marker.Response, response) {
		t.Fatalf("persisted Service/marker = %#v/%#v", repository.record, repository.marker)
	}
}
