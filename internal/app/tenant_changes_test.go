package app

import (
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

const testTenantChangeID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestTenantChangeServicePersistsEditAndRenameResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		route      string
		wantRecord etcd.TenantRecord
		call       func(*tenantChangeService) (etcd.IdempotencyResponse, error)
	}{
		{
			name: "edit", method: http.MethodPatch, route: tenantEditRoute,
			wantRecord: etcd.TenantRecord{
				ID: testTenantChangeID, Slug: "acme", Name: "Acme Inc", Description: "Production",
			},
			call: func(service *tenantChangeService) (etcd.IdempotencyResponse, error) {
				name := "Acme Inc"
				description := "Production"
				return service.EditTenant(context.Background(), testTenantChangeID, hierarchy.EditTenantInput{
					Name: &name, Description: &description,
				}, "tenant-change-key-0001")
			},
		},
		{
			name: "rename", method: http.MethodPost, route: tenantRenameRoute,
			wantRecord: etcd.TenantRecord{
				ID: testTenantChangeID, Slug: "acme-inc", Name: "Acme", Description: "",
			},
			call: func(service *tenantChangeService) (etcd.IdempotencyResponse, error) {
				return service.RenameTenant(context.Background(), testTenantChangeID, hierarchy.RenameTenantInput{
					Slug: "acme-inc",
				}, "tenant-change-key-0001")
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			repository := &fakeTenantChangeRepository{current: etcd.Versioned[etcd.TenantRecord]{
				Record:   etcd.TenantRecord{ID: testTenantChangeID, Slug: "acme", Name: "Acme"},
				Revision: 7, ReadRevision: 7,
			}}
			idempotency := &fakeTenantChangeIdempotency{
				evidence:   tenantChangeEvidence{durable: tenantCreationTestEvidence().durable},
				resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
			}
			service, err := newTenantChangeService(repository, idempotency)
			if err != nil {
				t.Fatalf("newTenantChangeService() error = %v", err)
			}
			now := time.Date(2026, 8, 22, 14, 0, 0, 0, time.UTC)
			service.now = func() time.Time { return now }
			response, err := test.call(service)
			if err != nil {
				t.Fatalf("Tenant change error = %v", err)
			}
			if response.Status != http.StatusOK || response.ContentKind != "application/json" ||
				repository.replacement != test.wantRecord {
				t.Fatalf("Tenant change response/record = %#v/%#v", response, repository.replacement)
			}
			if repository.marker.Locator.Method != test.method || repository.marker.Locator.Route != test.route ||
				repository.marker.TerminalAt != now || !reflect.DeepEqual(repository.marker.Response, response) {
				t.Fatalf("Tenant change marker = %#v", repository.marker)
			}
		})
	}
}

func TestTenantChangeServiceReturnsReplayBeforeTargetRead(t *testing.T) {
	t.Parallel()

	want := etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"tenant"}`),
	}
	repository := &fakeTenantChangeRepository{}
	service, err := newTenantChangeService(repository, &fakeTenantChangeIdempotency{
		evidence:   tenantChangeEvidence{durable: tenantCreationTestEvidence().durable},
		existing:   true,
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionReplay, Response: want},
	})
	if err != nil {
		t.Fatalf("newTenantChangeService() error = %v", err)
	}
	got, err := service.RenameTenant(
		context.Background(), testTenantChangeID,
		hierarchy.RenameTenantInput{Slug: "acme-inc"}, "tenant-change-key-0002",
	)
	if err != nil || !reflect.DeepEqual(got, want) || repository.getCalls != 0 {
		t.Fatalf("RenameTenant(replay) = %#v, %v, get calls %d", got, err, repository.getCalls)
	}
}

type fakeTenantChangeRepository struct {
	current     etcd.Versioned[etcd.TenantRecord]
	replacement etcd.TenantRecord
	marker      etcd.IdempotencyMarker
	getCalls    int
}

func (repository *fakeTenantChangeRepository) GetTenant(
	context.Context,
	string,
) (etcd.Versioned[etcd.TenantRecord], error) {
	repository.getCalls++
	return repository.current, nil
}

func (repository *fakeTenantChangeRepository) MutateTenantIdempotent(
	_ context.Context,
	_ etcd.Versioned[etcd.TenantRecord],
	replacement etcd.TenantRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.replacement = replacement
	repository.marker = marker
	repository.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeTenantChangeIdempotency struct {
	evidence   tenantChangeEvidence
	existing   bool
	resolution idempotentintent.Resolution
}

func (idempotency *fakeTenantChangeIdempotency) PrepareEdit(
	context.Context,
	string,
	hierarchy.EditTenantInput,
) (tenantChangeEvidence, error) {
	return idempotency.evidence, nil
}

func (idempotency *fakeTenantChangeIdempotency) PrepareRename(
	context.Context,
	string,
	hierarchy.RenameTenantInput,
) (tenantChangeEvidence, error) {
	return idempotency.evidence, nil
}

func (idempotency *fakeTenantChangeIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	tenantChangeEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.existing, nil
}

func (idempotency *fakeTenantChangeIdempotency) ResolveKnown(
	context.Context,
	tenantChangeEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeTenantChangeIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	tenantChangeEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}
