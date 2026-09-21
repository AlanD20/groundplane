package hierarchy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestTenantCreationServicePersistsEffectiveInputAndExactResponse(t *testing.T) {
	t.Parallel()

	repository := &fakeTenantCreationRepository{}
	idempotency := &fakeTenantCreationIdempotency{
		evidence:   tenantCreationTestEvidence(),
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := NewTenantCreationService(repository, idempotency)
	if err != nil {
		t.Fatalf("NewTenantCreationService() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	response, err := service.CreateTenant(context.Background(), CreateTenantInput{
		Slug: "acme", Description: "Production workloads",
	}, "tenant-create-key-0001")
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	if response.Status != http.StatusCreated || response.ContentKind != "application/json" {
		t.Fatalf("CreateTenant() response = %#v", response)
	}
	var tenant apiTypes.Tenant
	if err := json.Unmarshal(response.Body, &tenant); err != nil {
		t.Fatalf("CreateTenant() body = %s, %v", response.Body, err)
	}
	if tenant.ID == "" || tenant.Slug != "acme" || tenant.Name != "acme" ||
		tenant.Description != "Production workloads" {
		t.Fatalf("CreateTenant() Tenant = %#v", tenant)
	}
	if repository.record != (testhierarchy.TenantRecord{
		ID: tenant.ID, Slug: tenant.Slug, Name: tenant.Name, Description: tenant.Description,
	}) {
		t.Fatalf("persisted Tenant = %#v", repository.record)
	}
	if idempotency.tenant.ID != tenant.ID || idempotency.tenant.Name != "acme" {
		t.Fatalf("canonical Tenant = %#v", idempotency.tenant)
	}
	marker := repository.marker
	if marker.Kind != testidempotency.IdempotencyMarkerDirect || marker.State != testidempotency.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != testidempotency.IdempotencyScopePlatform || marker.Locator.ScopeID != "-" ||
		marker.Locator.Method != http.MethodPost || marker.Locator.Route != tenantCreationRoute ||
		marker.Locator.Key != "tenant-create-key-0001" ||
		marker.CreatedAt != now ||
		marker.TerminalAt != now ||
		marker.RetainUntil != now.Add(90*24*time.Hour) ||
		!reflect.DeepEqual(marker.Response, response) {
		t.Fatalf("persisted marker = %#v", marker)
	}
}

func TestTenantCreationServiceReturnsExactReplay(t *testing.T) {
	t.Parallel()

	want := testidempotency.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: []byte(`{"id":"tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV","slug":"acme","name":"Acme","description":""}`),
	}
	service, err := NewTenantCreationService(
		&fakeTenantCreationRepository{},
		&fakeTenantCreationIdempotency{
			evidence: tenantCreationTestEvidence(),
			resolution: idempotentintent.Resolution{
				Kind: idempotentintent.ResolutionReplay, Response: want,
			},
		},
	)
	if err != nil {
		t.Fatalf("NewTenantCreationService() error = %v", err)
	}
	got, err := service.CreateTenant(
		context.Background(),
		CreateTenantInput{Slug: "acme"},
		"tenant-create-key-0002",
	)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("CreateTenant(replay) = %#v, %v", got, err)
	}
}

type fakeTenantCreationRepository struct {
	record testhierarchy.TenantRecord
	marker testidempotency.IdempotencyMarker
}

func (repository *fakeTenantCreationRepository) CreateTenantIdempotent(
	_ context.Context,
	record testhierarchy.TenantRecord,
	marker testidempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.record = record
	repository.marker = marker
	repository.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeTenantCreationIdempotency struct {
	evidence   tenantCreationEvidence
	tenant     core.Tenant
	resolution idempotentintent.Resolution
}

func (idempotency *fakeTenantCreationIdempotency) Prepare(
	_ context.Context,
	tenant core.Tenant,
) (tenantCreationEvidence, error) {
	idempotency.tenant = tenant
	return idempotency.evidence, nil
}

func (idempotency *fakeTenantCreationIdempotency) ResolveKnown(
	context.Context,
	tenantCreationEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeTenantCreationIdempotency) ResolveUnknown(
	context.Context, testidempotency.IdempotencyLocator,

	tenantCreationEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func tenantCreationTestEvidence() tenantCreationEvidence {
	ciphertext := []byte("protected-tenant-create-intent")
	digest := sha256.Sum256(ciphertext)
	return tenantCreationEvidence{durable: testidempotency.ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}}
}
