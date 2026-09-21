package hierarchy

import (
	"context"
	"encoding/json"
	"errors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"
	"time"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const tenantCreationRoute = "/tenants"

type tenantCreationRepository interface {
	CreateTenantIdempotent(
		context.Context,
		hierarchyrecord.TenantRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type tenantCreationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type tenantCreationIdempotency interface {
	Prepare(context.Context, core.Tenant) (tenantCreationEvidence, error)
	ResolveKnown(
		context.Context,
		tenantCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		tenantCreationEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableTenantCreationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewTenantCreationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableTenantCreationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Tenant creation idempotency is not configured")
	}
	return &durableTenantCreationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableTenantCreationIdempotency) Prepare(
	ctx context.Context,
	tenant core.Tenant,
) (tenantCreationEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  tenantCreationRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query:  requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "description", Value: requestidempotency.String(tenant.Description)},
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(tenant.Name)},
			requestidempotency.Field{Name: "slug", Value: requestidempotency.String(tenant.Slug)},
		)),
	})
	if err != nil {
		return tenantCreationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return tenantCreationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return tenantCreationEvidence{}, err
	}
	return tenantCreationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableTenantCreationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence tenantCreationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableTenantCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence tenantCreationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(
		ctx,
		service.repository,
		locator,
		evidence.candidate,
		original,
	)
}

type tenantCreationService struct {
	repository  tenantCreationRepository
	idempotency tenantCreationIdempotency
	now         func() time.Time
}

func NewTenantCreationService(
	repository tenantCreationRepository,
	idempotency tenantCreationIdempotency,
) (*tenantCreationService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Tenant creation service is not configured")
	}
	return &tenantCreationService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *tenantCreationService) CreateTenant(
	ctx context.Context,
	input CreateTenantInput,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Tenant creation context is required",
		)
	}
	tenant, err := PrepareTenant(input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.Prepare(ctx, tenant)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	responseBody, err := json.Marshal(apiTypes.Tenant{
		ID: tenant.ID, Slug: tenant.Slug, Name: tenant.Name, Description: tenant.Description,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: tenantCreationRoute, Key: idempotencyKey,
	}
	marker, err := idempotencyrecord.NewCompletedDirectIdempotencyMarker(
		locator,
		evidence.durable,
		response,
		service.now().UTC(),
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, createErr := service.repository.CreateTenantIdempotent(ctx, hierarchyrecord.TenantRecord{
		ID: tenant.ID, Slug: tenant.Slug, Name: tenant.Name, Description: tenant.Description,
	}, marker)
	var resolution requestidempotency.Resolution
	if createErr != nil {
		if !isUnknownTenantCreationOutcome(createErr) {
			return idempotencyrecord.IdempotencyResponse{}, createErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, createErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Tenant creation resolution is invalid",
		)
	}
}

func isUnknownTenantCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
