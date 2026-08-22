package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const tenantCreationRoute = "/tenants"

type tenantCreationRepository interface {
	CreateTenantIdempotent(
		context.Context,
		etcd.TenantRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type tenantCreationEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type tenantCreationIdempotency interface {
	Prepare(context.Context, core.Tenant) (tenantCreationEvidence, error)
	ResolveKnown(
		context.Context,
		tenantCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		tenantCreationEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableTenantCreationIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableTenantCreationIdempotency(
	coordinator *idempotentintent.Coordinator,
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
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  tenantCreationRoute,
		Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopePlatform},
		Query:  idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "description", Value: idempotentintent.String(tenant.Description)},
			idempotentintent.Field{Name: "name", Value: idempotentintent.String(tenant.Name)},
			idempotentintent.Field{Name: "slug", Value: idempotentintent.String(tenant.Slug)},
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
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableTenantCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence tenantCreationEvidence,
	original error,
) (idempotentintent.Resolution, error) {
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

func newTenantCreationService(
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
	input hierarchy.CreateTenantInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Tenant creation context is required")
	}
	tenant, err := hierarchy.PrepareTenant(input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.Prepare(ctx, tenant)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	responseBody, err := json.Marshal(apiTypes.Tenant{
		ID: tenant.ID, Slug: tenant.Slug, Name: tenant.Name, Description: tenant.Description,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: tenantCreationRoute, Key: idempotencyKey,
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, createErr := service.repository.CreateTenantIdempotent(ctx, etcd.TenantRecord{
		ID: tenant.ID, Slug: tenant.Slug, Name: tenant.Name, Description: tenant.Description,
	}, marker)
	var resolution idempotentintent.Resolution
	if createErr != nil {
		if !isUnknownTenantCreationOutcome(createErr) {
			return etcd.IdempotencyResponse{}, createErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, createErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Tenant creation resolution is invalid")
	}
}

func isUnknownTenantCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
