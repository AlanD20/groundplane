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

const (
	tenantEditRoute               = "/tenants/{id}"
	tenantRenameRoute             = "/tenants/{id}/rename"
	maximumTenantMutationAttempts = 3
)

type tenantChangeRepository interface {
	GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error)
	MutateTenantIdempotent(
		context.Context,
		etcd.Versioned[etcd.TenantRecord],
		etcd.TenantRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type tenantChangeEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type tenantChangeIdempotency interface {
	PrepareEdit(context.Context, string, hierarchy.EditTenantInput) (tenantChangeEvidence, error)
	PrepareRename(context.Context, string, hierarchy.RenameTenantInput) (tenantChangeEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		tenantChangeEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		tenantChangeEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		tenantChangeEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableTenantChangeIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableTenantChangeIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableTenantChangeIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Tenant change idempotency is not configured")
	}
	return &durableTenantChangeIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableTenantChangeIdempotency) PrepareEdit(
	ctx context.Context,
	id string,
	input hierarchy.EditTenantInput,
) (tenantChangeEvidence, error) {
	fields := make([]idempotentintent.Field, 0, 2)
	if input.Description != nil {
		fields = append(fields, idempotentintent.Field{
			Name: "description", Value: idempotentintent.String(*input.Description),
		})
	}
	if input.Name != nil {
		fields = append(fields, idempotentintent.Field{Name: "name", Value: idempotentintent.String(*input.Name)})
	}
	return service.prepare(ctx, http.MethodPatch, tenantEditRoute, id, idempotentintent.Object(fields...))
}

func (service *durableTenantChangeIdempotency) PrepareRename(
	ctx context.Context,
	id string,
	input hierarchy.RenameTenantInput,
) (tenantChangeEvidence, error) {
	return service.prepare(
		ctx,
		http.MethodPost,
		tenantRenameRoute,
		id,
		idempotentintent.Object(idempotentintent.Field{Name: "slug", Value: idempotentintent.String(input.Slug)}),
	)
}

func (service *durableTenantChangeIdempotency) prepare(
	ctx context.Context,
	method string,
	route string,
	id string,
	body idempotentintent.Value,
) (tenantChangeEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: method, Route: route,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopePlatform},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: id}},
		Query: idempotentintent.Object(), Body: idempotentintent.JSONBody(body),
	})
	if err != nil {
		return tenantChangeEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return tenantChangeEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return tenantChangeEvidence{}, err
	}
	return tenantChangeEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableTenantChangeIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence tenantChangeEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableTenantChangeIdempotency) ResolveKnown(
	ctx context.Context,
	evidence tenantChangeEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableTenantChangeIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence tenantChangeEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type tenantChangeService struct {
	repository  tenantChangeRepository
	idempotency tenantChangeIdempotency
	now         func() time.Time
}

func newTenantChangeService(
	repository tenantChangeRepository,
	idempotency tenantChangeIdempotency,
) (*tenantChangeService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Tenant change service is not configured")
	}
	return &tenantChangeService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *tenantChangeService) EditTenant(
	ctx context.Context,
	id string,
	input hierarchy.EditTenantInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if err := hierarchy.ValidateTenantEditInput(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.changeTenant(ctx, id, http.MethodPatch, tenantEditRoute, idempotencyKey,
		func(ctx context.Context) (tenantChangeEvidence, error) {
			return service.idempotency.PrepareEdit(ctx, id, input)
		},
		func(current core.Tenant) (core.Tenant, error) { return hierarchy.PrepareTenantEdit(current, input) },
	)
}

func (service *tenantChangeService) RenameTenant(
	ctx context.Context,
	id string,
	input hierarchy.RenameTenantInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if err := hierarchy.ValidateTenantRenameInput(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.changeTenant(ctx, id, http.MethodPost, tenantRenameRoute, idempotencyKey,
		func(ctx context.Context) (tenantChangeEvidence, error) {
			return service.idempotency.PrepareRename(ctx, id, input)
		},
		func(current core.Tenant) (core.Tenant, error) { return hierarchy.PrepareTenantRename(current, input) },
	)
}

func (service *tenantChangeService) changeTenant(
	ctx context.Context,
	id string,
	method string,
	route string,
	idempotencyKey string,
	prepare func(context.Context) (tenantChangeEvidence, error),
	change func(core.Tenant) (core.Tenant, error),
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Tenant change context is required")
	}
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-",
		Method: method, Route: route, Key: idempotencyKey,
	}
	for attempt := 0; attempt < maximumTenantMutationAttempts; attempt++ {
		response, err := service.changeTenantOnce(ctx, id, locator, prepare, change)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumTenantMutationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Tenant change retry bound was not enforced")
}

func (service *tenantChangeService) changeTenantOnce(
	ctx context.Context,
	id string,
	locator etcd.IdempotencyLocator,
	prepare func(context.Context) (tenantChangeEvidence, error),
	change func(core.Tenant) (core.Tenant, error),
) (etcd.IdempotencyResponse, error) {
	evidence, err := prepare(ctx)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Tenant change replay resolution is invalid")
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	current, err := service.repository.GetTenant(ctx, id)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentCore := core.Tenant{
		ID: current.Record.ID, Slug: current.Record.Slug,
		Name: current.Record.Name, Description: current.Record.Description,
	}
	replacement, err := change(currentCore)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.Tenant{
		ID: replacement.ID, Slug: replacement.Slug,
		Name: replacement.Name, Description: replacement.Description,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.MutateTenantIdempotent(ctx, current, etcd.TenantRecord{
		ID: replacement.ID, Slug: replacement.Slug,
		Name: replacement.Name, Description: replacement.Description,
	}, marker)
	if mutationErr != nil {
		if !isUnknownTenantChangeOutcome(mutationErr) {
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Tenant change resolution is invalid")
	}
}

func isUnknownTenantChangeOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
