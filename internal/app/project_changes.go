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
	projectEditRoute               = "/projects/{id}"
	projectRenameRoute             = "/projects/{id}/rename"
	maximumProjectMutationAttempts = 3
)

type projectChangeRepository interface {
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	MutateProjectIdempotent(
		context.Context,
		etcd.Versioned[etcd.ProjectRecord],
		etcd.ProjectRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type projectChangeEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type projectChangeIdempotency interface {
	PrepareEdit(context.Context, string, hierarchy.EditProjectInput) (projectChangeEvidence, error)
	PrepareRename(context.Context, string, hierarchy.RenameProjectInput) (projectChangeEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		projectChangeEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		projectChangeEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		projectChangeEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableProjectChangeIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableProjectChangeIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableProjectChangeIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Project change idempotency is not configured")
	}
	return &durableProjectChangeIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableProjectChangeIdempotency) PrepareEdit(
	ctx context.Context,
	id string,
	input hierarchy.EditProjectInput,
) (projectChangeEvidence, error) {
	return service.prepare(ctx, http.MethodPatch, projectEditRoute, id, idempotentintent.Object(
		idempotentintent.Field{Name: "name", Value: idempotentintent.String(*input.Name)},
	))
}

func (service *durableProjectChangeIdempotency) PrepareRename(
	ctx context.Context,
	id string,
	input hierarchy.RenameProjectInput,
) (projectChangeEvidence, error) {
	return service.prepare(ctx, http.MethodPost, projectRenameRoute, id, idempotentintent.Object(
		idempotentintent.Field{Name: "slug", Value: idempotentintent.String(input.Slug)},
	))
}

func (service *durableProjectChangeIdempotency) prepare(
	ctx context.Context,
	method string,
	route string,
	id string,
	body idempotentintent.Value,
) (projectChangeEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: method, Route: route,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeProject, ID: id},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: id}},
		Query: idempotentintent.Object(), Body: idempotentintent.JSONBody(body),
	})
	if err != nil {
		return projectChangeEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return projectChangeEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return projectChangeEvidence{}, err
	}
	return projectChangeEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableProjectChangeIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence projectChangeEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableProjectChangeIdempotency) ResolveKnown(
	ctx context.Context,
	evidence projectChangeEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableProjectChangeIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence projectChangeEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type projectChangeService struct {
	repository  projectChangeRepository
	idempotency projectChangeIdempotency
	now         func() time.Time
}

func newProjectChangeService(
	repository projectChangeRepository,
	idempotency projectChangeIdempotency,
) (*projectChangeService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Project change service is not configured")
	}
	return &projectChangeService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *projectChangeService) EditProject(
	ctx context.Context,
	id string,
	input hierarchy.EditProjectInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if err := hierarchy.ValidateProjectEditInput(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.changeProject(ctx, id, http.MethodPatch, projectEditRoute, idempotencyKey,
		func(ctx context.Context) (projectChangeEvidence, error) {
			return service.idempotency.PrepareEdit(ctx, id, input)
		},
		func(current core.Project) (core.Project, error) { return hierarchy.PrepareProjectEdit(current, input) },
	)
}

func (service *projectChangeService) RenameProject(
	ctx context.Context,
	id string,
	input hierarchy.RenameProjectInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if err := hierarchy.ValidateProjectRenameInput(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.changeProject(ctx, id, http.MethodPost, projectRenameRoute, idempotencyKey,
		func(ctx context.Context) (projectChangeEvidence, error) {
			return service.idempotency.PrepareRename(ctx, id, input)
		},
		func(current core.Project) (core.Project, error) {
			return hierarchy.PrepareProjectRename(current, input)
		},
	)
}

func (service *projectChangeService) changeProject(
	ctx context.Context,
	id string,
	method string,
	route string,
	idempotencyKey string,
	prepare func(context.Context) (projectChangeEvidence, error),
	change func(core.Project) (core.Project, error),
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Project change context is required")
	}
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeProject, ScopeID: id,
		Method: method, Route: route, Key: idempotencyKey,
	}
	for attempt := 0; attempt < maximumProjectMutationAttempts; attempt++ {
		response, err := service.changeProjectOnce(ctx, id, locator, prepare, change)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumProjectMutationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Project change retry bound was not enforced")
}

func (service *projectChangeService) changeProjectOnce(
	ctx context.Context,
	id string,
	locator etcd.IdempotencyLocator,
	prepare func(context.Context) (projectChangeEvidence, error),
	change func(core.Project) (core.Project, error),
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
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Project change replay resolution is invalid")
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	current, err := service.repository.GetProject(ctx, id)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentCore := core.Project{
		ID: current.Record.ID, TenantID: current.Record.TenantID, Slug: current.Record.Slug,
		Name: current.Record.Name, Description: current.Record.Description, Kind: core.ProjectKind(current.Record.Kind),
	}
	replacement, err := change(currentCore)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.Project{
		ID: replacement.ID, TenantID: replacement.TenantID, Slug: replacement.Slug,
		Name: replacement.Name, Description: replacement.Description, Kind: string(replacement.Kind),
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.MutateProjectIdempotent(ctx, current, etcd.ProjectRecord{
		ID: replacement.ID, TenantID: replacement.TenantID, Slug: replacement.Slug,
		Name: replacement.Name, Description: replacement.Description, Kind: etcd.ProjectKind(replacement.Kind),
	}, marker)
	if mutationErr != nil {
		if !isUnknownProjectChangeOutcome(mutationErr) {
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Project change resolution is invalid")
	}
}

func isUnknownProjectChangeOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
