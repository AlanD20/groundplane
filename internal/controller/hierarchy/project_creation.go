package hierarchy

import (
	"context"
	"encoding/json"
	"errors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"net/http"
	"time"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	projectCreationRoute           = "/projects"
	maximumProjectCreationAttempts = 3
)

type projectCreationRepository interface {
	CreateProjectIdempotent(
		context.Context,
		hierarchyrecord.ProjectRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type projectCreationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type projectCreationIdempotency interface {
	Prepare(context.Context, core.Project) (projectCreationEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		projectCreationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		projectCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		projectCreationEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableProjectCreationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewProjectCreationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableProjectCreationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Project creation idempotency is not configured")
	}
	return &durableProjectCreationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableProjectCreationIdempotency) Prepare(
	ctx context.Context,
	project core.Project,
) (projectCreationEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  projectCreationRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeTenant, ID: project.TenantID},
		Query:  requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "description", Value: requestidempotency.String(project.Description)},
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(project.Name)},
			requestidempotency.Field{Name: "slug", Value: requestidempotency.String(project.Slug)},
			requestidempotency.Field{Name: "tenant_id", Value: requestidempotency.String(project.TenantID)},
		)),
	})
	if err != nil {
		return projectCreationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return projectCreationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return projectCreationEvidence{}, err
	}
	return projectCreationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableProjectCreationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence projectCreationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableProjectCreationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence projectCreationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableProjectCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence projectCreationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type projectCreationService struct {
	repository  projectCreationRepository
	idempotency projectCreationIdempotency
	now         func() time.Time
}

func NewProjectCreationService(
	repository projectCreationRepository,
	idempotency projectCreationIdempotency,
) (*projectCreationService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Project creation service is not configured")
	}
	return &projectCreationService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *projectCreationService) CreateProject(
	ctx context.Context,
	input CreateProjectInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Project creation context is required")
	}
	for attempt := 0; attempt < maximumProjectCreationAttempts; attempt++ {
		response, err := service.createProjectOnce(ctx, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumProjectCreationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Project creation retry bound was not enforced")
}

func (service *projectCreationService) createProjectOnce(
	ctx context.Context,
	input CreateProjectInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	project, err := PrepareProject(input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.Prepare(ctx, project)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeTenant, ScopeID: project.TenantID,
		Method: http.MethodPost, Route: projectCreationRoute, Key: idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Project creation replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	responseBody, err := json.Marshal(apiTypes.Project{
		ID: project.ID, TenantID: project.TenantID, Slug: project.Slug,
		Name: project.Name, Description: project.Description, Kind: string(project.Kind),
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, createErr := service.repository.CreateProjectIdempotent(ctx, hierarchyrecord.ProjectRecord{
		ID: project.ID, TenantID: project.TenantID, Slug: project.Slug,
		Name: project.Name, Description: project.Description, Kind: hierarchyrecord.ProjectKindTenant,
	}, marker)
	if createErr != nil {
		if !isUnknownProjectCreationOutcome(createErr) {
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
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Project creation resolution is invalid")
	}
}

func isUnknownProjectCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
