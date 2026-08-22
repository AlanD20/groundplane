package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	environmentRenameRoute             = "/environments/{id}/rename"
	maximumEnvironmentMutationAttempts = 3
)

type environmentChangeRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	MutateEnvironmentIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.EnvironmentRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type environmentChangeEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type environmentChangeIdempotency interface {
	PrepareRename(context.Context, string, hierarchy.RenameEnvironmentInput) (environmentChangeEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		environmentChangeEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		environmentChangeEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		environmentChangeEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableEnvironmentChangeIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableEnvironmentChangeIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEnvironmentChangeIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Environment change idempotency is not configured")
	}
	return &durableEnvironmentChangeIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEnvironmentChangeIdempotency) PrepareRename(
	ctx context.Context,
	id string,
	input hierarchy.RenameEnvironmentInput,
) (environmentChangeEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost, Route: environmentRenameRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: id},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: id}},
		Query: idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "name", Value: idempotentintent.String(input.Name)},
		)),
	})
	if err != nil {
		return environmentChangeEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return environmentChangeEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return environmentChangeEvidence{}, err
	}
	return environmentChangeEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEnvironmentChangeIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence environmentChangeEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEnvironmentChangeIdempotency) ResolveKnown(
	ctx context.Context,
	evidence environmentChangeEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEnvironmentChangeIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence environmentChangeEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type environmentChangeService struct {
	repository  environmentChangeRepository
	idempotency environmentChangeIdempotency
	now         func() time.Time
}

func newEnvironmentChangeService(
	repository environmentChangeRepository,
	idempotency environmentChangeIdempotency,
) (*environmentChangeService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Environment change service is not configured")
	}
	return &environmentChangeService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *environmentChangeService) RenameEnvironment(
	ctx context.Context,
	id string,
	input hierarchy.RenameEnvironmentInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment change context is required")
	}
	if err := hierarchy.ValidateEnvironmentRenameInput(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: id,
		Method: http.MethodPost, Route: environmentRenameRoute, Key: idempotencyKey,
	}
	for attempt := 0; attempt < maximumEnvironmentMutationAttempts; attempt++ {
		response, err := service.renameEnvironmentOnce(ctx, id, input, locator)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEnvironmentMutationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment change retry bound was not enforced")
}

func (service *environmentChangeService) renameEnvironmentOnce(
	ctx context.Context,
	id string,
	input hierarchy.RenameEnvironmentInput,
	locator etcd.IdempotencyLocator,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.PrepareRename(ctx, id, input)
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
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Environment change replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	current, err := service.repository.GetEnvironment(ctx, id)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	replacement := current.Record
	replacement.Name, err = hierarchy.PrepareEnvironmentRename(current.Record.Name, input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(environmentAPI(replacement))
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
	result, mutationErr := service.repository.MutateEnvironmentIdempotent(ctx, current, replacement, marker)
	if mutationErr != nil {
		if !isUnknownEnvironmentChangeOutcome(mutationErr) {
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment change resolution is invalid")
	}
}

func environmentAPI(record etcd.EnvironmentRecord) apiTypes.Environment {
	var createTaskID *string
	if record.ProvisioningState != etcd.EnvironmentProvisioningReady {
		value := record.CreateTaskID
		createTaskID = &value
	}
	return apiTypes.Environment{
		ID: record.ID, ProjectID: record.ProjectID, Name: record.Name, VolumeDir: record.VolumeDir,
		ProvisioningState: apiTypes.EnvironmentProvisioningState(record.ProvisioningState),
		CreateTaskID:      createTaskID,
	}
}

func isUnknownEnvironmentChangeOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
