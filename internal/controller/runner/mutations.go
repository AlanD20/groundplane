package runner

import (
	"context"
	"encoding/json"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/slug"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	runnerEditRoute        = "/runners/{id}"
	maximumMutationRetries = 3
)

type mutationRepository interface {
	GetRunner(context.Context, string) (etcdstore.Versioned[runnerrecord.RunnerRecord], error)
	GetRunnerObservation(context.Context, string) (etcdstore.Versioned[runnerrecord.RunnerObservationRecord], bool, error)
	ReplaceRunnerSlugIdempotent(
		context.Context,
		string,
		string,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type MutationService struct {
	repository  mutationRepository
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
	now         func() time.Time
}

func NewMutationService(
	repository mutationRepository,
	idempotency *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
) (*MutationService, error) {
	if repository == nil || idempotency == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "Runner mutation service is not configured")
	}
	return &MutationService{
		repository: repository, idempotency: idempotency, coordinator: coordinator, now: time.Now,
	}, nil
}

func (service *MutationService) RenameRunner(
	ctx context.Context,
	runnerID string,
	request apiTypes.RunnerEditRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner edit context is required")
	}
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Runner id is invalid")
	}
	if !slug.Valid(request.Slug) {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Runner slug must be a lowercase ASCII label of 1-63 bytes",
		)
	}
	for attempt := 0; attempt < maximumMutationRetries; attempt++ {
		response, err := service.renameOnce(ctx, runnerID, request, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumMutationRetries-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner edit retry bound was not enforced")
}

func (service *MutationService) renameOnce(
	ctx context.Context,
	runnerID string,
	request apiTypes.RunnerEditRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetRunner, ID: runnerID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodPatch, runnerEditRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replay(ctx, locator, runnerID, request)
	}
	current, err := service.repository.GetRunner(ctx, runnerID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	locator = runnerLocator(current.Record.Desired, idempotencyKey)
	evidence, err := service.protectIntent(ctx, locator, runnerID, request.Slug)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, existing, err := service.coordinator.ResolveExisting(
		ctx, service.idempotency, locator, evidence,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner edit replay resolution is invalid")
		}
		return cloneResponse(resolution.Response), nil
	}
	observation, observed, err := service.repository.GetRunnerObservation(ctx, runnerID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	replacement := current.Record
	replacement.Desired.Slug = request.Slug
	var observationRecord *runnerrecord.RunnerObservationRecord
	if observed {
		observationRecord = &observation.Record
	}
	public, err := PublicProjection(replacement, observationRecord, nil)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	body, err := json.Marshal(public)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	marker, err := idempotencyrecord.NewCompletedDirectIdempotencyMarker(locator, durable, response, service.now().UTC())
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	marker.ReplayTarget = &target
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.ReplaceRunnerSlugIdempotent(
		ctx, runnerID, request.Slug, marker,
	)
	if mutationErr != nil {
		if !unknownMutationOutcome(mutationErr) {
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.coordinator.ResolveUnknown(
			ctx, service.idempotency, locator, evidence, mutationErr,
		)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return cloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return cloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner edit resolution is invalid")
	}
}

func (service *MutationService) replay(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	runnerID string,
	request apiTypes.RunnerEditRequest,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.protectIntent(ctx, locator, runnerID, request.Slug)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, existing, err := service.coordinator.ResolveExisting(
		ctx, service.idempotency, locator, evidence,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner edit replay index is inconsistent")
	}
	return cloneResponse(resolution.Response), nil
}

func (service *MutationService) protectIntent(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	runnerID string,
	slugValue string,
) (requestidempotency.ProtectedEvidence, error) {
	scopeKind := requestidempotency.ScopeTenant
	if locator.ScopeKind == idempotencyrecord.IdempotencyScopeProject {
		scopeKind = requestidempotency.ScopeProject
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPatch, Route: runnerEditRoute,
		Scope: requestidempotency.Scope{Kind: scopeKind, ID: locator.ScopeID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: runnerID}},
		Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "slug", Value: requestidempotency.String(slugValue)},
		)),
	})
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, err
	}
	return service.coordinator.ProtectIntent(ctx, version, digest)
}

func runnerLocator(desired runnerrecord.RunnerDesiredRecord, key string) idempotencyrecord.IdempotencyLocator {
	scopeKind := idempotencyrecord.IdempotencyScopeTenant
	if desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		scopeKind = idempotencyrecord.IdempotencyScopeProject
	}
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: scopeKind, ScopeID: desired.OwnerID,
		Method: http.MethodPatch, Route: runnerEditRoute, Key: key,
	}
}

func cloneResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}

func unknownMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
