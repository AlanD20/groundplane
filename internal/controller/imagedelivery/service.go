// Package imagedelivery owns host image inventory and explicit registry fetches.
// It has no Service mutation or deployment dependency.
package imagedelivery

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const FetchRoute = "/images/fetch"
const RemoveRoute = "/images/{id}"

type Registry interface {
	Resolve(context.Context, string) (imagefetch.Plan, error)
	Fetch(context.Context, imagefetch.Plan, imagefetch.Reporter) (string, error)
	AssignTag(context.Context, imagefetch.Plan) (imagefetch.TagAssignmentOutcome, error)
	List(context.Context) ([]imagefetch.LocalImage, error)
	Remove(context.Context, string) error
}

type TaskStore interface {
	CreateImageOperation(context.Context, imageoperations.Input, idempotencyrecord.IdempotencyMarker) (etcd.IdempotencyTransactionResult, error)
}

type Service struct {
	registry    Registry
	tasks       TaskStore
	evidence    requestidempotency.EvidenceRepository
	intents     *requestidempotency.Coordinator
	store       ImageStore
	operations  *imageoperations.Repository
	diagnostics *executiondiagnostics.Repository
	releases    *etcd.ReleaseLedger
}

func New(
	registry Registry,
	store ImageStore,
	tasks TaskStore,
	evidence requestidempotency.EvidenceRepository,
	intents *requestidempotency.Coordinator,
	diagnostics *executiondiagnostics.Repository,
	releases *etcd.ReleaseLedger,
) (*Service, error) {
	if registry == nil || store == nil || tasks == nil || evidence == nil || intents == nil || diagnostics == nil || releases == nil {
		return nil, errs.New(errs.KindInternal, "image delivery dependencies are incomplete")
	}
	operations, err := imageoperations.NewRepository(store)
	if err != nil {
		return nil, err
	}
	return &Service{registry: registry, store: store, tasks: tasks, evidence: evidence, intents: intents, operations: operations, diagnostics: diagnostics, releases: releases}, nil
}

func (service *Service) FetchImage(
	ctx context.Context,
	requested, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if _, err := imagefetch.ParseRequested(requested); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.accept(ctx, requested, key, false)
}

func (service *Service) RemoveImage(
	ctx context.Context,
	imageID, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if _, err := imagefetch.RemovalHash(imageID); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.accept(ctx, imageID, key, true)
}

func (service *Service) accept(
	ctx context.Context,
	requested, key string,
	remove bool,
) (idempotencyrecord.IdempotencyResponse, error) {
	method, route := http.MethodPost, FetchRoute
	intentInput := requestidempotency.CanonicalIntentV1{
		Method: method, Route: route, Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "image", Value: requestidempotency.String(requested)},
		)),
	}
	if remove {
		method, route = http.MethodDelete, RemoveRoute
		intentInput.Method, intentInput.Route = method, route
		intentInput.Path = []requestidempotency.PathBinding{{Name: "id", Value: requested}}
		intentInput.Body = requestidempotency.NoBody()
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, intentInput)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := service.intents.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotencyrecord.IdempotencyLocator{ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-",
		Method: method, Route: route, Key: key}
	if err := idempotencyrecord.ValidateIdempotencyLocator(locator); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	existing, found, err := service.intents.ResolveExisting(ctx, service.evidence, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if found {
		return replay(existing)
	}
	// Resolution precedes publication but follows replay lookup. The atomic
	// marker selects one winning plan when requests race; no pull happens here.
	now := time.Now().UTC()
	input, raw, err := service.prepareOperation(ctx, requested, now, remove)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(raw)
	response := idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        raw,
	}
	intent, err := evidence.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(intent.Ciphertext)
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: intent, Response: response, TaskID: input.TaskID, CreatedAt: now, UpdatedAt: now,
	}
	result, publicationErr := service.tasks.CreateImageOperation(ctx, input, marker)
	var resolution requestidempotency.Resolution
	if publicationErr != nil {
		if !errors.Is(publicationErr, context.DeadlineExceeded) &&
			!errors.Is(publicationErr, errs.New(errs.KindStorageUnavailable, "")) {
			return idempotencyrecord.IdempotencyResponse{}, publicationErr
		}
		resolution, err = service.intents.ResolveUnknown(ctx, service.evidence, locator, evidence, publicationErr)
	} else {
		resolution, err = service.intents.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		return requestidempotency.CloneResponse(response), nil
	}
	return replay(resolution)
}

func replay(resolution requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "image fetch replay is invalid")
	}
	return requestidempotency.CloneResponse(resolution.Response), nil
}
