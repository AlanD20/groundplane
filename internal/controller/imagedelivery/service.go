// Package imagedelivery owns host image inventory and explicit registry fetches.
// It has no Service mutation or deployment dependency.
package imagedelivery

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
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

type Service struct {
	registry    Registry
	evidence    *etcd.IdempotencyRepository
	submitter   *acceptedaction.Submitter
	intents     *requestidempotency.Coordinator
	store       ImageStore
	operations  *imageoperations.Repository
	diagnostics *executiondiagnostics.Repository
	releases    *etcd.ReleaseLedger
}

func New(
	registry Registry,
	store ImageStore,
	evidence *etcd.IdempotencyRepository,
	intents *requestidempotency.Coordinator,
	diagnostics *executiondiagnostics.Repository,
	releases *etcd.ReleaseLedger,
	submitter *acceptedaction.Submitter,
) (*Service, error) {
	if registry == nil || store == nil || submitter == nil || evidence == nil || intents == nil || diagnostics == nil ||
		releases == nil {
		return nil, errs.New(errs.KindInternal, "image delivery dependencies are incomplete")
	}
	operations, err := imageoperations.NewRepository(store)
	if err != nil {
		return nil, err
	}
	return &Service{
		registry:    registry,
		store:       store,
		submitter:   submitter,
		evidence:    evidence,
		intents:     intents,
		operations:  operations,
		diagnostics: diagnostics,
		releases:    releases,
	}, nil
}

func (service *Service) FetchImage(
	ctx context.Context,
	requested, key string,
	after []string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if _, err := imagefetch.ParseRequested(requested); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.accept(ctx, requested, key, false, after)
}

func (service *Service) RemoveImage(
	ctx context.Context,
	imageID, key string,
	after []string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if _, err := imagefetch.RemovalHash(imageID); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.accept(ctx, imageID, key, true, after)
}

func (service *Service) accept(
	ctx context.Context,
	requested, key string,
	remove bool,
	after []string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if len(after) != 0 {
		if err := workexecution.ValidateTaskDependencies(ids.New(ids.KindTask), after); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	afterValues := make([]requestidempotency.Value, len(after))
	for i, id := range after {
		afterValues[i] = requestidempotency.String(id)
	}
	method, route := http.MethodPost, FetchRoute
	intentInput := requestidempotency.CanonicalIntentV1{
		Method: method, Route: route, Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "image", Value: requestidempotency.String(requested)},
			requestidempotency.Field{Name: "after", Value: requestidempotency.List(afterValues...)},
		)),
	}
	if remove {
		method, route = http.MethodDelete, RemoveRoute
		intentInput.Method, intentInput.Route = method, route
		intentInput.Path = []requestidempotency.PathBinding{{Name: "id", Value: requested}}
		intentInput.Body = requestidempotency.JSONBody(
			requestidempotency.Object(
				requestidempotency.Field{Name: "after", Value: requestidempotency.List(afterValues...)},
			),
		)
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
	host, err := hostbindings.ReadLocalIdentity(ctx, service.store, 0)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if host.Revision == 0 {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Enroll an Agent before requesting host image work",
		)
	}
	now := time.Now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Action: workexecution.ActionFetch, TargetID: host.Record, TargetName: requested,
		After: append([]string(nil), after...), CreatedAt: now, UpdatedAt: now,
		Deadline: now.Add(acceptedaction.QueueTimeout)}
	kind := "image-fetch"
	if remove {
		task.Action, task.TargetID, kind = workexecution.ActionRemove, requested, "image-remove"
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	return acceptedaction.Submit(ctx, service.submitter,
		acceptedaction.Submission{Task: task, Kind: kind, Locator: locator, Intent: durable, Evidence: evidence},
		imageRequest{Requested: requested, HostID: host.Record})
}

func replay(resolution requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "image fetch replay is invalid")
	}
	return requestidempotency.CloneResponse(resolution.Response), nil
}
