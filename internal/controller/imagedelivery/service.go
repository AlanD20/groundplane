// Package imagedelivery publishes and executes explicit private-registry fetches.
// It has no Service mutation or deployment dependency.
package imagedelivery

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const FetchRoute = "/images/fetch"

type Registry interface {
	Resolve(context.Context, string) (imagefetch.Plan, error)
	Fetch(context.Context, imagefetch.Plan) (string, error)
}

type TaskStore interface {
	CreateTask(
		context.Context,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type Service struct {
	registry Registry
	tasks    TaskStore
	evidence requestidempotency.EvidenceRepository
	intents  *requestidempotency.Coordinator
}

func New(
	registry Registry,
	tasks TaskStore,
	evidence requestidempotency.EvidenceRepository,
	intents *requestidempotency.Coordinator,
) (*Service, error) {
	if registry == nil || tasks == nil || evidence == nil || intents == nil {
		return nil, errs.New(errs.KindInternal, "image delivery dependencies are incomplete")
	}
	return &Service{registry: registry, tasks: tasks, evidence: evidence, intents: intents}, nil
}

func (service *Service) FetchImage(
	ctx context.Context,
	requested, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if _, err := imagefetch.ParseRequested(requested); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: FetchRoute, Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "image", Value: requestidempotency.String(requested)},
		)),
	})
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
		Method: http.MethodPost, Route: FetchRoute, Key: key}
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
	plan, err := service.registry.Resolve(ctx, requested)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if plan.Requested != requested {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"registry resolved another image request",
		)
	}
	now := time.Now().UTC()
	task, err := newTask(plan, key, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	raw, err := json.Marshal(
		apiTypes.ImageFetchAccepted{TaskID: task.ID, Image: plan.Reference(), ConfigDigest: plan.ConfigDigest},
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
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
	result, publicationErr := service.tasks.CreateTask(ctx, task, idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: intent, Response: response, TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	})
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

func newTask(plan imagefetch.Plan, key string, now time.Time) (etcd.TaskRecord, error) {
	input, hash, err := imagefetch.Encode(plan)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan),
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorOperator, Executor: taskjournal.TaskExecutorController,
		IdempotencyKey: key, Type: taskjournal.TaskFetch, Target: plan.Reference(), PlanHash: hash, RenderGeneration: 1,
		Params: map[string]string{
			taskjournal.TaskResourceKindParam:    taskjournal.TaskResourceImage,
			taskjournal.TaskImageFetchInputParam: input,
		},
		TimeoutSeconds: imagefetch.TimeoutSeconds, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	return task, etcd.ValidateTaskRecord(task)
}

// Execute runs under the existing durable Controller claim, timeout and abort
// machinery. A crash after pull is safe: replay inspects the same pinned image.
func (service *Service) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if err := etcd.ValidateTaskRecord(task); err != nil {
		return err
	}
	if task.Type != taskjournal.TaskFetch || task.Executor != taskjournal.TaskExecutorController ||
		task.Status != taskjournal.TaskStatusRunning {
		return errs.New(errs.KindValidationFailed, "image fetch requires a running Controller Task")
	}
	plan, err := imagefetch.Decode(task.Params[taskjournal.TaskImageFetchInputParam], task.PlanHash)
	if err != nil {
		return err
	}
	configDigest, err := service.registry.Fetch(ctx, plan)
	if err != nil {
		return err
	}
	if configDigest != plan.ConfigDigest {
		return errs.New(errs.KindStateConflict, "fetched image differs from the accepted content")
	}
	return nil
}
