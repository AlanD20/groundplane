package runner

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	runnerRemoveRoute          = "/runners/{id}"
	runnerRemoveTimeoutSeconds = int64(300)
)

type removalRepository interface {
	GetRunner(context.Context, string) (etcdstore.Versioned[runnerrecord.RunnerRecord], error)
	PrepareRunnerRemoval(
		context.Context,
		etcdstore.Versioned[runnerrecord.RunnerRecord],
		string,
		string,
		string,
		time.Time,
	) (etcd.RunnerRemovalSource, error)
	PrepareRunnerRemovalCapture(
		context.Context,
		etcd.RunnerRemovalSource,
		workexecution.Task,
		workexecution.Item,
	) (workqueue.Change, error)
}

// RemovalService fences one managed Runner and publishes its native cleanup
// Task. Durable allocation remains owned until exact host absence is proven.
type RemovalService struct {
	submitter   *acceptedaction.Submitter
	repository  removalRepository
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
	now         func() time.Time
}

func NewRemovalService(
	repository removalRepository,
	idempotency *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
	submitter *acceptedaction.Submitter,
) (*RemovalService, error) {
	if repository == nil || idempotency == nil || coordinator == nil || submitter == nil {
		return nil, errs.New(errs.KindInternal, "Runner removal service is not configured")
	}
	return &RemovalService{
		repository: repository, submitter: submitter, idempotency: idempotency, coordinator: coordinator, now: time.Now,
	}, nil
}

func (service *RemovalService) RemoveRunner(
	ctx context.Context,
	runnerID string,
	input apiTypes.TaskRunAfterRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Runner removal context is required",
		)
	}
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Runner id is invalid")
	}
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetRunner,
		ID:   runnerID,
	}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, runnerRemoveRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replay(ctx, locator, runnerID, input.After)
	}
	current, err := service.repository.GetRunner(ctx, runnerID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	locator = runnerRemovalLocator(current.Record.Desired, idempotencyKey)
	evidence, err := service.protectIntent(ctx, locator, runnerID, input.After)
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
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Runner removal replay is invalid",
			)
		}
		return cloneResponse(resolution.Response), nil
	}

	task := newRunnerAcceptedTask(
		current.Record.Desired,
		workexecution.OperatorActor,
		workexecution.ActionRemove,
		input.After,
		service.now().UTC(),
	)
	return submitRunnerAction(ctx, service.submitter, task, "runner-remove", locator, evidence, input)
}

func (service *RemovalService) replay(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	runnerID string, after []string,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.protectIntent(ctx, locator, runnerID, after)
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
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Runner removal replay index is inconsistent",
		)
	}
	return cloneResponse(resolution.Response), nil
}

func (service *RemovalService) protectIntent(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	runnerID string, after []string,
) (requestidempotency.ProtectedEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: runnerRemoveRoute,
		Scope: runnerIntentScope(locator),
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: runnerID}},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(after)})),
	})
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, err
	}
	return service.coordinator.ProtectIntent(ctx, version, digest)
}

func runnerRemovalLocator(desired runnerrecord.RunnerDesiredRecord, key string) idempotencyrecord.IdempotencyLocator {
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: desired.IdempotencyScope(), ScopeID: desired.OwnerID,
		Method: http.MethodDelete, Route: runnerRemoveRoute, Key: key,
	}
}
