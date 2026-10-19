package tasks

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const taskRetryRoute = "/tasks/{id}/retry"

type taskRetryService struct {
	queue       *workqueue.Repository
	retries     *workdispatch.Retries
	receipts    *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
}

func NewRetryService(queue *workqueue.Repository, retries *workdispatch.Retries,
	receipts *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
) (*taskRetryService, error) {
	if queue == nil || retries == nil || receipts == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "execution retry dependencies are required")
	}
	return &taskRetryService{queue: queue, retries: retries, receipts: receipts, coordinator: coordinator}, nil
}

func (service *taskRetryService) RetryTask(
	ctx context.Context,
	taskID, key string,
	request apiTypes.TaskRetryRequest,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || ids.Validate(ids.KindWorkItem, request.WorkItemID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Retry requires the exact Task, work item and attempt",
		)
	}
	fields := []requestidempotency.Field{
		{Name: "work_item_id", Value: requestidempotency.String(request.WorkItemID)},
		{Name: "attempt", Value: requestidempotency.UnsignedInteger(request.Attempt)},
	}
	if request.QueuedFailureAt != nil {
		fields = append(
			fields,
			requestidempotency.Field{
				Name:  "queued_failure_at",
				Value: requestidempotency.String(request.QueuedFailureAt.UTC().Format(time.RFC3339Nano)),
			},
		)
	}
	task, _, err := service.queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	scope, kind, owner := retryScope(task.Task.Scope)
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: taskRetryRoute, Scope: scope,
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: taskID}},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(fields...)),
	})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	locator := idempotency.IdempotencyLocator{
		ScopeKind: kind,
		ScopeID:   owner,
		Method:    http.MethodPost,
		Route:     taskRetryRoute,
		Key:       key,
	}
	resolution, existing, err := service.coordinator.ResolveExisting(ctx, service.receipts, locator, evidence)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Retry receipt is inconsistent")
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	at := time.Now().UTC()
	change, err := service.retries.Prepare(
		ctx,
		taskID,
		request.WorkItemID,
		request.Attempt,
		request.QueuedFailureAt,
		at,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return idempotency.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotency.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}
	marker := idempotency.IdempotencyMarker{
		Kind:        idempotency.IdempotencyMarkerDirect,
		State:       idempotency.IdempotencyMarkerCompleted,
		Locator:     locator,
		Intent:      durable,
		Response:    response,
		CreatedAt:   at,
		UpdatedAt:   at,
		TerminalAt:  at,
		RetainUntil: at.Add(idempotency.MarkerRetention),
	}
	result, applyErr := service.receipts.ApplyExecutionRetry(ctx, change, marker)
	if applyErr != nil {
		if !errs.IsRetryable(applyErr) && ctx.Err() == nil {
			return idempotency.IdempotencyResponse{}, applyErr
		}
		resolution, err = service.coordinator.ResolveUnknown(ctx, service.receipts, locator, evidence, applyErr)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Retry receipt resolution is inconsistent",
		)
	}
}

func retryScope(scope workexecution.Scope) (requestidempotency.Scope, idempotency.IdempotencyScopeKind, string) {
	if scope.EnvironmentID != "" {
		return requestidempotency.Scope{
			Kind: requestidempotency.ScopeEnvironment,
			ID:   scope.EnvironmentID,
		}, idempotency.IdempotencyScopeEnvironment, scope.EnvironmentID
	}
	if scope.ProjectID != "" {
		return requestidempotency.Scope{
			Kind: requestidempotency.ScopeProject,
			ID:   scope.ProjectID,
		}, idempotency.IdempotencyScopeProject, scope.ProjectID
	}
	if scope.TenantID != "" {
		return requestidempotency.Scope{
			Kind: requestidempotency.ScopeTenant,
			ID:   scope.TenantID,
		}, idempotency.IdempotencyScopeTenant, scope.TenantID
	}
	return requestidempotency.Scope{Kind: requestidempotency.ScopePlatform}, idempotency.IdempotencyScopePlatform, "-"
}
