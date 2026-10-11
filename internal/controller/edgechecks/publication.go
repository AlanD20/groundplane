package edgechecks

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) publish(ctx context.Context, source etcd.EdgeCheckSource, check checkrecord.Record,
	task etcd.TaskRecord, locator idempotency.IdempotencyLocator, evidence requestidempotency.ProtectedEvidence,
) (idempotency.IdempotencyResponse, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotency.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotency.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	marker := idempotency.IdempotencyMarker{
		Kind:      idempotency.IdempotencyMarkerTask,
		State:     idempotency.IdempotencyMarkerPending,
		Locator:   locator,
		Intent:    durable,
		Response:  response,
		TaskID:    task.ID,
		CreatedAt: task.CreatedAt,
		UpdatedAt: task.CreatedAt,
	}
	result, err := service.routes.CreateEdgeCheck(ctx, source, check, task, marker)
	if err != nil {
		kind, _ := errs.KindOf(err)
		if !errors.Is(err, context.DeadlineExceeded) && kind != errs.KindStorageUnavailable {
			return idempotency.IdempotencyResponse{}, err
		}
		resolution, resolveErr := service.coordinator.ResolveUnknown(ctx, service.idempotency, locator, evidence, err)
		return resolution.Response, resolveErr
	}
	resolution, err := service.coordinator.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		return requestidempotency.CloneResponse(response), nil
	}
	return resolution.Response, nil
}
