package dnsresolver

import (
	"context"
	"encoding/json"
	"net/http"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func prepareNativeResolverMarker(ctx context.Context, coordinator *requestidempotency.Coordinator, work PreparedWork) (idempotency.IdempotencyMarker, error) {
	intent, err := platformComponentLifecycleIntent(ctx, coordinator, work.Input.ComponentID, "update", platformComponentUpdateRoute)
	if err != nil {
		return idempotency.IdempotencyMarker{}, err
	}
	response, err := json.Marshal(apiTypes.TaskAccepted{TaskID: work.Input.TaskID})
	if err != nil {
		clear(intent.durable.Ciphertext)
		return idempotency.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	return idempotency.IdempotencyMarker{
		Kind: idempotency.IdempotencyMarkerTask, State: idempotency.IdempotencyMarkerPending,
		Locator: idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopePlatform, ScopeID: "-",
			Method: http.MethodPost, Route: platformComponentUpdateRoute, Key: work.Input.OperationID},
		Intent: intent.durable, Response: idempotency.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: response},
		TaskID: work.Input.TaskID, CreatedAt: work.Input.CreatedAt, UpdatedAt: work.Input.CreatedAt,
	}, nil
}
