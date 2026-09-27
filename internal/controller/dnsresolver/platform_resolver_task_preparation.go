package dnsresolver

import (
	"context"
	"encoding/json"
	"net/http"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// NewPlatformResolverTaskPreparer prepares the sealed plan and durable intent
// together. The repository publishes both in the originating transaction.
func NewPlatformResolverTaskPreparer(
	planner *PlatformRenderPlanner,
	coordinator *requestidempotency.Coordinator,
) etcd.PlatformResolverTaskPreparer {
	return func(
		ctx context.Context,
		current etcdstore.Versioned[componentrecord.Record],
		projection resolutionrecord.HostResolutionProjectionRecord,
		task etcd.TaskRecord,
		prior *platformcomponents.ComponentObservationRecord,
	) (platformcomponents.PlatformComponentTaskRenderInput, idempotencyrecord.IdempotencyMarker, error) {
		desired, err := componentrecord.ProjectRecord(current.Record)
		if err != nil {
			return platformcomponents.PlatformComponentTaskRenderInput{}, idempotencyrecord.IdempotencyMarker{}, err
		}
		input, err := planner.PrepareConfigTaskAtProjection(ctx, current, desired, task, projection, prior)
		if err != nil {
			return platformcomponents.PlatformComponentTaskRenderInput{}, idempotencyrecord.IdempotencyMarker{}, err
		}
		marker, err := prepareResolverTaskMarker(ctx, coordinator, task)
		return input, marker, err
	}
}

func prepareResolverTaskMarker(
	ctx context.Context,
	coordinator *requestidempotency.Coordinator,
	task etcd.TaskRecord,
) (idempotencyrecord.IdempotencyMarker, error) {
	intent, err := platformComponentLifecycleIntent(
		ctx,
		coordinator,
		task.Target,
		"update",
		platformComponentUpdateRoute,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyMarker{}, err
	}
	response, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		clear(intent.durable.Ciphertext)
		return idempotencyrecord.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	return idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: idempotencyrecord.IdempotencyLocator{
			ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPost,
			Route: platformComponentUpdateRoute, Key: task.OperationID,
		},
		Intent: intent.durable,
		Response: idempotencyrecord.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: response,
		},
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}, nil
}
