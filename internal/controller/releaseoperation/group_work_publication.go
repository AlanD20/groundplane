package releaseoperation

import (
	"context"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
)

// publishGroupWork accepts the complete native graph and immutable member
// captures. No member can dispatch until publication has installed every typed
// source and descriptor; retries never rebuild this capture from current state.
func (service *Service) publishGroupWork(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	group releasegroup.Versioned, candidates []releaseCandidateInput, locator idempotency.IdempotencyLocator,
	durable idempotency.ProtectedIntentRecord, protected requestidempotency.ProtectedEvidence,
) (idempotency.IdempotencyResponse, error) {
	if err := service.sealCandidates(ctx, candidates); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	projection, err := service.captureDesiredProjection(ctx, scope)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	proxies := make([]core.Service, len(candidates))
	for index, candidate := range candidates {
		proxies[index] = candidate.planning.Service.Record.Desired
		proxies[index].Expose = proxies[index].ProxyExposures()
	}
	addresses, err := taskplanning.PrepareServiceProxyAddresses(ctx, service.ledger.Planner, projection, proxies)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer addresses.Clear()
	capture, err := service.captureGroupWork(
		ctx,
		scope,
		group,
		projection,
		candidates,
		addresses,
		durable.CiphertextDigest,
	)
	defer capture.Clear()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	task, err := capture.Root.Task()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	response, _, err := releaseAcceptedResponse(task.TargetID, task.ID, capture.Root.Policy.Manifest.OperationID,
		capture.Root.Policy.Manifest.Members)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	marker := idempotency.IdempotencyMarker{Kind: idempotency.IdempotencyMarkerTask,
		State: idempotency.IdempotencyMarkerPending, Locator: locator, Intent: durable, Response: response,
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt}
	result, err := service.ledger.PublishGroupReleaseWithWork(ctx, capture, addresses, marker)
	if err != nil {
		if !releaseGroupUnknownOutcome(err) {
			return idempotency.IdempotencyResponse{}, err
		}
		resolution, resolveErr := service.coordinator.ResolveUnknown(ctx, service.idempotency, locator, protected, err)
		if resolveErr != nil {
			return idempotency.IdempotencyResponse{}, resolveErr
		}
		return releaseOperationResolution(resolution, response)
	}
	resolution, err := service.coordinator.ResolveKnown(ctx, protected, result)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	return releaseOperationResolution(resolution, response)
}
