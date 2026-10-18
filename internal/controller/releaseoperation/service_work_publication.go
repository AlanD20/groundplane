package releaseoperation

import (
	"context"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
)

func (service *Service) publishServiceWork(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	candidate releaseCandidateInput, locator idempotency.IdempotencyLocator,
	durable idempotency.ProtectedIntentRecord, protected requestidempotency.ProtectedEvidence,
) (idempotency.IdempotencyResponse, error) {
	candidates := []releaseCandidateInput{candidate}
	if err := service.sealCandidates(ctx, candidates); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	candidate = candidates[0]
	projection, err := service.captureDesiredProjection(ctx, scope)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	proxy := candidate.planning.Service.Record.Desired
	proxy.Expose = proxy.ProxyExposures()
	addresses, err := taskplanning.PrepareServiceProxyAddresses(ctx, service.ledger.Planner, projection, []core.Service{proxy})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer addresses.Clear()
	task, item, request, rollout, err := service.captureServiceWork(ctx, scope, projection, candidate, addresses, durable.CiphertextDigest)
	defer clear(rollout.CandidateArtifact)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	response, _, err := releaseAcceptedResponse("", task.ID, request.Intent.OperationID,
		[]domain.GroupMember{{Ordinal: 1, ServiceID: item.TargetID, ReleaseID: request.Intent.ID}})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	marker := idempotency.IdempotencyMarker{Kind: idempotency.IdempotencyMarkerTask,
		State: idempotency.IdempotencyMarkerPending, Locator: locator, Intent: durable,
		Response: response, TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt}
	result, err := service.ledger.PublishDirectReleaseWithWork(ctx, task, item, request, rollout, addresses, marker)
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
