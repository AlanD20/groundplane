package network

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *zoneDeletionService) publishZoneRemovalWork(
	ctx context.Context,
	request etcd.NetworkWorkPublication,
	projectionEvidence controllerrevision.ProjectionEvidence,
	affected []string,
	evidence zoneDeletionEvidence,
	impactToken string,
) (idempotency.IdempotencyResponse, error) {
	placement, err := service.repository.ResolveResourceHost(ctx)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	scope := workexecution.Scope{TenantID: request.Project.Record.TenantID,
		ProjectID: request.Project.Record.ID, EnvironmentID: request.Environment.Record.ID}
	request.Task, request.Item, request.Input, err = buildZoneRemovalWork(scope, placement, request.Claim,
		request.Zone.Record.Desired.ID, request.Zone.Record.Desired.Name, request.Zone.Record.Desired.Subnet, affected)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	var cascade capturedBackingZoneWork
	defer cascade.Clear()
	if request.Project.Record.Kind == hierarchy.ProjectKindBacking {
		snapshots, err := service.repository.CaptureBackingZoneDetaches(ctx, request.Project.Record.ID,
			request.Zone.Record.Desired.ID, request.Zone.ReadRevision)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		cascade, err = captureBackingZoneWork(ctx, service.repository, request.Task, request.Item,
			request.Zone.ReadRevision, impactToken, snapshots)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		request.Task = cascade.Graph.Task
		request.Item = cascade.Graph.Items[len(cascade.Graph.Items)-1]
		request.Cascade = &etcd.NetworkCascadePublication{SnapshotRevision: cascade.Graph.SnapshotRevision,
			Items: cascade.Graph.Items, Inputs: cascade.Inputs, Captures: cascade.Hooks}
	}
	desired, err := controllerrevision.DeriveCurrentMutationDesiredInput(ctx, service.repository,
		request.Environment.Record.ID, request.Authorities.Desired.Record.RevisionID, request.Projection,
		request.Environment.Record.NetworkPool, func(*core.BlueprintDesiredInput) error { return nil })
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim: request.Claim, Mutation: &blueprints.EnvironmentDesiredMutationAudit{Zone: &blueprints.EnvironmentZoneMutationAudit{
			Action: blueprints.EnvironmentZoneMutationRemove, BaseRevisionID: request.Authorities.Desired.Record.RevisionID,
			ZoneID: request.Zone.Record.Desired.ID,
		}}, DesiredInput: desired, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: request.Task.ID})
	if err != nil {
		return idempotency.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotency.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}
	request.Marker = idempotency.IdempotencyMarker{
		Kind: idempotency.IdempotencyMarkerTask, State: idempotency.IdempotencyMarkerPending,
		Locator: request.Claim.Locator, Intent: request.Claim.Intent, TaskID: request.Task.ID,
		ReplayTarget: &idempotency.IdempotencyReplayTarget{
			Kind: idempotency.IdempotencyReplayTargetZone,
			ID:   request.Task.TargetID,
		},
		Response: response, CreatedAt: request.Task.CreatedAt, UpdatedAt: request.Task.CreatedAt,
	}
	result, publicationErr := service.repository.PublishNetworkWork(ctx, request)
	var resolution requestidempotency.Resolution
	if publicationErr != nil {
		if !isUnknownZoneDeletionOutcome(publicationErr) {
			return idempotency.IdempotencyResponse{}, publicationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, request.Claim.Locator, evidence, publicationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Zone removal publication has no durable outcome",
		)
	}
}
